"""End-to-end orchestration: AWS Price List -> GCS NDJSON -> BQ partition -> live table swap.

The hot path is the version diff: for ~6000 (service, region) offer files, only a
handful change per day. We:

1. List every (service, region) target from AWS' master + region indices.
2. Compare each target's `version` against `aws_pricing_versions`.
3. Download only the changed targets, in parallel, into GCS NDJSON.
4. Prepare today's partition:
   - same-day rerun (today already has rows): DELETE only the rows for the
     changed (service, region) pairs — unchanged ones stay.
   - new day: carry forward yesterday's rows for unchanged pairs into today's
     partition with `INSERT ... AS ingestion_date = @run_date`.
5. LOAD the changed NDJSON files into today's history partition with
   WRITE_APPEND. TRUNCATE here would wipe the data set up in step 4.
6. MERGE the new (service, region, version) tuples into `aws_pricing_versions`.
7. `CREATE OR REPLACE TABLE aws_pricing` from today's partition.
8. Delete GCS staging.

The live table is only rebuilt at step 7, so consumers see yesterday's snapshot
until everything is consistent.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import uuid4

from google.cloud import bigquery, storage

from app.bq_client import get_bq_client
from app.bq_setup import (
    HISTORY_SCHEMA_PATH,
    HISTORY_TABLE,
    VERSIONS_TABLE,
)
from app.config import Settings, get_settings
from app.diagnostics import log_memory
from app.gcs_client import delete_prefix, get_gcs_client, upload_jsonl
from app.services import aws_client, transform
from app.services import runs as runs_service

logger = logging.getLogger(__name__)

LIVE_TABLE = "aws_pricing"

_UNSAFE_BLOB_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass
class LoadResult:
    run_id: str
    run_date: date
    rows_loaded: int
    services_changed: int
    services_skipped: int
    elapsed_s: float


def _table_fqn(settings: Settings, table: str) -> str:
    return f"`{settings.gcp_project}.{settings.bq_dataset}.{table}`"


def _safe_blob_name(target: aws_client.OfferTarget) -> str:
    raw = f"{target.service_code}--{target.region_code}--{target.version}"
    return _UNSAFE_BLOB_CHARS.sub("_", raw)


def _load_known_versions(
    bq_client: bigquery.Client, settings: Settings
) -> set[tuple[str, str, str, str]]:
    """Pull the full version state from `aws_pricing_versions`."""
    sql = (
        f"SELECT service_code, region_code, offer_type, version "
        f"FROM {_table_fqn(settings, VERSIONS_TABLE)}"
    )
    job = bq_client.query(sql)
    return {
        (r["service_code"], r["region_code"], r["offer_type"], r["version"])
        for r in job.result()
    }


def _latest_previous_partition(
    bq_client: bigquery.Client, settings: Settings, run_date: date
) -> date | None:
    sql = (
        f"SELECT MAX(ingestion_date) AS d FROM {_table_fqn(settings, HISTORY_TABLE)} "
        f"WHERE ingestion_date < @run_date"
    )
    job = bq_client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
        ),
    )
    rows = list(job.result())
    if not rows:
        return None
    val = rows[0]["d"]
    return val if isinstance(val, date) else None


def _partition_has_data(
    bq_client: bigquery.Client, settings: Settings, run_date: date
) -> bool:
    sql = (
        f"SELECT COUNT(*) AS c FROM {_table_fqn(settings, HISTORY_TABLE)} "
        f"WHERE ingestion_date = @run_date"
    )
    job = bq_client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
        ),
    )
    rows = list(job.result())
    return bool(rows) and (rows[0]["c"] or 0) > 0


def _download_one(
    target: aws_client.OfferTarget,
    settings: Settings,
    gcs_client: storage.Client,
    staging_prefix: str,
    ingestion_date_str: str,
    ingested_at_str: str,
) -> int:
    """Download offer JSON to a temp file, stream-flatten, stream-upload NDJSON.

    Two memory bounds in series:
      1) the offer JSON itself is never fully materialized in Python — it lands
         on disk via requests' iter_content, then ijson walks it incrementally
         (one (sku, term_id_map) pair at a time) for service offers
      2) `upload_jsonl` accepts a generator and streams rows through its own
         temp file before chunked resumable upload

    Both bounds matter for EC2 us-east-1 (~200 MB JSON, ~1M+ flattened rows) and
    similar large offers. Without these the worker process would briefly hold the
    full offer dict (>1 GB Python heap) plus a fully-buffered NDJSON BytesIO.
    """
    # One session per worker — keeps connection pools small and avoids cross-thread state.
    session = aws_client.make_session(settings)
    # `.json.gz` triggers gzip-on-write inside `download_offer_to_file`. JSON
    # compresses ~6-8x, which materially shrinks the tmpfs footprint on Cloud
    # Run where /tmp is RAM-backed.
    with tempfile.NamedTemporaryFile(
        mode="wb", delete=False, suffix=".json.gz"
    ) as tmp:
        tmp_path = tmp.name
    try:
        try:
            aws_client.download_offer_to_file(
                target, tmp_path, settings, session=session
            )
            rows_iter = transform.offer_file_to_rows(
                target,
                tmp_path,
                ingestion_date_str=ingestion_date_str,
                ingested_at_str=ingested_at_str,
                include_reserved=settings.aws_include_reserved,
            )
            # `.jsonl.gz` — upload_jsonl gzips the file so the on-disk and
            # on-GCS footprint stays ~10x smaller than uncompressed NDJSON.
            blob_name = f"{staging_prefix}{_safe_blob_name(target)}.jsonl.gz"
            rows_written = upload_jsonl(
                gcs_client, settings.gcs_staging_bucket, blob_name, rows_iter
            )
        finally:
            session.close()
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
    if rows_written == 0:
        logger.info(
            "loader.target.empty service=%s region=%s version=%s",
            target.service_code,
            target.region_code,
            target.version,
        )
    else:
        logger.info(
            "loader.target.uploaded service=%s region=%s rows=%d",
            target.service_code,
            target.region_code,
            rows_written,
        )
    # Per-target memory snapshot: lets us correlate peak RSS with the largest
    # offers (EC2 / RDS us-east-1) when post-morteming a Cloud Run log stream.
    log_memory(
        "download.done",
        service=target.service_code,
        region=target.region_code,
        rows=rows_written,
    )
    return rows_written


def _csv_of_changed_pairs(changed: list[aws_client.OfferTarget]) -> str:
    """Build the '|'-joined CSV used in SQL parameters.

    Format: "AmazonEC2|us-east-1,AmazonEC2|eu-west-1,AmazonS3|us-east-1"
    """
    return ",".join(f"{t.service_code}|{t.region_code}" for t in changed)


_HISTORY_COLS = (
    "service_code, service_name, region_code, version, publication_date, offer_type, "
    "sku, rate_code, offer_term_code, term_type, price_per_unit, currency, unit, "
    "starting_range, ending_range, effective_date, description, product_family, "
    "attributes, term_attributes, price_per_unit_raw, source_url, ingested_at"
)


def _delete_changed_from_today(
    bq_client: bigquery.Client,
    settings: Settings,
    run_date: date,
    changed_csv: str,
) -> None:
    """Drop today's rows for `(service, region)` pairs we're about to re-load.

    Used on same-day reruns: today's partition has rows from an earlier run, and
    we want to replace JUST the changed pairs while leaving the unchanged ones
    untouched. Without this, the LOAD JOB (now WRITE_APPEND) would duplicate
    rows for the changed pairs.
    """
    sql = (
        f"DELETE FROM {_table_fqn(settings, HISTORY_TABLE)} "
        f"WHERE ingestion_date = @run_date "
        f"  AND CONCAT(service_code, '|', region_code) IN UNNEST(SPLIT(@changed_csv, ','))"
    )
    bq_client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                bigquery.ScalarQueryParameter("changed_csv", "STRING", changed_csv),
            ]
        ),
    ).result()
    logger.info(
        "bq.history.deleted_changed_in_today pairs=%d",
        changed_csv.count(",") + 1 if changed_csv else 0,
    )


def _carry_forward_previous_partition(
    bq_client: bigquery.Client,
    settings: Settings,
    run_date: date,
    previous_date: date,
    changed_csv: str,
) -> None:
    """Copy unchanged (service, region) rows from previous_date into today's partition.

    Rows whose (service_code|region_code) appears in @changed_csv are SKIPPED
    here because they will be re-loaded by the LOAD JOB.
    """
    sql = (
        f"INSERT INTO {_table_fqn(settings, HISTORY_TABLE)} "
        f"(ingestion_date, {_HISTORY_COLS}) "
        f"SELECT @run_date AS ingestion_date, {_HISTORY_COLS} "
        f"FROM {_table_fqn(settings, HISTORY_TABLE)} "
        f"WHERE ingestion_date = @previous_date "
        f"  AND CONCAT(service_code, '|', region_code) NOT IN UNNEST(SPLIT(@changed_csv, ','))"
    )
    job = bq_client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                bigquery.ScalarQueryParameter("previous_date", "DATE", previous_date),
                bigquery.ScalarQueryParameter("changed_csv", "STRING", changed_csv),
            ]
        ),
    )
    job.result()
    logger.info(
        "bq.history.carry_forward previous=%s -> today=%s",
        previous_date,
        run_date,
    )


def _merge_versions(
    bq_client: bigquery.Client,
    settings: Settings,
    run_date: date,
    changed_csv: str,
) -> None:
    """Upsert the changed (service, region, offer_type, version) rows.

    Source rows are derived from today's freshly-loaded history partition so
    we never need to ship structured Python data into a BQ parameter.
    """
    sql = (
        f"MERGE {_table_fqn(settings, VERSIONS_TABLE)} T "
        f"USING ( "
        f"  SELECT service_code, region_code, offer_type, "
        f"         ANY_VALUE(version) AS version, "
        f"         ANY_VALUE(publication_date) AS publication_date, "
        f"         ANY_VALUE(ingested_at) AS loaded_at, "
        f"         ANY_VALUE(source_url) AS source_url "
        f"  FROM {_table_fqn(settings, HISTORY_TABLE)} "
        f"  WHERE ingestion_date = @run_date "
        f"    AND CONCAT(service_code, '|', region_code) IN UNNEST(SPLIT(@changed_csv, ',')) "
        f"  GROUP BY service_code, region_code, offer_type "
        f") S "
        f"ON T.service_code = S.service_code "
        f"   AND T.region_code = S.region_code "
        f"   AND T.offer_type = S.offer_type "
        f"WHEN MATCHED THEN UPDATE SET "
        f"  version = S.version, publication_date = S.publication_date, "
        f"  loaded_at = S.loaded_at, source_url = S.source_url "
        f"WHEN NOT MATCHED THEN "
        f"  INSERT (service_code, region_code, offer_type, version, publication_date, loaded_at, source_url) "
        f"  VALUES (S.service_code, S.region_code, S.offer_type, S.version, S.publication_date, S.loaded_at, S.source_url)"
    )
    bq_client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                bigquery.ScalarQueryParameter("changed_csv", "STRING", changed_csv),
            ]
        ),
    ).result()
    logger.info("bq.versions.merged run_date=%s", run_date)


def _swap_live_table(bq_client: bigquery.Client, settings: Settings, run_date: date) -> None:
    swap_sql = (
        f"CREATE OR REPLACE TABLE {_table_fqn(settings, LIVE_TABLE)}\n"
        f"CLUSTER BY service_code, region_code, sku\n"
        f"AS SELECT * EXCEPT(ingestion_date)\n"
        f"   FROM {_table_fqn(settings, HISTORY_TABLE)}\n"
        f"   WHERE ingestion_date = @run_date"
    )
    bq_client.query(
        swap_sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
        ),
    ).result()
    logger.info("bq.live_table.swapped table=%s", LIVE_TABLE)


def run_load(
    settings: Settings | None = None,
    *,
    force: bool = False,
    bq_client: bigquery.Client | None = None,
    gcs_client: storage.Client | None = None,
) -> LoadResult:
    """Single end-to-end load. See module docstring for the full state diagram."""
    settings = settings or get_settings()

    if not settings.gcp_project:
        raise ValueError("GCP_PROJECT is required")
    if not settings.gcs_staging_bucket:
        raise ValueError("GCS_STAGING_BUCKET is required")

    bq_client = bq_client or get_bq_client(settings)
    gcs_client = gcs_client or get_gcs_client(settings)

    run_id = uuid4().hex
    now = datetime.now(UTC)
    run_date = now.date()
    ingestion_date_str = run_date.isoformat()
    ingested_at_str = now.isoformat()
    started = time.monotonic()

    staging_prefix = (
        f"{settings.gcs_staging_prefix.rstrip('/')}/{run_id}/"
        if settings.gcs_staging_prefix
        else f"{run_id}/"
    )

    logger.info(
        "loader.start run_id=%s run_date=%s bucket=%s prefix=%s service_filter=%r force=%s",
        run_id,
        run_date,
        settings.gcs_staging_bucket,
        staging_prefix,
        settings.aws_service_filter or None,
        force,
    )
    log_memory("run.start", run_id=run_id)

    runs_service.start_run(
        bq_client,
        settings,
        run_id=run_id,
        service_filter=settings.aws_service_filter,
        ingestion_date=run_date,
        started_at=now,
    )

    try:
        # ---- 1) Discover ----
        targets = aws_client.discover_targets(
            settings,
            service_filter=settings.service_filter_set() or None,
            include_savings_plans=settings.aws_include_savings_plans,
        )
        if not targets:
            raise RuntimeError(
                "AWS Price List returned zero targets; refusing to swap the live table."
            )

        # ---- 2) Diff against versions ----
        if force:
            known: set[tuple[str, str, str, str]] = set()
        else:
            known = _load_known_versions(bq_client, settings)
        changed, skipped = aws_client.diff_targets(targets, known)
        logger.info(
            "loader.diff total=%d changed=%d skipped=%d force=%s",
            len(targets),
            len(changed),
            skipped,
            force,
        )
        log_memory(
            "discovery.done",
            targets=len(targets),
            changed=len(changed),
            skipped=skipped,
        )

        previous_date = _latest_previous_partition(bq_client, settings, run_date)
        today_has_data = _partition_has_data(bq_client, settings, run_date)

        # ---- 3) Idempotent short-circuit ----
        if not changed and today_has_data and not force:
            logger.info("loader.skip nothing changed and today's partition already populated")
            runs_service.finish_run(
                bq_client,
                settings,
                run_id=run_id,
                rows_loaded=0,
                services_changed=0,
                services_skipped=skipped,
            )
            elapsed = time.monotonic() - started
            return LoadResult(
                run_id=run_id,
                run_date=run_date,
                rows_loaded=0,
                services_changed=0,
                services_skipped=skipped,
                elapsed_s=elapsed,
            )

        # ---- 4) Download + transform + upload (parallel) ----
        rows_loaded = 0
        if changed:
            with ThreadPoolExecutor(max_workers=settings.aws_max_workers) as pool:
                futs = {
                    pool.submit(
                        _download_one,
                        t,
                        settings,
                        gcs_client,
                        staging_prefix,
                        ingestion_date_str,
                        ingested_at_str,
                    ): t
                    for t in changed
                }
                for fut in as_completed(futs):
                    rows_loaded += fut.result()
            log_memory("downloads.done", rows=rows_loaded)

            if rows_loaded == 0:
                # Every changed target returned zero rows — very suspicious; bail out
                # rather than truncating today's partition with nothing.
                raise RuntimeError(
                    "Downloaded changed targets but produced zero rows; refusing to update."
                )

        # ---- 5) Prepare today's partition for the LOAD JOB ----
        #
        # We must NOT use WRITE_TRUNCATE on the LOAD JOB. If a previous run
        # already populated today (same-day rerun), TRUNCATE would wipe all the
        # unchanged services' rows; `_latest_previous_partition` only sees
        # `ingestion_date < today`, so the carry-forward step couldn't restore
        # them. Result: live table ends up with only the changed services and
        # the unchanged ones disappear.
        #
        # Instead: split into two cases and use WRITE_APPEND on the LOAD JOB.
        #   - same-day rerun (today_has_data=True): DELETE today's rows for the
        #     changed (service, region) pairs only — unchanged rows stay put.
        #   - new day (today_has_data=False): carry forward yesterday's rows
        #     for unchanged pairs into today's partition, then append the
        #     changed ones from the LOAD JOB.
        changed_csv = _csv_of_changed_pairs(changed)
        if today_has_data:
            if changed:
                _delete_changed_from_today(bq_client, settings, run_date, changed_csv)
        elif previous_date:
            _carry_forward_previous_partition(
                bq_client, settings, run_date, previous_date, changed_csv
            )
        # else: first-ever run, no historical partitions to carry from; today
        # stays empty until the LOAD JOB below appends the changed (= all) rows.

        # ---- 6) LOAD JOB appends changed rows to today's history partition ----
        partition_decorator = run_date.strftime("%Y%m%d")
        destination = (
            f"{settings.gcp_project}.{settings.bq_dataset}."
            f"{HISTORY_TABLE}${partition_decorator}"
        )

        if changed:
            # NDJSON files are gzipped by upload_jsonl (extension .jsonl.gz).
            # BigQuery LOAD JOB transparently decompresses gzip for the
            # NEWLINE_DELIMITED_JSON source format, so no extra config needed.
            source_uri = f"gs://{settings.gcs_staging_bucket}/{staging_prefix}*.jsonl.gz"
            schema = bq_client.schema_from_json(str(HISTORY_SCHEMA_PATH))
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                # APPEND, not TRUNCATE — see comment on step 5 above.
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema,
            )
            load_job = bq_client.load_table_from_uri(
                source_uri, destination, job_config=job_config
            )
            logger.info(
                "bq.load_job.submitted run_id=%s job_id=%s source=%s dest=%s",
                run_id,
                load_job.job_id,
                source_uri,
                destination,
            )
            load_job.result(timeout=3300)
            if load_job.errors:
                raise RuntimeError(f"BigQuery load job failed: {load_job.errors}")
            logger.info(
                "bq.load_job.complete run_id=%s job_id=%s output_rows=%s",
                run_id,
                load_job.job_id,
                getattr(load_job, "output_rows", None),
            )
            log_memory("load_job.done")

        # ---- 7) Upsert version state for changed targets ----
        if changed:
            _merge_versions(bq_client, settings, run_date, changed_csv)

        # ---- 8) Atomic swap of live table ----
        _swap_live_table(bq_client, settings, run_date)
        log_memory("swap.done")

        # ---- 9) Clean up GCS staging on success ----
        delete_prefix(gcs_client, settings.gcs_staging_bucket, staging_prefix)

        # ---- 10) Audit success ----
        runs_service.finish_run(
            bq_client,
            settings,
            run_id=run_id,
            rows_loaded=rows_loaded,
            services_changed=len(changed),
            services_skipped=skipped,
        )

    except Exception as exc:
        logger.exception("loader.failed run_id=%s", run_id)
        try:
            runs_service.fail_run(bq_client, settings, run_id=run_id, error=str(exc))
        except Exception:
            logger.exception("loader.fail_run.also_failed run_id=%s", run_id)
        raise

    elapsed = time.monotonic() - started
    logger.info(
        "loader.complete run_id=%s rows=%d changed=%d skipped=%d elapsed=%.1fs",
        run_id,
        rows_loaded,
        len(changed),
        skipped,
        elapsed,
    )
    log_memory("run.complete", rows=rows_loaded, elapsed_s=f"{elapsed:.1f}")
    return LoadResult(
        run_id=run_id,
        run_date=run_date,
        rows_loaded=rows_loaded,
        services_changed=len(changed),
        services_skipped=skipped,
        elapsed_s=elapsed,
    )
