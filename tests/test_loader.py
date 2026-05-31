from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from app.services import loader
from app.services.aws_client import OfferTarget


def _target(service: str, region: str, version: str, offer_type: str = "service") -> OfferTarget:
    prefix = "offers" if offer_type == "service" else "savingsPlan"
    return OfferTarget(
        service_code=service,
        region_code=region,
        offer_type=offer_type,
        version=version,
        offer_url=f"https://pricing.us-east-1.amazonaws.com/{prefix}/v1.0/aws/{service}/{version}/{region}/index.json",
    )


def _mock_bq_client(
    *,
    known_versions: list[tuple[str, str, str, str]] | None = None,
    previous_date: date | None = None,
    today_has_data: bool = False,
):
    """Configure a MagicMock BQ client whose `query()` returns the right shape per SQL fragment."""
    client = MagicMock(spec=bigquery.Client)

    # LOAD JOB
    load_job = MagicMock()
    load_job.errors = None
    load_job.job_id = "test-load-job"
    load_job.output_rows = 0
    client.load_table_from_uri.return_value = load_job

    # schema_from_json: load the real schema for richness
    real_client = bigquery.Client.__new__(bigquery.Client)
    schema = bigquery.Client.schema_from_json(real_client, str(loader.HISTORY_SCHEMA_PATH))
    client.schema_from_json.return_value = schema

    # query(...) returns a job whose .result() yields rows shaped by the SQL.
    def _query(sql, *args, **kwargs):
        job = MagicMock()
        if "FROM `test-project.test_dataset.aws_pricing_versions`" in sql and "SELECT" in sql.split()[0]:
            job.result.return_value = iter(
                {"service_code": v[0], "region_code": v[1], "offer_type": v[2], "version": v[3]}
                for v in (known_versions or [])
            )
        elif "MAX(ingestion_date)" in sql:
            job.result.return_value = iter([{"d": previous_date}])
        elif "COUNT(*)" in sql:
            job.result.return_value = iter([{"c": 1 if today_has_data else 0}])
        else:
            # Mutating DML / MERGE / INSERT / DELETE / swap / audit -> empty result
            job.result.return_value = iter([])
        return job

    client.query.side_effect = _query
    return client, load_job


def _mock_gcs_client():
    client = MagicMock()
    deleted = []

    def _bucket(name):
        bucket = MagicMock()
        bucket.name = name

        def _blob(blob_name):
            blob = MagicMock()
            blob.name = blob_name
            return blob

        bucket.blob.side_effect = _blob
        return bucket

    client.bucket.side_effect = _bucket
    client.list_blobs.side_effect = lambda bucket, prefix=None: (
        deleted.append(prefix) or iter([])
    )
    return client, deleted


def _stub_discover(targets):
    return MagicMock(return_value=targets)


def _stub_fetch_offer(offer):
    return MagicMock(return_value=offer)


_SAMPLE_ROW = {
    "ingestion_date": "2026-05-30",
    "service_code": "AmazonS3",
    "region_code": "us-east-1",
    "version": "v1",
    "offer_type": "service",
    "sku": "SKU",
    "rate_code": "SKU.T.R",
    "term_type": "OnDemand",
    "price_per_unit": "0.10",
    "currency": "USD",
    "unit": "Hrs",
    "ingested_at": "2026-05-30T00:00:00+00:00",
}


def _patch_offer_io(rows=None):
    """Patches the (download, parse) boundary used by `_download_one`.

    The loader streams the offer to disk via `aws_client.download_offer_to_file`
    and parses it incrementally via `transform.offer_file_to_rows`. Tests don't
    want to hit the real disk / network, so both are stubbed: download is a
    no-op returning a fake byte count, parse returns the supplied rows iter.
    """
    return (
        patch.object(loader.aws_client, "download_offer_to_file", return_value=100),
        patch.object(
            loader.transform,
            "offer_file_to_rows",
            return_value=iter(rows if rows is not None else [_SAMPLE_ROW]),
        ),
    )


def test_run_load_first_run_no_previous_partition(settings):
    """First-ever run: nothing in versions table, no previous partition, all targets are changed."""
    targets = [_target("AmazonS3", "us-east-1", "v1")]
    bq_client, load_job = _mock_bq_client(
        known_versions=[], previous_date=None, today_has_data=False
    )
    gcs_client, deleted_prefixes = _mock_gcs_client()

    download_patch, parse_patch = _patch_offer_io()
    with (
        patch.object(loader.aws_client, "discover_targets", return_value=targets),
        download_patch,
        parse_patch,
        patch.object(loader, "upload_jsonl", return_value=1) as upload_mock,
    ):
        result = loader.run_load(
            settings=settings, bq_client=bq_client, gcs_client=gcs_client
        )

    # ---- Upload happened ----
    assert upload_mock.call_count == 1
    upload_blob_name = upload_mock.call_args.args[2]
    assert upload_blob_name.endswith(".jsonl")
    assert result.run_id in upload_blob_name
    # ---- LOAD JOB submitted to today's partition decorator ----
    bq_client.load_table_from_uri.assert_called_once()
    _, dest = bq_client.load_table_from_uri.call_args.args[:2]
    assert dest.endswith("$" + result.run_date.strftime("%Y%m%d"))
    assert "aws_pricing_history" in dest
    # ---- No carry-forward INSERT (no previous partition) ----
    all_sqls = [c.args[0] for c in bq_client.query.call_args_list]
    assert not any("INSERT INTO" in s and "FROM `test-project.test_dataset.aws_pricing_history`" in s for s in all_sqls)
    # ---- MERGE to versions DID happen ----
    assert any("MERGE" in s and "aws_pricing_versions" in s for s in all_sqls)
    # ---- Live table swap ----
    assert any("CREATE OR REPLACE TABLE" in s and "aws_pricing" in s for s in all_sqls)
    # ---- Audit start + finish ----
    assert any("INSERT INTO" in s and "pricing_runs" in s and "'running'" in s for s in all_sqls)
    assert any("UPDATE" in s and "pricing_runs" in s and "'succeeded'" in s for s in all_sqls)
    # ---- Staging cleaned up ----
    assert len(deleted_prefixes) == 1
    # ---- Result ----
    assert result.services_changed == 1
    assert result.services_skipped == 0
    assert result.rows_loaded == 1


def test_run_load_skips_unchanged_targets(settings):
    """When all targets match known versions, the loader should not download anything."""
    targets = [
        _target("AmazonS3", "us-east-1", "v1"),
        _target("AmazonS3", "eu-west-1", "v1"),
    ]
    known = [(t.service_code, t.region_code, t.offer_type, t.version) for t in targets]
    bq_client, _ = _mock_bq_client(
        known_versions=known,
        previous_date=date(2026, 5, 26),
        today_has_data=True,
    )
    gcs_client, deleted_prefixes = _mock_gcs_client()

    with (
        patch.object(loader.aws_client, "discover_targets", return_value=targets),
        patch.object(loader.aws_client, "download_offer_to_file") as download_mock,
        patch.object(loader.transform, "offer_file_to_rows") as parse_mock,
        patch.object(loader, "upload_jsonl", return_value=1) as upload_mock,
    ):
        result = loader.run_load(
            settings=settings, bq_client=bq_client, gcs_client=gcs_client
        )

    download_mock.assert_not_called()
    parse_mock.assert_not_called()
    upload_mock.assert_not_called()
    bq_client.load_table_from_uri.assert_not_called()
    # No swap, no carryforward, no version MERGE — just audit start + finish
    all_sqls = [c.args[0] for c in bq_client.query.call_args_list]
    assert not any("CREATE OR REPLACE TABLE" in s for s in all_sqls)
    assert any("UPDATE" in s and "'succeeded'" in s for s in all_sqls)
    assert result.services_changed == 0
    assert result.services_skipped == 2
    assert result.rows_loaded == 0
    # Idempotent skip path did not even register a staging delete because no work happened.
    assert deleted_prefixes == []


def test_run_load_carries_forward_unchanged_pairs(settings):
    """Some targets change, others don't: changed are loaded, unchanged carried forward."""
    t_changed = _target("AmazonEC2", "us-east-1", "v2")
    t_unchanged = _target("AmazonRDS", "us-east-1", "v9")
    bq_client, _ = _mock_bq_client(
        known_versions=[("AmazonRDS", "us-east-1", "service", "v9")],
        previous_date=date(2026, 5, 26),
        today_has_data=False,
    )
    gcs_client, _deleted = _mock_gcs_client()

    with (
        patch.object(loader.aws_client, "discover_targets", return_value=[t_changed, t_unchanged]),
        patch.object(loader.aws_client, "download_offer_to_file", return_value=100),
        patch.object(loader.transform, "offer_file_to_rows", return_value=iter([_SAMPLE_ROW])),
        patch.object(loader, "upload_jsonl", return_value=1),
    ):
        result = loader.run_load(
            settings=settings, bq_client=bq_client, gcs_client=gcs_client
        )

    sqls = [c.args[0] for c in bq_client.query.call_args_list]
    # Carry-forward INSERT references both partition dates and the changed-pairs CSV.
    carry = next(
        s for s in sqls if "INSERT INTO" in s and "@previous_date" in s and "@changed_csv" in s
    )
    assert "CONCAT(service_code, '|', region_code)" in carry
    # Verify the changed CSV used at SQL invocation contained the changed target
    carry_call = next(
        c
        for c in bq_client.query.call_args_list
        if "@previous_date" in c.args[0] and "@changed_csv" in c.args[0]
    )
    params = {p.name: p for p in carry_call.kwargs["job_config"].query_parameters}
    assert params["previous_date"].value == date(2026, 5, 26)
    assert params["changed_csv"].value == "AmazonEC2|us-east-1"

    assert result.services_changed == 1
    assert result.services_skipped == 1


def test_run_load_force_ignores_known_versions(settings):
    """--force makes the loader treat everything as changed."""
    targets = [_target("AmazonS3", "us-east-1", "v1")]
    bq_client, _ = _mock_bq_client(
        known_versions=[("AmazonS3", "us-east-1", "service", "v1")],
        previous_date=None,
        today_has_data=False,
    )
    gcs_client, _deleted = _mock_gcs_client()

    with (
        patch.object(loader.aws_client, "discover_targets", return_value=targets),
        patch.object(loader.aws_client, "download_offer_to_file", return_value=100),
        patch.object(loader.transform, "offer_file_to_rows", return_value=iter([_SAMPLE_ROW])),
        patch.object(loader, "upload_jsonl", return_value=1) as upload_mock,
    ):
        result = loader.run_load(
            settings=settings, bq_client=bq_client, gcs_client=gcs_client, force=True
        )

    # Even though the version is "known", force=True bypasses the diff.
    upload_mock.assert_called_once()
    bq_client.load_table_from_uri.assert_called_once()
    assert result.services_changed == 1
    assert result.services_skipped == 0


def test_run_load_empty_discovery_refuses_to_proceed(settings):
    """If AWS returns no targets, loader must abort rather than swap a stale live table."""
    bq_client, _ = _mock_bq_client()
    gcs_client, _deleted = _mock_gcs_client()
    with (
        patch.object(loader.aws_client, "discover_targets", return_value=[]),
    ):
        with pytest.raises(RuntimeError, match="zero targets"):
            loader.run_load(
                settings=settings, bq_client=bq_client, gcs_client=gcs_client
            )
    # fail_run was recorded
    sqls = [c.args[0] for c in bq_client.query.call_args_list]
    assert any("UPDATE" in s and "'failed'" in s for s in sqls)


def test_run_load_load_job_error_leaves_state_intact(settings):
    targets = [_target("AmazonS3", "us-east-1", "v1")]
    bq_client, load_job = _mock_bq_client(
        known_versions=[], previous_date=None, today_has_data=False
    )
    load_job.errors = [{"reason": "invalid", "message": "boom"}]
    gcs_client, deleted_prefixes = _mock_gcs_client()

    with (
        patch.object(loader.aws_client, "discover_targets", return_value=targets),
        patch.object(loader.aws_client, "download_offer_to_file", return_value=100),
        patch.object(loader.transform, "offer_file_to_rows", return_value=iter([_SAMPLE_ROW])),
        patch.object(loader, "upload_jsonl", return_value=1),
        pytest.raises(RuntimeError, match="BigQuery load job failed"),
    ):
        loader.run_load(settings=settings, bq_client=bq_client, gcs_client=gcs_client)

    sqls = [c.args[0] for c in bq_client.query.call_args_list]
    # No swap, no version merge — the swap only fires after a successful LOAD.
    assert not any("CREATE OR REPLACE TABLE" in s for s in sqls)
    assert not any("MERGE" in s and "aws_pricing_versions" in s for s in sqls)
    assert any("UPDATE" in s and "'failed'" in s for s in sqls)
    # Staging is intentionally left in place for inspection.
    assert deleted_prefixes == []


def test_run_load_requires_gcp_project_and_bucket(settings):
    settings.gcp_project = ""
    with pytest.raises(ValueError, match="GCP_PROJECT"):
        loader.run_load(settings=settings, bq_client=MagicMock(), gcs_client=MagicMock())
    settings.gcp_project = "p"
    settings.gcs_staging_bucket = ""
    with pytest.raises(ValueError, match="GCS_STAGING_BUCKET"):
        loader.run_load(settings=settings, bq_client=MagicMock(), gcs_client=MagicMock())


def test_csv_of_changed_pairs_joins_with_pipe_and_comma():
    pairs = [
        _target("AmazonEC2", "us-east-1", "v1"),
        _target("AmazonRDS", "eu-west-1", "v2"),
    ]
    assert (
        loader._csv_of_changed_pairs(pairs)
        == "AmazonEC2|us-east-1,AmazonRDS|eu-west-1"
    )
    assert loader._csv_of_changed_pairs([]) == ""


def test_safe_blob_name_strips_unsafe_characters():
    target = _target("Amazon/EC2", "us-east-1", "v1")
    name = loader._safe_blob_name(target)
    assert "/" not in name
    assert "us-east-1" in name
