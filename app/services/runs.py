"""CRUD over the `pricing_runs` audit table.

Each loader invocation:
- `start_run(...)` inserts a row with `status='running'`.
- `finish_run(...)` updates it to `status='succeeded'` with row counts.
- `fail_run(...)` updates it to `status='failed'` with the error message.

Uses parameterized DML via `bigquery.Client.query(...)` rather than streaming
inserts so the audit table costs nothing to write to.
"""

from dataclasses import dataclass
from datetime import date, datetime

from google.cloud import bigquery

from app.bq_setup import RUNS_TABLE
from app.config import Settings, get_settings

_ERROR_MAX_LEN = 8000


@dataclass
class PricingRun:
    run_id: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    rows_loaded: int | None
    services_changed: int | None
    services_skipped: int | None
    service_filter: str | None
    ingestion_date: date | None
    error: str | None


def _table_fqn(settings: Settings) -> str:
    return f"`{settings.gcp_project}.{settings.bq_dataset}.{RUNS_TABLE}`"


def _await(
    client: bigquery.Client, sql: str, params: list[bigquery.ScalarQueryParameter]
) -> bigquery.QueryJob:
    """Run a parameterized DML statement and block until complete."""
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()
    return job


def start_run(
    client: bigquery.Client,
    settings: Settings | None,
    *,
    run_id: str,
    service_filter: str,
    ingestion_date: date,
    started_at: datetime,
) -> None:
    settings = settings or get_settings()
    sql = (
        f"INSERT INTO {_table_fqn(settings)} "
        f"(run_id, started_at, status, service_filter, ingestion_date) "
        f"VALUES (@run_id, @started_at, 'running', @service_filter, @ingestion_date)"
    )
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
        bigquery.ScalarQueryParameter("service_filter", "STRING", service_filter),
        bigquery.ScalarQueryParameter("ingestion_date", "DATE", ingestion_date),
    ]
    _await(client, sql, params)


def finish_run(
    client: bigquery.Client,
    settings: Settings | None,
    *,
    run_id: str,
    rows_loaded: int,
    services_changed: int,
    services_skipped: int,
) -> None:
    settings = settings or get_settings()
    sql = (
        f"UPDATE {_table_fqn(settings)} "
        f"SET status = 'succeeded', "
        f"    finished_at = CURRENT_TIMESTAMP(), "
        f"    rows_loaded = @rows_loaded, "
        f"    services_changed = @services_changed, "
        f"    services_skipped = @services_skipped "
        f"WHERE run_id = @run_id"
    )
    params = [
        bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
        bigquery.ScalarQueryParameter("services_changed", "INT64", services_changed),
        bigquery.ScalarQueryParameter("services_skipped", "INT64", services_skipped),
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
    ]
    _await(client, sql, params)


def fail_run(
    client: bigquery.Client,
    settings: Settings | None,
    *,
    run_id: str,
    error: str,
) -> None:
    settings = settings or get_settings()
    sql = (
        f"UPDATE {_table_fqn(settings)} "
        f"SET status = 'failed', "
        f"    finished_at = CURRENT_TIMESTAMP(), "
        f"    error = @error "
        f"WHERE run_id = @run_id"
    )
    params = [
        bigquery.ScalarQueryParameter("error", "STRING", (error or "")[:_ERROR_MAX_LEN]),
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
    ]
    _await(client, sql, params)


_SELECT_COLS = (
    "run_id, started_at, finished_at, status, rows_loaded, "
    "services_changed, services_skipped, service_filter, ingestion_date, error"
)


def _row_to_run(row) -> PricingRun:
    return PricingRun(
        run_id=row["run_id"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        status=row["status"],
        rows_loaded=row["rows_loaded"],
        services_changed=row["services_changed"],
        services_skipped=row["services_skipped"],
        service_filter=row["service_filter"],
        ingestion_date=row["ingestion_date"],
        error=row["error"],
    )


def list_runs(
    client: bigquery.Client,
    settings: Settings | None = None,
    *,
    limit: int = 50,
) -> list[PricingRun]:
    settings = settings or get_settings()
    sql = f"SELECT {_SELECT_COLS} FROM {_table_fqn(settings)} ORDER BY started_at DESC LIMIT @limit"
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    job = _await(client, sql, params)
    return [_row_to_run(r) for r in job.result()]


def get_run(
    client: bigquery.Client,
    settings: Settings | None,
    *,
    run_id: str,
) -> PricingRun | None:
    settings = settings or get_settings()
    sql = f"SELECT {_SELECT_COLS} FROM {_table_fqn(settings)} WHERE run_id = @run_id LIMIT 1"
    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    job = _await(client, sql, params)
    rows = list(job.result())
    return _row_to_run(rows[0]) if rows else None
