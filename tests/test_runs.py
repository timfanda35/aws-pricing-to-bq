from datetime import UTC, date, datetime
from unittest.mock import MagicMock

from google.cloud import bigquery

from app.services import runs as runs_service


def _mock_client(rows=None):
    client = MagicMock(spec=bigquery.Client)
    job = MagicMock()
    job.result.return_value = iter(rows or [])
    client.query.return_value = job
    return client, job


def test_start_run_inserts_running_row(settings):
    client, job = _mock_client()
    started = datetime(2026, 5, 27, 12, 0, tzinfo=UTC)
    runs_service.start_run(
        client,
        settings,
        run_id="abc123",
        service_filter="AmazonEC2,AmazonRDS",
        ingestion_date=date(2026, 5, 27),
        started_at=started,
    )

    sql = client.query.call_args.args[0]
    assert "INSERT INTO" in sql
    assert "pricing_runs" in sql
    assert "'running'" in sql
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert params["run_id"].value == "abc123"
    assert params["service_filter"].value == "AmazonEC2,AmazonRDS"
    assert params["ingestion_date"].value == date(2026, 5, 27)
    assert params["started_at"].value == started
    job.result.assert_called_once()


def test_finish_run_records_change_counts(settings):
    client, job = _mock_client()
    runs_service.finish_run(
        client,
        settings,
        run_id="abc123",
        rows_loaded=42,
        services_changed=2,
        services_skipped=198,
    )
    sql = client.query.call_args.args[0]
    assert "UPDATE" in sql
    assert "'succeeded'" in sql
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert params["rows_loaded"].value == 42
    assert params["services_changed"].value == 2
    assert params["services_skipped"].value == 198
    assert params["run_id"].value == "abc123"
    job.result.assert_called_once()


def test_fail_run_truncates_long_errors(settings):
    client, _job = _mock_client()
    long_error = "x" * 10000
    runs_service.fail_run(client, settings, run_id="abc123", error=long_error)
    sql = client.query.call_args.args[0]
    assert "UPDATE" in sql
    assert "'failed'" in sql
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert len(params["error"].value) == 8000


def test_fail_run_handles_empty_error(settings):
    client, _job = _mock_client()
    runs_service.fail_run(client, settings, run_id="abc123", error="")
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert params["error"].value == ""


def test_list_runs_returns_dataclasses(settings):
    rows = [
        {
            "run_id": "r1",
            "started_at": datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
            "finished_at": datetime(2026, 5, 27, 12, 5, tzinfo=UTC),
            "status": "succeeded",
            "rows_loaded": 100,
            "services_changed": 3,
            "services_skipped": 197,
            "service_filter": "",
            "ingestion_date": date(2026, 5, 27),
            "error": None,
        }
    ]
    client, _job = _mock_client(rows=rows)
    result = runs_service.list_runs(client, settings, limit=5)
    sql = client.query.call_args.args[0]
    assert "ORDER BY started_at DESC" in sql
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert params["limit"].value == 5
    assert len(result) == 1
    assert result[0].services_changed == 3
    assert result[0].services_skipped == 197


def test_get_run_returns_none_when_missing(settings):
    client, _job = _mock_client(rows=[])
    assert runs_service.get_run(client, settings, run_id="missing") is None
