from unittest.mock import MagicMock

from google.cloud import bigquery

from app import bq_setup


def _real_schema(path):
    real_client = bigquery.Client.__new__(bigquery.Client)
    return bigquery.Client.schema_from_json(real_client, str(path))


def _mock_bq_client_with_real_schema():
    history_schema = _real_schema(bq_setup.HISTORY_SCHEMA_PATH)
    versions_schema = _real_schema(bq_setup.VERSIONS_SCHEMA_PATH)
    runs_schema = _real_schema(bq_setup.RUNS_SCHEMA_PATH)

    def _schema_from_json(path):
        if path == str(bq_setup.HISTORY_SCHEMA_PATH):
            return history_schema
        if path == str(bq_setup.VERSIONS_SCHEMA_PATH):
            return versions_schema
        if path == str(bq_setup.RUNS_SCHEMA_PATH):
            return runs_schema
        raise AssertionError(f"unexpected schema path: {path}")

    client = MagicMock()
    client.schema_from_json.side_effect = _schema_from_json
    return client


def test_ensure_dataset_and_tables_creates_dataset_and_all_three_tables(settings):
    client = _mock_bq_client_with_real_schema()
    bq_setup.ensure_dataset_and_tables(client=client, settings=settings)

    # Dataset created
    client.create_dataset.assert_called_once()
    ds_arg = client.create_dataset.call_args.args[0]
    assert ds_arg.location == settings.bq_location
    assert client.create_dataset.call_args.kwargs.get("exists_ok") is True

    # Three DDL statements
    assert client.query.call_count == 3
    ddls = [c.args[0] for c in client.query.call_args_list]

    history_ddl = next(d for d in ddls if "aws_pricing_history" in d)
    assert "CREATE TABLE IF NOT EXISTS" in history_ddl
    assert "PARTITION BY ingestion_date" in history_ddl
    assert "CLUSTER BY service_code, region_code, sku" in history_ddl
    assert "require_partition_filter = TRUE" in history_ddl
    # Prices must be BIGNUMERIC; NUMERIC's 9 fractional digits would truncate AWS prices.
    assert "`price_per_unit` BIGNUMERIC" in history_ddl
    assert "`starting_range` BIGNUMERIC" in history_ddl
    assert "`ending_range` BIGNUMERIC" in history_ddl
    assert " NUMERIC" not in history_ddl.replace("BIGNUMERIC", "")
    # attributes / term_attributes / price_per_unit_raw must be native JSON.
    assert "`attributes` JSON" in history_ddl
    assert "`term_attributes` JSON" in history_ddl
    assert "`price_per_unit_raw` JSON" in history_ddl

    versions_ddl = next(d for d in ddls if "aws_pricing_versions" in d)
    assert "CREATE TABLE IF NOT EXISTS" in versions_ddl
    assert "CLUSTER BY service_code, region_code" in versions_ddl
    # The versions table is intentionally not partitioned (small + read-modify-write).
    assert "PARTITION BY" not in versions_ddl

    runs_ddl = next(d for d in ddls if "pricing_runs" in d)
    assert "CREATE TABLE IF NOT EXISTS" in runs_ddl
    assert "PARTITION BY" not in runs_ddl
    assert "services_changed" in runs_ddl
    assert "services_skipped" in runs_ddl

    assert client.query.return_value.result.call_count == 3


def test_ensure_dataset_and_tables_is_idempotent(settings):
    client = _mock_bq_client_with_real_schema()
    bq_setup.ensure_dataset_and_tables(client=client, settings=settings)
    bq_setup.ensure_dataset_and_tables(client=client, settings=settings)

    # Each call: 1 create_dataset + 3 queries (history + versions + runs).
    assert client.create_dataset.call_count == 2
    assert client.query.call_count == 6
    for call in client.create_dataset.call_args_list:
        assert call.kwargs.get("exists_ok") is True
    for call in client.query.call_args_list:
        assert "CREATE TABLE IF NOT EXISTS" in call.args[0]
