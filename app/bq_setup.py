import logging
from pathlib import Path

from google.cloud import bigquery

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent / "bq_schema"
HISTORY_SCHEMA_PATH = SCHEMA_DIR / "aws_pricing_history.json"
VERSIONS_SCHEMA_PATH = SCHEMA_DIR / "aws_pricing_versions.json"
RUNS_SCHEMA_PATH = SCHEMA_DIR / "pricing_runs.json"

HISTORY_TABLE = "aws_pricing_history"
VERSIONS_TABLE = "aws_pricing_versions"
RUNS_TABLE = "pricing_runs"


def _history_table_ddl(project: str, dataset: str, schema_cols_sql: str) -> str:
    """CREATE TABLE IF NOT EXISTS for the history table.

    Schema rendered inline (rather than relying on schema_from_json) so
    PARTITION BY / CLUSTER BY / OPTIONS can be set at creation time, and so
    require_partition_filter is enforced from day one.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{HISTORY_TABLE}` (\n"
        f"{schema_cols_sql}\n"
        f")\n"
        f"PARTITION BY ingestion_date\n"
        f"CLUSTER BY service_code, region_code, sku\n"
        f"OPTIONS(require_partition_filter = TRUE,\n"
        f"        description = 'AWS pricing snapshots. One row per priceDimension. attributes is JSON to absorb per-service variability.')"
    )


def _versions_table_ddl(project: str, dataset: str, schema_cols_sql: str) -> str:
    """CREATE TABLE IF NOT EXISTS for the version-tracking table."""
    return (
        f"CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{VERSIONS_TABLE}` (\n"
        f"{schema_cols_sql}\n"
        f")\n"
        f"CLUSTER BY service_code, region_code\n"
        f"OPTIONS(description = 'Tracks the last AWS-supplied version per (service, region) so the loader can skip unchanged offers.')"
    )


def _runs_table_ddl(project: str, dataset: str, schema_cols_sql: str) -> str:
    """CREATE TABLE IF NOT EXISTS for the audit table."""
    return (
        f"CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{RUNS_TABLE}` (\n"
        f"{schema_cols_sql}\n"
        f")\n"
        f"OPTIONS(description = 'Audit row per loader invocation.')"
    )


def _schema_field_to_sql(field: bigquery.SchemaField) -> str:
    """Render a SchemaField as the column fragment of a CREATE TABLE DDL."""
    if field.field_type in ("RECORD", "STRUCT"):
        inner = ", ".join(_schema_field_to_sql(f) for f in field.fields)
        base = f"STRUCT<{inner}>"
    else:
        base = field.field_type
    if field.mode == "REPEATED":
        base = f"ARRAY<{base}>"
    not_null = " NOT NULL" if field.mode == "REQUIRED" else ""
    return f"  `{field.name}` {base}{not_null}"


def _schema_cols_sql(schema: list[bigquery.SchemaField]) -> str:
    return ",\n".join(_schema_field_to_sql(f) for f in schema)


def ensure_dataset_and_tables(
    client: bigquery.Client | None = None,
    settings: Settings | None = None,
) -> None:
    """Ensure dataset + history + versions + runs tables exist. Idempotent.

    The live `aws_pricing` table is created by the loader's final
    CREATE OR REPLACE TABLE swap and is not provisioned here.
    """
    settings = settings or get_settings()
    client = client or bigquery.Client(project=settings.gcp_project, location=settings.bq_location)

    dataset_ref = bigquery.Dataset(f"{settings.gcp_project}.{settings.bq_dataset}")
    dataset_ref.location = settings.bq_location
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info(
        "bq.dataset.ensured project=%s dataset=%s location=%s",
        settings.gcp_project,
        settings.bq_dataset,
        settings.bq_location,
    )

    history_schema = client.schema_from_json(str(HISTORY_SCHEMA_PATH))
    history_ddl = _history_table_ddl(
        settings.gcp_project, settings.bq_dataset, _schema_cols_sql(history_schema)
    )
    client.query(history_ddl).result()
    logger.info(
        "bq.table.ensured table=%s.%s.%s",
        settings.gcp_project,
        settings.bq_dataset,
        HISTORY_TABLE,
    )

    versions_schema = client.schema_from_json(str(VERSIONS_SCHEMA_PATH))
    versions_ddl = _versions_table_ddl(
        settings.gcp_project, settings.bq_dataset, _schema_cols_sql(versions_schema)
    )
    client.query(versions_ddl).result()
    logger.info(
        "bq.table.ensured table=%s.%s.%s",
        settings.gcp_project,
        settings.bq_dataset,
        VERSIONS_TABLE,
    )

    runs_schema = client.schema_from_json(str(RUNS_SCHEMA_PATH))
    runs_ddl = _runs_table_ddl(
        settings.gcp_project, settings.bq_dataset, _schema_cols_sql(runs_schema)
    )
    client.query(runs_ddl).result()
    logger.info(
        "bq.table.ensured table=%s.%s.%s",
        settings.gcp_project,
        settings.bq_dataset,
        RUNS_TABLE,
    )
