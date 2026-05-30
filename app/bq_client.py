from google.cloud import bigquery

from app.config import Settings, get_settings


def get_bq_client(settings: Settings | None = None) -> bigquery.Client:
    """Build a BigQuery client bound to the configured project + location.

    Patch point for tests: `app.bq_client.bigquery.Client`.
    """
    settings = settings or get_settings()
    return bigquery.Client(project=settings.gcp_project, location=settings.bq_location)
