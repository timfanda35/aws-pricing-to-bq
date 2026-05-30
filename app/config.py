from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    gcp_project: str = ""
    bq_dataset: str = "aws_pricing"
    bq_location: str = "US"
    gcs_staging_bucket: str = ""
    gcs_staging_prefix: str = "ingestion/"

    aws_pricing_base_url: str = "https://pricing.us-east-1.amazonaws.com"
    aws_request_timeout_s: int = 60
    aws_max_retries: int = 5
    aws_max_workers: int = Field(default=10, ge=1, le=32)

    # Comma-separated AWS offerCodes to include; empty = all. Useful for MVP smoke tests.
    aws_service_filter: str = ""
    aws_include_savings_plans: bool = True
    aws_include_reserved: bool = True

    http_proxy: str = ""
    https_proxy: str = ""
    no_proxy: str = ""

    log_level: str = "INFO"

    jsonl_batch_size: int = Field(default=10000, ge=100, le=200000)

    def service_filter_set(self) -> set[str]:
        """Parsed AWS_SERVICE_FILTER as a set, or empty set if no filter."""
        return {s.strip() for s in self.aws_service_filter.split(",") if s.strip()}


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
