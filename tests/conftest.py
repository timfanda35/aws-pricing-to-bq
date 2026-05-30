import pytest

from app.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        gcp_project="test-project",
        bq_dataset="test_dataset",
        bq_location="US",
        gcs_staging_bucket="test-bucket",
        gcs_staging_prefix="ingestion/",
        aws_pricing_base_url="https://pricing.us-east-1.amazonaws.com",
        aws_request_timeout_s=5,
        aws_max_retries=3,
        aws_max_workers=2,
        aws_service_filter="",
        aws_include_savings_plans=True,
        aws_include_reserved=True,
        jsonl_batch_size=100,
    )
