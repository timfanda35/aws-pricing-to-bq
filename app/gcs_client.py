import json
import logging
import os
import tempfile
from collections.abc import Iterable

import requests
from google.cloud import storage
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

# 8 MiB. GCS resumable uploads require chunk_size to be a multiple of 256 KiB.
# Setting this on the Blob forces google-resumable-media to transmit in chunks
# of this size; without it, the library sends the whole payload in one HTTP
# request, which trips macOS / Cloud Run SSL write timeouts on multi-hundred-MB
# uploads.
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


def get_gcs_client(settings: Settings | None = None) -> storage.Client:
    """Build a GCS client bound to the configured project.

    Patch point for tests: `app.gcs_client.storage.Client`.
    """
    settings = settings or get_settings()
    return storage.Client(project=settings.gcp_project or None)


def upload_jsonl(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    items: Iterable[dict],
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """Stream items as NDJSON via a temp file and upload to GCS in chunks.

    Memory is bounded to one row at a time — important for AWS offers like
    AWSComputeSavingsPlan/us-east-1 (~850K rows) or AmazonEC2/us-east-1 (~1M+ rows)
    where buffering everything before upload would balloon RAM.

    Uploads use chunked resumable transfers (8 MiB by default) so a single slow
    network write can't kill the whole upload, and the call is wrapped in a
    tenacity retry on transient connection errors.

    Returns the row count. If `items` is empty, no blob is created and 0 is returned.
    """
    count = 0
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".jsonl") as tmp:
        tmp_path = tmp.name
        for item in items:
            tmp.write(
                json.dumps(item, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            )
            tmp.write(b"\n")
            count += 1
    try:
        if count == 0:
            logger.info(
                "gcs.upload.skipped bucket=%s blob=%s rows=0", bucket_name, blob_name
            )
            return 0
        size = os.path.getsize(tmp_path)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name, chunk_size=chunk_size)

        @retry(
            retry=retry_if_exception_type(
                (
                    requests.ConnectionError,
                    requests.Timeout,
                    ConnectionError,
                    TimeoutError,
                )
            ),
            stop=stop_after_attempt(5),
            wait=wait_exponential_jitter(initial=2, max=60),
            reraise=True,
        )
        def _do_upload() -> None:
            # (connect, read) timeouts. Read is generous to cover slow chunk
            # writes; chunked uploads still respect this per-chunk.
            blob.upload_from_filename(
                tmp_path,
                content_type="application/x-ndjson",
                timeout=(120, 1200),
            )

        _do_upload()
        logger.info(
            "gcs.upload.complete bucket=%s blob=%s rows=%d bytes=%d",
            bucket_name,
            blob_name,
            count,
            size,
        )
        return count
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def delete_prefix(client: storage.Client, bucket_name: str, prefix: str) -> int:
    """Delete every blob under a prefix. Returns the number of blobs deleted."""
    bucket = client.bucket(bucket_name)
    deleted = 0
    for blob in client.list_blobs(bucket, prefix=prefix):
        blob.delete()
        deleted += 1
    logger.info(
        "gcs.prefix.deleted bucket=%s prefix=%s deleted=%d", bucket_name, prefix, deleted
    )
    return deleted
