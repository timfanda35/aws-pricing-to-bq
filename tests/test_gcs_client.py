"""Tests for the GCS upload path.

These guard the production-only properties:
- Memory bounded: rows are streamed to a temp file, not buffered in RAM.
- Chunked upload: the blob is constructed with a chunk_size so resumable
  uploads don't try to send a multi-hundred-MB payload in one HTTP request.
- Transient connection errors are retried, not surfaced.
- Empty iterables do NOT create a zero-byte blob (which BQ would reject).
"""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from app import gcs_client


class _CapturingBlob:
    """Mock blob that records (chunk_size, uploaded_filename, content_type)."""

    def __init__(self, name: str, chunk_size: int | None = None):
        self.name = name
        self.chunk_size = chunk_size
        self.uploaded_from: str | None = None
        self.content_type: str | None = None
        self.upload_attempts = 0

    def upload_from_filename(self, path: str, *, content_type: str, timeout=None) -> None:
        self.upload_attempts += 1
        self.uploaded_from = path
        self.content_type = content_type


def _mock_client_with_capture():
    captured: list[_CapturingBlob] = []

    def _bucket(_name):
        bucket = MagicMock()

        def _blob(name, chunk_size=None):
            b = _CapturingBlob(name, chunk_size=chunk_size)
            captured.append(b)
            return b

        bucket.blob.side_effect = _blob
        return bucket

    client = MagicMock()
    client.bucket.side_effect = _bucket
    return client, captured


def test_upload_jsonl_streams_rows_to_temp_file_and_uploads():
    client, captured = _mock_client_with_capture()
    rows = [{"a": i, "b": f"value-{i}"} for i in range(3)]

    count = gcs_client.upload_jsonl(client, "bucket", "path/file.jsonl", iter(rows))

    assert count == 3
    assert len(captured) == 1
    blob = captured[0]
    # `.gz` suffix is forced on the blob name so BigQuery LOAD JOB picks up the
    # compression automatically via NEWLINE_DELIMITED_JSON + extension.
    assert blob.name == "path/file.jsonl.gz"
    # 8 MiB chunk_size forces chunked resumable upload — the fix for the
    # 'write operation timed out' regression on million-row offers.
    assert blob.chunk_size == 8 * 1024 * 1024
    assert blob.content_type == "application/x-ndjson"
    assert blob.upload_attempts == 1


def test_upload_jsonl_writes_gzip_to_temp_file(tmp_path):
    """Inspect the temp file that upload_jsonl built before upload — must be
    gzipped NDJSON. On Cloud Run /tmp is RAM-backed; storing the unfortunately
    bulky AWS pricing NDJSON uncompressed was the source of the EC2 us-east-1
    OOM (~1.2 GB tmpfs per worker for ~1M rows). Gzip cuts that ~10x."""
    import gzip as gz

    captured_path = {}

    class _Blob:
        chunk_size = None

        def __init__(self, *args, **kwargs):
            self.chunk_size = kwargs.get("chunk_size")

        def upload_from_filename(self, path, *, content_type, timeout=None):
            captured_path["path"] = path
            captured_path["raw"] = open(path, "rb").read()

    client = MagicMock()
    bucket = MagicMock()
    bucket.blob.side_effect = lambda name, chunk_size=None: _Blob(chunk_size=chunk_size)
    client.bucket.return_value = bucket

    rows = [{"sku": "A", "n": 1}, {"sku": "B", "n": 2}]
    gcs_client.upload_jsonl(client, "bucket", "x.jsonl", iter(rows))

    raw = captured_path["raw"]
    # Magic bytes: file must actually be gzipped.
    assert raw[:2] == b"\x1f\x8b", "upload_jsonl must emit gzip-compressed NDJSON"

    # Decompressed content matches the NDJSON we expect.
    content = gz.decompress(raw).decode("utf-8")
    lines = [ln for ln in content.split("\n") if ln]
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"sku": "A", "n": 1}
    assert json.loads(lines[1]) == {"sku": "B", "n": 2}


def test_upload_jsonl_skips_upload_for_empty_iterable():
    """Zero-row case must not create a zero-byte blob — BQ would reject it."""
    client, captured = _mock_client_with_capture()
    count = gcs_client.upload_jsonl(client, "bucket", "empty.jsonl", iter([]))
    assert count == 0
    # No blob was constructed at all.
    assert captured == []


def test_upload_jsonl_retries_on_transient_connection_error():
    """A ConnectionError should be retried, not surfaced — that was the user-observed bug."""
    attempts = {"n": 0}

    class _FlakyBlob:
        def __init__(self, name, chunk_size=None):
            self.chunk_size = chunk_size

        def upload_from_filename(self, path, *, content_type, timeout=None):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise requests.ConnectionError("write operation timed out")

    client = MagicMock()
    bucket = MagicMock()
    bucket.blob.side_effect = lambda name, chunk_size=None: _FlakyBlob(name, chunk_size=chunk_size)
    client.bucket.return_value = bucket

    with patch("time.sleep"):  # don't actually wait between retries
        count = gcs_client.upload_jsonl(client, "bucket", "x.jsonl", iter([{"a": 1}]))

    assert count == 1
    assert attempts["n"] == 3


def test_upload_jsonl_gives_up_after_max_retries():
    class _DoomedBlob:
        def __init__(self, name, chunk_size=None):
            pass

        def upload_from_filename(self, path, *, content_type, timeout=None):
            raise requests.ConnectionError("permanent failure")

    client = MagicMock()
    bucket = MagicMock()
    bucket.blob.side_effect = lambda name, chunk_size=None: _DoomedBlob(name, chunk_size=chunk_size)
    client.bucket.return_value = bucket

    with patch("time.sleep"), pytest.raises(requests.ConnectionError):
        gcs_client.upload_jsonl(client, "bucket", "x.jsonl", iter([{"a": 1}]))
