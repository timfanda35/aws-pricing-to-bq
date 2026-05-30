"""Cloud Run Job entry point.

Ensures the BigQuery dataset + tables exist, then runs an incremental pricing load.
Exits non-zero on failure so Cloud Run will surface it.
"""

import argparse
import logging
import sys
import time

from app import bq_setup
from app.config import get_settings
from app.services import loader


def _configure_logging() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_job")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass version diff: treat every (service, region) as changed.",
    )
    parser.add_argument(
        "--service-filter",
        default=None,
        help="Comma-separated AWS offerCodes, e.g. 'AmazonEC2,AmazonRDS,AmazonS3'.",
    )
    args = parser.parse_args(argv)

    _configure_logging()
    started = time.monotonic()
    settings = get_settings()
    if args.service_filter is not None:
        settings.aws_service_filter = args.service_filter

    print("[1/2] ensure dataset + tables...", flush=True)
    try:
        bq_setup.ensure_dataset_and_tables(settings=settings)
    except Exception as exc:
        print(f"FATAL: setup failed: {exc}", file=sys.stderr, flush=True)
        return 1

    print("[2/2] load...", flush=True)
    try:
        result = loader.run_load(settings=settings, force=args.force)
    except Exception as exc:
        print(f"FATAL: load failed: {exc}", file=sys.stderr, flush=True)
        return 1

    elapsed = time.monotonic() - started
    print(
        f"done. run_id={result.run_id} rows={result.rows_loaded} "
        f"changed={result.services_changed} skipped={result.services_skipped} "
        f"elapsed={elapsed:.1f}s",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
