"""Lightweight memory + runtime diagnostics.

On Cloud Run there's no shell to `ps` from — observability has to come from
stdout. This module reads RSS from `/proc/self/status` (current) and
`resource.getrusage` (peak since process start) and emits a single log line
per checkpoint that you can grep / aggregate from Cloud Logging.

Cloud Run also publishes the container-level
`run.googleapis.com/container/memory/utilizations` metric automatically — that's
the source of truth for "did we OOM"; these per-stage in-process readings are
the complement: they tell you *which stage* drove the peak.
"""

from __future__ import annotations

import logging
import resource
import sys

logger = logging.getLogger(__name__)


def rss_mb() -> float:
    """Current resident-set size in MB. Returns 0 if unavailable (non-Linux)."""
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    # Format: "VmRSS:\t   123456 kB"
                    parts = line.split()
                    return float(parts[1]) / 1024.0  # KB -> MB
    except OSError:
        pass
    return 0.0


def peak_rss_mb() -> float:
    """Peak RSS this process has ever held, in MB.

    `getrusage` returns `ru_maxrss` in kilobytes on Linux but **bytes** on
    macOS — handle both so local smoke tests give sensible numbers too.
    """
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return ru / (1024 * 1024)  # bytes -> MB
    return ru / 1024  # KB -> MB (Linux)


def log_memory(label: str, **kv) -> None:
    """Emit a structured `mem.snapshot` log line.

    Extra keys land as `key=value` pairs, e.g.
    `log_memory("download.done", service="AmazonEC2", region="us-east-1", rows=1234567)`
    becomes:
    `mem.snapshot label=download.done rss_mb=412.3 peak_rss_mb=415.7 service=AmazonEC2 region=us-east-1 rows=1234567`
    """
    extras = " ".join(f"{k}={v}" for k, v in kv.items())
    logger.info(
        "mem.snapshot label=%s rss_mb=%.1f peak_rss_mb=%.1f%s",
        label,
        rss_mb(),
        peak_rss_mb(),
        f" {extras}" if extras else "",
    )
