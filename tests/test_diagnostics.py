"""Smoke tests for the memory-diagnostics helpers.

These don't try to assert anything tight about RSS values — those depend on
platform, kernel, and what else is loaded. The tests just verify the helpers
return sane numbers and that `log_memory` emits a well-formed log line.
"""

from __future__ import annotations

import logging

import pytest

from app import diagnostics


def test_rss_mb_returns_non_negative_float():
    val = diagnostics.rss_mb()
    assert isinstance(val, float)
    assert val >= 0.0


def test_peak_rss_mb_returns_positive_float():
    """Peak RSS is always at least as large as current RSS, and is > 0 for a
    running Python process on every supported platform."""
    val = diagnostics.peak_rss_mb()
    assert isinstance(val, float)
    # A Python process holding pytest + the app modules will always exceed
    # a few MB on every supported platform — guard against unit mistakes
    # (e.g. forgetting to convert bytes -> MB on macOS).
    assert val > 1.0
    assert val < 100_000.0  # < 100 GB — anything bigger means a unit slip


def test_log_memory_emits_structured_line(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO, logger="app.diagnostics"):
        diagnostics.log_memory("test.checkpoint", service="AmazonEC2", rows=12345)

    matches = [r for r in caplog.records if "mem.snapshot" in r.getMessage()]
    assert len(matches) == 1
    msg = matches[0].getMessage()
    assert "label=test.checkpoint" in msg
    assert "rss_mb=" in msg
    assert "peak_rss_mb=" in msg
    # Extra kwargs are appended as key=value pairs.
    assert "service=AmazonEC2" in msg
    assert "rows=12345" in msg


def test_log_memory_handles_zero_extras(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO, logger="app.diagnostics"):
        diagnostics.log_memory("bare")
    matches = [r for r in caplog.records if "label=bare" in r.getMessage()]
    assert matches, "log_memory must work without extra kwargs"
