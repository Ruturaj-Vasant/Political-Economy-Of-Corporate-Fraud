"""Centralized configuration for SEC downloads.

You can tune defaults here or override via environment variables/CLI
in a follow‑up iteration. We keep the interface minimal and well‑commented.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class SecConfig:
    # Where files are written. Defaults are resolved at load time via load_config().
    data_root: str

    # HTTP politeness
    user_agent: str
    min_interval_seconds: float  # delay between requests per process
    max_retries: int

    # Validation knobs
    verify_enabled: bool
    min_html_size_bytes: int  # 2KB threshold for stub detection
    # Optional smoke test for DEF 14A (off by default). If enabled, mark `extract_smoke_ok` in meta.
    smoke_test_def14a: bool


def load_config() -> SecConfig:
    """Resolve environment variables at call time."""
    return SecConfig(
        data_root=os.getenv("SEC_DATA_ROOT", "data"),
        user_agent=os.getenv("SEC_USER_AGENT", "you@example.com"),
        min_interval_seconds=float(os.getenv("SEC_MIN_INTERVAL", "1")),
        max_retries=int(os.getenv("SEC_MAX_RETRIES", "3")),
        verify_enabled=True,
        min_html_size_bytes=int(os.getenv("SEC_MIN_HTML_SIZE", "2048")),
        smoke_test_def14a=bool(int(os.getenv("SEC_SMOKE_DEF14A", "0"))),
    )
