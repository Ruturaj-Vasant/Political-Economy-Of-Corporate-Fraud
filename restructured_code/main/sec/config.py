"""Centralized configuration for SEC downloads.

You can tune defaults here or override via environment variables/CLI
in a follow‑up iteration. We keep the interface minimal and well‑commented.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class SecConfig:
    # Where files are written. We keep the original `data/` root as requested.
    data_root: str = os.getenv("SEC_DATA_ROOT", "data")

    # HTTP politeness
    user_agent: str = os.getenv("SEC_USER_AGENT", "you@example.com")
    min_interval_seconds: float = float(os.getenv("SEC_MIN_INTERVAL", "0.5"))  # delay between requests per process
    max_retries: int = int(os.getenv("SEC_MAX_RETRIES", "3"))

    # Validation knobs
    verify_enabled: bool = True
    min_html_size_bytes: int = int(os.getenv("SEC_MIN_HTML_SIZE", "2048"))  # 2KB threshold for stub detection
    # Optional smoke test for DEF 14A (off by default). If enabled, mark `extract_smoke_ok` in meta.
    smoke_test_def14a: bool = bool(int(os.getenv("SEC_SMOKE_DEF14A", "0")))


def load_config() -> SecConfig:
    return SecConfig()

