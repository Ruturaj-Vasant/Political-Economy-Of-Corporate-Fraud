"""Validation utilities for downloaded filings.

Goals:
- Catch obviously bad responses (empty, tiny size, block pages).
- Ensure HTML is parseable by lxml/bs4.
- Optional smoke test for DEF 14A to flag extraction risk.
"""
from __future__ import annotations

from typing import Tuple
import re


BLOCK_PATTERNS = [
    re.compile(r"access denied", re.I),
    re.compile(r"request has been blocked", re.I),
    re.compile(r"forbidden", re.I),
]


def is_html_parseable(html_bytes: bytes) -> bool:
    try:
        # Try lxml first (fast/strict)
        from lxml import html as _html  # type: ignore
        _html.fromstring(html_bytes)
        return True
    except Exception:
        try:
            # Fallback to bs4+lxml parser
            from bs4 import BeautifulSoup  # type: ignore
            BeautifulSoup(html_bytes, "lxml")
            return True
        except Exception:
            return False


def basic_html_check(html_bytes: bytes, min_size: int) -> Tuple[bool, str]:
    """Return (ok, reason). Does not throw.

    Checks size, block phrases, and parseability.
    """
    if not html_bytes:
        return False, "empty"
    if len(html_bytes) < min_size:
        return False, f"too_small:<{min_size}"
    sample = html_bytes[:4096].decode("utf-8", errors="ignore")
    for pat in BLOCK_PATTERNS:
        if pat.search(sample):
            return False, "blocked"
    if not is_html_parseable(html_bytes):
        return False, "unparseable"
    return True, "ok"


def smoke_test_def14a(html_bytes: bytes) -> bool:
    """Very cheap DEF 14A smoke test.

    We just scan for phrases commonly present near the SCT and
    headings within DEF 14A. This is not extraction; it reduces the
    chance of silently accepting a corrupted HTML.
    """
    text = html_bytes.decode("utf-8", errors="ignore")
    needles = [
        r"summary\s+compensation\s+table",
        r"name\s+and\s+principal\s+position",
        r"principal\s+position",
        r"\bexecutive\s+compensation\b",
    ]
    return any(re.search(n, text, re.I) for n in needles)

