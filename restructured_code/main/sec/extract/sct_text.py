"""Text-based SCT extractor (regex slice only).

Given a DEF 14A .txt submission, locate the Summary Compensation Table block
using a permissive regex and return the raw snippet. No parsing, no LLM.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional


# Broad pattern: from the SCT heading to the closing TABLE tag
_SCT_PATTERN = re.compile(
    r"SUMMARY\s+COMPENSATION\s+TABLE[\s\S]*?</TABLE>", re.IGNORECASE | re.DOTALL
)


def extract_sct_snippet(text: str) -> Optional[str]:
    m = _SCT_PATTERN.search(text)
    if m:
        return m.group(0)
    return None


def extract_sct_snippet_from_file(path: str | Path, encoding: str = "utf-8") -> Optional[str]:
    p = Path(path)
    try:
        raw = p.read_text(encoding=encoding, errors="ignore")
    except Exception:
        try:
            raw = p.read_bytes().decode("latin-1", errors="ignore")
        except Exception:
            return None
    return extract_sct_snippet(raw)

