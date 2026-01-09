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
    r"SUMMARY\s+COMPENSATION\s+TABLE[\s\S]*?</TABLE>", re.DOTALL
)


def extract_sct_snippet(text: str) -> Optional[str]:
    # Find all candidate blocks first (case-sensitive heading)
    candidates = _SCT_PATTERN.findall(text)
    if not candidates:
        return None
    # Cross-validate: require keywords similar to HTML XPath guard
    for tbl in candidates:
        t = tbl.lower()
        if all(k in t for k in ("name", "principal", "position")):
            return tbl
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
