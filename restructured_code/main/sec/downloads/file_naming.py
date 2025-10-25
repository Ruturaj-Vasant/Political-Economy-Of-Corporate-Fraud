"""Centralized file naming.

Pattern:
  data/<TICKER>/<FORM_FS>/<FILING_DATE>_<FORM_FS>.<ext>

Where <FORM_FS> is a filesystem‑safe version of the form (spaces→underscores),
e.g., "DEF 14A" → "DEF_14A"; hyphens are preserved.

Examples:
  data/AAPL/DEF_14A/2002-03-21_DEF_14A.html
  data/AAPL/10-K/2024-02-01_10-K.txt
"""
from __future__ import annotations

from pathlib import Path
import re


def normalize_form_for_fs(form: str) -> str:
    f = form.strip()
    f = re.sub(r"\s+", "_", f)
    return f


def build_rel_paths(data_root: str, ticker: str, form: str, filing_date: str, prefer_ext: str = "html") -> tuple[Path, Path]:
    """Return (html_relpath, txt_relpath) under the data root.

    We always compute both, so the downloader can easily switch to TXT fallback
    if HTML validation fails or isn't available.
    """
    t = ticker.upper().strip()
    f_fs = normalize_form_for_fs(form)
    base = Path(t) / f_fs / f"{filing_date}_{f_fs}"
    html_rel = base.with_suffix(".html")
    txt_rel = base.with_suffix(".txt")
    return html_rel, txt_rel
