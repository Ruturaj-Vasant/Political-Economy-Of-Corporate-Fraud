"""Centralized file naming.

Pattern (per your guidance):
  data/<TICKER>/<FORM>/<FILING_DATE>_<FORM>.<ext>

Examples:
  data/AAPL/DEF 14A/2002-03-21_DEF 14A.html
  data/AAPL/10-K/2024-02-01_10-K.txt

We retain spaces in form names for readability; the filesystem handles them.
If you prefer normalized form codes (e.g., DEF14A), you can change here.
"""
from __future__ import annotations

from pathlib import Path


def build_rel_paths(data_root: str, ticker: str, form: str, filing_date: str, prefer_ext: str = "html") -> tuple[Path, Path]:
    """Return (html_relpath, txt_relpath) under the data root.

    We always compute both, so the downloader can easily switch to TXT fallback
    if HTML validation fails or isn't available.
    """
    t = ticker.upper().strip()
    f = form.strip()  # keep as given (spaces allowed)
    base = Path(t) / f / f"{filing_date}_{f}"
    html_rel = base.with_suffix(".html")
    txt_rel = base.with_suffix(".txt")
    return html_rel, txt_rel

