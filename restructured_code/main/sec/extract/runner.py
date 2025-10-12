"""SCT extraction runner utilities.

Implements per-file and per-ticker extraction using the XPath-first logic.
Output path per request:
  data/<TICKER>/<FORM>/extracted/<TICKER>_<REPORT_DATE>_SCT.csv

We derive REPORT_DATE from the source HTML filename, which follows
<DATE>_<FORM>.html as created by the downloader.

If multiple SCT tables are detected in a single filing, we write a single CSV
that concatenates the tables (outer join of columns), preserving rows. One CSV
per HTML input.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import re
import pandas as pd

from ..config import load_config
from .sct_xpath import extract_sct_tables_from_file


def _report_date_from_filename(p: Path) -> Optional[str]:
    # Expect filename like 'YYYY-MM-DD_<FORM>.html'
    m = re.match(r"(\d{4}-\d{2}-\d{2})_", p.stem)
    if m:
        return m.group(1)
    return None


def _extracted_csv_path(data_root: Path, ticker: str, form: str, report_date: str) -> Path:
    return data_root / ticker / form / "extracted" / f"{ticker}_{report_date}_SCT.csv"


def extract_one_file(file_path: str | Path, ticker: str, form: str, overwrite: bool = False) -> Optional[Path]:
    cfg = load_config()
    data_root = Path(cfg.data_root)
    p = Path(file_path)
    report_date = _report_date_from_filename(p)
    if not report_date:
        return None
    out = _extracted_csv_path(data_root, ticker.upper(), form, report_date)
    if out.exists() and not overwrite:
        return out

    dfs = extract_sct_tables_from_file(p)
    if not dfs:
        return None
    # Concatenate tables; outer join columns, reset index
    try:
        csv_df = pd.concat(dfs, ignore_index=True, sort=False)
    except Exception:
        # Fallback: write first
        csv_df = dfs[0]

    out.parent.mkdir(parents=True, exist_ok=True)
    csv_df.to_csv(out, index=False)
    return out


def iter_form_html_files(ticker: str, form: str) -> List[Path]:
    cfg = load_config()
    data_root = Path(cfg.data_root)
    base = data_root / ticker.upper() / form
    if not base.exists():
        return []
    return sorted(base.glob("*.html"))


def extract_for_ticker(ticker: str, form: str = "DEF 14A", overwrite: bool = False, limit: Optional[int] = None) -> List[Path]:
    files = iter_form_html_files(ticker, form)
    outs: List[Path] = []
    for i, fp in enumerate(files):
        if limit is not None and i >= limit:
            break
        out = extract_one_file(fp, ticker=ticker, form=form, overwrite=overwrite)
        if out is not None:
            outs.append(out)
    return outs


def detect_tickers_with_form_htmls(form: str = "DEF 14A") -> List[str]:
    """Detect tickers that have at least one HTML file for the given form.

    Scans `<data_root>/<TICKER>/<form>/*.html`. Returns sorted, upper-cased tickers.
    """
    cfg = load_config()
    data_root = Path(cfg.data_root)
    if not data_root.exists():
        return []
    tickers: List[str] = []
    for child in data_root.iterdir():
        if not child.is_dir() or child.name.startswith('.'):
            continue
        form_dir = child / form
        try:
            has_html = any(form_dir.glob('*.html'))
        except Exception:
            has_html = False
        if has_html:
            tickers.append(child.name.upper())
    return sorted(set(tickers))
