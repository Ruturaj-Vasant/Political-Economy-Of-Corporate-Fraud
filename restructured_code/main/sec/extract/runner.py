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
from .sct_text import extract_sct_snippet_from_file
from ..downloads.file_naming import normalize_form_for_fs


def _report_date_from_filename(p: Path) -> Optional[str]:
    # Expect filename like 'YYYY-MM-DD_<FORM>.html'
    m = re.match(r"(\d{4}-\d{2}-\d{2})_", p.stem)
    if m:
        return m.group(1)
    return None


def _extracted_csv_path(data_root: Path, ticker: str, form: str, report_date: str) -> Path:
    f_fs = normalize_form_for_fs(form)
    return data_root / ticker / f_fs / "extracted" / f"{ticker}_{report_date}_SCT.csv"


def _extracted_txt_path(data_root: Path, ticker: str, form: str, report_date: str) -> Path:
    f_fs = normalize_form_for_fs(form)
    return data_root / ticker / f_fs / "extracted" / f"{ticker}_{report_date}_SCT.txt"


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
    t = ticker.upper()
    f_fs = normalize_form_for_fs(form)
    bases = [data_root / t / f_fs]
    # Backward-compat: also scan unsanitized form folder if present
    if (data_root / t / form) != bases[0]:
        bases.append(data_root / t / form)
    outs: List[Path] = []
    for b in bases:
        if b.exists():
            outs.extend(sorted(b.glob("*.html")))
    return outs


def iter_form_text_files(ticker: str, form: str) -> List[Path]:
    cfg = load_config()
    data_root = Path(cfg.data_root)
    t = ticker.upper()
    f_fs = normalize_form_for_fs(form)
    bases = [data_root / t / f_fs]
    if (data_root / t / form) != bases[0]:
        bases.append(data_root / t / form)
    outs: List[Path] = []
    for b in bases:
        if b.exists():
            outs.extend(sorted(b.glob("*.txt")))
    return outs


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


def extract_text_for_ticker(ticker: str, form: str = "DEF 14A", overwrite: bool = False, limit: Optional[int] = None) -> List[Path]:
    files = iter_form_text_files(ticker, form)
    outs: List[Path] = []
    for i, fp in enumerate(files):
        if limit is not None and i >= limit:
            break
        report_date = _report_date_from_filename(fp)
        if not report_date:
            continue
        cfg = load_config()
        data_root = Path(cfg.data_root)
        out = _extracted_txt_path(data_root, ticker.upper(), form, report_date)
        if out.exists() and not overwrite:
            outs.append(out)
            continue
        snippet = extract_sct_snippet_from_file(fp)
        if not snippet:
            continue
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            out.write_text(snippet, encoding="utf-8")
        except Exception:
            out.write_bytes(snippet.encode("utf-8", errors="ignore"))
        outs.append(out)
    return outs


def extract_for_ticker_all(ticker: str, form: str = "DEF 14A", overwrite: bool = False, limit: Optional[int] = None, include_txt: bool = True) -> List[Path]:
    outs: List[Path] = []
    outs += extract_for_ticker(ticker, form=form, overwrite=overwrite, limit=limit)
    if include_txt:
        outs += extract_text_for_ticker(ticker, form=form, overwrite=overwrite, limit=limit)
    return outs


def detect_tickers_with_form_htmls(form: str = "DEF 14A") -> List[str]:
    """Detect tickers that have at least one HTML or TXT file for the given form.

    Scans both sanitized (`<FORM_FS>`) and unsanitized (`<FORM>`) folders under each ticker.
    Returns sorted, upper-cased tickers.
    """
    cfg = load_config()
    data_root = Path(cfg.data_root)
    if not data_root.exists():
        return []
    tickers: List[str] = []
    for child in data_root.iterdir():
        if not child.is_dir() or child.name.startswith('.'):
            continue
        t = child.name.upper()
        f_fs = normalize_form_for_fs(form)
        candidates = [child / f_fs]
        if (child / form) != candidates[0]:
            candidates.append(child / form)
        found = False
        for d in candidates:
            try:
                if d.exists() and (any(d.glob('*.html')) or any(d.glob('*.txt'))):
                    found = True
                    break
            except Exception:
                continue
        if found:
            tickers.append(t)
    return sorted(set(tickers))
