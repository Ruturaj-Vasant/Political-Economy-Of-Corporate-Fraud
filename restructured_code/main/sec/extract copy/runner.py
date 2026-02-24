"""SCT extraction runner utilities.

Supports three extractor modes for HTML:
- xpath: legacy XPath guard (returns first matching table) -> HTML stub
- score: scoring-based selector -> HTML stub
- both: try score first, fall back to xpath -> HTML stub

Outputs:
  data/<TICKER>/<FORM>/extracted/<TICKER>_<REPORT_DATE>_SCT.html

TXT extraction (regex slice) is unchanged.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import re

from ..config import load_config
from .sct_xpath import extract_best_sct_html_from_file as extract_xpath_html
from .sct_score import extract_best_sct_html_from_file as extract_score_html
from .sct_text import extract_sct_snippet_from_file
from ..downloads.file_naming import normalize_form_for_fs
from .extract_index import ExtractionIndex
from .run_log import TickerStats


def _report_date_from_filename(p: Path) -> Optional[str]:
    # Expect filename like 'YYYY-MM-DD_<FORM>.html'
    m = re.match(r"(\d{4}-\d{2}-\d{2})_", p.stem)
    if m:
        return m.group(1)
    return None


def _extracted_html_path(data_root: Path, ticker: str, form: str, report_date: str) -> Path:
    f_fs = normalize_form_for_fs(form)
    return data_root / ticker / f_fs / "extracted" / f"{ticker}_{report_date}_SCT.html"


def _extracted_txt_path(data_root: Path, ticker: str, form: str, report_date: str) -> Path:
    f_fs = normalize_form_for_fs(form)
    return data_root / ticker / f_fs / "extracted" / f"{ticker}_{report_date}_SCT.txt"


def extract_one_file(
    file_path: str | Path,
    ticker: str,
    form: str,
    overwrite: bool = False,
    save_parquet: bool = False,
    extractor: str = "xpath",  # xpath | score | both
    index: Optional[ExtractionIndex] = None,
    stats: Optional[TickerStats] = None,
) -> Optional[Path]:
    cfg = load_config()
    data_root = Path(cfg.data_root)
    p = Path(file_path)
    report_date = _report_date_from_filename(p)
    if not report_date:
        return None
    out_html = _extracted_html_path(data_root, ticker.upper(), form, report_date)
    if out_html.exists() and not overwrite:
        # Record skip in extraction index
        if index is not None:
            try:
                rel_html = out_html.relative_to(data_root).as_posix()
            except Exception:
                rel_html = out_html.as_posix()
            try:
                rel_src = p.relative_to(data_root).as_posix()
            except Exception:
                rel_src = p.as_posix()
            form_fs = normalize_form_for_fs(form)
            index.record_csv(
                ticker=ticker.upper(),
                form=form_fs,
                report_date=report_date,
                csv_relpath=rel_html,
                rows=None,
                cols=None,
                columns=None,
                source_html_relpath=rel_src,
                status="skipped_existing",
                parquet_relpath=None,
            )
        if stats is not None:
            stats.csv_skipped += 1
            stats.total_entries += 1
            if report_date:
                stats.file_status_html[report_date] = "skipped_existing"
        return out_html

    html_stub_str: Optional[str] = None
    if extractor == "score":
        html_stub_str = extract_score_html(p)
    elif extractor == "both":
        html_stub_str = extract_score_html(p)
        if not html_stub_str:
            html_stub_str = extract_xpath_html(p)
    else:  # xpath
        html_stub_str = extract_xpath_html(p)

    if not html_stub_str:
        # Record 'no_sct_found'
        if index is not None:
            try:
                rel_src = p.relative_to(data_root).as_posix()
            except Exception:
                rel_src = p.as_posix()
            form_fs = normalize_form_for_fs(form)
            index.record_status(
                ticker=ticker.upper(),
                form=form_fs,
                report_date=report_date,
                status="no_sct_found",
                source_relpath=rel_src,
            )
        if stats is not None:
            stats.html_no_sct += 1
            stats.total_entries += 1
            if report_date:
                stats.file_status_html[report_date] = "no_sct_found"
        return None

    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html_stub_str, encoding="utf-8")
    parquet_rel = None
    # Record successful HTML extraction (reuses CSV metadata slots for compatibility)
    if index is not None:
        try:
            rel_csv = out_html.relative_to(data_root).as_posix()
        except Exception:
            rel_csv = out_html.as_posix()
        try:
            rel_src = p.relative_to(data_root).as_posix()
        except Exception:
            rel_src = p.as_posix()
        form_fs = normalize_form_for_fs(form)
        index.record_csv(
            ticker=ticker.upper(),
            form=form_fs,
            report_date=report_date,
            csv_relpath=rel_csv,
            rows=None,
            cols=None,
            columns=None,
            source_html_relpath=rel_src,
            status="extracted",
            parquet_relpath=parquet_rel,
        )
    if stats is not None:
        stats.csv_extracted += 1
        stats.total_entries += 1
        if report_date:
            stats.file_status_html[report_date] = "extracted"
    return out_html


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


def extract_for_ticker(
    ticker: str,
    form: str = "DEF 14A",
    overwrite: bool = False,
    limit: Optional[int] = None,
    save_parquet: bool = False,
    extractor: str = "xpath",
    index: Optional[ExtractionIndex] = None,
    stats: Optional[TickerStats] = None,
) -> List[Path]:
    files = iter_form_html_files(ticker, form)
    outs: List[Path] = []
    for i, fp in enumerate(files):
        if limit is not None and i >= limit:
            break
        out = extract_one_file(
            fp,
            ticker=ticker,
            form=form,
            overwrite=overwrite,
            save_parquet=save_parquet,
            extractor=extractor,
            index=index,
            stats=stats,
        )
        if out is not None:
            outs.append(out)
    return outs


def extract_text_for_ticker(ticker: str, form: str = "DEF 14A", overwrite: bool = False, limit: Optional[int] = None, index: Optional[ExtractionIndex] = None, stats: Optional[TickerStats] = None) -> List[Path]:
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
            # Record skip
            if index is not None:
                try:
                    rel_txt = out.relative_to(data_root).as_posix()
                except Exception:
                    rel_txt = out.as_posix()
                try:
                    rel_src = Path(fp).relative_to(data_root).as_posix()
                except Exception:
                    rel_src = Path(fp).as_posix()
                form_fs = normalize_form_for_fs(form)
                index.record_txt(
                    ticker=ticker.upper(),
                    form=form_fs,
                    report_date=report_date,
                    txt_relpath=rel_txt,
                    snippet_chars=None,
                    status="skipped_existing",
                    source_txt_relpath=rel_src,
                )
            outs.append(out)
            if stats is not None:
                stats.txt_skipped += 1
                stats.total_entries += 1
                if report_date:
                    stats.file_status_txt[report_date] = "skipped_existing"
            continue
        snippet = extract_sct_snippet_from_file(fp)
        if not snippet:
            # Record not found
            if index is not None:
                try:
                    rel_src = Path(fp).relative_to(data_root).as_posix()
                except Exception:
                    rel_src = Path(fp).as_posix()
                form_fs = normalize_form_for_fs(form)
                index.record_status(
                    ticker=ticker.upper(),
                    form=form_fs,
                    report_date=report_date,
                    status="no_sct_found",
                    source_relpath=rel_src,
                )
            if stats is not None:
                stats.txt_no_sct += 1
                stats.total_entries += 1
                if report_date:
                    stats.file_status_txt[report_date] = "no_sct_found"
            continue
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            out.write_text(snippet, encoding="utf-8")
        except Exception:
            out.write_bytes(snippet.encode("utf-8", errors="ignore"))
        # Record text extraction
        if index is not None:
            try:
                rel_txt = out.relative_to(data_root).as_posix()
            except Exception:
                rel_txt = out.as_posix()
            try:
                rel_src = Path(fp).relative_to(data_root).as_posix()
            except Exception:
                rel_src = Path(fp).as_posix()
            form_fs = normalize_form_for_fs(form)
            index.record_txt(
                ticker=ticker.upper(),
                form=form_fs,
                report_date=report_date,
                txt_relpath=rel_txt,
                snippet_chars=len(snippet or ""),
                status="extracted",
                source_txt_relpath=rel_src,
            )
        outs.append(out)
        if stats is not None:
            stats.txt_extracted += 1
            stats.total_entries += 1
            if report_date:
                stats.file_status_txt[report_date] = "extracted"
    return outs


def extract_for_ticker_all(
    ticker: str,
    form: str = "DEF 14A",
    overwrite: bool = False,
    limit: Optional[int] = None,
    include_txt: bool = True,
    save_parquet: bool = False,
    extractor: str = "xpath",
    index: Optional[ExtractionIndex] = None,
    stats: Optional[TickerStats] = None,
) -> List[Path]:
    outs: List[Path] = []
    outs += extract_for_ticker(
        ticker,
        form=form,
        overwrite=overwrite,
        limit=limit,
        save_parquet=save_parquet,
        extractor=extractor,
        index=index,
        stats=stats,
    )
    if include_txt:
        outs += extract_text_for_ticker(
            ticker,
            form=form,
            overwrite=overwrite,
            limit=limit,
            index=index,
            stats=stats,
        )
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
