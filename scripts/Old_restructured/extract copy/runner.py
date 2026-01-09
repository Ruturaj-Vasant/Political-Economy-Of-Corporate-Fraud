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
from typing import List, Optional, Tuple
import re
import pandas as pd

from ..config import load_config
from .sct_xpath import extract_sct_tables_from_file, normalize_sct_dataframe
from .sct_text import extract_sct_snippet_from_file
from ..downloads.file_naming import normalize_form_for_fs
from .extract_index import ExtractionIndex


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


def _extracted_parquet_path(data_root: Path, ticker: str, form: str, report_date: str) -> Path:
    f_fs = normalize_form_for_fs(form)
    return data_root / ticker / f_fs / "extracted" / f"{ticker}_{report_date}_SCT.parquet"


def _write_parquet_safe(df: pd.DataFrame, out_path: Path) -> Optional[Tuple[Path, str]]:
    """Attempt to write Parquet using pyarrow, then fastparquet.

    Returns the path on success, or None if no engine is available.
    """
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, engine="pyarrow", index=False)
        return out_path, "pyarrow"
    except Exception:
        # Try fastparquet as a fallback engine
        try:
            df.to_parquet(out_path, engine="fastparquet", index=False)
            return out_path, "fastparquet"
        except Exception:
            return None


def extract_one_file(
    file_path: str | Path,
    ticker: str,
    form: str,
    overwrite: bool = False,
    save_parquet: bool = False,
    index: Optional[ExtractionIndex] = None,
) -> Optional[Path]:
    cfg = load_config()
    data_root = Path(cfg.data_root)
    p = Path(file_path)
    report_date = _report_date_from_filename(p)
    if not report_date:
        return None
    out_csv = _extracted_csv_path(data_root, ticker.upper(), form, report_date)
    out_parquet = _extracted_parquet_path(data_root, ticker.upper(), form, report_date) if save_parquet else None
    if out_csv.exists() and not overwrite:
        # If CSV exists and Parquet requested, ensure Parquet exists (best-effort)
        if save_parquet and out_parquet is not None and not out_parquet.exists():
            try:
                # Attempt to read back CSV to write parquet
                csv_df = pd.read_csv(out_csv)
                _write_parquet_safe(csv_df, out_parquet)
            except Exception:
                pass
        # Record skip in extraction index
        if index is not None:
            try:
                rel_csv = out_csv.relative_to(data_root).as_posix()
            except Exception:
                rel_csv = out_csv.as_posix()
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
                status="skipped_existing",
                normalized=True,
                deduped_cols=True,
                parquet_relpath=(out_parquet.relative_to(data_root).as_posix() if (out_parquet and out_parquet.exists()) else None),
            )
        return out_csv

    dfs = extract_sct_tables_from_file(p)
    if not dfs:
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
        return None
    # Concatenate tables; outer join columns, reset index
    try:
        csv_df = pd.concat(dfs, ignore_index=True, sort=False)
    except Exception:
        # Fallback: write first
        csv_df = dfs[0]
    # Final normalization pass to ensure standardized, non-duplicate columns
    try:
        csv_df = normalize_sct_dataframe(csv_df)
    except Exception:
        # Minimal safeguard: drop duplicate-named columns
        csv_df = csv_df.loc[:, ~csv_df.columns.duplicated()]

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    # Ensure no duplicate column names before writing
    csv_df = csv_df.loc[:, ~csv_df.columns.duplicated()]
    csv_df.to_csv(out_csv, index=False)
    # Optional Parquet write
    parquet_rel = None
    if save_parquet and out_parquet is not None:
        res = _write_parquet_safe(csv_df, out_parquet)
        if res is not None:
            p_path, _engine = res
            try:
                parquet_rel = p_path.relative_to(data_root).as_posix()
            except Exception:
                parquet_rel = p_path.as_posix()
    # Record successful CSV extraction
    if index is not None:
        try:
            rel_csv = out_csv.relative_to(data_root).as_posix()
        except Exception:
            rel_csv = out_csv.as_posix()
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
            rows=int(csv_df.shape[0]),
            cols=int(csv_df.shape[1]),
            columns=[str(c) for c in csv_df.columns],
            source_html_relpath=rel_src,
            status="extracted",
            normalized=True,
            deduped_cols=True,
            parquet_relpath=parquet_rel,
        )
    return out_csv


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
    index: Optional[ExtractionIndex] = None,
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
            index=index,
        )
        if out is not None:
            outs.append(out)
    return outs


def extract_text_for_ticker(ticker: str, form: str = "DEF 14A", overwrite: bool = False, limit: Optional[int] = None, index: Optional[ExtractionIndex] = None) -> List[Path]:
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
    return outs


def extract_for_ticker_all(
    ticker: str,
    form: str = "DEF 14A",
    overwrite: bool = False,
    limit: Optional[int] = None,
    include_txt: bool = True,
    save_parquet: bool = False,
    index: Optional[ExtractionIndex] = None,
) -> List[Path]:
    outs: List[Path] = []
    outs += extract_for_ticker(
        ticker,
        form=form,
        overwrite=overwrite,
        limit=limit,
        save_parquet=save_parquet,
        index=index,
    )
    if include_txt:
        outs += extract_text_for_ticker(
            ticker,
            form=form,
            overwrite=overwrite,
            limit=limit,
            index=index,
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
