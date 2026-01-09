from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple
import json
import re

from ..config import load_config
from ..downloads.file_naming import normalize_form_for_fs
from .csv_to_json import process_csv
try:  # optional progress bar
    from tqdm import tqdm  # type: ignore
    _HAS_TQDM = True
except Exception:  # pragma: no cover
    _HAS_TQDM = False

# No deterministic fallback in this flow; reserved for separate tooling


def list_extracted_csvs(ticker: str, form: str) -> List[Path]:
    """Return CSVs from either sanitized (DEF_14A) or unsanitized (DEF_14A) form folders.

    This mirrors the extractor's dual-scan behavior so users can pass either spelling.
    """
    cfg = load_config()
    root = Path(cfg.data_root) / ticker.upper()
    f_fs = normalize_form_for_fs(form)
    candidates = [root / f_fs / "extracted"]
    if f_fs != form:
        candidates.append(root / form / "extracted")
    outs: List[Path] = []
    for d in candidates:
        if d.exists():
            outs.extend(sorted(d.glob("*_SCT.csv")))
    return outs


def _kor_json_path_for(csv_path: Path) -> Path:
    return csv_path.with_name(csv_path.stem + "_kor.json")


def _filename_parts(csv_path: Path) -> Tuple[str, str]:
    """Return (ticker, date) parsed from <TICKER>_<YYYY-MM-DD>_SCT.csv"""
    m = re.match(r"([A-Z0-9\.-]+)_(\d{4}-\d{2}-\d{2})_SCT$", csv_path.stem, flags=re.I)
    if not m:
        # Fallback: infer from directory + stem
        ticker = csv_path.parent.parent.parent.name.upper()
        return ticker, ""
    return m.group(1).upper(), m.group(2)


def _json_matches_filename(json_path: Path, csv_path: Path) -> Tuple[bool, str]:
    """Validate that JSON structure is non-empty and ticker/date match filename.

    Returns (ok, reason_if_not_ok).
    """
    try:
        data = json.loads(json_path.read_text())
    except Exception as e:
        return False, f"json_parse_error: {e}"

    # Filter out known invalid shells
    if isinstance(data, dict) and ("raw_response" in data or "parse_error" in data):
        return False, "raw_or_parse_error_present"

    comp = (data or {}).get("company") if isinstance(data, dict) else None
    if not isinstance(comp, dict):
        return False, "missing_company"
    reps = comp.get("reports")
    if not isinstance(reps, list) or not reps:
        return False, "missing_reports"
    execs = (reps[0] or {}).get("executives") if isinstance(reps[0], dict) else None
    if not isinstance(execs, dict) or not execs:
        return False, "empty_executives"

    # Ticker/date match
    f_ticker, f_date = _filename_parts(csv_path)
    jticker = str(comp.get("ticker") or "").upper()
    jdate = str((reps[0] or {}).get("report_date") or "")
    if f_ticker and jticker and f_ticker != jticker:
        return False, f"ticker_mismatch: file={f_ticker} json={jticker}"
    if f_date and jdate and f_date != jdate:
        return False, f"date_mismatch: file={f_date} json={jdate}"

    # At least one numeric field under one year
    NUM_KEYS = {"salary","bonus","stock_awards","option_awards","non_equity_incentive","pension_value","all_other_comp","total"}
    has_numeric = False
    for _name, years in execs.items():
        if isinstance(years, dict):
            for _yr, rec in years.items():
                if isinstance(rec, dict) and any(k in rec for k in NUM_KEYS):
                    has_numeric = True
                    break
        if has_numeric:
            break
    if not has_numeric:
        return False, "no_numeric_fields"
    return True, "ok"


# Deterministic fallback intentionally not wired here


def detect_tickers_with_csvs(form: str = "DEF_14A") -> List[str]:
    """Return tickers that have at least one `*_SCT.csv` under `<data_root>/<TICKER>/<form>/extracted`.

    Sorted alphabetically. Case-insensitive on filesystem, returns upper-cased symbols.
    """
    cfg = load_config()
    base = Path(cfg.data_root)
    if not base.exists():
        return []
    tickers: List[str] = []
    for child in base.iterdir():
        if not child.is_dir() or child.name.startswith('.'):
            continue
        # Check both sanitized and unsanitized form folders
        f_fs = normalize_form_for_fs(form)
        candidates = [child / f_fs / "extracted"]
        if (child / form) != (child / f_fs):
            candidates.append(child / form / "extracted")
        has = False
        for d in candidates:
            try:
                if d.exists() and any(d.glob("*_SCT.csv")):
                    has = True
                    break
            except Exception:
                continue
        if has:
            tickers.append(child.name.upper())
    return sorted(set(tickers))


def run_for_ticker(
    ticker: str,
    form: str = "DEF_14A",
    model: str = "llama3:8b",
    limit: Optional[int] = None,
    overwrite: bool = False,
    show_progress: bool = True,
) -> List[Path]:
    outs: List[Path] = []
    csvs = list_extracted_csvs(ticker, form)
    # Filter to pending based on valid kor.json presence
    pending: List[Path] = []
    for p in csvs:
        j = _kor_json_path_for(p)
        if not j.exists():
            pending.append(p)
            continue
        ok, _reason = _json_matches_filename(j, p)
        if overwrite or not ok:
            pending.append(p)
    if limit is not None:
        pending = pending[: int(limit)]

    iterator = None
    use_bar = show_progress and _HAS_TQDM
    if use_bar:
        iterator = tqdm(total=len(pending), desc=f"{ticker} files", unit="file")

    for p in pending:
        try:
            jp = process_csv(str(p), model=model)
        except Exception:
            jp = None

        valid = False
        out_json_path: Optional[Path] = None
        if jp:
            out_json_path = Path(jp)
            ok, _reason = _json_matches_filename(out_json_path, p)
            valid = ok

        if valid and out_json_path is not None:
            outs.append(out_json_path)
        if iterator:
            iterator.update(1)

    if iterator is not None:
        iterator.close()
    return outs
