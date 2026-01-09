from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple
import os
import json
import re

from ..config import load_config
from ..downloads.file_naming import normalize_form_for_fs
from .csv_to_json import (
    process_csv,  # backward compatibility
    generate_json_for_csv,
    save_json_output,
    write_attempt_sidecars,
)
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


def _expected_report_date(csv_path: Path) -> str:
    _ticker, f_date = _filename_parts(csv_path)
    return f_date or ""


def _collect_report_dates_from_data(obj: object, out: set) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.lower() == "report_date" and isinstance(v, str) and v.strip():
                out.add(v.strip())
            _collect_report_dates_from_data(v, out)
    elif isinstance(obj, list):
        for it in obj:
            _collect_report_dates_from_data(it, out)


def _data_has_report_date(data: object, expected_date: str) -> bool:
    try:
        dates: set[str] = set()
        _collect_report_dates_from_data(data, dates)
        if not dates:
            return False
        if expected_date:
            return expected_date in dates
        return True
    except Exception:
        return False


def _json_file_contains_report_date(json_path: Path, expected_date: str) -> bool:
    try:
        txt = json_path.read_text(encoding="utf-8", errors="ignore")
        if expected_date and (expected_date in txt):
            return True
        # Fallback: try to parse minimally
        data = json.loads(txt)
        return _data_has_report_date(data, expected_date)
    except Exception:
        return False


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
    models: Optional[List[str]] = None,
    attempts: int = 3,
    limit: Optional[int] = None,
    overwrite: bool = False,
    show_progress: bool = True,
) -> List[Path]:
    outs: List[Path] = []
    csvs = list_extracted_csvs(ticker, form)
    # Filter to pending based on existence+size+report_date match
    pending: List[Path] = []
    for p in csvs:
        j = _kor_json_path_for(p)
        if overwrite or (not j.exists()):
            pending.append(p)
            continue
        try:
            size_ok = j.stat().st_size > 5  # treat tiny '{}' as invalid
        except Exception:
            size_ok = False
        expected_date = _expected_report_date(p)
        has_date = _json_file_contains_report_date(j, expected_date) if size_ok else False
        if not (size_ok and has_date):
            pending.append(p)
    if limit is not None:
        pending = pending[: int(limit)]

    iterator = None
    use_bar = show_progress and _HAS_TQDM
    if use_bar:
        iterator = tqdm(total=len(pending), desc=f"{ticker} files", unit="file")

    for p in pending:
        # Build model list: if explicit list provided, use it; else fall back to single `model`
        model_list: List[str] = []
        if models:
            model_list = [m.strip() for m in models if m and m.strip()]
        if not model_list:
            model_list = [model]

        success = False
        final_json_path: Optional[Path] = None
        for mname in model_list:
            for i in range(1, max(1, int(attempts)) + 1):
                try:
                    data, stdout_text, stderr_text, returncode, prompt_chars, shape = generate_json_for_csv(str(p), model=mname)
                except Exception as e:
                    # Generation failure; record attempt sidecars and continue
                    write_attempt_sidecars(
                        csv_path=str(p),
                        model=mname,
                        attempt=i,
                        stdout_text="",
                        stderr_text=str(e),
                        returncode=-1,
                        prompt_chars=0,
                        rows=0,
                        cols=0,
                        status="error",
                        error=str(e),
                    )
                    continue

                expected_date = _expected_report_date(p)
                ok = _data_has_report_date(data, expected_date)
                reason = None if ok else ("missing_or_mismatched_report_date" if expected_date else "missing_report_date")
                status = "ok" if ok else "invalid"
                write_attempt_sidecars(
                    csv_path=str(p),
                    model=mname,
                    attempt=i,
                    stdout_text=stdout_text,
                    stderr_text=stderr_text,
                    returncode=returncode,
                    prompt_chars=prompt_chars,
                    rows=shape[0],
                    cols=shape[1],
                    status=status,
                    error=reason,
                )

                if ok:
                    out_path_str = save_json_output(data, str(p))
                    final_json_path = Path(out_path_str)
                    try:
                        if final_json_path.stat().st_size > 5:
                            success = True
                            break
                        else:
                            success = False
                    except Exception:
                        success = False
            if success:
                break

        if success and final_json_path is not None:
            outs.append(final_json_path)
        else:
            try:
                print(f"Failed to produce JSON with report_date after {attempts} attempt(s) across {len(model_list)} model(s): {p}")
            except Exception:
                pass
        if iterator:
            iterator.update(1)

    if iterator is not None:
        iterator.close()
    return outs
