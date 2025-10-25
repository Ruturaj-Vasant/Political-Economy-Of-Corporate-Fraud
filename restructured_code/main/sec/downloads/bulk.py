"""Bulk downloader CLI for all tickers in the reference JSON.

Usage examples:
  export SEC_USER_AGENT='you@example.com'
  python -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all --dry-run
  python -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all
  python -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --forms "10-K" --years 1994:2025 --limit 50
  python -m restructured_code.main.sec.downloads.bulk --tickers AAPL,MSFT --forms "DEF 14A" --base ./trial
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from .downloader import download_filings_for_ticker
from ..utils.ticker_lookup import tickers_by_permnos
from ..utils.list_loader import load_tickers_from_file, load_permnos_from_file
from ..config import load_config
from .data_index import DataIndex


DEFAULT_TICKER_JSON = Path("restructured_code/json/sec_company_tickers.json")


def _parse_years(s: Optional[str]) -> Optional[Tuple[int, int]]:
    if not s:
        return None
    if ":" in s:
        a, b = s.split(":", 1)
        return (int(a), int(b))
    y = int(s)
    return (y, y)


def _parse_forms(forms_args: List[str]) -> List[str]:
    if not forms_args:
        return ["DEF 14A"]
    out: List[str] = []
    for f in forms_args:
        parts = [x.strip() for x in f.split(",") if x.strip()]
        out.extend(parts)
    # preserve order, dedupe
    seen = set()
    result = []
    for f in out:
        if f not in seen:
            result.append(f)
            seen.add(f)
    return result


def _load_all_tickers(json_path: Path) -> List[str]:
    with json_path.open("r") as fh:
        data = json.load(fh)
    tickers: List[str] = []
    if isinstance(data, dict):
        for rec in data.values():
            if isinstance(rec, dict) and rec.get("ticker"):
                tickers.append(str(rec["ticker"]).strip().upper())
    else:
        # if the json is a list
        for rec in data:
            if isinstance(rec, dict) and rec.get("ticker"):
                tickers.append(str(rec["ticker"]).strip().upper())
    # stable order
    return tickers


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Bulk SEC downloader across tickers/forms with resume and index")
    ap.add_argument("--forms", action="append", default=[], help="Form names (repeat or comma-separate). Default: DEF 14A")
    ap.add_argument("--base", default="", help="Base folder (data is created under <base>/data). If empty, uses default data root")
    ap.add_argument("--dry-run", action="store_true", help="List what would be downloaded without fetching")
    ap.add_argument("--years", default="", help="Year range 'YYYY0:YYYY1' or single 'YYYY' (blank = all)")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N tickers (for testing)")
    ap.add_argument("--tickers", default="", help="Comma-separated tickers to restrict to (optional)")
    ap.add_argument("--tickers-file", default="", help="Path to CSV/TSV/txt with tickers (optional)")
    ap.add_argument("--permnos", default="", help="Comma-separated PERMNOs to resolve to tickers (optional)")
    ap.add_argument("--permnos-file", default="", help="Path to CSV/TSV/txt with PERMNOs (optional)")
    ap.add_argument("--max-new-files", type=int, default=0, help="Stop after saving N new files (HTML/TXT). Real runs only")
    ap.add_argument("--max-new-tickers", type=int, default=0, help="Stop after M tickers saved at least one new file. Real runs only")
    ap.add_argument("--json", default=str(DEFAULT_TICKER_JSON), help="Path to sec_company_tickers.json")

    args = ap.parse_args(argv)

    forms = _parse_forms(args.forms)
    years = _parse_years(args.years)
    base_dir = args.base if args.base else None

    ref = Path(args.json)
    if not ref.exists():
        ap.error(f"Ticker JSON not found: {ref}")

    all_tickers = _load_all_tickers(ref)
    tickers: List[str] = []
    # Seed from file if provided
    if args.tickers_file:
        tickers.extend(load_tickers_from_file(args.tickers_file))
    # Merge explicit tickers
    if args.tickers:
        tickers.extend([t.strip().upper() for t in args.tickers.split(",") if t.strip()])
    # If still empty, use JSON universe
    if not tickers:
        tickers = all_tickers
    # Merge permnos (resolved to tickers) if provided
    permnos_inputs: List[str] = []
    if args.permnos:
        permnos_inputs.extend([p.strip() for p in args.permnos.split(",") if p.strip()])
    if args.permnos_file:
        permnos_inputs.extend(load_permnos_from_file(args.permnos_file))
    if permnos_inputs:
        resolved = tickers_by_permnos(permnos_inputs, json_path=ref)
        # merge while preserving order preference: permnos first, then existing order
        merged: List[str] = []
        seen: set[str] = set()
        for t in resolved + tickers:
            tu = t.upper()
            if tu not in seen:
                merged.append(tu)
                seen.add(tu)
        tickers = merged
    if args.limit and args.limit > 0:
        tickers = tickers[: args.limit]

    print(f"Processing {len(tickers)} tickers, forms={forms}, years={years or 'all-years'}, base={base_dir or '(default)'}")
    if args.dry_run and (args.max_new_files or args.max_new_tickers):
        print("Note: --max-new-files/--max-new-tickers apply only to real runs; ignoring in dry-run.")

    # Determine effective data root for index monitoring
    cfg = load_config()
    effective_root = (Path(base_dir).expanduser() / "data") if base_dir else Path(cfg.data_root)

    total_new_files = 0
    total_new_tickers = 0

    # The downloader handles politeness, identity, and index updates
    for i, ticker in enumerate(tickers, start=1):
        print("\n" + "-" * 60)
        print(f"[{i}/{len(tickers)}] Ticker: {ticker}")
        try:
            # Snapshot index totals before
            idx_before = DataIndex.load(effective_root)
            total_before = int(idx_before._data.get("totals", {}).get("total_files", 0))
            t_before = int(idx_before._data.get("tickers", {}).get(ticker, {}).get("total_files", 0))

            download_filings_for_ticker(ticker, forms=forms, years=years, base_dir=base_dir, dry_run=args.dry_run)

            if not args.dry_run:
                # Reload and diff
                idx_after = DataIndex.load(effective_root)
                total_after = int(idx_after._data.get("totals", {}).get("total_files", 0))
                t_after = int(idx_after._data.get("tickers", {}).get(ticker, {}).get("total_files", 0))
                added_files = max(0, total_after - total_before)
                added_for_ticker = max(0, t_after - t_before)
                if added_files:
                    total_new_files += added_files
                if added_for_ticker > 0:
                    total_new_tickers += 1
                # Check thresholds
                if args.max_new_files and total_new_files >= args.max_new_files:
                    print(f"Reached --max-new-files threshold: {total_new_files} >= {args.max_new_files}. Stopping.")
                    break
                if args.max_new_tickers and total_new_tickers >= args.max_new_tickers:
                    print(f"Reached --max-new-tickers threshold: {total_new_tickers} >= {args.max_new_tickers}. Stopping.")
                    break
        except KeyboardInterrupt:
            print("Interrupted by user.")
            return 130
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
