#!/usr/bin/env python3
"""
Count Beneficial Ownership (BO) and SCT tables in the master company JSON.

Usage:
  python3 scripts/Stand_alone_scripts/count_bo_sct_in_master.py \
    --master restructured_code/json/sec_company_tickers.json \
    --output out/master_bo_sct_counts.csv
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple


def load_master(path: Path) -> Dict:
    return json.loads(path.read_text())


def count_sct_entries(entry: Dict) -> int:
    sct = entry.get("summary_compensation_table")
    if isinstance(sct, dict):
        return len(sct)
    if isinstance(sct, list):
        return len(sct)
    return 0


def count_bo_entries_and_years(entry: Dict) -> Tuple[int, int]:
    bo = entry.get("beneficial_ownership")
    if isinstance(bo, dict):
        items = list(bo.values())
    elif isinstance(bo, list):
        items = bo
    else:
        items = []

    count = len(items)
    years = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        rd = str(it.get("report_date", "")).strip()
        if len(rd) >= 4 and rd[:4].isdigit():
            years.add(rd[:4])
    return count, len(years)


def write_counts(rows: List[Tuple[str, int, int, int]], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    lines = ["ticker,sct_count,bo_count,bo_year_count"]
    for ticker, sct_count, bo_count, bo_years in rows:
        lines.append(f"{ticker},{sct_count},{bo_count},{bo_years}")
    dest.write_text("\n".join(lines))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Count BO and SCT tables in master JSON (per ticker and totals)."
    )
    ap.add_argument(
        "--master",
        default="restructured_code/json/sec_company_tickers.json",
        help="Path to master JSON (default: restructured_code/json/sec_company_tickers.json).",
    )
    ap.add_argument(
        "--output",
        help="Optional output CSV with per-ticker counts (ticker,sct_count,bo_count,bo_year_count).",
    )
    args = ap.parse_args()

    master_path = Path(args.master).expanduser().resolve()
    master = load_master(master_path)

    per_ticker: List[Tuple[str, int, int, int]] = []
    tickers_with_sct = tickers_with_bo = 0
    total_sct = total_bo = total_bo_years = 0

    for entry in master.values():
        ticker = str(entry.get("ticker", "")).upper()
        sct_cnt = count_sct_entries(entry)
        bo_cnt, bo_years = count_bo_entries_and_years(entry)

        if sct_cnt > 0:
            tickers_with_sct += 1
            total_sct += sct_cnt
        if bo_cnt > 0:
            tickers_with_bo += 1
            total_bo += bo_cnt
            total_bo_years += bo_years

        if ticker:
            per_ticker.append((ticker, sct_cnt, bo_cnt, bo_years))

    if args.output:
        write_counts(per_ticker, Path(args.output).expanduser().resolve())
        print(f"Per-ticker counts written to: {args.output}")

    print(f"Master entries: {len(master)}")
    print(f"Tickers with SCT data: {tickers_with_sct}")
    print(f"Total SCT tables: {total_sct}")
    print(f"Tickers with BO data: {tickers_with_bo}")
    print(f"Total BO tables: {total_bo}")
    print(f"Total BO years (sum of per-ticker unique years): {total_bo_years}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
