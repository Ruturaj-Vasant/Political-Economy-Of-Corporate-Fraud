#!/usr/bin/env python3
"""
Count report_date entries inside summary_compensation_table for each ticker in the master JSON.

Usage:
  python3 scripts/Stand_alone_scripts/count_sct_years_in_master.py \
    --master restructured_code/json/sec_company_tickers.json \
    --output out/master_sct_report_counts.txt
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple


def load_master(path: Path) -> Dict:
    """Load the master company JSON."""
    return json.loads(path.read_text())


def count_sct_entries(entry: Dict) -> int:
    """
    Count how many report_date entries exist in summary_compensation_table.
    Handles both dict (report_date -> data) and list forms defensively.
    """
    sct = entry.get("summary_compensation_table")
    if isinstance(sct, dict):
        return len(sct)
    if isinstance(sct, list):
        # In some schemas, it might be a list of report blocks.
        return len(sct)
    return 0


def write_counts(counts: List[Tuple[str, int]], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{ticker},{count}" for ticker, count in counts]
    dest.write_text("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Count report_date entries in summary_compensation_table for each ticker in master JSON."
    )
    parser.add_argument(
        "--master",
        default="restructured_code/json/sec_company_tickers.json",
        help="Path to master JSON (default: restructured_code/json/sec_company_tickers.json).",
    )
    parser.add_argument(
        "--output",
        help="Optional output CSV (ticker,count). If omitted, only prints summary.",
    )
    args = parser.parse_args()

    master_path = Path(args.master).expanduser().resolve()
    master = load_master(master_path)

    per_ticker_counts: List[Tuple[str, int]] = []
    tickers_with_sct = 0
    total_report_dates = 0

    for entry in master.values():
        ticker = str(entry.get("ticker", "")).upper()
        count = count_sct_entries(entry)
        if count > 0:
            tickers_with_sct += 1
            total_report_dates += count
        if ticker:
            per_ticker_counts.append((ticker, count))

    if args.output:
        write_counts(per_ticker_counts, Path(args.output).expanduser().resolve())
        print(f"Per-ticker counts written to: {args.output}")

    print(f"Master entries: {len(master)}")
    print(f"Tickers with SCT data: {tickers_with_sct}")
    print(f"Total report_date entries across master: {total_report_dates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
