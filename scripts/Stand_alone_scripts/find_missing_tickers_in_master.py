#!/usr/bin/env python3
"""
Scan ticker folders under a data root and list those not present in the master JSON.

Usage:
  python3 scripts/Stand_alone_scripts/find_missing_tickers_in_master.py \
    --master restructured_code/json/sec_company_tickers.json \
    --data-root /path/to/data \
    --output out/missing_tickers_in_master.txt
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Set


def load_master(master_path: Path) -> Dict:
    """Load the master company JSON."""
    return json.loads(master_path.read_text())


def build_ticker_index(master: Dict) -> Set[str]:
    """
    Build a set of tickers from the master.
    Master is keyed by numeric strings; each entry holds a 'ticker' field.
    """
    tickers: Set[str] = set()
    for entry in master.values():
        t = entry.get("ticker")
        if t:
            tickers.add(str(t).upper())
    return tickers


def discover_ticker_dirs(data_root: Path) -> List[str]:
    """List top-level directory names under data_root (expected ticker folders)."""
    return sorted([p.name for p in data_root.iterdir() if p.is_dir()])


def write_list(lines: List[str], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="Find ticker folders missing from master JSON.")
    parser.add_argument("--master", required=True, help="Path to master JSON file.")
    parser.add_argument("--data-root", required=True, help="Data root containing ticker folders.")
    parser.add_argument(
        "--output",
        default="out/missing_tickers_in_master.txt",
        help="Output text file for missing tickers (default: out/missing_tickers_in_master.txt).",
    )
    args = parser.parse_args()

    master_path = Path(args.master).expanduser().resolve()
    data_root = Path(args.data_root).expanduser().resolve()
    out_path = Path(args.output).expanduser().resolve()

    master = load_master(master_path)
    master_tickers = build_ticker_index(master)
    ticker_dirs = discover_ticker_dirs(data_root)

    missing = [t for t in ticker_dirs if t.upper() not in master_tickers]

    write_list(missing, out_path)

    print(f"Master entries: {len(master_tickers)}")
    print(f"Ticker folders scanned: {len(ticker_dirs)}")
    print(f"Missing in master: {len(missing)}")
    print(f"Written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
