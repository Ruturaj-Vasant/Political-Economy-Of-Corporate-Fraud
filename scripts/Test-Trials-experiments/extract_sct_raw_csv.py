#!/usr/bin/env python3
"""Convert extracted SCT HTML stubs to raw CSV with zero normalization.

Inputs:   data-root/<TICKER>/<FORM>/extracted/*.html (already chosen tables)
Outputs:  data-root/<TICKER>/<FORM>/extracted_raw/*.csv (same basename)

This intentionally does no cleaning:
- reads the first table in each HTML via pandas.read_html
- writes it as-is to CSV (index dropped)
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

import pandas as pd


def iter_extracted_html(data_root: Path, tickers: List[str], form: str) -> Iterable[Path]:
    if tickers:
        for ticker in tickers:
            extracted_dir = data_root / ticker / form / "extracted"
            if not extracted_dir.is_dir():
                continue
            yield from sorted(extracted_dir.glob("*.html"))
    else:
        for tdir in sorted(data_root.iterdir()):
            if not tdir.is_dir():
                continue
            extracted_dir = tdir / form / "extracted"
            if not extracted_dir.is_dir():
                continue
            yield from sorted(extracted_dir.glob("*.html"))


def save_raw_csv(html_path: Path) -> bool:
    try:
        tables = pd.read_html(html_path)
    except Exception:
        return False
    if not tables:
        return False
    df = tables[0]
    out_dir = html_path.parent.parent / "extracted_raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (html_path.stem + "_raw.csv")
    df.to_csv(out_path, index=False)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Dump extracted SCT HTML tables to raw CSV.")
    parser.add_argument("--data-root", default="data", help="Root folder (default: data)")
    parser.add_argument("--form", default="DEF_14A", help="Form subfolder (default: DEF_14A)")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers (default: all)")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    total_files = 0
    total_saved = 0
    for html_path in iter_extracted_html(data_root, tickers, args.form):
        total_files += 1
        if save_raw_csv(html_path):
            total_saved += 1

    print(f"extracted_html_files={total_files}")
    print(f"raw_csv_saved={total_saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
