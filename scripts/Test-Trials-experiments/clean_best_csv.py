#!/usr/bin/env python3
"""Clean best SCT CSVs with light, deterministic rules (no name/position split)."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List

import pandas as pd


def clean_text_basic(raw) -> str:
    s = str(raw)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"(\w)-\s+(\w)", r"\1\2", s)
    s = s.replace("/", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s*\([^)]*\)", "", s)
    s = s.replace("$", "")
    s = s.replace(",", "")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def normalize_header(val: str) -> str:
    s = clean_text_basic(val)
    if "name" in s and "position" in s:
        return "name_position"
    if "fiscal" in s or "year" in s:
        return "fiscal_year"
    if "salary" in s:
        return "salary"
    if "bonus" in s:
        return "bonus"
    if "other annual" in s:
        return "other_annual_comp"
    if "stock" in s and "award" in s:
        return "stock_awards"
    if "option" in s:
        return "option_awards"
    if "incentive" in s:
        return "non_equity_incentive"
    if "pension" in s:
        return "pension_value"
    if "all other" in s:
        return "all_other_comp"
    if "total" in s:
        return "total"
    return s.replace(" ", "_")


def fix_two_row_header(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    first_row = df.iloc[0].astype(str).str.lower().tolist()
    col_names = df.columns.astype(str).str.lower().tolist()
    matches = sum([1 for x in first_row if x in col_names])
    if matches < 2:
        return df

    new_cols: List[str] = []
    for old, new in zip(df.columns, df.iloc[0]):
        new = str(new).strip().lower()
        old = str(old).strip().lower()

        if new == old:
            new_cols.append(old)
            continue
        if new in ("nan", "", None):
            new_cols.append(old)
            continue
        if old in ("nan", "", None):
            new_cols.append(new)
            continue
        if new in old:
            new_cols.append(old)
            continue
        if old in new:
            new_cols.append(new)
            continue
        new_cols.append(f"{old}_{new}")

    df.columns = new_cols
    df = df.iloc[1:].reset_index(drop=True)
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Fix two-row header if pattern fits
    df = fix_two_row_header(df)

    # Clean all cells
    df = df.map(clean_text_basic)

    # Normalize headers
    df.columns = [normalize_header(c) for c in df.columns]

    # Drop columns to the left of name_position if present
    if "name_position" in df.columns:
        matches = [i for i, c in enumerate(df.columns) if c == "name_position"]
        if matches:
            idx = matches[0]
            df = df.iloc[:, idx:]

    # NaN handling: replace blanks with NaN
    df = df.replace({"": pd.NA, " ": pd.NA})

    # Drop fully empty rows/cols
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")

    # Remove duplicate rows
    df = df.drop_duplicates()

    return df


def process_file(csv_path: Path) -> bool:
    try:
        df = pd.read_csv(csv_path, dtype=str)
    except Exception:
        return False
    cleaned = clean_dataframe(df)
    out_dir = csv_path.parent.parent / "extracted_best_clean"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (csv_path.stem.replace("_best", "_clean") + ".csv")
    cleaned.to_csv(out_path, index=False)
    return True


def iter_best_csv(data_root: Path, tickers: List[str], form: str):
    if tickers:
        for ticker in tickers:
            best_dir = data_root / ticker / form / "extracted_best_csv"
            if not best_dir.is_dir():
                continue
            yield from sorted(best_dir.glob("*_SCT_best.csv"))
    else:
        for tdir in sorted(data_root.iterdir()):
            if not tdir.is_dir():
                continue
            best_dir = tdir / form / "extracted_best_csv"
            if not best_dir.is_dir():
                continue
            yield from sorted(best_dir.glob("*_SCT_best.csv"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean best SCT CSVs with simple header/text normalization."
    )
    parser.add_argument("--data-root", default="data", help="Root folder (default: data)")
    parser.add_argument("--form", default="DEF_14A", help="Form subfolder (default: DEF_14A)")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers (default: all)")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    total = 0
    saved = 0
    for csv_path in iter_best_csv(data_root, tickers, args.form):
        total += 1
        if process_file(csv_path):
            saved += 1

    print(f"best_csv_found={total}")
    print(f"clean_csv_saved={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
