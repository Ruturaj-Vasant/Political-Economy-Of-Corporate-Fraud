#!/usr/bin/env python3
"""Clean best SCT HTML stubs to CSV using header detection and canonical mapping (no name split)."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

# Keyword set for header detection
HEADER_KEYS = [
    "name",
    "principal",
    "position",
    "salary",
    "bonus",
    "total",
    "stock",
    "option",
    "non-equity",
    "non equity",
    "incentive",
    "pension",
    "all other",
    "compensation",
    "awards",
    "cash",
    "year",
]


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


def detect_header_row(df: pd.DataFrame, max_rows: int = 6) -> int:
    best_idx = 0
    best_score = -1
    for i in range(min(max_rows, len(df))):
        row = df.iloc[i].fillna("").astype(str).str.lower()
        text = " ".join(row.tolist())
        score = sum(1 for kw in HEADER_KEYS if kw in text)
        if score > best_score:
            best_score = score
            best_idx = i
    return best_idx


def merge_next_header(df: pd.DataFrame, header_idx: int) -> Optional[List[str]]:
    if header_idx + 1 >= len(df):
        return None
    h1 = df.iloc[header_idx].fillna("").astype(str).tolist()
    h2 = df.iloc[header_idx + 1].fillna("").astype(str).tolist()
    merged: List[str] = []
    for a, b in zip(h1, h2):
        a_clean = clean_text_basic(a)
        b_clean = clean_text_basic(b)
        if not b_clean:
            merged.append(a_clean)
        elif b_clean and b_clean not in a_clean:
            merged.append(f"{a_clean} {b_clean}".strip())
        else:
            merged.append(a_clean)
    return merged


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


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Detect header row
    header_idx = detect_header_row(df, max_rows=6)
    merged = merge_next_header(df, header_idx)
    headers = merged if merged else df.iloc[header_idx].tolist()
    df = df.iloc[header_idx + 1:].reset_index(drop=True)

    # Apply basic cleaning to all cells
    df = df.map(clean_text_basic)

    # Normalize headers
    norm_headers = [normalize_header(h) for h in headers]
    # Align lengths
    if len(norm_headers) < df.shape[1]:
        norm_headers += [f"col_{i}" for i in range(len(norm_headers), df.shape[1])]
    df.columns = norm_headers[: df.shape[1]]

    # Trim to SCT region
    if "name_position" in df.columns:
        matches = [i for i, c in enumerate(df.columns) if c == "name_position"]
        if matches:
            df = df.iloc[:, matches[0]:]

    # Replace blanks with NaN and drop empty rows/cols
    df = df.replace({"": pd.NA, " ": pd.NA})
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")

    # Drop duplicate rows
    df = df.drop_duplicates()
    return df


def process_html(html_path: Path) -> bool:
    try:
        tables = pd.read_html(html_path)
    except Exception:
        return False
    if not tables:
        return False
    df = tables[0]
    cleaned = clean_dataframe(df)
    out_path = html_path.parent / (html_path.stem + "_clean.csv")
    cleaned.to_csv(out_path, index=False)
    return True


def iter_best_html(data_root: Path, tickers: List[str], form: str):
    def _iter_for_ticker(ticker_path: Path):
        extracted_dir = ticker_path / form / "extracted"
        if not extracted_dir.is_dir():
            return []
        best = sorted(extracted_dir.glob("*_SCT_best.html"))
        if best:
            return best
        # fallback: all html stubs in extracted
        return sorted(extracted_dir.glob("*.html"))

    if tickers:
        for ticker in tickers:
            tpath = data_root / ticker
            if not tpath.is_dir():
                continue
            for p in _iter_for_ticker(tpath):
                yield p
    else:
        for tdir in sorted(data_root.iterdir()):
            if not tdir.is_dir():
                continue
            for p in _iter_for_ticker(tdir):
                yield p


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean best SCT HTML stubs to CSV using header detection/canonical mapping."
    )
    parser.add_argument("--data-root", default="data", help="Root folder (default: data)")
    parser.add_argument("--form", default="DEF_14A", help="Form subfolder (default: DEF_14A)")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers (default: all)")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    total = 0
    saved = 0
    for html_path in iter_best_html(data_root, tickers, args.form):
        total += 1
        if process_html(html_path):
            saved += 1

    print(f"best_html_found={total}")
    print(f"clean_csv_saved={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
