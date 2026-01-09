#!/usr/bin/env python3
"""Clean SCT extracted CSVs into normalized CSVs (deterministic)."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List

import pandas as pd


KEYWORDS = [
    "name",
    "principal",
    "position",
    "year",
    "salary",
    "bonus",
    "stock",
    "option",
    "non-equity",
    "non equity",
    "pension",
    "deferred",
    "all other",
    "total",
]


def _iter_csv_tables(data_root: Path, form: str, tickers: List[str]) -> Iterable[Path]:
    def is_source_csv(p: Path) -> bool:
        name = p.name
        if not name.endswith("_SCT.csv"):
            return False
        if "_clean_" in name:
            return False
        return True

    if tickers:
        for t in tickers:
            tdir = data_root / t / form / "extracted"
            if not tdir.is_dir():
                continue
            for p in sorted(tdir.glob("*_SCT.csv")):
                if is_source_csv(p):
                    yield p
    else:
        for tdir in sorted(data_root.iterdir()):
            if not tdir.is_dir():
                continue
            edir = tdir / form / "extracted"
            if not edir.is_dir():
                continue
            for p in sorted(edir.glob("*_SCT.csv")):
                if is_source_csv(p):
                    yield p


def _normalize_text(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_header(s: str) -> str:
    s = _normalize_text(s)
    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _keyword_score(headers: pd.Series) -> int:
    score = 0
    for cell in headers.fillna("").astype(str):
        lc = cell.lower()
        if any(k in lc for k in KEYWORDS):
            score += 1
    return score


def _is_numeric_row(row: pd.Series) -> bool:
    vals = [str(v).strip() for v in row.fillna("")]
    vals = [v for v in vals if v]
    if not vals:
        return False
    return all(re.fullmatch(r"\d+", v) for v in vals)


def _drop_numeric_rows(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    drop = 0
    for i in range(min(max_rows, len(df))):
        if _is_numeric_row(df.iloc[i]):
            drop = i + 1
        else:
            break
    if drop:
        df = df.iloc[drop:].reset_index(drop=True)
    return df


def _combine_header_rows(df: pd.DataFrame, max_rows: int) -> tuple[pd.DataFrame, pd.Series]:
    top = df.head(max_rows).fillna("").astype(str)
    best_k = 1
    best_score = -1
    best_headers = None
    for k in range(1, max_rows + 1):
        combined = top.iloc[:k].agg(" ".join, axis=0)
        score = _keyword_score(combined)
        if score > best_score:
            best_score = score
            best_k = k
            best_headers = combined
    if best_headers is None:
        best_headers = top.iloc[:1].agg(" ".join, axis=0)
    headers = best_headers.apply(_normalize_header)
    df_out = df.iloc[best_k:].reset_index(drop=True)
    df_out.columns = headers
    return df_out, headers


def _drop_empty(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    return df


def clean_csv_table(csv_path: Path, max_header_rows: int) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(csv_path, header=None, dtype=str)
    except Exception:
        return None
    df = _drop_numeric_rows(df, max_rows=2)
    df, _ = _combine_header_rows(df, max_header_rows)
    df = _drop_empty(df)
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean SCT CSVs to CSV.")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--form", default="DEF_14A")
    parser.add_argument("--tickers", default="", help="Comma-separated list")
    parser.add_argument("--max-header-rows", type=int, default=3)
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    scanned = 0
    saved = 0
    for csv_path in _iter_csv_tables(data_root, args.form, tickers):
        scanned += 1
        df = clean_csv_table(csv_path, args.max_header_rows)
        if df is None or df.empty:
            continue
        out_path = csv_path.with_name(f"{csv_path.stem}_clean_csv.csv")
        df.to_csv(out_path, index=False)
        saved += 1

    print(f"files_scanned={scanned}")
    print(f"files_saved={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
