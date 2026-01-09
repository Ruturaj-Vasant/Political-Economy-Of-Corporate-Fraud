#!/usr/bin/env python3
"""Clean SCT HTML table stubs into normalized CSVs (deterministic)."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

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


def _iter_html_tables(data_root: Path, form: str, tickers: List[str]) -> Iterable[Path]:
    if tickers:
        for t in tickers:
            tdir = data_root / t / form / "extracted"
            if not tdir.is_dir():
                continue
            yield from sorted(tdir.glob("*_SCT*.html"))
    else:
        for tdir in sorted(data_root.iterdir()):
            if not tdir.is_dir():
                continue
            edir = tdir / form / "extracted"
            if not edir.is_dir():
                continue
            yield from sorted(edir.glob("*_SCT*.html"))


def _group_by_base(paths: Iterable[Path]) -> Dict[str, List[Path]]:
    groups: Dict[str, List[Path]] = {}
    for p in paths:
        base = re.sub(r"_table\\d+$", "", p.stem)
        groups.setdefault(base, []).append(p)
    return groups


def _clean_text(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _keyword_score(df: pd.DataFrame, header_rows: int) -> int:
    top = df.head(header_rows).fillna("").astype(str)
    score = 0
    for cell in top.values.ravel():
        lc = str(cell).lower()
        if any(k in lc for k in KEYWORDS):
            score += 1
    return score


def _numeric_score(df: pd.DataFrame, header_rows: int) -> int:
    body = df.iloc[header_rows:].fillna("").astype(str)
    score = 0
    for cell in body.values.ravel():
        if re.search(r"\\d", cell):
            score += 1
    return score


def _best_header_rows(df: pd.DataFrame, max_rows: int) -> Tuple[int, int]:
    best_k = 1
    best_score = -1
    max_rows = min(max_rows, len(df))
    for k in range(1, max_rows + 1):
        header_score = _keyword_score(df, k)
        numeric_score = _numeric_score(df, k)
        size_bonus = 5 if df.shape[1] >= 4 else 0
        score = header_score * 10 + numeric_score + size_bonus
        if score > best_score:
            best_score = score
            best_k = k
    return best_k, best_score


def _drop_empty(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace(r"^\\s*$", pd.NA, regex=True)
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    return df


def _read_table(html_path: Path) -> pd.DataFrame | None:
    try:
        tables = pd.read_html(str(html_path), header=None)
    except Exception:
        return None
    if not tables:
        return None
    return tables[0]


def _clean_table_with_headers(df: pd.DataFrame, header_rows: int) -> pd.DataFrame:
    df = df.applymap(lambda x: _clean_text(x) if isinstance(x, str) else x)
    df = _drop_empty(df)
    header_rows = min(header_rows, len(df))
    header_block = df.iloc[:header_rows].fillna("").astype(str)
    arrays = [header_block.iloc[i].map(_clean_text).tolist() for i in range(header_rows)]
    df_out = df.iloc[header_rows:].reset_index(drop=True)
    df_out.columns = pd.MultiIndex.from_arrays(arrays)
    df_out = _drop_empty(df_out)
    return df_out


def clean_html_table(html_path: Path, max_header_rows: int) -> Tuple[pd.DataFrame | None, int]:
    df = _read_table(html_path)
    if df is None or df.empty:
        return None, 0
    header_rows, _ = _best_header_rows(df, max_header_rows)
    return _clean_table_with_headers(df, header_rows), header_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean SCT HTML stubs to CSV.")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--form", default="DEF_14A")
    parser.add_argument("--tickers", default="", help="Comma-separated list")
    parser.add_argument("--max-header-rows", type=int, default=3)
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    scanned = 0
    saved = 0
    groups = _group_by_base(_iter_html_tables(data_root, args.form, tickers))
    for base, paths in groups.items():
        best_path = None
        best_score = -1
        best_header_rows = 0
        for html_path in paths:
            scanned += 1
            df = _read_table(html_path)
            if df is None or df.empty:
                continue
            header_rows, score = _best_header_rows(df, args.max_header_rows)
            if score > best_score:
                best_score = score
                best_path = html_path
                best_header_rows = header_rows
        if best_path is None:
            continue
        df = _read_table(best_path)
        if df is None or df.empty:
            continue
        cleaned = _clean_table_with_headers(df, best_header_rows)
        if cleaned is None or cleaned.empty:
            continue
        out_path = best_path.with_name(f"{base}_clean_html.csv")
        cleaned.to_csv(out_path, index=False)
        saved += 1

    print(f"files_scanned={scanned}")
    print(f"files_saved={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
