#!/usr/bin/env python3
"""
Extract the best ownership/beneficial-ownership table from HTML files by scoring headers.

Usage examples:
  # Single original DEF 14A file
  python3 scripts/Stand_alone_scripts/extract_ownership_table.py \
    --html data/AA/DEF_14A/2017-03-17_DEF_14A.html

  # Multiple tickers (comma-separated), finds all *_DEF_14A.html under each ticker
  python3 scripts/Stand_alone_scripts/extract_ownership_table.py \
    --tickers AA,ABMD \
    --data-root /path/to/data \
    --form "DEF 14A"

Tables are saved under .../extracted/ as <TICKER>_<DATE>_BOT_<N>.html
(e.g., data/AA/DEF_14A/extracted/AA_2017-03-17_BOT_1.html). Override with --output for single-file mode.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Tuple

from bs4 import BeautifulSoup
import pandas as pd


# --- Simple header/content heuristic -----------------------------------------

OWNERSHIP_KEYWORDS = [
    "beneficial ownership",
    "security ownership",
    "beneficial owner",
]

PERCENT_KEYWORDS = [
    "percentage of common stock",
    "percent of common stock",
    "percent of class",
    "% of class",
    "percent owned",
    "% owned",
]


def table_text(table) -> str:
    return table.get_text(" ", strip=True).lower()


def table_has_keyword(table, keywords: Iterable[str]) -> bool:
    text = table_text(table)
    return any(k in text for k in keywords)


def table_score(table) -> int:
    """Simple score based on keyword occurrences and percent signs."""
    text = table_text(table)
    beneficial = any(k in text for k in OWNERSHIP_KEYWORDS)
    percent = any(k in text for k in PERCENT_KEYWORDS) or ("%" in text)
    if not (beneficial and percent):
        return -1
    score = 0
    for k in OWNERSHIP_KEYWORDS:
        score += text.count(k)
    for k in PERCENT_KEYWORDS:
        score += text.count(k)
    score += text.count("%")
    return score


# --- Core extraction ---------------------------------------------------------

def find_ownership_tables(html_path: Path) -> List[Tuple[str, pd.DataFrame | None, int]]:
    """
    Find tables whose text contains ownership keywords. Returns list of (html, df_or_none, score),
    sorted by descending score.
    """
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "html.parser")
    tables = soup.find_all("table")
    results: List[Tuple[str, pd.DataFrame | None, int]] = []
    for tbl in tables:
        sc = table_score(tbl)
        if sc < 0:
            continue
        html_str = str(tbl)
        df = None
        try:
            # parse this table only
            parsed = pd.read_html(html_str)
            if parsed:
                df = parsed[0]
        except Exception:
            df = None
        results.append((html_str, df, sc))
    results.sort(key=lambda x: x[2], reverse=True)
    return results


def derive_bot_output_path(html_path: Path, table_idx: int) -> Path:
    """Default output path: data/<TICKER>/DEF_14A/extracted/<TICKER>_<DATE>_BOT_<N>.html"""
    ticker = html_path.parent.parent.name.upper()
    m = re.search(r"\d{4}-\d{2}-\d{2}", html_path.name)
    report_date = m.group(0) if m else html_path.stem
    outdir = html_path.parent / "extracted"
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / f"{ticker}_{report_date}_BOT_{table_idx}.html"


# --- CLI ---------------------------------------------------------------------

def process_file(html_path: Path, output_path: Path | None, max_tables: int) -> None:
    tables = find_ownership_tables(html_path)
    if not tables:
        print(f"[SKIP] {html_path} (no ownership keywords found)")
        return

    if output_path:
        out = output_path
        html_str, df, _ = tables[0]
        if df is not None:
            out.write_text(df.to_html(index=False))
        else:
            out.write_text(html_str)
        print(f"[OK] {html_path} -> {out} (1 table)")
        return

    total_tables = len(tables)
    limit = 1 if output_path else (max_tables if max_tables else total_tables)
    for idx, (html_str, df, _score) in enumerate(tables, start=1):
        if max_tables and idx > max_tables:
            break
        out = output_path if output_path else derive_bot_output_path(html_path, idx)
        out.parent.mkdir(parents=True, exist_ok=True)
        if df is not None:
            out.write_text(df.to_html(index=False))
        else:
            out.write_text(html_str)
        if idx == 1:
            print(f"[OK] {html_path} -> {out} (saving up to {limit} table(s))")
        if output_path:
            break


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract best ownership table from HTML by header scoring.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--html", help="Path to a single source HTML file.")
    ap.add_argument(
        "--tickers",
        help="Comma-separated tickers to batch process (use with --data-root; finds all *_SCT.html for each).",
    )
    ap.add_argument(
        "--data-root",
        default="data",
        help="Data root containing ticker/form/extracted folders (used when --tickers is provided).",
    )
    ap.add_argument("--form", default="DEF 14A", help='Form name (default: "DEF 14A") for ticker discovery.')
    ap.add_argument("--output", help="Optional output HTML path for the best table (single-file mode only).")
    ap.add_argument(
        "--max-tables",
        type=int,
        default=3,
        help="Maximum tables to save per filing (0 = no limit). Default: 3.",
    )
    args = ap.parse_args()

    if not args.html and not args.tickers:
        ap.error("Provide either --html for a single file or --tickers for batch mode.")

    if args.html:
        html_path = Path(args.html).expanduser().resolve()
        if not html_path.exists():
            print(f"HTML not found: {html_path}")
            return 1
        output_path = Path(args.output).expanduser().resolve() if args.output else None
        process_file(html_path, output_path, max_tables=args.max_tables)
        return 0

    # Batch mode
    data_root = Path(args.data_root).expanduser().resolve()
    form_fs = args.form.replace(" ", "_").upper()
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        ap.error("No tickers parsed from --tickers.")

    for t in tickers:
        base_dir = data_root / t / form_fs
        if not base_dir.exists():
            print(f"[WARN] Missing form dir for {t}: {base_dir}")
            continue
        html_files = sorted(base_dir.glob("*_DEF_14A.html"))
        if not html_files:
            print(f"[WARN] No *_DEF_14A.html files for {t} in {base_dir}")
            continue
        for hp in html_files:
            process_file(hp, output_path=None, max_tables=args.max_tables)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
