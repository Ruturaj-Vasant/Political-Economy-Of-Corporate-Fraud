#!/usr/bin/env python3
"""Select the best SCT table from already extracted HTML stubs and save as CSV (raw)."""
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from lxml import html as LH  # type: ignore

# Scoring keywords
COMP_KEYS = [
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

GRANT_KEYS = ["grant", "grant date", "estimated future payouts"]


def normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").lower().split()) if text else ""


def extract_header_text(table) -> str:
    rows = table.xpath(".//tr")[:12]
    bits: List[str] = []
    for tr in rows:
        cells = tr.xpath("./td|./th")
        for cell in cells:
            bits.append(normalize(" ".join(cell.itertext())))
    return " ".join(bits)


def score_table(table) -> int:
    header = extract_header_text(table)
    full_text = normalize(" ".join(table.itertext()))
    score = 0
    # Name/position cues
    name_ok = "name" in header
    principal_ok = "principal" in header or "named executive" in header
    position_ok = "position" in header or "occupation" in header
    if name_ok and principal_ok and position_ok:
        score += 6
    elif name_ok and (principal_ok or position_ok):
        score += 4
    elif name_ok:
        score += 2

    # Compensation keywords
    comp_hits = {k for k in COMP_KEYS if k in header}
    score += min(len(comp_hits), 8)

    # Money / numeric cues
    if re.search(r"\$|\b\d{3,}\b", full_text):
        score += 2

    # Penalty for grant-focused tables
    if any(g in header for g in GRANT_KEYS):
        score -= 3

    return score


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


def parse_table(html_path: Path):
    try:
        tree = LH.fromstring(html_path.read_bytes())
    except Exception:
        return None
    tables = tree.xpath("//table")
    return tables[0] if tables else None


def group_key(path: Path) -> Tuple[str, str]:
    stem = path.stem  # e.g., TICKER_YYYY-MM-DD_SCT_table1
    parts = stem.split("_")
    if len(parts) >= 2:
        return parts[0], parts[1]
    return stem, ""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pick best SCT table from extracted HTML stubs and save raw CSV."
    )
    parser.add_argument("--data-root", default="data", help="Root folder (default: data)")
    parser.add_argument("--form", default="DEF_14A", help="Form subfolder (default: DEF_14A)")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers (default: all)")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    groups: Dict[Tuple[str, str], List[Tuple[int, Path]]] = defaultdict(list)
    total_files = 0
    for html_path in iter_extracted_html(data_root, tickers, args.form):
        total_files += 1
        table = parse_table(html_path)
        if table is None:
            continue
        score = score_table(table)
        groups[group_key(html_path)].append((score, html_path))

    saved = 0
    for key, items in groups.items():
        items.sort(key=lambda x: (-x[0], x[1].name))
        best_score, best_path = items[0]
        try:
            df = pd.read_html(best_path)[0]
        except Exception:
            continue
        out_dir = best_path.parent.parent / "extracted_best_csv"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_name = f"{key[0]}_{key[1]}_SCT_best.csv" if key[1] else f"{best_path.stem}_best.csv"
        out_path = out_dir / out_name
        df.to_csv(out_path, index=False)
        saved += 1

    print(f"extracted_html_files={total_files}")
    print(f"groups_processed={len(groups)}")
    print(f"best_csv_saved={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
