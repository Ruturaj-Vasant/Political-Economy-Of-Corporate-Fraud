#!/usr/bin/env python3
"""Trial extractor: score every HTML table and pick the most likely SCT."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from lxml import html as LH  # type: ignore


TITLE_XPATH = (
    "//*[contains(translate(normalize-space(string(.)), "
    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
    "'summary compensation table')]"
)

COMP_KEYWORDS: Tuple[str, ...] = (
    "salary",
    "bonus",
    "stock",
    "option",
    "non-equity",
    "non equity",
    "incentive",
    "pension",
    "all other",
    "total",
    "compensation",
    "awards",
    "cash",
    "year",
)

MONEY_RE = re.compile(r"\$|\b\d{3,}\b")
YEAR_RE = re.compile(r"\b20\d{2}\b|\b19\d{2}\b")


def iter_html_files(data_root: Path, tickers: List[str], form: str) -> Iterable[Path]:
    if tickers:
        for ticker in tickers:
            tdir = data_root / ticker / form
            if not tdir.is_dir():
                continue
            yield from sorted(tdir.glob("*.html"))
    else:
        for tdir in sorted(data_root.iterdir()):
            if not tdir.is_dir():
                continue
            fdir = tdir / form
            if not fdir.is_dir():
                continue
            yield from sorted(fdir.glob("*.html"))


def html_stub(table_html: str) -> str:
    return (
        "<!doctype html>\n"
        "<html><head><meta charset=\"utf-8\"></head><body>\n"
        f"{table_html}\n"
        "</body></html>\n"
    )


def parse_tables(tree) -> List:
    return tree.xpath("//table")


def normalize_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.replace("\xa0", " ").lower().split())


def table_header_text(table, max_rows: int = 10) -> str:
    rows = table.xpath(".//tr")[:max_rows]
    header_bits: List[str] = []
    for tr in rows:
        cells = tr.xpath("./td|./th")
        for cell in cells:
            header_bits.append(normalize_text(" ".join(cell.itertext())))
    return " ".join(header_bits)


def build_title_bonus_map(tree, tables: Sequence) -> set[int]:
    table_index_by_id = {id(t): idx for idx, t in enumerate(tables)}
    bonus_indices: set[int] = set()
    for node in tree.xpath(TITLE_XPATH):
        # If the title text lives inside a table, give that table a bonus.
        for anc in node.iterancestors():
            if anc.tag == "table":
                idx = table_index_by_id.get(id(anc))
                if idx is not None:
                    bonus_indices.add(idx)
                break
        following = node.xpath("following::table[1]")
        if following:
            idx = table_index_by_id.get(id(following[0]))
            if idx is not None:
                bonus_indices.add(idx)
    return bonus_indices


def score_table(table, title_bonus_indices: set[int], table_idx: int) -> int:
    header = table_header_text(table, max_rows=12)
    full_text = normalize_text(" ".join(table.itertext()))

    score = 0
    name_ok = "name" in header
    principal_ok = "principal" in header or "named executive" in header
    position_ok = "position" in header or "occupation" in header
    if name_ok and principal_ok and position_ok:
        score += 7
    elif name_ok and (principal_ok or position_ok):
        score += 5
    elif name_ok:
        score += 2

    comp_matches = {kw for kw in COMP_KEYWORDS if kw in header}
    score += min(len(comp_matches), 7)

    if MONEY_RE.search(full_text):
        score += 2
    if YEAR_RE.search(header):
        score += 1

    rows = table.xpath(".//tr")
    max_cols = max((len(r.xpath("./td|./th")) for r in rows), default=0)
    if len(rows) >= 3:
        score += 1
    if max_cols >= 4:
        score += 1

    if table.xpath("./caption[contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'summary compensation table')]"):
        score += 4

    if table_idx in title_bonus_indices:
        score += 4

    return score


def select_top_tables(tables: Sequence, title_bonus_indices: set[int], limit: int = 2) -> List[Tuple[int, int]]:
    scored: List[Tuple[int, int]] = []
    for idx, table in enumerate(tables):
        scored.append((score_table(table, title_bonus_indices, idx), idx))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return scored[:limit]


def save_tables(html_path: Path, tables: List, scored_indices: List[Tuple[int, int]]) -> int:
    if not scored_indices:
        return 0
    ticker = html_path.parent.parent.name
    date_str = html_path.stem.split("_")[0]
    out_dir = html_path.parent / "extracted"
    out_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    if len(scored_indices) == 1:
        out_name = f"{ticker}_{date_str}_SCT.html"
        out_path = out_dir / out_name
        out_path.write_text(html_stub(LH.tostring(tables[scored_indices[0][1]], encoding="unicode", method="html")), encoding="utf-8")
        saved = 1
    else:
        for rank, (_, idx) in enumerate(scored_indices, start=1):
            out_name = f"{ticker}_{date_str}_SCT_table{rank}.html"
            out_path = out_dir / out_name
            out_path.write_text(html_stub(LH.tostring(tables[idx], encoding="unicode", method="html")), encoding="utf-8")
            saved += 1
    return saved


def process_file(html_path: Path, top_k: int) -> int:
    try:
        html_bytes = html_path.read_bytes()
    except Exception:
        return 0
    try:
        tree = LH.fromstring(html_bytes)
    except Exception:
        return 0
    tables = parse_tables(tree)
    if not tables:
        return 0
    title_bonus_indices = build_title_bonus_map(tree, tables)
    scored_indices = select_top_tables(tables, title_bonus_indices, limit=top_k)
    return save_tables(html_path, tables, scored_indices)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract SCT candidates by scoring every table in HTML files."
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="Root folder containing ticker directories (default: data)",
    )
    parser.add_argument(
        "--form",
        default="DEF_14A",
        help="Form subfolder name (default: DEF_14A)",
    )
    parser.add_argument(
        "--tickers",
        default="",
        help="Comma-separated tickers to process (default: all)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=2,
        help="Number of top-scoring tables to save per file (default: 2)",
    )
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    total_files = 0
    total_tables_saved = 0
    for html_path in iter_html_files(data_root, tickers, args.form):
        total_files += 1
        total_tables_saved += process_file(html_path, args.top_k)

    print(f"files_scanned={total_files}")
    print(f"tables_saved={total_tables_saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
