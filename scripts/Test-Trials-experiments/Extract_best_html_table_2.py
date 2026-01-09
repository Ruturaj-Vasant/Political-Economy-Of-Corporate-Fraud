#!/usr/bin/env python3
"""Pick the best SCT-like table per DEF 14A filing and save it as an HTML stub."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from lxml import html as LH  # type: ignore

# Keywords and penalties
COMP_KEYS: Tuple[str, ...] = (
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
GRANT_KEYS = ("grant", "grant date", "estimated future payouts")

TITLE_XPATH = (
    "//*[contains(translate(normalize-space(string(.)), "
    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
    "'summary compensation table')]"
)


def normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").lower().split()) if text else ""


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


def build_title_bonus_map(tree, tables: Sequence) -> set[int]:
    table_index_by_id = {id(t): idx for idx, t in enumerate(tables)}
    bonus: set[int] = set()
    for node in tree.xpath(TITLE_XPATH):
        # Table containing the title
        for anc in node.iterancestors():
            if anc.tag == "table":
                idx = table_index_by_id.get(id(anc))
                if idx is not None:
                    bonus.add(idx)
                break
        # Next table after the title
        nxt = node.xpath("following::table[1]")
        if nxt:
            idx = table_index_by_id.get(id(nxt[0]))
            if idx is not None:
                bonus.add(idx)
    return bonus


def extract_header_text(table) -> str:
    rows = table.xpath(".//tr")[:12]
    bits: List[str] = []
    for tr in rows:
        for cell in tr.xpath("./td|./th"):
            bits.append(normalize(" ".join(cell.itertext())))
    return " ".join(bits)


def score_table(table, title_bonus: set[int], idx: int) -> int:
    header = extract_header_text(table)
    full_text = normalize(" ".join(table.itertext()))
    score = 0

    # Name / position cues
    name_ok = "name" in header
    principal_ok = "principal" in header or "named executive" in header
    position_ok = "position" in header or "occupation" in header
    if name_ok and principal_ok and position_ok:
        score += 7
    elif name_ok and (principal_ok or position_ok):
        score += 5
    elif name_ok:
        score += 2

    # Compensation keywords
    comp_hits = {k for k in COMP_KEYS if k in header}
    score += min(len(comp_hits), 8)

    # Money / numeric cues
    if re.search(r"\$|\b\d{3,}\b", full_text):
        score += 2

    # Grant / plan penalty
    if any(g in header for g in GRANT_KEYS):
        score -= 4

    # Caption with SCT
    if table.xpath("./caption[contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'summary compensation table')]"):
        score += 4

    # Title proximity bonus
    if idx in title_bonus:
        score += 4

    return score


def parse_tables(tree) -> List:
    return tree.xpath("//table")


def save_best_table(html_path: Path, table_html: str) -> None:
    ticker = html_path.parent.parent.name
    date_str = html_path.stem.split("_")[0]
    out_dir = html_path.parent / "extracted"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_name = f"{ticker}_{date_str}_SCT_best.html"
    out_path = out_dir / out_name
    out_path.write_text(html_stub(table_html), encoding="utf-8")


def process_file(html_path: Path) -> bool:
    try:
        html_bytes = html_path.read_bytes()
    except Exception:
        return False
    try:
        tree = LH.fromstring(html_bytes)
    except Exception:
        return False
    tables = parse_tables(tree)
    if not tables:
        return False
    title_bonus = build_title_bonus_map(tree, tables)
    scored = []
    for idx, table in enumerate(tables):
        scored.append((score_table(table, title_bonus, idx), idx, table))
    scored.sort(key=lambda x: (-x[0], x[1]))
    best = scored[0][2]
    save_best_table(html_path, LH.tostring(best, encoding="unicode", method="html"))
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Select best SCT table per DEF 14A filing and save as HTML stub."
    )
    parser.add_argument("--data-root", default="data", help="Root folder (default: data)")
    parser.add_argument("--form", default="DEF_14A", help="Form subfolder (default: DEF_14A)")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers (default: all)")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    total_files = 0
    saved = 0
    for html_path in iter_html_files(data_root, tickers, args.form):
        total_files += 1
        if process_file(html_path):
            saved += 1

    print(f"files_scanned={total_files}")
    print(f"best_tables_saved={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
