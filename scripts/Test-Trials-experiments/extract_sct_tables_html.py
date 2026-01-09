#!/usr/bin/env python3
"""Extract candidate SCT tables via XPath and save as HTML stubs."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Optional

from lxml import html as LH  # type: ignore


_XPATH_SCT_HEADER_TR = r"""
//tr[
  .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
  and (
    (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
    or (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
    or (
      following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
    or (
      following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
  )
]
"""

_TITLE_XPATH = (
    "//*[contains(translate(normalize-space(string(.)), "
    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
    "'summary compensation table')]"
)

_COMP_KEYWORDS = (
    "salary",
    "bonus",
    "stock",
    "option",
    "non-equity",
    "non equity",
    "incentive",
    "pension",
    "all other compensation",
    "other compensation",
    "total",
    "compensation",
    "awards",
    "cash",
    "year",
)

_NAME_PRINCIPAL_RE = re.compile(
    r"name\s*(and|&|/)\s*principal\s*(position|occupation)",
    re.I,
)
_MONEY_RE = re.compile(r"\$|\b\d{3,}\b")
_YEAR_RE = re.compile(r"\b20\d{2}\b")


def _unique_tables(tr_nodes) -> List:
    seen = set()
    out = []
    for tr in tr_nodes:
        table = tr.getparent()
        while table is not None and getattr(table, "tag", None) != "table":
            table = table.getparent()
        if table is None:
            continue
        key = LH.tostring(table)[:200]
        if key in seen:
            continue
        seen.add(key)
        out.append(table)
    return out


def _unique_table_elements(tables) -> List:
    seen = set()
    out = []
    for table in tables:
        key = LH.tostring(table)[:200]
        if key in seen:
            continue
        seen.add(key)
        out.append(table)
    return out


def _iter_html_files(data_root: Path, tickers: List[str], form: str) -> Iterable[Path]:
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


def _html_stub(table_html: str) -> str:
    return (
        "<!doctype html>\n"
        "<html><head><meta charset=\"utf-8\"></head><body>\n"
        f"{table_html}\n"
        "</body></html>\n"
    )


def _parse_date_from_filename(name: str) -> str:
    stem = name.rsplit(".", 1)[0]
    return stem.split("_")[0] if "_" in stem else stem


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.replace("\xa0", " ").lower().split())


def _extract_table_rows(table, max_rows: Optional[int] = None) -> List[List[str]]:
    rows = []
    for tr in table.xpath(".//tr"):
        cells = []
        for cell in tr.xpath("./td|./th"):
            cell_text = _normalize_text(" ".join(cell.itertext()))
            if cell_text:
                cells.append(cell_text)
        if cells:
            rows.append(cells)
            if max_rows and len(rows) >= max_rows:
                break
    return rows


def _score_table(table) -> int:
    rows = _extract_table_rows(table, max_rows=25)
    if not rows:
        return 0
    header_text = " ".join(cell for row in rows[:10] for cell in row)
    full_text = " ".join(cell for row in rows for cell in row)

    score = 0
    if _NAME_PRINCIPAL_RE.search(header_text):
        score += 6
    else:
        name_present = "name" in header_text
        principal_present = (
            "principal position" in header_text
            or "principal occupation" in header_text
            or "principal" in header_text
        )
        position_present = "position" in header_text or "occupation" in header_text
        if name_present and principal_present and position_present:
            score += 5
        elif name_present and (principal_present or position_present):
            score += 3
        elif name_present:
            score += 2

    comp_matches = {kw for kw in _COMP_KEYWORDS if kw in header_text}
    score += min(len(comp_matches), 6)

    if _MONEY_RE.search(full_text):
        score += 2
    if _YEAR_RE.search(header_text):
        score += 1
    if len(rows) >= 3:
        score += 1
    max_cols = max((len(r) for r in rows), default=0)
    if max_cols >= 4:
        score += 1
    return score


def _find_title_tables(tree) -> List:
    tables = []
    for node in tree.xpath(_TITLE_XPATH):
        table = node
        while table is not None and getattr(table, "tag", None) != "table":
            table = table.getparent()
        if table is not None:
            tables.append(table)

        following = node.xpath("following::*")
        table_count = 0
        for elem in following[:25]:
            if getattr(elem, "tag", None) == "table":
                tables.append(elem)
                table_count += 1
                if table_count >= 3:
                    break
        if len(tables) >= 8:
            break
    return _unique_table_elements(tables)


def _select_top_tables(tables, min_score: int, limit: int) -> List:
    scored = []
    for idx, table in enumerate(tables):
        score = _score_table(table)
        scored.append((score, idx, table))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [table for score, _, table in scored if score >= min_score][:limit]


def _fallback_sct_tables(tree) -> List:
    title_tables = _find_title_tables(tree)
    if title_tables:
        selected = _select_top_tables(title_tables, min_score=4, limit=2)
        if selected:
            return selected
    return _select_top_tables(tree.xpath("//table"), min_score=6, limit=2)


def extract_tables_from_file(html_path: Path) -> List[str]:
    try:
        html_bytes = html_path.read_bytes()
    except Exception:
        return []
    try:
        tree = LH.fromstring(html_bytes)
    except Exception:
        return []
    tr_nodes = tree.xpath(_XPATH_SCT_HEADER_TR)
    if tr_nodes:
        tables = _unique_tables(tr_nodes)
    else:
        tables = _fallback_sct_tables(tree)
    if not tables:
        return []
    return [LH.tostring(t, encoding="unicode", method="html") for t in tables]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract SCT tables using XPath and save as HTML stubs."
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
    args = parser.parse_args()

    data_root = Path(args.data_root)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    total_files = 0
    total_tables = 0
    for html_path in _iter_html_files(data_root, tickers, args.form):
        total_files += 1
        tables = extract_tables_from_file(html_path)
        if not tables:
            continue
        ticker = html_path.parent.parent.name
        date_str = _parse_date_from_filename(html_path.name)
        out_dir = html_path.parent / "extracted"
        out_dir.mkdir(parents=True, exist_ok=True)
        if len(tables) == 1:
            out_name = f"{ticker}_{date_str}_SCT.html"
            out_path = out_dir / out_name
            out_path.write_text(_html_stub(tables[0]), encoding="utf-8")
            total_tables += 1
        else:
            for idx, table_html in enumerate(tables, start=1):
                out_name = f"{ticker}_{date_str}_SCT_table{idx}.html"
                out_path = out_dir / out_name
                out_path.write_text(_html_stub(table_html), encoding="utf-8")
                total_tables += 1

    print(f"files_scanned={total_files}")
    print(f"tables_saved={total_tables}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
