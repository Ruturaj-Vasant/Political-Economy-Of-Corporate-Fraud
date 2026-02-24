"""Beneficial Ownership table extraction via XPath (all matches).

Returns HTML stubs that preserve original styling (head includes + base href).
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import re

from lxml import html as LH  # type: ignore

# XPath for Beneficial Ownership tables (provided pattern)
XPATH_BO_TABLES = """
//table[
  .//tr[
    (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'beneficial')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'owner')]
    )
    or
    (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'address')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'beneficial')]
    )
  ]
]
""".strip()


def _collect_head_includes(tree) -> str:
    nodes = tree.xpath(
        "//head/style | //head/link[translate(@rel, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='stylesheet']"
    )
    fragments: List[str] = []
    for n in nodes:
        fragments.append(LH.tostring(n, encoding="unicode", method="html"))
    return "\n".join(fragments)


def _html_stub(table_html: str, *, base_href: Optional[str], head_includes: str) -> str:
    head_parts = ['<meta charset="utf-8">']
    if base_href:
        head_parts.append(f'<base href="{base_href}">')
    if head_includes:
        head_parts.append(head_includes.strip())
    head = "\n".join(head_parts)
    return (
        "<!doctype html>\n"
        "<html><head>\n"
        f"{head}\n"
        "</head><body>\n"
        f"{table_html}\n"
        "</body></html>\n"
    )


def _table_shape_ok(tbl) -> bool:
    """Require >=2 rows with >=2 non-empty cells to avoid gutter tables."""
    rows = tbl.xpath(".//tr")
    if len(rows) < 2:
        return False
    meaningful_rows = 0
    for r in rows:
        cells = r.xpath("./td|./th")
        texts = [("".join(c.itertext()) or "").strip() for c in cells]
        non_empty = [t for t in texts if t]
        if len(non_empty) >= 2:
            meaningful_rows += 1
    return meaningful_rows >= 2


def extract_bo_tables_from_bytes(html_bytes: bytes, *, base_href: Optional[str]) -> List[str]:
    try:
        tree = LH.fromstring(html_bytes)
    except Exception:
        return []
    tables = tree.xpath(XPATH_BO_TABLES)
    if not tables:
        return []
    head_includes = _collect_head_includes(tree)
    stubs: List[str] = []
    for tbl in tables:
        if not _table_shape_ok(tbl):
            continue
        tbl_html = LH.tostring(tbl, encoding="unicode", method="html", with_tail=False)
        stub = _html_stub(tbl_html, base_href=base_href, head_includes=head_includes)
        stubs.append(stub)
    return stubs


def extract_bo_tables_from_file(path: str | Path, max_tables: Optional[int] = None) -> List[str]:
    p = Path(path).expanduser().resolve()
    try:
        html_bytes = p.read_bytes()
    except Exception:
        return []
    base_href = p.parent.as_uri()
    stubs = extract_bo_tables_from_bytes(html_bytes, base_href=base_href)
    if max_tables and max_tables > 0:
        return stubs[:max_tables]
    return stubs
