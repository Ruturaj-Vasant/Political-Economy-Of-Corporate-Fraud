"""Summary Compensation Table (SCT) extraction via XPath (HTML stub).

This keeps the XPath-first strategy from the legacy extractor but now
returns a single HTML stub of the best candidate table instead of
DataFrames. It is used as the fallback/alternative when the scoring
extractor is not selected.
"""
from __future__ import annotations

from typing import List
from pathlib import Path

from lxml import html as LH  # type: ignore


def html_stub(table_html: str) -> str:
    return (
        "<!doctype html>\n"
        "<html><head><meta charset=\"utf-8\"></head><body>\n"
        f"{table_html}\n"
        "</body></html>\n"
    )


_XPATH_SCT_HEADER_TR = r"""
//tr[
  .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
  and (
    (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
    or (
      following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
  )
]
"""


def _unique_tables(tr_nodes) -> List:
    seen = set()
    out = []
    for tr in tr_nodes:
        table = tr.getparent()
        while table is not None and getattr(table, "tag", None) != "table":
            table = table.getparent()
        if table is None:
            continue
        key = LH.tostring(table)[:200]  # cheap content hash key
        if key in seen:
            continue
        seen.add(key)
        out.append(table)
    return out


def extract_best_sct_html_from_bytes(html_bytes: bytes) -> str | None:
    """Return the best SCT table as an HTML stub, or None if not found."""
    try:
        tree = LH.fromstring(html_bytes)
    except Exception:
        return None
    tr_nodes = tree.xpath(_XPATH_SCT_HEADER_TR)
    if not tr_nodes:
        return None
    tables = _unique_tables(tr_nodes)
    if not tables:
        return None
    # Use first matching table as "best" (legacy behavior)
    best = tables[0]
    best_html = LH.tostring(best, encoding="unicode", method="html")
    return html_stub(best_html)


def extract_best_sct_html_from_file(path: str | Path) -> str | None:
    p = Path(path)
    try:
        html_bytes = p.read_bytes()
    except Exception:
        return None
    return extract_best_sct_html_from_bytes(html_bytes)

