"""Summary Compensation Table (SCT) extraction via XPath (core logic).

This module ports the XPath-first strategy from your legacy
scripts/SEC_Documents/Compiling_functions.py with small, well-documented
helpers and safe defaults.

Main entry points:
- extract_sct_tables_from_bytes(html_bytes) -> List[pd.DataFrame]
- extract_sct_tables_from_file(path) -> List[pd.DataFrame]

Notes
- We return a list of candidate tables; caller decides how to persist.
- Cleaning mirrors the legacy behavior: flatten headers, strip strings,
  drop empty columns, dedupe columns.
"""
from __future__ import annotations

from typing import List
from pathlib import Path

import pandas as pd
from lxml import html as LH  # type: ignore


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(x) for x in col if "Unnamed" not in str(x)]).strip()
            for col in df.columns.values
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def _strip_cells(df: pd.DataFrame) -> pd.DataFrame:
    # map over all cells
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)


def process_extracted_table(table_el) -> pd.DataFrame:
    """Convert an lxml <table> element into a cleaned DataFrame."""
    df_list = pd.read_html(LH.tostring(table_el))
    if not df_list:
        return pd.DataFrame()
    df = df_list[0]
    df = _flatten_columns(df)
    df = _strip_cells(df)
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    # Remove duplicate column names (keep first)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


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


def extract_sct_tables_from_bytes(html_bytes: bytes) -> List[pd.DataFrame]:
    """Return candidate SCT tables as cleaned DataFrames (may be empty list)."""
    try:
        tree = LH.fromstring(html_bytes)
    except Exception:
        return []
    tr_nodes = tree.xpath(_XPATH_SCT_HEADER_TR)
    if not tr_nodes:
        return []
    tables = _unique_tables(tr_nodes)
    dfs: List[pd.DataFrame] = []
    for t in tables:
        try:
            df = process_extracted_table(t)
            if not df.empty:
                dfs.append(df)
        except Exception:
            continue
    return dfs


def extract_sct_tables_from_file(path: str | Path) -> List[pd.DataFrame]:
    p = Path(path)
    try:
        html_bytes = p.read_bytes()
    except Exception:
        return []
    return extract_sct_tables_from_bytes(html_bytes)

