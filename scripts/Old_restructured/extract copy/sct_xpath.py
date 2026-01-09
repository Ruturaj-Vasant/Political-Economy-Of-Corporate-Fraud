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
import re


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
    # Apply deterministic normalization to standardize and consolidate columns
    df = normalize_sct_dataframe(df)
    return df


def normalize_sct_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize SCT DataFrame headers and consolidate duplicate logical columns.

    - Detects when the first row contains real headers (generic/Unnamed columns case)
    - Standardizes common SCT column names (salary, bonus, stock_awards, etc.)
    - Consolidates duplicates by preferring series with more numeric content
    - Drops all-empty cols/rows and duplicate rows
    """
    if df.empty:
        return df

    # If the very first row is all-NaN, drop it early (junk row from read_html)
    if len(df) and df.iloc[0].isna().all():
        df = df.iloc[1:].reset_index(drop=True)

    # Determine if current columns look generic (e.g., Unnamed or plain digits)
    orig_columns = list(df.columns)
    cols_as_str = [str(c) for c in orig_columns]
    looks_generic = all(re.match(r"^(Unnamed.*|\d+)$", c) for c in cols_as_str)

    if looks_generic and len(df) > 0:
        # Promote first row to header text; keep mapping by original column labels
        header_row = df.iloc[0].astype(str).str.replace(r"\n|\r", "", regex=True).str.strip()
        header_row.index = pd.Index(orig_columns)
        data_df = df.iloc[1:].reset_index(drop=True)
        header_by_label = header_row
    else:
        # Use existing column labels as header texts
        data_df = df.reset_index(drop=True)
        ser = pd.Series([str(c) for c in data_df.columns], index=data_df.columns)
        header_by_label = ser

    # Drop columns that are entirely NaN and remove fully-empty first-column rows
    data_df = data_df.dropna(axis=1, how="all")
    if data_df.shape[1] == 0:
        return data_df
    # Ensure there's a first column to check for empties
    data_df = data_df[data_df.iloc[:, 0].notna()].reset_index(drop=True)

    # Drop symbol-only columns (e.g., columns with only '$', dashes, punctuation)
    def _is_symbol_only_col(s: pd.Series) -> bool:
        if s is None:
            return False
        vals = s.dropna().astype(str).str.strip().str.replace("\u00a0", "", regex=False)
        if vals.empty:
            return False  # already handled by dropna(all)
        # True if every non-null cell contains no letters or digits
        return bool((~vals.str.contains(r"[A-Za-z0-9]", regex=True)).all())

    keep_cols = []
    for c in list(data_df.columns):
        if _is_symbol_only_col(data_df[c]):
            continue
        keep_cols.append(c)
    if len(keep_cols) != len(data_df.columns):
        data_df = data_df[keep_cols]

    # Mapping for canonical SCT columns (aligned with AI pipeline expectations)
    column_mapping = {
        'salary': ['salary'],
        'bonus': ['bonus'],
        'stock_awards': ['stock awards', 'stock-awards'],
        'option_awards': ['option awards', 'option-awards'],
        'non_equity_incentive_plan': [
            'non-equity incentive plan compensation',
            'non-equity incentive',
            'non equity incentive',
        ],
        'pension_value': ['change in pension', 'pension value', 'deferred compensation compensation'],
        'all_other_compensation': ['all other compensation', 'all other comp'],
        'total': ['total salary and incentive compensation', 'total'],
        'year': ['year', 'fiscal year ended'],
        'name_position': ['name and principal position', 'name & principal position', 'principal position', 'name'],
    }

    def clean_string_for_matching(text: str) -> str:
        t = str(text).lower().strip()
        t = re.sub(r"\s*\([^)]*\)", "", t)  # drop parentheticals
        t = re.sub(r"[^a-z0-9 ]", "", t)      # keep alnum + spaces
        t = re.sub(r"\s+", " ", t).strip()
        return t.replace(" ", "")

    reverse_mapping = {}
    for std, variants in column_mapping.items():
        for v in variants:
            reverse_mapping[clean_string_for_matching(v)] = std

    def normalize_column_name(name: str) -> str:
        key = clean_string_for_matching(name)
        if key in reverse_mapping:
            return reverse_mapping[key]
        # Fallbacks
        if 'salary' in key:
            return 'salary'
        if 'bonus' in key:
            return 'bonus'
        if 'stockaward' in key:
            return 'stock_awards'
        if 'optionaward' in key:
            return 'option_awards'
        if 'incentive' in key:
            return 'non_equity_incentive_plan'
        if 'pension' in key:
            return 'pension_value'
        if 'othercomp' in key or 'allother' in key:
            return 'all_other_compensation'
        if 'total' in key:
            return 'total'
        if 'year' in key:
            return 'year'
        if 'nameprincipalposition' in key or 'principalposition' in key or key == 'name':
            return 'name_position'
        return str(name)

    # Consolidate columns by normalized name; prefer series with more numeric content
    final_cols: dict[str, pd.Series] = {}
    for col_label in list(data_df.columns):
        header_text = header_by_label.get(col_label, str(col_label))
        norm = normalize_column_name(header_text)
        series = data_df[col_label].reset_index(drop=True)

        if norm not in final_cols:
            final_cols[norm] = series
            continue

        def as_numeric(s: pd.Series) -> pd.Series:
            s = s.astype(str).str.replace('$', '', regex=False) \
                         .str.replace(',', '', regex=False) \
                         .str.replace('—', '', regex=False) \
                         .str.replace('(', '-', regex=False) \
                         .str.replace(')', '', regex=False) \
                         .str.strip()
            return pd.to_numeric(s, errors='coerce')

        existing = final_cols[norm]
        num_exist = as_numeric(existing)
        num_new = as_numeric(series)
        c_exist = num_exist.dropna().nunique()
        c_new = num_new.dropna().nunique()

        if c_new > c_exist:
            final_cols[norm] = series
        elif c_new == c_exist and c_new > 0:
            if series.notna().sum() > existing.notna().sum():
                final_cols[norm] = series
        elif existing.isna().all() and series.notna().any():
            final_cols[norm] = series

    cleaned = pd.DataFrame(final_cols)
    # Ensure unique columns and drop duplicate rows
    cleaned = cleaned.loc[:, ~cleaned.columns.duplicated()]
    cleaned.drop_duplicates(inplace=True)
    return cleaned


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
