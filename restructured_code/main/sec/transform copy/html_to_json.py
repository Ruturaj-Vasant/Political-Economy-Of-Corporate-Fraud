"""Clean SCT HTML tables and convert to JSON.

Functions are copied in order from the notebook `Final_copy_of_Data_cleaning.ipynb`
up to the propagate-fields step, then assembled into a pipeline that emits JSON.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import json
import re

import numpy as np
import pandas as pd
import warnings

from ..config import load_config
from ..downloads.file_naming import normalize_form_for_fs


# ---------------------------
# Reading / combining tables
# ---------------------------
def combined_html_df(htmlfile: str | Path) -> pd.DataFrame:
    # extract tables
    try:
        tables = pd.read_html(htmlfile)
    except ValueError:
        return pd.DataFrame()
    if not tables:
        return pd.DataFrame()
    combined = pd.concat(tables, ignore_index=True)
    return combined


# ---------------------------
# Basic text cleaning
# ---------------------------
def clean_text_basic(raw: str) -> str:
    if raw is None:
        return ""

    s = str(raw)
    s = s.replace("\u00a0", " ")                     # non-breaking space
    s = re.sub(r"(\w)-\s+(\w)", r"\1\2", s)         # fix hyphen splits
    s = s.replace("/", " ")                         # slash spacing
    s = re.sub(r"\s+", " ", s).strip()              # collapse whitespace
    s = re.sub(r"\s*\([^)]*\)", "", s)              # footnotes
    s = s.replace("$", "")
    s = s.replace(",", "")
    s = re.sub(r"\s+", " ", s).strip().lower()

    if not s:
        return ""

    return s


def clean_basic_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    for col in df2.columns:
        df2[col] = df2[col].apply(
            lambda x: clean_text_basic(x) if isinstance(x, str) else x
        )
    return df2


# ---------------------------
# Dedup helpers
# ---------------------------
def drop_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()


def drop_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.T.drop_duplicates().T


# ---------------------------
# Detect header row
# ---------------------------
CANON_ORDER = [
    'executive_name','position','year',
    'salary','bonus','stock_awards','option_awards',
    'non_equity_incentive','pension_value','all_other_comp','total',
]
PAY_COLS = [
    "salary",
    "bonus",
    "stock_awards",
    "option_awards",
    "non_equity_incentive",
    "pension_value",
    "all_other_comp",
    "total",
]
KEY_TOKENS: Dict[str, List[str]] = {
    'salary': ['salary'],
    'bonus': ['bonus'],
    'stock_awards': ['stock awards','stock-awards'],
    'option_awards': ['option awards','option-awards'],
    'non_equity_incentive': ['non-equity incentive','non equity incentive'],
    'pension_value': ['change in pension','pension value','deferred compensation earnings'],
    'all_other_comp': ['all other compensation'],
    'total': ['total'],
    'year': ['year','fiscal year'],
    'name_position': ['name and principal position','name & principal position','principal position','name'],
}
PLACEHOLDER_HEADERS = {'','$','—','–','-'}


def detect_header_row(df: pd.DataFrame, max_rows: int = 15) -> pd.DataFrame:
    for i in range(min(max_rows, len(df))):
        row_low = df.iloc[i].astype(str).str.lower()
        joined = ' '.join(list(row_low))
        hits = 0
        for toks in KEY_TOKENS.values():
            if any(tok in joined for tok in toks):
                hits += 1
        if hits >= 2:
            df2 = df.copy()
            df2.columns = df2.iloc[i].astype(str).tolist()
            df2 = df2.iloc[i+1:].reset_index(drop=True)
            return df2
    return df


# ---------------------------
# Flatten columns
# ---------------------------
def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(x) for x in col if "Unnamed" not in str(x)]).strip()
            for col in df.columns.values
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


# ---------------------------
# Normalize headers (semantic)
# ---------------------------
def normalize_semantic_header(raw: str) -> str:
    if raw is None:
        return ""

    s = str(raw).lower().strip()

    # semantic mapping (SCT-focused)
    if "name" in s and "position" in s:
        return "name_position"
    if "fiscal" in s or "year" in s:
        return "year"
    if "salary" in s:
        return "salary"
    if "bonus" in s:
        return "bonus"
    if "other annual" in s:
        return "other_annual_comp"
    if "stock" in s and "award" in s:
        return "stock_awards"
    if ("option" in s or "options" in s) and "award" in s:
        return "option_awards"
    if "incentive" in s:
        return "non_equity_incentive"
    if "pension" in s or "deferred compensation" in s:
        return "pension_value"
    if "all other" in s:
        return "all_other_comp"
    if "total" in s:
        return "total"

    # fallback snake_case
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def normalize_headers_only(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = [normalize_semantic_header(c) for c in df2.columns]
    return df2


# def blanks_to_nan(df: pd.DataFrame) -> pd.DataFrame:
#     """Replace empty/whitespace-only strings with NaN; keep dashes as-is."""
#     return df.replace(r"^\s*$", pd.NA, regex=True)
def blanks_to_nan(df: pd.DataFrame) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        return (
            df.replace(r"^\s*$", pd.NA, regex=True)  # empty/whitespace strings
              .replace({"—": pd.NA, "–": pd.NA, "-": pd.NA})  # common dash placeholders
              .infer_objects(copy=False)
        )


# ---------------------------
# Independent drop NA
# ---------------------------
def drop_all_empty(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(axis=0, how='all')
    df = df.dropna(axis=1, how='all')
    return df


# ---------------------------
# Trim column (keep rows by year cell)
# ---------------------------
YEAR_RE = re.compile(r"(19|20)\d{2}")


def keep_row_by_year_cell(x) -> bool:
    # Keep title/blank rows
    if pd.isna(x):
        return True
    s = str(x).strip()
    if s == "":
        return True
    # Keep if it contains at least one 4-digit year anywhere (handles "2006 2005" or "2006\n2005")
    return bool(YEAR_RE.search(s))


def merge_header_rows_until_first_year(df: pd.DataFrame, year_regex: re.Pattern = YEAR_RE) -> pd.DataFrame:
    """Merge multi-row headers up to the first row containing a year anywhere."""
    if df.empty:
        return df
    try:
        year_mask = df.applymap(lambda v: bool(year_regex.search(str(v))) if not pd.isna(v) else False)
    except Exception:
        return df
    any_year = year_mask.any(axis=1)
    if not any_year.any():
        return df
    first_idx = any_year[any_year].index[0]
    if first_idx == 0:
        return df  # header already on first row

    header_rows = df.iloc[:first_idx]
    body = df.iloc[first_idx:].copy()

    new_cols: List[str] = []
    for col_idx in range(df.shape[1]):
        parts: List[str] = []
        try:
            col_vals = header_rows.iloc[:, col_idx].tolist()
        except Exception:
            col_vals = []
        for v in col_vals:
            if pd.isna(v):
                continue
            s = str(v).strip()
            if not s:
                continue
            if s.lower().startswith("unnamed"):
                continue
            parts.append(s)
        if parts:
            new_cols.append(" ".join(parts))
        else:
            col_name = df.columns[col_idx]
            if isinstance(col_name, tuple):
                col_name = " ".join([str(x) for x in col_name if x and not str(x).startswith("Unnamed")]).strip()
            new_cols.append(str(col_name))

    body.columns = new_cols
    return body.reset_index(drop=True)


def _coalesce_year_column(df: pd.DataFrame, year_col: str = "year") -> Optional[pd.Series]:
    """Return a single year Series even if multiple duplicate year columns exist."""
    year_cols = [c for c in df.columns if c == year_col]
    if not year_cols:
        return None
    if len(year_cols) == 1:
        return df[year_col]
    year_df = df[year_cols]
    # take first non-empty per row
    series = year_df.apply(
        lambda r: next((v for v in r if pd.notna(v) and str(v).strip() != ""), np.nan),
        axis=1,
    )
    return series


def filter_rows_by_year_presence(df: pd.DataFrame, year_col: str = "year") -> pd.DataFrame:
    """Apply keep_row_by_year_cell across the year column, preserving header/title rows."""
    series = _coalesce_year_column(df, year_col=year_col)
    if series is None:
        return df
    # ensure a single year column going forward
    df2 = df.copy()
    year_cols = [c for c in df2.columns if c == year_col]
    if len(year_cols) > 1:
        df2 = df2.drop(columns=year_cols)
        df2[year_col] = series
    mask = series.apply(keep_row_by_year_cell)
    return df2[mask].reset_index(drop=True)


# ---------------------------
# Pre-data junk rows
# ---------------------------
HEADER_JUNK_RE = re.compile(
    r"(name|principal|position|year|fiscal|salary|bonus|stock|option|incentive|pension|all other|total)",
    re.IGNORECASE,
)


def drop_pre_data_junk_rows(df: pd.DataFrame, year_col: str = "year") -> pd.DataFrame:
    df2 = df.copy()

    if not year_col or year_col not in df2.columns:
        return df2

    # Find first row that contains a year anywhere (handles "2006 2005" too)
    year_has_year = df2[year_col].apply(lambda x: bool(YEAR_RE.search(str(x))) if not pd.isna(x) else False)
    if not year_has_year.any():
        return df2

    first_data_idx = year_has_year[year_has_year].index.min()

    # Only inspect rows from top to first_data_idx-1
    top = df2.loc[:first_data_idx - 1].copy()

    def row_is_junk(r) -> bool:
        joined = " ".join("" if pd.isna(v) else str(v) for v in r.values)

        # If it contains header-like tokens, it's likely a leftover header row
        if HEADER_JUNK_RE.search(joined):
            return True

        # Or if it contains no digits at all, it’s usually just structural/header fluff
        if not re.search(r"\d", joined):
            return True

        return False

    junk_mask = top.apply(row_is_junk, axis=1)

    # Keep only non-junk rows in the top section, then append from first_data_idx onward
    cleaned_top = top.loc[~junk_mask]
    cleaned = pd.concat([cleaned_top, df2.loc[first_data_idx:]], axis=0).reset_index(drop=True)

    return cleaned


# ---------------------------
# Expand stacked rows if needed
# ---------------------------
SCT_NUMERIC_COLS = {
    "salary",
    "bonus",
    "option",
    "other_annual_comp",
    "stock_awards",
    "option_awards",
    "non_equity_incentive",
    "pension_value",
    "all_other_comp",
    "total",
}

YEAR_FULL_RE = re.compile(r"^(?:19|20)\d{2}$")
SPLIT_RE = re.compile(r"\s+")
PLACEHOLDERS = {"—", "–", "-", ""}


def _split_tokens_keep_slots(x):
    """Split by whitespace/newlines; keep placeholder tokens as slots."""
    if pd.isna(x):
        return []
    s = str(x).strip()
    if s == "":
        return []
    s = s.replace("–", "—")
    return [t for t in SPLIT_RE.split(s) if t is not None and t != ""]


def _to_num_or_zero(tok):
    """Placeholder -> 0.0, numeric -> float, unparseable -> NaN."""
    if tok is None:
        return 0.0
    s = str(tok).strip()
    if s in PLACEHOLDERS or s == "—":
        return 0.0
    s = s.replace(",", "").replace("$", "")
    s = s.replace("(", "-").replace(")", "")
    s = re.sub(r"[^\d\.\-]", "", s)
    if s in {"", "-", "."}:
        return 0.0
    try:
        return float(s)
    except Exception:
        return np.nan


def _find_driver_year_col(df: pd.DataFrame, year_name: str = "year") -> int | None:
    """
    Find the first 'year' column (by position) that contains a multi-year cell (2+ years).
    Works with duplicate column names; does not collapse columns.
    """
    year_col_idxs = [i for i, c in enumerate(df.columns) if c == year_name]
    if not year_col_idxs:
        return None

    for col_i in year_col_idxs:
        for v in df.iloc[:, col_i].tolist():
            if pd.isna(v):
                continue
            if len(YEAR_RE.findall(str(v))) >= 2:
                return col_i
    return None


def expand_stacked_rows_if_needed(df: pd.DataFrame, year_name: str = "year") -> pd.DataFrame:
    """
    Single entrypoint:
      1) Detect if table needs expansion (multi-year stacked cells).
      2) If not needed, return df unchanged.
      3) If needed, expand rows with stacked years and align numeric cols by slot,
         treating dash/blank placeholders as 0.0.
    Does NOT collapse duplicate columns; uses column positions (iloc).
    """
    driver_year_col = _find_driver_year_col(df, year_name=year_name)
    if driver_year_col is None:
        return df  # no expansion needed

    col_names = list(df.columns)  # keep duplicates and order
    out_rows = []

    for r_i in range(len(df)):
        row_cells = df.iloc[r_i, :].tolist()

        # derive year list from the driver year col
        year_cell = row_cells[driver_year_col]
        driver_years = YEAR_RE.findall("" if pd.isna(year_cell) else str(year_cell))

        # if this row isn't multi-year, keep as-is
        if len(driver_years) < 2:
            out_rows.append(row_cells)
            continue

        n = len(driver_years)

        # pre-tokenize numeric/year columns by column position
        token_lists = [None] * len(col_names)
        for c_i, c_name in enumerate(col_names):
            if c_name == year_name:
                toks = [t for t in _split_tokens_keep_slots(row_cells[c_i]) if YEAR_FULL_RE.match(t)]
                token_lists[c_i] = toks
            elif c_name in SCT_NUMERIC_COLS:
                token_lists[c_i] = _split_tokens_keep_slots(row_cells[c_i])
            else:
                token_lists[c_i] = None  # replicate text cols

        # emit expanded rows
        for k in range(n):
            new_row = []
            for c_i, c_name in enumerate(col_names):
                cell = row_cells[c_i]

                # Year columns: use kth year if available else fallback to driver list
                if c_name == year_name:
                    toks = token_lists[c_i] or []
                    if len(toks) >= n:
                        new_row.append(int(toks[k]))
                    elif len(toks) == 1 and YEAR_FULL_RE.match(toks[0]):
                        new_row.append(int(toks[0]))
                    else:
                        new_row.append(int(driver_years[k]))
                    continue

                # Numeric cols: zip by slot, pad with 0.0
                if c_name in SCT_NUMERIC_COLS:
                    toks = token_lists[c_i] or []
                    if len(toks) >= n:
                        new_row.append(_to_num_or_zero(toks[k]))
                    elif len(toks) == 1:
                        new_row.append(_to_num_or_zero(toks[0]))
                    elif len(toks) == 0:
                        new_row.append(0.0)
                    else:
                        new_row.append(_to_num_or_zero(toks[k]) if k < len(toks) else 0.0)
                    continue

                # Text/other cols: replicate
                new_row.append(cell)

            out_rows.append(new_row)

    return pd.DataFrame(out_rows, columns=col_names).reset_index(drop=True)


# ---------------------------
# Identify executive blocks
# ---------------------------
def identify_executive_blocks(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy().reset_index(drop=True)
    df_copy['exec_block_id'] = -1

    # Convert 'year' to numeric for proper comparison
    df_copy['year_numeric'] = pd.to_numeric(df_copy['year'], errors='coerce')

    current_exec_block_id = 0
    previous_year = None

    for i in range(len(df_copy)):
        current_year = df_copy.iloc[i]['year_numeric']
        name_position = df_copy.iloc[i]['name_position']

        # Condition 1: Increasing or equal year indicates a new block
        if pd.notna(current_year) and previous_year is not None and current_year >= previous_year:
            current_exec_block_id += 1
        # Condition 2 removed: blank years with name_position now continue the current block

        df_copy.loc[i, 'exec_block_id'] = current_exec_block_id

        if pd.notna(current_year):
            previous_year = current_year

    # Fill forward any remaining -1 values (e.g., if first rows had NaN years and no name_position to trigger a block change)
    df_copy['exec_block_id'] = df_copy['exec_block_id'].replace(-1, np.nan).ffill().fillna(0).astype(int)

    # Drop the temporary column
    df_copy = df_copy.drop(columns=['year_numeric'])
    return df_copy


# ---------------------------
# Consolidate name/position
# ---------------------------
def consolidate_name_position(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    # Coalesce duplicate name_position columns if present
    name_cols = [c for c in df_copy.columns if c == "name_position"]
    if len(name_cols) > 1:
        name_df = df_copy[name_cols]
        # take first non-empty per row
        first_non_empty = name_df.apply(lambda r: next((x for x in r if pd.notna(x) and str(x).strip() != ""), ""), axis=1)
        df_copy = df_copy.drop(columns=name_cols)
        df_copy["name_position"] = first_non_empty

    if 'name_position' not in df_copy.columns or 'exec_block_id' not in df_copy.columns:
        return df_copy

    # Group by exec_block_id and consolidate name_position
    def combine_names(series):
        # Filter out NaN, empty strings, and duplicates, then join
        clean_names = series.dropna().astype(str).apply(lambda x: x.strip()).loc[lambda x: x != ''].unique()
        if len(clean_names) > 0:
            return ' | '.join(clean_names)
        return ""

    consolidated_names = df_copy.groupby('exec_block_id')['name_position'].transform(combine_names)
    df_copy['name_position_consolidated'] = consolidated_names

    return df_copy


# ---------------------------
# Split name and position (refined)
# ---------------------------
_base_position_keywords = [
    r"chief\s*executive\s*officer", r"ceo",
    r"chief\s*financial\s*officer", r"cfo",
    r"chief\s*operating\s*officer", r"coo",
    r"president", r"vice\s*president", r"vp",
    r"senior\s*vice\s*president", r"svp",
    r"executive\s*vice\s*president", r"evp",
    r"director", r"chairman", r"chair", r"officer"
]
_additional_position_keywords = [
    r"secretary", r"treasurer", r"principal", r"general\s*counsel",
    r"principal\s*accounting\s*officer", r"pao", r"chief\s*legal\s*officer", r"clo",
    r"vice\s*chairman", r"founder", r"lead\s*director"
]
POSITION_KEYWORDS = list(dict.fromkeys(_base_position_keywords + _additional_position_keywords))
POSITION_REGEX = re.compile(r'\b(?:' + '|'.join(POSITION_KEYWORDS) + r')\b', re.IGNORECASE)

NAME_PATTERNS = [
    r"((?:[a-z]\.?\s*){1,2}\s*[a-z]+(?:\s+(?:jr\.?|sr\.?|iii|iv))?)",
    r"([a-z]+(?:\s+[a-z]+){1,2}(?:\s+(?:jr\.?|sr\.?|iii|iv))?)",
    r"([a-z]+,\s*[a-z]\.?)"
]


def _looks_like_name(s: str) -> bool:
    """
    Refined helper function to robustly identify name-like strings while distinguishing them from position titles.
    Assumes input `s` is already lowercased due to `clean_text_basic` processing.
    """
    s_lower = s.strip()
    if not s_lower:
        return False

    words = re.findall(r"[a-z]+", s_lower)
    has_name_indicators = bool(re.search(r"\b[a-z]\.?\b|,|jr\.?|sr\.?|iii|iv", s_lower))
    has_position_keywords = bool(POSITION_REGEX.search(s_lower))
    is_full_position_match = bool(POSITION_REGEX.fullmatch(s_lower))

    # Case 1: Contains clear name indicators (initials, commas, suffixes)
    if has_name_indicators:
        if not is_full_position_match:
            return True

    # Case 2: Multi-word string
    if len(words) >= 2:
        if not has_position_keywords:
            return True
        if has_position_keywords and not is_full_position_match:
            return True

    # Case 3: Check against specific NAME_PATTERNS
    for pattern in NAME_PATTERNS:
        if re.search(pattern, s_lower, re.IGNORECASE):
            return True

    if len(words) == 1 and is_full_position_match:
        return False

    return False


def split_name_and_position(consolidated_text: str) -> tuple[str, str]:
    if not isinstance(consolidated_text, str) or not consolidated_text.strip():
        return "", ""

    text = consolidated_text.strip()
    executive_name = ""
    position = ""

    # Strategy 1: Split by ' | ' delimiter
    parts = [p.strip() for p in text.split(' | ') if p.strip()]

    if parts:
        for i, part in enumerate(parts):
            if _looks_like_name(part):
                executive_name = part
                remaining_parts = [p for j, p in enumerate(parts) if j != i]
                position = " ".join(remaining_parts).strip()
                return executive_name, position

    # Strategy 2: keyword split
    pos_match = POSITION_REGEX.search(text)
    if pos_match:
        pre_position_text = text[:pos_match.start()].strip()
        matched_position_part = pos_match.group(0).strip()
        post_position_text = text[pos_match.end():].strip()

        if _looks_like_name(pre_position_text):
            executive_name = pre_position_text
            position = (matched_position_part + " " + post_position_text).strip()
        else:
            executive_name = ""
            position = (pre_position_text + " " + matched_position_part + " " + post_position_text).strip()

        if position.lower().startswith(("and ", "of ", "at ", "for ")):
            position = position[position.find(" ") + 1:].strip()
        if position.startswith(':'):
            position = position[1:].strip()

        return executive_name, position

    # Strategy 3: comma split
    if "," in text:
        name_part, pos_part = text.split(",", 1)
        name_part = name_part.strip()
        pos_part = pos_part.strip()

        if _looks_like_name(name_part):
            return name_part, pos_part
        elif _looks_like_name(pos_part):
            return pos_part, name_part

    if _looks_like_name(text):
        return text, ""
    else:
        return "", text


# ---------------------------
# Propagate fields to rows
# ---------------------------
def propagate_fields(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    if 'executive_name' in df2.columns and 'position' in df2.columns and 'exec_block_id' in df2.columns:
        df2['executive_name'] = df2.groupby('exec_block_id')['executive_name'].transform(lambda x: x.ffill().bfill())
        df2['position'] = df2.groupby('exec_block_id')['position'].transform(lambda x: x.ffill().bfill())
    return df2


# ---------------------------
# Pipeline assembly
# ---------------------------
def clean_sct_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Follow notebook sequence as-is
    df = clean_basic_dataframe(df)
    df = drop_duplicate_rows(df)
    df = drop_duplicate_columns(df)
    # df = merge_header_rows_until_first_year(df, year_regex=YEAR_RE)
    
    df = detect_header_row(df)
    df = _flatten_columns(df)
    df = normalize_headers_only(df)
    df = drop_all_empty(df)
    df = filter_rows_by_year_presence(df, year_col="year")
    df = drop_pre_data_junk_rows(df, year_col="year" if "year" in df.columns else None)
    df = expand_stacked_rows_if_needed(df, year_name="year" if "year" in df.columns else "year")
    df = blanks_to_nan(df)
    df = drop_all_empty(df)
    # print(f"Expanded\n{df.head()}")
    df = identify_executive_blocks(df) if "year" in df.columns and "name_position" in df.columns else df
    df = consolidate_name_position(df)
    if 'name_position_consolidated' in df.columns:
        df[['executive_name', 'position']] = df['name_position_consolidated'].apply(lambda x: pd.Series(split_name_and_position(x)))
    df = propagate_fields(df)
    return df


# ---------------------------
# JSON conversion
# ---------------------------
def _parse_ticker_and_date(html_path: Path) -> tuple[str, Optional[str], Optional[str]]:
    base = html_path.stem  # e.g., TICKER_YYYY-MM-DD_SCT
    m = re.match(r"([A-Z0-9\.-]+)_(\d{4})-(\d{2})-(\d{2})_SCT", base, flags=re.I)
    if not m:
        return base.split("_")[0].upper(), None, None
    ticker = m.group(1).upper()
    report_date = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
    report_year = m.group(2)
    return ticker, report_date, report_year


def _uniquify_columns(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for c in cols:
        cnt = seen.get(c, 0)
        if cnt == 0:
            out.append(c)
        else:
            out.append(f"{c}_{cnt+1}")
        seen[c] = cnt + 1
    return out


def _base_name(col: str) -> str:
    """Strip trailing _<number> to get base column name."""
    m = re.match(r"^(.*?)(?:_(\d+))?$", col)
    return m.group(1) if m else col


def _merge_values(existing, new):
    """Merge duplicate column values using best-effort coalescing."""
    def _is_missing(v):
        if v is None:
            return True
        if isinstance(v, float) and pd.isna(v):
            return True
        if isinstance(v, (int, float, np.integer, np.floating)):
            try:
                if float(v) == 0.0:
                    return True
            except Exception:
                pass
        if isinstance(v, str):
            if v.strip() == "":
                return True
            try:
                if float(v.strip()) == 0.0:
                    return True
            except Exception:
                pass
        return False

    def _to_num(v):
        try:
            return float(v)
        except Exception:
            return None

    if _is_missing(existing):
        return new, False  # replace missing
    if _is_missing(new):
        return existing, False  # keep existing

    # numeric comparison if possible
    ex_num = _to_num(existing)
    new_num = _to_num(new)
    if ex_num is not None and new_num is not None:
        if ex_num == new_num:
            return existing, False  # identical numeric
        else:
            return (existing, new), True  # conflict

    # string comparison
    if isinstance(existing, str) and isinstance(new, str):
        if existing == new:
            return existing, False
        if existing.strip() == "" and new.strip() != "":
            return new, False
        return (existing, new), True  # conflict

    # fallback: keep existing if not clearly missing
    return existing, False


def dataframe_to_json(df: pd.DataFrame, ticker: str, report_date: str, report_year: str) -> dict:
    df_clean = df.copy()
    # Ensure year numeric and drop rows without year
    df_clean['year'] = pd.to_numeric(df_clean.get('year'), errors='coerce')
    df_clean = df_clean.dropna(subset=['year'])
    df_clean['year'] = df_clean['year'].astype(int)

    # Uniquify columns to avoid key collisions
    df_clean.columns = _uniquify_columns(list(df_clean.columns))

    executives: Dict[str, dict] = {}
    for _, row in df_clean.iterrows():
        exec_key = row.get('name_position_consolidated') or row.get('executive_name') or ""
        exec_key = str(exec_key).strip()
        if exec_key == "":
            exec_key = "unknown"
        year_key = str(int(row.get('year')))
        exec_entry = executives.setdefault(exec_key, {})
        year_entry: dict = {}
        seen_bases: Dict[str, str] = {}  # base -> first key used
        for col in df_clean.columns:
            val = row[col]
            if pd.isna(val):
                val = None
            # preserve full column name; merge on exact name
            if col in ("year", "name_position"):
                # ensure native types for JSON
                if isinstance(val, (np.integer,)):
                    year_entry[col] = int(val)
                elif isinstance(val, (np.floating,)):
                    year_entry[col] = float(val)
                else:
                    year_entry[col] = val
                continue
            base = _base_name(col)
            first_key_for_base = seen_bases.get(base)
            if first_key_for_base is None:
                if isinstance(val, (np.integer,)):
                    year_entry[col] = int(val)
                elif isinstance(val, (np.floating,)):
                    year_entry[col] = float(val)
                else:
                    year_entry[col] = val
                seen_bases[base] = col
                continue
            merged, conflict = _merge_values(year_entry[first_key_for_base], val)
            if conflict:
                # keep existing under original name, add a suffix for the new value
                idx = 2
                new_key = f"{base}_{idx}"
                while new_key in year_entry or new_key == first_key_for_base:
                    idx += 1
                    new_key = f"{base}_{idx}"
                new_val = merged[1] if isinstance(merged, tuple) else merged
                if isinstance(new_val, (np.integer,)):
                    new_val = int(new_val)
                elif isinstance(new_val, (np.floating,)):
                    new_val = float(new_val)
                year_entry[new_key] = new_val

                first_val = merged[0] if isinstance(merged, tuple) else year_entry[first_key_for_base]
                if isinstance(first_val, (np.integer,)):
                    first_val = int(first_val)
                elif isinstance(first_val, (np.floating,)):
                    first_val = float(first_val)
                year_entry[first_key_for_base] = first_val
            else:
                if isinstance(merged, (np.integer,)):
                    merged = int(merged)
                elif isinstance(merged, (np.floating,)):
                    merged = float(merged)
                year_entry[first_key_for_base] = merged
        exec_entry[year_key] = year_entry

    return {
        "company": {
            "ticker": ticker,
            "report_year": report_year,
            "summary_compensation_table": [
                {
                    "report_date": report_date,
                    "executives": executives,
                }
            ],
        }
    }


def _save_debug_csv(df: pd.DataFrame, html_path: Path, stage: str) -> None:
    """Write a CSV next to the source HTML for inspection."""
    try:
        out_path = html_path.with_name(f"{html_path.stem}_{stage}.csv")
        df.to_csv(out_path, index=False)
    except Exception:
        pass


def write_combined_json(ticker: str, form: str, data_root: Path) -> Optional[Path]:
    """Aggregate per-report SCT JSONs into a single combined file for the ticker."""
    t = ticker.upper()
    form_fs = normalize_form_for_fs(form)
    json_dir = data_root / t / form_fs / "json"
    if not json_dir.exists():
        return None
    combined: dict = {"ticker": t, "summary_compensation_table": {}}
    report_years: set[str] = set()
    for fp in sorted(json_dir.glob(f"{t}_*_SCT.json")):
        # skip combined outputs
        if fp.name.endswith("_SCT_combined.json"):
            continue
        try:
            data = json.loads(fp.read_text())
        except Exception:
            continue
        comp = data.get("company", {})
        sct_list = comp.get("summary_compensation_table") or []
        if not sct_list:
            continue
        entry = sct_list[0]
        report_date = entry.get("report_date")
        executives = entry.get("executives")
        if report_date and isinstance(executives, dict) and executives:
            combined["summary_compensation_table"][report_date] = {"executives": executives}
        ry = comp.get("report_year")
        if ry is not None:
            report_years.add(str(ry))
    if report_years:
        combined["report_years"] = sorted(report_years)
    if not combined["summary_compensation_table"]:
        return None
    out_path = json_dir / f"{t}_SCT_combined.json"
    out_path.write_text(json.dumps(combined, indent=2))
    return out_path


def process_html_file_to_json(html_path: str | Path, form: str = "DEF 14A") -> Optional[Path]:
    cfg = load_config()
    data_root = Path(cfg.data_root)
    p = Path(html_path)
    ticker, report_date, report_year = _parse_ticker_and_date(p)
    if not report_date:
        return None
    df = combined_html_df(p)
    if df.empty:
        return None
    # Debug view: print full DataFrame (may be large)
    # _save_debug_csv(df, p, "combined")
    df = clean_sct_dataframe(df)
    if df.empty:
        return None
    _save_debug_csv(df, p, "cleaned")
    payload = dataframe_to_json(df, ticker=ticker, report_date=report_date, report_year=report_year or report_date[:4])
    executives = (
        payload.get("company", {})
        .get("summary_compensation_table", [{}])[0]
        .get("executives", {})
    )
    if not executives:
        return None

    form_fs = normalize_form_for_fs(form)
    out_dir = data_root / ticker / form_fs / "json"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ticker}_{report_date}_SCT.json"
    out_path.write_text(json.dumps(payload, indent=2))
    # Update combined SCT JSON for this ticker
    write_combined_json(ticker, form, data_root)
    return out_path
