"""Convert Beneficial Ownership (BOT) extracted HTML tables to JSON."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import json
import re

import pandas as pd

from ..config import load_config
from ..downloads.file_naming import normalize_form_for_fs
from .html_to_json import combined_html_df, drop_all_empty, blanks_to_nan, _uniquify_columns

# ---------------------------
# Header normalization (BOT)
# ---------------------------

BOT_KEY_TOKENS: Dict[str, List[str]] = {
    "name": ["name", "beneficial owner", "beneficially owned", "holder"],
    "address": ["address", "address of", "address of beneficial owner"],
    "shares": [
        "amount and nature",
        "amount",
        "shares",
        "share",
        "beneficially owned",
        "number of shares",
        "ownership",
        "outstanding",
    ],
    "percent": ["percent", "%", "percent of class", "% of class", "of class"],
    "class": ["class", "title of class", "stock class", "common stock"],
    "notes": ["footnote", "note", "(c)", "(a)", "(b)", "warrant", "option"],
}

BOT_KIND_TOKENS = {
    "major_holder": ["5%", "5 percent", "greater than 5", "certain beneficial owners", "principal stockholders", "principal shareholders"],
    "management": ["directors", "executive officers", "as a group", "management"],
}


def normalize_bot_header(raw: str) -> str:
    s = str(raw).lower().strip()
    if s in ("", "nan", "none"):
        return ""
    if not s:
        return ""
    for key, toks in BOT_KEY_TOKENS.items():
        if any(tok in s for tok in toks):
            return key
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def normalize_bot_headers_only(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = [normalize_bot_header(c) for c in df2.columns]
    return df2


def detect_bot_header_row(df: pd.DataFrame, max_rows: int = 10) -> pd.DataFrame:
    """Use early rows to set header if we find multiple BOT key tokens."""
    for i in range(min(max_rows, len(df))):
        row = df.iloc[i].astype(str).str.lower()
        joined = " ".join(list(row))
        hits = 0
        for toks in BOT_KEY_TOKENS.values():
            if any(tok in joined for tok in toks):
                hits += 1
        if hits >= 2:
            df2 = df.copy()
            df2.columns = df2.iloc[i].astype(str).tolist()
            df2 = df2.iloc[i + 1 :].reset_index(drop=True)
            return df2
    return df


# ---------------------------
# Row filtering and shaping
# ---------------------------

def filter_bot_rows(df: pd.DataFrame) -> pd.DataFrame:
    # Require at least a name or address and some numeric (shares/percent) info
    if df.empty:
        return df
    cols = set(df.columns)
    name_cols = [c for c in cols if c == "name"]
    address_cols = [c for c in cols if c == "address"]
    shares_cols = [c for c in cols if c == "shares"]
    percent_cols = [c for c in cols if c == "percent"]

    def row_ok(r) -> bool:
        has_name = any(str(r[c]).strip() for c in name_cols) if name_cols else False
        has_addr = any(str(r[c]).strip() for c in address_cols) if address_cols else False
        has_shares = any(str(r[c]).strip() for c in shares_cols) if shares_cols else False
        has_pct = any(str(r[c]).strip() for c in percent_cols) if percent_cols else False
        return (has_name or has_addr) and (has_shares or has_pct)

    mask = df.apply(row_ok, axis=1)
    return df[mask].reset_index(drop=True)


def collapse_empty_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Drop leading/trailing empty columns and empty rows."""
    df2 = df.copy()
    df2 = drop_all_empty(df2)
    return df2


_PERCENT_RE = re.compile(r"%")
_STAR_RE = re.compile(r"\*")
_INT_RE = re.compile(r"^[\d,]+$")
_FLOAT_RE = re.compile(r"^\d{1,3}(?:,\d{3})*(?:\.\d+)?$")
_CLASS_NOTE_RE = re.compile(r"^\([a-z]\)$")
_PCT_SPLIT_RE = re.compile(r"^\d{0,3}(?:\.\d+)?$")


def assign_fallback_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Assign headers to blank columns based on content when no key is present."""
    df2 = df.copy()
    headers = list(df2.columns)

    for idx, h in enumerate(headers):
        norm = normalize_bot_header(h)
        if norm != "":
            continue
        series = df2.iloc[:, idx]
        vals = [str(v).strip() for v in series.tolist() if not pd.isna(v) and str(v).strip() != ""]
        if not vals:
            headers[idx] = ""
            continue
        if any(_CLASS_NOTE_RE.match(v) for v in vals):
            headers[idx] = "class"
            continue
        if any(_PERCENT_RE.search(v) or _STAR_RE.search(v) for v in vals):
            headers[idx] = "percent"
            continue
        if any(_FLOAT_RE.match(v) for v in vals):
            headers[idx] = "percent"
            continue
        if any(_INT_RE.match(v) for v in vals):
            headers[idx] = "shares"
            continue
        headers[idx] = "notes"

    df2.columns = headers
    return df2


def _mostly(series, predicate) -> bool:
    vals = [str(v).strip() for v in series.tolist() if not pd.isna(v) and str(v).strip() != ""]
    if not vals:
        return False
    hits = sum(1 for v in vals if predicate(v))
    return hits / len(vals) >= 0.6


def dedupe_bot_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reduce duplicate name columns by mapping numeric duplicates to shares/percent."""
    df2 = df.copy()
    new_cols: List[str] = []
    name_seen = 0
    for idx, c in enumerate(df2.columns):
        if c == "name":
            if name_seen == 0:
                new_cols.append("name")
            else:
                series = df2.iloc[:, idx]
                if _mostly(series, lambda v: _PERCENT_RE.search(v) or _STAR_RE.search(v) or _PCT_SPLIT_RE.match(v) or _FLOAT_RE.match(v)):
                    new_cols.append("percent")
                elif _mostly(series, lambda v: _INT_RE.match(v) or _FLOAT_RE.match(v)):
                    new_cols.append("shares")
                else:
                    new_cols.append("name_extra")
            name_seen += 1
        else:
            new_cols.append(c)
    df2.columns = new_cols
    return df2


def classify_table_kind(df: pd.DataFrame) -> str:
    """Classify BOT table as major_holder vs management using header/body text."""
    text = " ".join(df.astype(str).fillna("").agg(" ".join, axis=1).tolist()).lower()
    major_hits = sum(1 for tok in BOT_KIND_TOKENS["major_holder"] if tok in text)
    mgmt_hits = sum(1 for tok in BOT_KIND_TOKENS["management"] if tok in text)
    if major_hits >= mgmt_hits and major_hits > 0:
        return "major_holder"
    if mgmt_hits > 0:
        return "management"
    return "unknown"


# ---------------------------
# JSON assembly
# ---------------------------

def _parse_bot_filename(html_path: Path) -> tuple[str, Optional[str]]:
    base = html_path.stem  # e.g., TICKER_YYYY-MM-DD_BOT_1
    m = re.match(r"([A-Z0-9\.-]+)_(\d{4}-\d{2}-\d{2})_BOT", base, flags=re.I)
    if not m:
        return base.split("_")[0].upper(), None
    ticker = m.group(1).upper()
    report_date = m.group(2)
    return ticker, report_date


def dataframe_to_bot_json(df: pd.DataFrame, ticker: str, report_date: str, table_kind: str) -> dict:
    df_clean = df.copy()
    df_clean.columns = _uniquify_columns(list(df_clean.columns))
    holders: List[dict] = []
    for _, row in df_clean.iterrows():
        entry: dict = {}
        for col in df_clean.columns:
            val = row[col]
            if pd.isna(val):
                continue
            if isinstance(val, (pd.Timestamp,)):
                val = val.isoformat()
            entry[col] = val
        if entry:
            holders.append(entry)
    return {
        "company": {
            "ticker": ticker,
            "beneficial_ownership": [
                {
                    "report_date": report_date,
                    "table_kind": table_kind,
                    "holders": holders,
                }
            ],
        }
    }


def process_bot_html_to_json(html_path: str | Path, form: str = "DEF 14A") -> Optional[Path]:
    cfg = load_config()
    data_root = Path(cfg.data_root)
    p = Path(html_path)
    ticker, report_date = _parse_bot_filename(p)
    if not report_date:
        return None

    df = combined_html_df(p)
    if df.empty:
        return None
    df = detect_bot_header_row(df)
    df = collapse_empty_headers(df)
    df = assign_fallback_headers(df)
    df = normalize_bot_headers_only(df)
    df = dedupe_bot_columns(df)
    df = drop_all_empty(df)
    df = blanks_to_nan(df)
    df = drop_all_empty(df)
    df = filter_bot_rows(df)
    if df.empty:
        return None

    table_kind = classify_table_kind(df)
    payload = dataframe_to_bot_json(df, ticker=ticker, report_date=report_date, table_kind=table_kind)
    holders = (
        payload.get("company", {})
        .get("beneficial_ownership", [{}])[0]
        .get("holders", [])
    )
    if not holders:
        return None

    form_fs = normalize_form_for_fs(form)
    out_dir = data_root / ticker / form_fs / "json"
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = "BOT" if table_kind == "unknown" else f"BOT_{table_kind}"
    out_path = out_dir / f"{ticker}_{report_date}_{suffix}.json"
    out_path.write_text(json.dumps(payload, indent=2))
    return out_path
