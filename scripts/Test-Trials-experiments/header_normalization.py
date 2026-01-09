#!/usr/bin/env python3
"""Header normalization helpers for SCT CSV cleaning (test-only)."""
from __future__ import annotations

import re


def normalize_sct_header(raw: str) -> str:
    """Normalize a raw header to a canonical SCT field name.

    Returns one of:
      - name_and_position, name, position, year, salary, bonus,
        other_annual_comp, stock_awards, option_awards,
        shares_underlying_options, ltip_payouts,
        non_equity_incentive, pension_value,
        all_other_comp, total
    or a cleaned snake_case fallback for unknown headers.
    """
    if raw is None:
        return "unknown"

    s = str(raw).replace("\u00a0", " ")
    # Fix hyphenated line breaks like "Compen- sation".
    s = re.sub(r"(\w)-\s+(\w)", r"\1\2", s)
    s = s.replace("/", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return "unknown"

    # Strip footnote markers and currency symbols for matching.
    s = re.sub(r"\([^)]*\)", "", s)
    s = s.replace("$", "")
    s = re.sub(r"\s+", " ", s).strip()
    sl = s.lower()

    patterns = [
        ("name_and_position", [r"name and principal position"]),
        ("name", [r"\bname\b"]),
        (
            "position",
            [
                r"principal position",
                r"positions? and offices",
                r"principal occupation",
                r"position with",
            ],
        ),
        ("year", [r"\byear\b", r"fiscal year", r"year ended"]),
        ("salary", [r"salary", r"base salary"]),
        ("bonus", [r"bonus", r"cash bonus", r"aip payout", r"annual incentive"]),
        ("other_annual_comp", [r"other annual compensation", r"other annual comp"]),
        (
            "shares_underlying_options",
            [r"shares? underlying options?", r"securities underlying options"],
        ),
        ("stock_awards", [r"stock awards?", r"restricted stock", r"rsu", r"stock unit awards?"]),
        (
            "option_awards",
            [r"option awards?", r"stock options?", r"options? granted", r"sars?"],
        ),
        ("ltip_payouts", [r"ltip payouts?", r"long[- ]term incentive payouts?"]),
        ("non_equity_incentive", [r"non[- ]equity incentive", r"incentive plan compensation"]),
        (
            "pension_value",
            [
                r"pension value",
                r"change in pension",
                r"deferred compensation",
            ],
        ),
        ("all_other_comp", [r"all other compensation", r"other compensation"]),
        ("total", [r"\btotal\b", r"total compensation"]),
    ]

    for canonical, pats in patterns:
        if any(re.search(p, sl) for p in pats):
            return canonical

    # Fallback: normalized snake_case label for traceability.
    fallback = re.sub(r"[^a-z0-9]+", "_", sl).strip("_")
    return fallback or "unknown"


def normalize_sct_text(raw: str) -> str:
    """Normalize any SCT-related text (headers or cells) to a stable key."""
    if raw is None:
        return ""

    s = str(raw).replace("\u00a0", " ")
    s = re.sub(r"(\w)-\s+(\w)", r"\1\2", s)
    s = s.replace("/", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""

    # Remove footnotes and currency markers.
    s = re.sub(r"\([^)]*\)", "", s)
    s = s.replace("$", "")
    s = re.sub(r"\s+", " ", s).strip().lower()

    if "name" in s and "position" in s:
        return "name_position"
    if "fiscal" in s or re.search(r"\byear\b", s):
        return "fiscal_year"
    if "salary" in s:
        return "salary"
    if "bonus" in s:
        return "bonus"
    if "other annual" in s:
        return "other_annual_comp"
    if "stock" in s and "award" in s:
        return "stock_awards"
    if "option" in s:
        return "option_awards"
    if "non-equity" in s or "non equity" in s or "incentive plan" in s:
        return "non_equity_incentive"
    if "pension" in s or "deferred compensation" in s:
        return "pension_value"
    if "all other" in s:
        return "all_other_comp"
    if "total" in s:
        return "total"

    # Fallback: snake_case for unknown text.
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def normalize_dataframe_text(df):
    """Normalize headers + all string cells using normalize_sct_text()."""
    out = df.copy()
    out.columns = [normalize_sct_text(c) for c in out.columns]

    def _norm_cell(x):
        return normalize_sct_text(x) if isinstance(x, str) else x

    return out.apply(lambda col: col.map(_norm_cell))
