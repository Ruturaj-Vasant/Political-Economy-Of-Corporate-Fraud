"""Helpers to load ticker and PERMNO lists from CSV/TSV or text files.

Behavior
- Accepts CSV/TSV with a header containing 'ticker' or 'permno' (case-insensitive).
- If no header match, uses the first column.
- Also accepts newline-delimited plain text files.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional
import csv


def _open_text(path: Path) -> str:
    return Path(path).expanduser().read_text(encoding="utf-8", errors="ignore")


def _rows_from_csv(text: str) -> Optional[list[list[str]]]:
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(text[:2048])
        reader = csv.reader(text.splitlines(), dialect)
        return [list(r) for r in reader]
    except Exception:
        return None


def _header_index(header: list[str], names: list[str]) -> Optional[int]:
    lower = [h.strip().lower() for h in header]
    for n in names:
        try:
            return lower.index(n.lower())
        except ValueError:
            continue
    return None


def load_tickers_from_file(path: str | Path) -> List[str]:
    """Return a list of uppercased tickers from CSV/TSV or text file.

    Handles these shapes:
    - Single row with many comma-separated tickers
    - Column of tickers (with or without header 'ticker')
    - Generic newline-delimited text
    """
    p = Path(path).expanduser()
    text = _open_text(p)
    rows = _rows_from_csv(text)
    values: List[str] = []
    if rows and rows[0]:
        header = rows[0]
        # Prefer explicit header when present
        idx = _header_index(header, ["ticker"])  # prefer explicit column
        if idx is not None and len(rows) > 1:
            for r in rows[1:]:
                if not r:
                    continue
                v = str(r[idx]).strip()
                if v:
                    values.append(v.upper())
            return values
        # Single row with many values (list of tickers in one row)
        if len(rows) == 1 and len(header) > 1:
            for cell in header:
                v = str(cell).strip()
                if v:
                    values.append(v.upper())
            return values
        # Fallback: take first column from each row (no header)
        for r in rows:
            if not r:
                continue
            v = str(r[0]).strip()
            if v:
                values.append(v.upper())
        return values
    # Fallback: newline delimited
    for line in text.splitlines():
        v = line.strip()
        if v:
            values.append(v.upper())
    return values


def load_permnos_from_file(path: str | Path) -> List[str]:
    """Return a list of PERMNO strings from CSV/TSV or text file.

    Handles these shapes:
    - Single row with many comma-separated PERMNOs
    - Column of PERMNOs (with or without header 'permno')
    - Generic newline-delimited text
    """
    p = Path(path).expanduser()
    text = _open_text(p)
    rows = _rows_from_csv(text)
    values: List[str] = []
    if rows and rows[0]:
        header = rows[0]
        idx = _header_index(header, ["permno", "permnos"])  # allow plural
        if idx is not None and len(rows) > 1:
            for r in rows[1:]:
                if not r:
                    continue
                v = str(r[idx]).strip()
                if v:
                    values.append(v)
            return values
        # Single row with many values
        if len(rows) == 1 and len(header) > 1:
            for cell in header:
                v = str(cell).strip()
                if v:
                    values.append(v)
            return values
        # Fallback: first column
        for r in rows:
            if not r:
                continue
            v = str(r[0]).strip()
            if v:
                values.append(v)
        return values
    # Fallback: newline delimited
    for line in text.splitlines():
        v = line.strip()
        if v:
            values.append(v)
    return values
