"""Utilities to resolve identifiers to tickers from the reference JSON.

Default source: restructured_code/json/sec_company_tickers.json
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional
import json


DEFAULT_JSON = Path("restructured_code/json/sec_company_tickers.json")


def _load_records(json_path: Path | str = DEFAULT_JSON) -> List[dict]:
    p = Path(json_path)
    data = json.loads(p.read_text())
    if isinstance(data, dict):
        return [v for v in data.values() if isinstance(v, dict)]
    return [r for r in data if isinstance(r, dict)]


def ticker_by_permno(permno: int | str, json_path: Path | str = DEFAULT_JSON) -> Optional[str]:
    target = int(str(permno))
    for rec in _load_records(json_path):
        if "permno" in rec:
            try:
                if int(rec["permno"]) == target:
                    t = str(rec.get("ticker", "")).strip().upper()
                    return t or None
            except Exception:
                continue
    return None


def tickers_by_permnos(permnos: Iterable[int | str], json_path: Path | str = DEFAULT_JSON) -> List[str]:
    want = {int(str(p)) for p in permnos}
    out: List[str] = []
    seen: set[str] = set()
    # Keep order by permnos given
    for p in permnos:
        t = ticker_by_permno(p, json_path=json_path)
        if t and t not in seen:
            out.append(t)
            seen.add(t)
    return out

