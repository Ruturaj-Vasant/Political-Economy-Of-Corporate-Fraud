from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from ..config import load_config
from .csv_to_json import process_csv


def list_extracted_csvs(ticker: str, form: str) -> List[Path]:
    cfg = load_config()
    base = Path(cfg.data_root) / ticker.upper() / form / "extracted"
    if not base.exists():
        return []
    return sorted(base.glob("*_SCT.csv"))


def detect_tickers_with_csvs(form: str = "DEF 14A") -> List[str]:
    """Return tickers that have at least one `*_SCT.csv` under `<data_root>/<TICKER>/<form>/extracted`.

    Sorted alphabetically. Case-insensitive on filesystem, returns upper-cased symbols.
    """
    cfg = load_config()
    base = Path(cfg.data_root)
    if not base.exists():
        return []
    tickers: List[str] = []
    for child in base.iterdir():
        if not child.is_dir() or child.name.startswith('.'):
            continue
        extracted_dir = child / form / "extracted"
        try:
            has = any(extracted_dir.glob("*_SCT.csv"))
        except Exception:
            has = False
        if has:
            tickers.append(child.name.upper())
    return sorted(set(tickers))


def run_for_ticker(ticker: str, form: str = "DEF 14A", model: str = "llama3:8b", limit: Optional[int] = None) -> List[Path]:
    outs: List[Path] = []
    for i, csvp in enumerate(list_extracted_csvs(ticker, form)):
        if limit is not None and i >= limit:
            break
        jp = process_csv(str(csvp), model=model)
        if jp:
            outs.append(Path(jp))
    return outs
