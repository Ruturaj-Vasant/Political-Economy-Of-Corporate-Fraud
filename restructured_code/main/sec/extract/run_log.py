from __future__ import annotations

from dataclasses import dataclass, asdict, field
from pathlib import Path
import json
import datetime as _dt
from typing import Optional


def _now_iso() -> str:
    now = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def new_run_id() -> str:
    # Compact UTC timestamp suitable for filenames
    return _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


@dataclass
class TickerStats:
    csv_extracted: int = 0
    csv_skipped: int = 0
    html_no_sct: int = 0
    txt_extracted: int = 0
    txt_skipped: int = 0
    txt_no_sct: int = 0
    parquet_written: int = 0
    total_entries: int = 0
    # Per-file date -> status (for compact per-ticker entries)
    file_status_html: dict[str, str] = field(default_factory=dict)
    file_status_txt: dict[str, str] = field(default_factory=dict)


class RunFileLogger:
    """Per-run, per-ticker logger that writes a single JSON file.

    Note: csv_* fields now correspond to HTML outputs (for compatibility).

    File schema:
    {
      "version": 1,
      "run_id": "...",
      "root": "/abs/path/to/data",
      "form": "DEF 14A",
      "started_at": "ISO",
      "updated_at": "ISO",
      "totals": {"total_csv":0,"total_txt":0,"total_parquet":0,"total_entries":0,"tickers_done":0},
      "tickers": {
        "AA": {"csv_extracted":..., "csv_skipped":..., "html_no_sct":..., "txt_extracted":..., "txt_skipped":..., "txt_no_sct":..., "parquet_written":..., "total_entries":..., "last_updated": "ISO"}
      }
    }
    """

    def __init__(self, root: Path, run_id: str, form: str) -> None:
        self.root = Path(root)
        self.run_id = run_id
        self.form = form
        self.path = self.root / f"metadata-extract-{run_id}.json"
        self._ensure_file()

    def _ensure_file(self) -> None:
        if self.path.exists():
            return
        payload = {
            "version": 1,
            "run_id": self.run_id,
            "root": str(self.root.resolve()),
            "form": self.form,
            "started_at": _now_iso(),
            "updated_at": _now_iso(),
            "totals": {
                "total_csv": 0,
                "total_txt": 0,
                "total_parquet": 0,
                "total_entries": 0,
                "tickers_done": 0,
            },
            "tickers": {},
        }
        self.path.write_text(json.dumps(payload, indent=2))

    def _load(self) -> dict:
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return {}

    def _save(self, data: dict) -> None:
        data["updated_at"] = _now_iso()
        self.path.write_text(json.dumps(data, indent=2))

    def write_ticker(self, ticker: str, stats: TickerStats) -> None:
        """Idempotent per-ticker update with additive totals.

        If ticker already exists, subtract its previous contribution from totals
        before adding the new one.
        """
        data = self._load()
        if not data:
            self._ensure_file()
            data = self._load()
        tickers = data.setdefault("tickers", {})
        totals = data.setdefault("totals", {
            "total_csv": 0,
            "total_txt": 0,
            "total_parquet": 0,
            "total_entries": 0,
            "tickers_done": 0,
        })

        t = ticker.upper()
        prev = tickers.get(t)
        if prev:
            # subtract previous contribution
            totals["total_csv"] -= int(prev.get("csv_extracted", 0)) + int(prev.get("csv_skipped", 0))
            totals["total_txt"] -= int(prev.get("txt_extracted", 0)) + int(prev.get("txt_skipped", 0))
            totals["total_parquet"] -= int(prev.get("parquet_written", 0))
            totals["total_entries"] -= int(prev.get("total_entries", 0))
        else:
            totals["tickers_done"] += 1

        # add new contribution
        totals["total_csv"] += int(stats.csv_extracted) + int(stats.csv_skipped)
        totals["total_txt"] += int(stats.txt_extracted) + int(stats.txt_skipped)
        totals["total_parquet"] += int(stats.parquet_written)
        totals["total_entries"] += int(stats.total_entries)

        # Merge per-date statuses (aggregate html/txt; extracted > skipped_existing > no_sct_found)
        precedence = {"extracted": 3, "skipped_existing": 2, "no_sct_found": 1}
        dates: dict[str, str] = {}
        for d, st in (stats.file_status_html or {}).items():
            cur = dates.get(d)
            if cur is None or precedence.get(st, 0) > precedence.get(cur, 0):
                dates[d] = st
        for d, st in (stats.file_status_txt or {}).items():
            cur = dates.get(d)
            if cur is None or precedence.get(st, 0) > precedence.get(cur, 0):
                dates[d] = st

        # Prepare new ticker payload
        new_entry = {
            "csv_extracted": int(stats.csv_extracted),
            "csv_skipped": int(stats.csv_skipped),
            "html_no_sct": int(stats.html_no_sct),
            "txt_extracted": int(stats.txt_extracted),
            "txt_skipped": int(stats.txt_skipped),
            "txt_no_sct": int(stats.txt_no_sct),
            "parquet_written": int(stats.parquet_written),
            "total_entries": int(stats.total_entries),
            "last_updated": _now_iso(),
        }
        # Preserve any existing per-date statuses and overwrite with newer ones
        if prev:
            for k, v in prev.items():
                if k and k[:4].isdigit() and k.count('-') == 2 and isinstance(v, dict) and 'status' in v:
                    new_entry.setdefault(k, v)
        for d, st in dates.items():
            new_entry[d] = {"status": st}

        tickers[t] = new_entry
        self._save(data)
