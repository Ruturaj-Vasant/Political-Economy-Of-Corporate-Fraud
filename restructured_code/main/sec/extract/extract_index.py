from __future__ import annotations

"""Extraction index for SCT outputs (metadata-extract.json).

Tracks per-ticker/form/report_date extractions and output artifacts.
Written under `<data_root>/metadata-extract.json`.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import datetime as _dt
import json


def _now_iso() -> str:
    now = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


@dataclass
class _FormExtractNode:
    entries: Dict[str, dict]
    count_csv: int
    count_txt: int
    count_parquet: int
    count_bo: int
    last_updated: str


class ExtractionIndex:
    def __init__(self, root: Path, data: dict) -> None:
        self.root = Path(root)
        self._data = data

    @classmethod
    def load(cls, root: Path | str) -> "ExtractionIndex":
        r = Path(root)
        r.mkdir(parents=True, exist_ok=True)
        path = r / "metadata-extract.json"
        if path.exists():
            try:
                data = json.loads(path.read_text())
            except Exception:
                data = {}
        else:
            data = {}
        if not data:
            data = {
                "version": 1,
                "root": str(r.resolve()),
                "generated_at": _now_iso(),
                "tickers": {},
                "totals": {
                    "total_csv": 0,
                    "total_txt": 0,
                    "total_parquet": 0,
                    "total_entries": 0,
                    "total_bo": 0,
                },
            }
        return cls(r, data)

    def save(self) -> None:
        # Recompute counts before saving
        total_csv = total_txt = total_parquet = total_entries = total_bo = 0
        for _t, tnode in self._data.get("tickers", {}).items():
            tfiles = 0
            for _f, fnode in tnode.get("forms", {}).items():
                entries = fnode.get("entries", {})
                c_csv = sum(1 for e in entries.values() if e.get("csv_relpath"))
                c_txt = sum(1 for e in entries.values() if e.get("txt_relpath"))
                c_parq = sum(1 for e in entries.values() if e.get("parquet_relpath"))
                c_bo = sum(len(e.get("bo_html_relpaths", []) or []) for e in entries.values())
                fnode["count_csv"] = c_csv
                fnode["count_txt"] = c_txt
                fnode["count_parquet"] = c_parq
                fnode["count_bo"] = c_bo
                fnode["last_updated"] = _now_iso()
                tfiles += len(entries)
                total_csv += c_csv
                total_txt += c_txt
                total_parquet += c_parq
                total_bo += c_bo
            tnode["total_extracted_files"] = tfiles
            total_entries += tfiles
        self._data["totals"]["total_csv"] = total_csv
        self._data["totals"]["total_txt"] = total_txt
        self._data["totals"]["total_parquet"] = total_parquet
        self._data["totals"]["total_bo"] = total_bo
        self._data["totals"]["total_entries"] = total_entries
        self._data["generated_at"] = _now_iso()
        (self.root / "metadata-extract.json").write_text(json.dumps(self._data, indent=2))

    # ---------------- Helpers ----------------
    def _ensure_nodes(self, ticker: str, form: str) -> dict:
        tickers = self._data.setdefault("tickers", {})
        tnode = tickers.setdefault(ticker, {"forms": {}, "total_extracted_files": 0})
        forms = tnode.setdefault("forms", {})
        fnode = forms.setdefault(form, {
            "entries": {},
            "count_csv": 0,
            "count_txt": 0,
            "count_parquet": 0,
            "count_bo": 0,
            "last_updated": _now_iso(),
        })
        return fnode

    def _ensure_entry(self, ticker: str, form: str, report_date: str) -> dict:
        fnode = self._ensure_nodes(ticker, form)
        entries: Dict[str, dict] = fnode.setdefault("entries", {})
        ent = entries.setdefault(report_date, {"report_date": report_date})
        ent["last_updated"] = _now_iso()
        return ent

    # ---------------- Record operations (write-through by default) ----------------
    def record_csv(
        self,
        *,
        ticker: str,
        form: str,
        report_date: str,
        csv_relpath: str,
        rows: Optional[int],
        cols: Optional[int],
        columns: Optional[list[str]] = None,
        source_html_relpath: Optional[str] = None,
        status: str = "extracted",
        normalized: Optional[bool] = None,
        deduped_cols: Optional[bool] = None,
        parquet_relpath: Optional[str] = None,
        write_immediately: bool = True,
    ) -> None:
        ent = self._ensure_entry(ticker, form, report_date)
        ent.update({
            "status": status,
            "csv_relpath": csv_relpath,
            "csv_rows": rows,
            "csv_cols": cols,
        })
        if columns is not None:
            ent["csv_columns"] = list(columns)
        if normalized is not None:
            ent["normalized"] = bool(normalized)
        if deduped_cols is not None:
            ent["deduped_cols"] = bool(deduped_cols)
        if parquet_relpath:
            ent["parquet_relpath"] = parquet_relpath
        if source_html_relpath:
            ent["source_html_relpath"] = source_html_relpath
        if write_immediately:
            self.save()

    def record_bo(
        self,
        *,
        ticker: str,
        form: str,
        report_date: str,
        html_relpaths: list[str],
        status: str = "extracted",
        source_html_relpath: Optional[str] = None,
        write_immediately: bool = True,
    ) -> None:
        ent = self._ensure_entry(ticker, form, report_date)
        ent.update({
            "status": status,
            "bo_html_relpaths": list(html_relpaths),
        })
        if source_html_relpath:
            ent["source_html_relpath"] = source_html_relpath
        if write_immediately:
            self.save()

    def record_txt(
        self,
        *,
        ticker: str,
        form: str,
        report_date: str,
        txt_relpath: str,
        snippet_chars: Optional[int] = None,
        status: str = "extracted",
        source_txt_relpath: Optional[str] = None,
        write_immediately: bool = True,
    ) -> None:
        ent = self._ensure_entry(ticker, form, report_date)
        ent.update({
            "status": status,
            "txt_relpath": txt_relpath,
            "snippet_chars": snippet_chars,
        })
        if source_txt_relpath:
            ent["source_txt_relpath"] = source_txt_relpath
        if write_immediately:
            self.save()

    def record_status(
        self,
        *,
        ticker: str,
        form: str,
        report_date: str,
        status: str,
        source_relpath: Optional[str] = None,
        write_immediately: bool = True,
    ) -> None:
        ent = self._ensure_entry(ticker, form, report_date)
        ent["status"] = status
        if source_relpath:
            ent["source_relpath"] = source_relpath
        if write_immediately:
            self.save()
