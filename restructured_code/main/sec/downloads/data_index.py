"""Dataset-level metadata index for SEC downloads.

Tracks which filings are already downloaded to avoid re-downloading.

Structure (metadata.json at the data root):
- version: schema version
- root: absolute data root path
- generated_at: ISO timestamp of last save
- tickers: { TICKER: { forms: { FORM: {
      entries: { FILING_DATE: {
          saved_as: "html"|"txt",
          relpath: "<TICKER>/<FORM>/<DATE>_<FORM>.<ext>",
          size: int,
          sha256: str,
          url: str,
          html_ok/html_reason/fallback/extract_smoke_ok: optional
      } },
      count_html: int,
      count_txt: int,
      last_updated: ISO
  } }, total_files: int } }
- totals: { total_files: int }

Sidecar file meta remains the source of truth for each individual file; this
index exists for fast membership checks and high-level counts.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import json
import datetime as _dt
import argparse


def _now_iso() -> str:
    now = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


@dataclass
class _FormNode:
    entries: Dict[str, dict]
    count_html: int
    count_txt: int
    last_updated: str


class DataIndex:
    def __init__(self, root: Path, data: dict) -> None:
        self.root = Path(root)
        self._data = data

    @classmethod
    def load(cls, root: Path | str) -> "DataIndex":
        r = Path(root)
        r.mkdir(parents=True, exist_ok=True)
        path = r / "metadata.json"
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
                "totals": {"total_files": 0},
            }
        return cls(r, data)

    def save(self) -> None:
        # Recompute totals before saving
        total_files = 0
        for _t, tnode in self._data.get("tickers", {}).items():
            tfiles = 0
            for _f, fnode in tnode.get("forms", {}).items():
                entries = fnode.get("entries", {})
                # Update counts based on entries
                count_html = sum(1 for e in entries.values() if e.get("saved_as") == "html")
                count_txt = sum(1 for e in entries.values() if e.get("saved_as") == "txt")
                fnode["count_html"] = count_html
                fnode["count_txt"] = count_txt
                fnode["last_updated"] = _now_iso()
                tfiles += len(entries)
            tnode["total_files"] = tfiles
            total_files += tfiles
        self._data["totals"]["total_files"] = total_files
        self._data["generated_at"] = _now_iso()
        (self.root / "metadata.json").write_text(json.dumps(self._data, indent=2))

    # ---------- Query / update helpers ----------
    def _ensure_nodes(self, ticker: str, form: str) -> dict:
        tickers = self._data.setdefault("tickers", {})
        tnode = tickers.setdefault(ticker, {"forms": {}, "total_files": 0})
        forms = tnode.setdefault("forms", {})
        fnode = forms.setdefault(form, {
            "entries": {},
            "count_html": 0,
            "count_txt": 0,
            "last_updated": _now_iso(),
        })
        return fnode

    def has_filing(self, ticker: str, form: str, filing_date: str) -> bool:
        tnode = self._data.get("tickers", {}).get(ticker, {})
        fnode = tnode.get("forms", {}).get(form, {})
        return filing_date in fnode.get("entries", {})

    def record(self, meta: dict, relpath: Optional[Path] = None) -> None:
        """Record or update a filing entry from its sidecar meta.

        Expected meta keys: ticker, form, filing_date, saved_as, size, sha256, url
        Optionally: html_ok, html_reason, fallback, extract_smoke_ok
        """
        ticker = str(meta.get("ticker", "")).strip().upper()
        form = str(meta.get("form", "")).strip()
        filing_date = str(meta.get("filing_date", "")).strip()
        saved_as = str(meta.get("saved_as", "")).strip().lower()
        if not (ticker and form and filing_date and saved_as):
            return
        fnode = self._ensure_nodes(ticker, form)
        entries: Dict[str, dict] = fnode.setdefault("entries", {})
        summary = {
            "saved_as": saved_as,
            "relpath": relpath.as_posix() if isinstance(relpath, Path) else meta.get("relpath"),
            "size": meta.get("size"),
            "sha256": meta.get("sha256"),
            "url": meta.get("url"),
        }
        # carry over diagnostic flags if present
        for k in (
            "html_ok",
            "html_reason",
            "fallback",
            "extract_smoke_ok",
            "doc_url",
            "doc_type",
            "attachment_count",
            "is_xbrl",
            "is_inline_xbrl",
        ):
            if k in meta:
                summary[k] = meta[k]
        entries[filing_date] = summary

    def scan_from_sidecars(self, storage_root: Path) -> int:
        """Populate index from existing sidecar meta files.

        Returns number of entries added/updated.
        """
        root = Path(storage_root)
        count = 0
        for meta_path in root.glob("**/*.meta.json"):
            try:
                meta = json.loads(meta_path.read_text())
            except Exception:
                continue
            # Compute the corresponding file relpath (drop .meta.json suffix)
            try:
                rel = meta_path.relative_to(root)
            except Exception:
                rel = meta_path
            # remove the .meta.json suffix -> actual file path
            # e.g., foo/bar.html.meta.json -> foo/bar.html
            if rel.suffix == ".json" and rel.name.endswith(".meta.json"):
                file_rel = Path(rel.as_posix()[:-len(".meta.json")])
            else:
                file_rel = rel
            self.record(meta, relpath=file_rel)
            count += 1
        return count


def rebuild_index(root: Path | str) -> int:
    """Rebuild the dataset index from all sidecar meta files under root.

    Returns number of entries indexed.
    """
    r = Path(root)
    idx = DataIndex.load(r)
    # Clear current tickers map before scanning
    idx._data["tickers"] = {}
    added = idx.scan_from_sidecars(r)
    idx.save()
    return added


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Manage dataset-level SEC downloads index (metadata.json)")
    ap.add_argument("--root", required=True, help="Path to the data root (folder containing ticker folders)")
    ap.add_argument(
        "--mode",
        choices=["rebuild", "scan"],
        default="rebuild",
        help="rebuild = wipe and rebuild from sidecars; scan = update existing index from sidecars",
    )
    args = ap.parse_args(argv)
    root = Path(args.root)
    if args.mode == "rebuild":
        added = rebuild_index(root)
        print(f"Rebuilt index at {root}/metadata.json with {added} entries.")
    else:
        idx = DataIndex.load(root)
        added = idx.scan_from_sidecars(root)
        idx.save()
        print(f"Scanned and updated index at {root}/metadata.json with {added} entries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
