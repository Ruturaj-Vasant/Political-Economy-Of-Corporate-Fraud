"""Downloader orchestration with verification and optional smoke tests.

This module implements the HTML→TXT fallback and writes sidecar meta.json.
It also supports resume: if a file exists without meta, we compute and write
meta after verifying validity.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional, Tuple

from ..config import load_config
from ..clients.edgar_client import init_identity, list_filings_for_ticker, fetch_html, fetch_text, FilingRef, fetch_best_content, get_filing_details
from ..storage.backends import LocalStorage
from .file_naming import build_rel_paths
from .validator import basic_html_check, smoke_test_def14a
from .data_index import DataIndex


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sleep_polite(delay: float) -> None:
    if delay > 0:
        time.sleep(delay)


def _meta_path_for(storage: LocalStorage, relpath: Path) -> Path:
    return (Path(storage.root) / relpath).with_suffix(relpath.suffix + ".meta.json")


def _write_meta(storage: LocalStorage, relpath: Path, meta: dict) -> None:
    storage.write_meta(relpath, meta)


def _validate_and_maybe_smoke(form: str, html_bytes: bytes, min_size: int, smoke_def14a: bool) -> Tuple[bool, dict]:
    ok, reason = basic_html_check(html_bytes, min_size)
    meta_bits = {"html_ok": ok, "html_reason": reason}
    if ok and smoke_def14a and form.strip().upper() == "DEF 14A":
        smoke_ok = smoke_test_def14a(html_bytes)
        meta_bits["extract_smoke_ok"] = bool(smoke_ok)
    return ok, meta_bits


def download_filings_for_ticker(
    ticker: str,
    forms: List[str],
    years: Optional[Tuple[int, int]] = None,
    base_dir: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """Download filings for a ticker across given forms.

    Years filter is currently applied post‑fetch by date prefix (YYYY-).
    This can be improved by adding planner logic to reduce listing scope.
    """
    cfg = load_config()
    init_identity(cfg.user_agent)
    # Determine effective data root: always nest under a 'data' folder for custom base_dir
    if base_dir:
        effective_root = (Path(base_dir).expanduser() / "data").as_posix()
    else:
        effective_root = cfg.data_root
    storage = LocalStorage(effective_root)
    index = DataIndex.load(storage.root)
    # Initialize index from sidecars if first time
    if not (Path(storage.root) / "metadata.json").exists():
        index.scan_from_sidecars(storage.root)
        index.save()

    for form in forms:
        filings = list_filings_for_ticker(ticker, form)
        for f in filings:
            if not f.filing_date:
                continue
            if years:
                y0, y1 = years
                try:
                    y = int(str(f.filing_date)[:4])
                except Exception:
                    continue
                if y < y0 or y > y1:
                    continue

            html_rel, txt_rel = build_rel_paths(storage.root.as_posix(), ticker, form, f.filing_date)
            target_rel = html_rel  # default to HTML

            # Skip logic using dataset index and existing files/meta
            if index.has_filing(ticker, form, f.filing_date):
                continue

            # If either HTML or TXT file already exists locally, ensure sidecar + index, then skip
            existing_rel: Optional[Path] = None
            if storage.exists(html_rel):
                existing_rel = html_rel
            elif storage.exists(txt_rel):
                existing_rel = txt_rel
            if existing_rel is not None:
                if storage.has_meta(existing_rel):
                    # Ensure index knows about it (in case metadata.json is stale)
                    meta_existing = storage.read_meta(existing_rel)
                    if meta_existing:
                        index.record(meta_existing, relpath=existing_rel)
                        continue
                # Sidecar missing: compute minimal meta and write it, then index + skip
                existing_bytes = storage.load_bytes(existing_rel)
                if existing_bytes is not None:
                    if existing_rel.suffix == ".html":
                        ok, bits = _validate_and_maybe_smoke(
                            form, existing_bytes, cfg.min_html_size_bytes, cfg.smoke_test_def14a
                        )
                        meta = {
                            "ticker": ticker,
                            "form": form,
                            "filing_date": f.filing_date,
                            "url": f.url,
                            "saved_as": "html",
                            **bits,
                            "size": len(existing_bytes),
                            "sha256": _sha256(existing_bytes),
                        }
                    else:
                        meta = {
                            "ticker": ticker,
                            "form": form,
                            "filing_date": f.filing_date,
                            "url": f.url,
                            "saved_as": "txt",
                            "size": len(existing_bytes),
                            "sha256": _sha256(existing_bytes),
                        }
                    _write_meta(storage, existing_rel, meta)
                    index.record(meta, relpath=existing_rel)
                    continue

            # Dry-run mode: report and continue without fetching
            if dry_run:
                print(f"Would download: {ticker} {form} {f.filing_date} -> {f.url}")
                continue

            # Download with polite delay + retries using best-content resolver
            success = False
            for attempt in range(cfg.max_retries):
                _sleep_polite(cfg.min_interval_seconds)
                saved_as, content, doc_url = fetch_best_content(f)
                details = get_filing_details(f)
                extra = {
                    "doc_url": doc_url,
                    "doc_type": saved_as if saved_as in {"html", "txt"} else None,
                    "attachments": details.get("attachments"),
                    "attachment_count": len(details.get("attachments", []) or []),
                    "cik": details.get("cik"),
                    "company": details.get("company"),
                    "report_date": details.get("report_date"),
                    "acceptance_datetime": details.get("acceptance_datetime"),
                    "accession_no": details.get("accession_no"),
                    "file_number": details.get("file_number"),
                    "items": details.get("items"),
                    "primary_document": details.get("primary_document"),
                    "primary_doc_description": details.get("primary_doc_description"),
                    "is_xbrl": details.get("is_xbrl"),
                    "is_inline_xbrl": details.get("is_inline_xbrl"),
                }
                if saved_as == "html" and isinstance(content, (bytes, bytearray)):
                    ok, bits = _validate_and_maybe_smoke(form, content, cfg.min_html_size_bytes, cfg.smoke_test_def14a)
                    if ok:
                        res = storage.save_html(target_rel, content)
                        meta = {
                            "ticker": ticker,
                            "form": form,
                            "filing_date": f.filing_date,
                            "url": f.url,
                            "saved_as": "html",
                            **extra,
                            **bits,
                            "size": res.size,
                            "sha256": res.sha256,
                        }
                        _write_meta(storage, target_rel, meta)
                        index.record(meta, relpath=target_rel)
                        success = True
                        break
                elif saved_as == "txt" and isinstance(content, str):
                    res2 = storage.save_text(txt_rel, content)
                    meta2 = {
                        "ticker": ticker,
                        "form": form,
                        "filing_date": f.filing_date,
                        "url": f.url,
                        "saved_as": "txt",
                        **extra,
                        "size": res2.size,
                        "sha256": res2.sha256,
                    }
                    _write_meta(storage, txt_rel, meta2)
                    index.record(meta2, relpath=txt_rel)
                    success = True
                    break
                # backoff
                time.sleep(1.0 * (2 ** attempt))

            if success:
                continue
            # If neither HTML nor TXT succeeded, we just move on (logged via absence)

    # Persist index at the end of the run
    index.save()
