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
from ..clients.edgar_client import init_identity, list_filings_for_ticker, fetch_html, fetch_text, FilingRef
from ..storage.backends import LocalStorage
from .file_naming import build_rel_paths
from .validator import basic_html_check, smoke_test_def14a


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


def download_filings_for_ticker(ticker: str, forms: List[str], years: Optional[Tuple[int, int]] = None) -> None:
    """Download filings for a ticker across given forms.

    Years filter is currently applied post‑fetch by date prefix (YYYY-).
    This can be improved by adding planner logic to reduce listing scope.
    """
    cfg = load_config()
    init_identity(cfg.user_agent)
    storage = LocalStorage(cfg.data_root)

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

            html_rel, txt_rel = build_rel_paths(cfg.data_root, ticker, form, f.filing_date)
            target_rel = html_rel  # default to HTML

            # Resume: if HTML exists and meta missing, recompute and write meta
            existing = storage.load_bytes(target_rel)
            meta_path = _meta_path_for(storage, target_rel)
            if existing is not None and not meta_path.exists():
                ok, bits = _validate_and_maybe_smoke(form, existing, cfg.min_html_size_bytes, cfg.smoke_test_def14a)
                meta = {
                    "ticker": ticker,
                    "form": form,
                    "filing_date": f.filing_date,
                    "url": f.url,
                    "saved_as": "html",
                    **bits,
                    "size": len(existing),
                    "sha256": _sha256(existing),
                }
                _write_meta(storage, target_rel, meta)
                if ok:
                    # Treat as done
                    continue
                # If invalid, we will fall through to re‑download.

            # Download with polite delay + retries
            html_ok = False
            html_bytes: Optional[bytes] = None
            reason = ""
            for attempt in range(cfg.max_retries):
                _sleep_polite(cfg.min_interval_seconds)
                html_bytes = fetch_html(f)
                if html_bytes:
                    ok, bits = _validate_and_maybe_smoke(form, html_bytes, cfg.min_html_size_bytes, cfg.smoke_test_def14a)
                    if ok:
                        html_ok = True
                        # Save HTML
                        res = storage.save_html(target_rel, html_bytes)
                        meta = {
                            "ticker": ticker,
                            "form": form,
                            "filing_date": f.filing_date,
                            "url": f.url,
                            "saved_as": "html",
                            **bits,
                            "size": res.size,
                            "sha256": res.sha256,
                        }
                        _write_meta(storage, target_rel, meta)
                        break
                    else:
                        reason = bits.get("html_reason", "invalid")
                # backoff
                time.sleep(1.0 * (2 ** attempt))

            if html_ok:
                continue

            # Fallback to TXT if allowed by library / form
            txt = fetch_text(f)
            if txt:
                res2 = storage.save_text(txt_rel, txt)
                meta2 = {
                    "ticker": ticker,
                    "form": form,
                    "filing_date": f.filing_date,
                    "url": f.url,
                    "saved_as": "txt",
                    "fallback": True,
                    "html_reason": reason or "html_unavailable",
                    "size": res2.size,
                    "sha256": res2.sha256,
                }
                _write_meta(storage, txt_rel, meta2)
                continue
            # If neither HTML nor TXT succeeded, we just move on (logged via absence)

