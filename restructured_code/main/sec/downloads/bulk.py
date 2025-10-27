"""Bulk downloader CLI that mirrors interactive option 3 (HTML with TXT fallback).

Usage examples:
  export SEC_USER_AGENT='you@example.com'
  python -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all --dry-run
  python -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all
  python -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --forms "10-K" --years 1994:2025 --limit 50
  python -m restructured_code.main.sec.downloads.bulk --tickers AAPL,MSFT --forms "DEF 14A" --base ./trial
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import time

from ..config import load_config
from .data_index import DataIndex, rebuild_index
from ..utils.ticker_lookup import tickers_by_permnos
from ..utils.list_loader import load_tickers_from_file, load_permnos_from_file
from ..clients.edgar_client import (
    init_identity,
    list_filings_for_ticker,
    fetch_best_content,
    get_filing_details,
)
from ..storage.backends import LocalStorage
from .file_naming import build_rel_paths
from .validator import basic_html_check, smoke_test_def14a


DEFAULT_TICKER_JSON = Path("restructured_code/json/sec_company_tickers.json")


def _parse_years(s: Optional[str]) -> Optional[Tuple[int, int]]:
    if not s:
        return None
    if ":" in s:
        a, b = s.split(":", 1)
        return (int(a), int(b))
    y = int(s)
    return (y, y)


def _parse_forms(forms_args: List[str]) -> List[str]:
    if not forms_args:
        return ["DEF 14A"]
    out: List[str] = []
    for f in forms_args:
        parts = [x.strip() for x in f.split(",") if x.strip()]
        out.extend(parts)
    # preserve order, dedupe
    seen = set()
    result = []
    for f in out:
        if f not in seen:
            result.append(f)
            seen.add(f)
    return result


def _load_all_tickers(json_path: Path) -> List[str]:
    with json_path.open("r") as fh:
        data = json.load(fh)
    tickers: List[str] = []
    if isinstance(data, dict):
        for rec in data.values():
            if isinstance(rec, dict) and rec.get("ticker"):
                tickers.append(str(rec["ticker"]).strip().upper())
    else:
        # if the json is a list
        for rec in data:
            if isinstance(rec, dict) and rec.get("ticker"):
                tickers.append(str(rec["ticker"]).strip().upper())
    # stable order
    return tickers


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Bulk SEC downloader across tickers/forms with resume and index")
    ap.add_argument("--forms", action="append", default=[], help="Form names (repeat or comma-separate). Default: DEF 14A")
    ap.add_argument("--base", default="", help="Base folder (data is created under <base>/data). If empty, uses default data root")
    ap.add_argument("--dry-run", action="store_true", help="List what would be downloaded without fetching")
    ap.add_argument("--years", default="", help="Year range 'YYYY0:YYYY1' or single 'YYYY' (blank = all)")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N tickers (for testing)")
    ap.add_argument("--tickers", default="", help="Comma-separated tickers to restrict to (optional)")
    ap.add_argument("--tickers-file", default="", help="Path to CSV/TSV/txt with tickers (optional)")
    ap.add_argument("--permnos", default="", help="Comma-separated PERMNOs to resolve to tickers (optional)")
    ap.add_argument("--permnos-file", default="", help="Path to CSV/TSV/txt with PERMNOs (optional)")
    ap.add_argument("--max-new-files", type=int, default=0, help="Stop after saving N new files (HTML/TXT). Real runs only")
    ap.add_argument("--max-new-tickers", type=int, default=0, help="Stop after M tickers saved at least one new file. Real runs only")
    ap.add_argument("--json", default=str(DEFAULT_TICKER_JSON), help="Path to sec_company_tickers.json")
    ap.add_argument("--rebuild-index", action="store_true", help="Rebuild index from sidecars before downloading")
    ap.add_argument("--scan-index", action="store_true", help="Scan/update index from sidecars before downloading")

    args = ap.parse_args(argv)

    forms = _parse_forms(args.forms)
    years = _parse_years(args.years)
    base_dir = args.base if args.base else None

    ref = Path(args.json)
    if not ref.exists():
        ap.error(f"Ticker JSON not found: {ref}")

    all_tickers = _load_all_tickers(ref)
    tickers: List[str] = []
    # Seed from file if provided
    if args.tickers_file:
        tickers.extend(load_tickers_from_file(args.tickers_file))
    # Merge explicit tickers
    if args.tickers:
        tickers.extend([t.strip().upper() for t in args.tickers.split(",") if t.strip()])
    # If still empty, use JSON universe
    if not tickers:
        tickers = all_tickers
    # Merge permnos (resolved to tickers) if provided
    permnos_inputs: List[str] = []
    if args.permnos:
        permnos_inputs.extend([p.strip() for p in args.permnos.split(",") if p.strip()])
    if args.permnos_file:
        permnos_inputs.extend(load_permnos_from_file(args.permnos_file))
    if permnos_inputs:
        resolved = tickers_by_permnos(permnos_inputs, json_path=ref)
        # merge while preserving order preference: permnos first, then existing order
        merged: List[str] = []
        seen: set[str] = set()
        for t in resolved + tickers:
            tu = t.upper()
            if tu not in seen:
                merged.append(tu)
                seen.add(tu)
        tickers = merged
    if args.limit and args.limit > 0:
        tickers = tickers[: args.limit]

    print(f"Processing {len(tickers)} tickers, forms={forms}, years={years or 'all-years'}, base={base_dir or '(default)'}")
    if args.dry_run and (args.max_new_files or args.max_new_tickers):
        print("Note: --max-new-files/--max-new-tickers apply only to real runs; ignoring in dry-run.")

    # Determine effective data root and initialize identity/storage/index
    cfg = load_config()
    init_identity(cfg.user_agent)
    effective_root = (Path(base_dir).expanduser() / "data") if base_dir else Path(cfg.data_root)
    storage = LocalStorage(effective_root.as_posix())
    index = DataIndex.load(storage.root)
    # Pre-run index maintenance
    if args.rebuild_index and args.scan_index:
        print("Both --rebuild-index and --scan-index set; performing rebuild.")
        args.scan_index = False
    if args.rebuild_index:
        added = rebuild_index(storage.root)
        print(f"Rebuilt index at {storage.root}/metadata.json with {added} entries.")
    elif args.scan_index:
        added = index.scan_from_sidecars(storage.root)
        index.save()
        print(f"Scanned and updated index at {storage.root}/metadata.json with {added} entries.")
    elif not (storage.root / "metadata.json").exists():
        added = index.scan_from_sidecars(storage.root)
        index.save()
        if added:
            print(f"Initialized index from {added} existing sidecar metas.")

    total_new_files = 0
    total_new_tickers = 0

    for i, ticker in enumerate(tickers, start=1):
        print("\n" + "-" * 60)
        print(f"[{i}/{len(tickers)}] Ticker: {ticker}")
        try:
            # Throttle before listing
            time.sleep(cfg.min_interval_seconds)

            # Snapshot index totals before
            total_before = int(index._data.get("totals", {}).get("total_files", 0))
            t_before = int(index._data.get("tickers", {}).get(ticker, {}).get("total_files", 0))

            for form in forms:
                filings = list_filings_for_ticker(ticker, form)
                if not filings:
                    print(f"No filings found for {ticker} with form {form}.")
                    continue
                print(f"Found {len(filings)} filings for form {form}.")

                if args.dry_run:
                    missing = [f for f in filings if f.filing_date and not index.has_filing(ticker, form, f.filing_date)]
                    if missing:
                        print("Missing filings (would download):")
                        for j, f in enumerate(missing, start=1):
                            print(f"  {j}. {ticker} {form} {f.filing_date} -> {f.url}")
                    else:
                        print("All listed filings already present.")
                    continue

                for idx_f, f in enumerate(filings, start=1):
                    fd = f.filing_date or f"index_{idx_f}"
                    print(f"Filing {idx_f}: Date: {fd}, URL: {f.url}")

                    # Skip via index
                    if f.filing_date and index.has_filing(ticker, form, f.filing_date):
                        print(f"  Skipping (already indexed): {fd}")
                        continue

                    # Skip if local file exists; ensure sidecar + index
                    html_rel, txt_rel = build_rel_paths(storage.root.as_posix(), ticker, form, f.filing_date or fd)
                    existing_rel = html_rel if storage.exists(html_rel) else (txt_rel if storage.exists(txt_rel) else None)
                    if existing_rel is not None:
                        if storage.has_meta(existing_rel):
                            meta_existing = storage.read_meta(existing_rel)
                            if meta_existing:
                                index.record(meta_existing, relpath=existing_rel)
                                print(f"  Skipping (already saved): {existing_rel}")
                                continue
                        # Create sidecar meta for existing file
                        existing_bytes = storage.load_bytes(existing_rel)
                        if existing_bytes is not None:
                            if existing_rel.suffix == ".html":
                                ok, reason = basic_html_check(existing_bytes, cfg.min_html_size_bytes)
                                meta = {
                                    "ticker": ticker,
                                    "form": form,
                                    "filing_date": f.filing_date,
                                    "url": f.url,
                                    "saved_as": "html",
                                    "html_ok": ok,
                                    "html_reason": reason,
                                    "size": len(existing_bytes),
                                    "sha256": __import__("hashlib").sha256(existing_bytes).hexdigest(),
                                }
                            else:
                                meta = {
                                    "ticker": ticker,
                                    "form": form,
                                    "filing_date": f.filing_date,
                                    "url": f.url,
                                    "saved_as": "txt",
                                    "size": len(existing_bytes),
                                    "sha256": __import__("hashlib").sha256(existing_bytes).hexdigest(),
                                }
                            storage.write_meta(existing_rel, meta)
                            index.record(meta, relpath=existing_rel)
                            print(f"  Recorded sidecar for existing file: {existing_rel}")
                            continue

                    # Throttle before network fetch
                    time.sleep(cfg.min_interval_seconds)

                    # Fetch content (HTML preferred; fallback to TXT)
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
                        ok, reason = basic_html_check(content, cfg.min_html_size_bytes)
                        if not ok:
                            print(f"  HTML invalid ({reason}); attempting TXT fallback…")
                        else:
                            # Optional smoke test for DEF 14A
                            if cfg.smoke_test_def14a and form.strip().upper() == "DEF 14A":
                                try:
                                    extra["extract_smoke_ok"] = bool(smoke_test_def14a(content))
                                except Exception:
                                    pass
                            res = storage.save_html(html_rel, content)
                            meta = {
                                "ticker": ticker,
                                "form": form,
                                "filing_date": f.filing_date,
                                "url": f.url,
                                "saved_as": "html",
                                "html_ok": True,
                                "html_reason": "ok",
                                **extra,
                                "size": res.size,
                                "sha256": res.sha256,
                            }
                            storage.write_meta(html_rel, meta)
                            index.record(meta, relpath=html_rel)
                            continue

                    # Fallback to TXT (if available or HTML invalid)
                    # Throttle before TXT fetch if content was not already TXT
                    if saved_as != "txt":
                        time.sleep(cfg.min_interval_seconds)
                        # Reuse fetch_best_content outcome; otherwise, try text via edgar
                    if isinstance(content, str) and saved_as == "txt":
                        txt = content
                    else:
                        # last resort: no content or invalid HTML, try text from edgar
                        from ..clients.edgar_client import fetch_text
                        txt = fetch_text(f) or ""
                    if txt:
                        res2 = storage.save_text(txt_rel, txt)
                        meta2 = {
                            "ticker": ticker,
                            "form": form,
                            "filing_date": f.filing_date,
                            "url": f.filing_url if hasattr(f, 'filing_url') else f.url,
                            "saved_as": "txt",
                            **extra,
                            "size": res2.size,
                            "sha256": res2.sha256,
                        }
                        storage.write_meta(txt_rel, meta2)
                        index.record(meta2, relpath=txt_rel)
                    else:
                        print(f"  Filing {idx_f}: neither valid HTML nor TXT available.")

            # Persist index after finishing a ticker
            try:
                index.save()
            except Exception:
                pass

            if not args.dry_run:
                total_after = int(index._data.get("totals", {}).get("total_files", 0))
                t_after = int(index._data.get("tickers", {}).get(ticker, {}).get("total_files", 0))
                added_files = max(0, total_after - total_before)
                added_for_ticker = max(0, t_after - t_before)
                if added_files:
                    total_new_files += added_files
                if added_for_ticker > 0:
                    total_new_tickers += 1
                if args.max_new_files and total_new_files >= args.max_new_files:
                    print(f"Reached --max-new-files threshold: {total_new_files} >= {args.max_new_files}. Stopping.")
                    break
                if args.max_new_tickers and total_new_tickers >= args.max_new_tickers:
                    print(f"Reached --max-new-tickers threshold: {total_new_tickers} >= {args.max_new_tickers}. Stopping.")
                    break
        except KeyboardInterrupt:
            print("Interrupted by user.")
            return 130
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
