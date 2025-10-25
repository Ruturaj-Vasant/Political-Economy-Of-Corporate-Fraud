"""Interactive downloader (terminal prompts), preserving legacy behavior.

Features
- Menu of forms (10-K, DEF 14A, 10-Q, 13F-HR, 8-K, 3/4/5, NPORT-P, D, C, MA-I, 144)
- Accept multiple tickers (comma-separated)
- Save methods:
  1) Open in browser (local convenience; not for HPC)
  2) Save as plain text (.txt)
  3) Save as HTML (.html); if HTML not available or invalid, fall back to .txt

Output naming (new): data/<TICKER>/<FORM_FS>/<DATE>_<FORM_FS>.<ext> (spaces→underscores)

Notes
- We rely on the `edgar` library, wrapped by clients/edgar_client.py.
- Validation is applied for HTML saves to avoid corrupt pages; fallback to TXT
  if HTML is missing or fails validation.
"""
from __future__ import annotations

import sys
import time
from typing import List, Optional

try:
    # Preferred: when run as a module (python -m restructured_code.main.sec.downloads.interactive)
    from ..config import load_config
    from ..clients.edgar_client import (
        init_identity,
        list_filings_for_ticker,
        fetch_html,
        fetch_text,
        FilingRef,
        fetch_best_content,
        get_filing_details,
    )
    from ..storage.backends import LocalStorage
    from .file_naming import build_rel_paths
    from .validator import basic_html_check, smoke_test_def14a
    from .data_index import DataIndex
    from ..utils.ticker_lookup import tickers_by_permnos
    from ..utils.list_loader import load_tickers_from_file, load_permnos_from_file
except ImportError:
    # Fallback: allow running this file directly via python path/to/interactive.py
    import os, sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
    from restructured_code.main.sec.config import load_config
    from restructured_code.main.sec.clients.edgar_client import (
        init_identity,
        list_filings_for_ticker,
        fetch_html,
        fetch_text,
        FilingRef,
        fetch_best_content,
        get_filing_details,
    )
    from restructured_code.main.sec.storage.backends import LocalStorage
    from restructured_code.main.sec.downloads.file_naming import build_rel_paths
    from restructured_code.main.sec.downloads.validator import basic_html_check, smoke_test_def14a
    from restructured_code.main.sec.downloads.data_index import DataIndex
    from restructured_code.main.sec.utils.ticker_lookup import tickers_by_permnos
    from restructured_code.main.sec.utils.list_loader import load_tickers_from_file, load_permnos_from_file


FORMS = {
    "1": "10-K",
    "2": "DEF 14A",
    "3": "10-Q",
    "4": "13F-HR",
    "5": "8-K",
    "6": "3",
    "7": "4",
    "8": "5",
    "9": "NPORT-P",
    "10": "D",
    "11": "C",
    "12": "MA-I",
    "13": "144",
}


def _prompt_forms() -> Optional[str]:
    print("Select the form type:")
    for k, v in FORMS.items():
        print(f"{k}. {v}")
    choice = input("Enter the number corresponding to the form type: ").strip()
    form = FORMS.get(choice)
    if not form:
        print("Invalid form choice. Exiting.")
        return None
    return form


def _prompt_tickers() -> List[str]:
    tickers_input = input("Enter ticker(s), separated by commas if multiple: ").strip()
    return [t.strip().upper() for t in tickers_input.split(",") if t.strip()]


def _prompt_permnos() -> List[str]:
    permnos_input = input("Enter PERMNO(s), separated by commas (optional, leave blank to skip): ").strip()
    return [p.strip() for p in permnos_input.split(",") if p.strip()]


def _prompt_tickers_file() -> str:
    return input("Tickers file path (CSV/TSV/TXT, optional): ").strip()


def _prompt_permnos_file() -> str:
    return input("PERMNOs file path (CSV/TSV/TXT, optional): ").strip()


def _prompt_save_method() -> Optional[str]:
    print("\nSelect the save method for each filing:")
    print("1. Open in browser")
    print("2. Save as text")
    print("3. Save as HTML (fallback to text)")
    save_choice = input("Enter the number corresponding to the save method: ").strip()
    if save_choice == "1":
        return "open"
    if save_choice == "2":
        return "txt"
    if save_choice == "3":
        return "html"
    print("Invalid save method choice. Exiting.")
    return None


def _prompt_base_dir(default_root: str) -> str:
    print("\nChoose base folder to download into.")
    print("- Leave blank to use default data root.")
    base = input("Base folder path (will create/use <base>/data): ").strip()
    if base:
        return base
    return ""


def _prompt_dry_run() -> bool:
    ans = input("Dry run (only list missing, no downloads)? [y/N]: ").strip().lower()
    return ans in {"y", "yes"}


def _save_html(storage: LocalStorage, index: DataIndex, ticker: str, form: str, f: FilingRef, html_bytes: bytes, min_size: int, smoke_def14a: bool, extra_meta: dict | None = None) -> None:
    html_rel, _ = build_rel_paths(storage.root.as_posix(), ticker, form, f.filing_date)
    ok, reason = basic_html_check(html_bytes, min_size)
    meta = {
        "ticker": ticker,
        "form": form,
        "filing_date": f.filing_date,
        "url": f.url,
        "saved_as": "html",
        "html_ok": ok,
        "html_reason": reason,
    }
    if extra_meta:
        meta.update(extra_meta)
    if not ok:
        raise ValueError("HTML validation failed")
    # Optional smoke test for DEF 14A
    if smoke_def14a and form.strip().upper() == "DEF 14A":
        meta["extract_smoke_ok"] = bool(smoke_test_def14a(html_bytes))
    res = storage.save_html(html_rel, html_bytes)
    meta.update({"size": res.size, "sha256": res.sha256})
    storage.write_meta(html_rel, meta)
    index.record(meta, relpath=html_rel)
    print(f"Saved HTML to {res.path}")


def _save_txt(storage: LocalStorage, index: DataIndex, ticker: str, form: str, f: FilingRef, txt: str, extra_meta: dict | None = None) -> None:
    _, txt_rel = build_rel_paths(storage.root.as_posix(), ticker, form, f.filing_date)
    res = storage.save_text(txt_rel, txt)
    meta = {
        "ticker": ticker,
        "form": form,
        "filing_date": f.filing_date,
        "url": f.url,
        "saved_as": "txt",
        "size": res.size,
        "sha256": res.sha256,
    }
    if extra_meta:
        meta.update(extra_meta)
    storage.write_meta(txt_rel, meta)
    index.record(meta, relpath=txt_rel)
    print(f"Saved TXT to {res.path}")


def run_interactive() -> int:
    cfg = load_config()
    init_identity(cfg.user_agent)
    # Prompt for base directory; always nest a 'data' folder when provided
    base_dir = _prompt_base_dir(cfg.data_root)
    if base_dir:
        from pathlib import Path as _P
        effective_root = (_P(base_dir).expanduser() / "data").as_posix()
        storage = LocalStorage(effective_root)
        print(f"Using data root: {storage.root}")
    else:
        storage = LocalStorage(cfg.data_root)
        print(f"Using default data root: {storage.root}")
    # Load or initialize dataset index
    index = DataIndex.load(storage.root)
    if not (storage.root / "metadata.json").exists():
        added = index.scan_from_sidecars(storage.root)
        index.save()
        if added:
            print(f"Initialized index from {added} existing sidecar metas.")

    form = _prompt_forms()
    if not form:
        return 1
    tickers = _prompt_tickers()
    permnos = _prompt_permnos()
    tickers_file = _prompt_tickers_file()
    permnos_file = _prompt_permnos_file()
    # Merge from files if provided
    if tickers_file:
        try:
            t_from_file = load_tickers_from_file(tickers_file)
            tickers = (tickers or []) + t_from_file
            print(f"Loaded {len(t_from_file)} tickers from file.")
        except Exception as e:
            print(f"Could not load tickers from file: {e}")
    if permnos_file:
        try:
            p_from_file = load_permnos_from_file(permnos_file)
            permnos = (permnos or []) + p_from_file
            print(f"Loaded {len(p_from_file)} PERMNOs from file.")
        except Exception as e:
            print(f"Could not load PERMNOs from file: {e}")
    if permnos:
        resolved = tickers_by_permnos(permnos)
        if resolved:
            print(f"Resolved {len(resolved)} tickers from PERMNOs.")
            # Dedupe preserving order: resolved first, then typed/file tickers
            merged: List[str] = []
            seen: set[str] = set()
            for t in resolved + (tickers or []):
                tu = t.upper()
                if tu not in seen:
                    merged.append(tu)
                    seen.add(tu)
            tickers = merged
        else:
            print("No tickers resolved from given PERMNOs.")
    if not tickers:
        print("No tickers provided. Exiting.")
        return 1
    method = _prompt_save_method()
    if not method:
        return 1
    dry_run = _prompt_dry_run()

    for ticker in tickers:
        print("\n" + "=" * 40)
        print(f"Processing ticker: {ticker}")
        print("=" * 40)
        try:
            filings = list_filings_for_ticker(ticker, form)
            if not filings:
                print(f"No filings found for {ticker} with form {form}.")
                continue
            print(f"Found {len(filings)} filings for form {form}.")

            if dry_run:
                missing = [f for f in filings if f.filing_date and not index.has_filing(ticker, form, f.filing_date)]
                if missing:
                    print("Missing filings (would download):")
                    for i, f in enumerate(missing, start=1):
                        print(f"  {i}. {ticker} {form} {f.filing_date} -> {f.url}")
                else:
                    print("All listed filings already present.")
                continue

            for idx, f in enumerate(filings, start=1):
                fd = f.filing_date or f"index_{idx}"
                print(f"Filing {idx}: Date: {fd}, URL: {f.url}")

                # Skip using index
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

                if method == "open":
                    try:
                        # Prefer library .open(), else fallback to webbrowser
                        if getattr(f._filing_obj, "open", None):
                            f._filing_obj.open()
                        else:
                            import webbrowser
                            webbrowser.open(f.url)
                        print(f"Opened filing {idx} in browser.")
                        time.sleep(0.2)
                    except Exception as e:
                        print(f"Error opening filing {idx} in browser: {e}")
                    continue

                if method == "txt":
                    try:
                        txt = fetch_text(f)
                        if txt:
                            _save_txt(storage, index, ticker, form, f, txt)
                        else:
                            print(f"No text available for filing {idx}")
                    except Exception as e:
                        print(f"Error saving filing {idx} as text: {e}")
                    continue

                # method == "html" (with fallback)
                try:
                    saved_as, content, doc_url = fetch_best_content(f)
                    details = get_filing_details(f)
                    # Build extra meta envelope
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
                        _save_html(storage, index, ticker, form, f, content, cfg.min_html_size_bytes, cfg.smoke_test_def14a, extra_meta=extra)
                    elif saved_as == "txt" and isinstance(content, str):
                        _save_txt(storage, index, ticker, form, f, content, extra_meta=extra)
                    else:
                        print(f"Filing {idx}: neither valid HTML nor TXT available.")
                except Exception as e:
                    print(f"Error retrieving filing {idx}: {e}")
        finally:
            # Persist index after finishing each ticker (robust against long sessions)
            try:
                index.save()
            except Exception:
                pass

    print("\nDone.")
    try:
        index.save()
    except Exception:
        pass
    return 0


def main() -> int:
    try:
        return run_interactive()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
