"""Interactive downloader (terminal prompts), preserving legacy behavior.

Features
- Menu of forms (10-K, DEF 14A, 10-Q, 13F-HR, 8-K, 3/4/5, NPORT-P, D, C, MA-I, 144)
- Accept multiple tickers (comma-separated)
- Save methods:
  1) Open in browser (local convenience; not for HPC)
  2) Save as plain text (.txt)
  3) Save as HTML (.html); if HTML not available or invalid, fall back to .txt

Output naming (new): data/<TICKER>/<FORM>/<DATE>_<FORM>.<ext>

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
    )
    from ..storage.backends import LocalStorage
    from .file_naming import build_rel_paths
    from .validator import basic_html_check, smoke_test_def14a
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
    )
    from restructured_code.main.sec.storage.backends import LocalStorage
    from restructured_code.main.sec.downloads.file_naming import build_rel_paths
    from restructured_code.main.sec.downloads.validator import basic_html_check, smoke_test_def14a


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


def _save_html(storage: LocalStorage, ticker: str, form: str, f: FilingRef, html_bytes: bytes, min_size: int, smoke_def14a: bool) -> None:
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
    if not ok:
        raise ValueError("HTML validation failed")
    # Optional smoke test for DEF 14A
    if smoke_def14a and form.strip().upper() == "DEF 14A":
        meta["extract_smoke_ok"] = bool(smoke_test_def14a(html_bytes))
    res = storage.save_html(html_rel, html_bytes)
    meta.update({"size": res.size, "sha256": res.sha256})
    storage.write_meta(html_rel, meta)
    print(f"Saved HTML to {res.path}")


def _save_txt(storage: LocalStorage, ticker: str, form: str, f: FilingRef, txt: str) -> None:
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
    storage.write_meta(txt_rel, meta)
    print(f"Saved TXT to {res.path}")


def run_interactive() -> int:
    cfg = load_config()
    init_identity(cfg.user_agent)
    storage = LocalStorage(cfg.data_root)

    form = _prompt_forms()
    if not form:
        return 1
    tickers = _prompt_tickers()
    if not tickers:
        print("No tickers provided. Exiting.")
        return 1
    method = _prompt_save_method()
    if not method:
        return 1

    for ticker in tickers:
        print("\n" + "=" * 40)
        print(f"Processing ticker: {ticker}")
        print("=" * 40)
        filings = list_filings_for_ticker(ticker, form)
        if not filings:
            print(f"No filings found for {ticker} with form {form}.")
            continue
        print(f"Found {len(filings)} filings for form {form}.")

        for idx, f in enumerate(filings, start=1):
            fd = f.filing_date or f"index_{idx}"
            print(f"Filing {idx}: Date: {fd}, URL: {f.url}")
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
                        _save_txt(storage, ticker, form, f, txt)
                    else:
                        print(f"No text available for filing {idx}")
                except Exception as e:
                    print(f"Error saving filing {idx} as text: {e}")
                continue

            # method == "html" (with fallback)
            try:
                html_bytes = fetch_html(f)
                if html_bytes:
                    try:
                        _save_html(storage, ticker, form, f, html_bytes, cfg.min_html_size_bytes, cfg.smoke_test_def14a)
                        continue
                    except Exception as ve:
                        print(f"HTML invalid for filing {idx} ({ve}); attempting TXT fallback…")
                # either no HTML or invalid — try TXT
                txt = fetch_text(f)
                if txt:
                    _save_txt(storage, ticker, form, f, txt)
                else:
                    print(f"Filing {idx}: neither valid HTML nor TXT available.")
            except Exception as e:
                print(f"Error retrieving filing {idx}: {e}")

    print("\nDone.")
    return 0


def main() -> int:
    try:
        return run_interactive()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
