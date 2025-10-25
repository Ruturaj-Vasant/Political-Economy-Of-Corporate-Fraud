"""Trial downloader for DEF 14A: resolves index pages and saves HTML and TXT side-by-side.

This is a safe, standalone script that does NOT touch your dataset index.
It writes into a separate base folder (default: ./trial_data/data).

Usage examples:
  PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.trial --tickers HRL,HSC --limit 5
  PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.trial --tickers-file metadata/tickers.csv --limit 20 --base ./trial_out
  PYTHONPATH=. SEC_USER_AGENT='you@yourdomain' python3 -m restructured_code.main.sec.downloads.trial --tickers AAPL --years 2020:2024
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple
import urllib.request
import urllib.parse

from ..config import load_config
from ..clients.edgar_client import init_identity, list_filings_for_ticker, FilingRef
from ..storage.backends import LocalStorage
from .file_naming import build_rel_paths
from .validator import basic_html_check
from ..utils.list_loader import load_tickers_from_file


def _http_get(url: str, timeout: int = 30) -> Optional[bytes]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": os.getenv("SEC_USER_AGENT", "you@example.com")})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception:
        return None


def _resolve_from_index(index_html: bytes, index_url: str, desired_form: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (html_doc_url, txt_doc_url) from an index page.

    Uses BeautifulSoup if available; otherwise falls back to a basic regex.
    """
    base = index_url
    html_url: Optional[str] = None
    txt_url: Optional[str] = None
    try:
        from bs4 import BeautifulSoup  # type: ignore
        soup = BeautifulSoup(index_html, "lxml")
        tables = soup.find_all("table", {"class": re.compile(r"tableFile", re.I)})
        # collect candidates
        rows = []
        for tbl in tables:
            rows.extend(tbl.find_all("tr")[1:])
        # Prefer DEF 14A type row for HTML
        dnorm = desired_form.replace(" ", "").upper()
        for tr in rows:
            tds = tr.find_all(["td", "th"]) or []
            ttext = " ".join(td.get_text(" ", strip=True) for td in tds)
            a = tr.find("a")
            href = a.get("href") if a and a.get("href") else None
            if not href:
                continue
            url = urllib.parse.urljoin(base, href)
            lower = href.lower()
            if dnorm in ttext.replace(" ", "").upper() and (lower.endswith(".htm") or lower.endswith(".html")):
                html_url = url
                break
        # Fallback: first HTML on page
        if not html_url:
            a = soup.find("a", href=re.compile(r"\.(htm|html)$", re.I))
            if a and a.get("href"):
                html_url = urllib.parse.urljoin(base, a.get("href"))
        # TXT: first .txt link (often the submission text file)
        a_txt = soup.find("a", href=re.compile(r"\.txt$", re.I))
        if a_txt and a_txt.get("href"):
            txt_url = urllib.parse.urljoin(base, a_txt.get("href"))
        return html_url, txt_url
    except Exception:
        pass
    # Regex fallback
    try:
        html = index_html.decode("utf-8", errors="ignore")
    except Exception:
        html = ""
    mhtml = re.search(r"href=\"([^\"]+\.(?:htm|html))\"", html, re.I)
    mtxt = re.search(r"href=\"([^\"]+\.txt)\"", html, re.I)
    if mhtml:
        html_url = urllib.parse.urljoin(base, mhtml.group(1))
    if mtxt:
        txt_url = urllib.parse.urljoin(base, mtxt.group(1))
    return html_url, txt_url


def _parse_years(s: Optional[str]) -> Optional[Tuple[int, int]]:
    if not s:
        return None
    if ":" in s:
        a, b = s.split(":", 1)
        return (int(a), int(b))
    y = int(s)
    return (y, y)


def run_trial(tickers: List[str], base: Path, years: Optional[Tuple[int, int]], limit: int, dry_run: bool) -> None:
    cfg = load_config()
    init_identity(cfg.user_agent)
    data_root = (base / "data").resolve()
    storage = LocalStorage(data_root.as_posix())

    print(f"Trial data root: {data_root}")
    saved_counts = {"html": 0, "txt": 0}
    for ti, ticker in enumerate(tickers, start=1):
        if limit and ti > limit:
            break
        print("\n" + "=" * 40)
        print(f"Ticker: {ticker}")
        filings = list_filings_for_ticker(ticker, "DEF 14A")
        if not filings:
            print("  No filings found.")
            continue
        print(f"  Found {len(filings)} filings.")
        for idx, f in enumerate(filings, start=1):
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
            print(f"  Filing {idx}: {f.filing_date} -> {f.url}")
            html_rel, txt_rel = build_rel_paths(data_root.as_posix(), ticker, "DEF 14A", f.filing_date)

            html_url = None
            txt_url = None
            if re.search(r"-index\.htm[l]?$", f.url, re.I):
                idx_bytes = _http_get(f.url)
                if not idx_bytes:
                    print("    Could not fetch index page.")
                    continue
                html_url, txt_url = _resolve_from_index(idx_bytes, f.url, "DEF 14A")
            else:
                # Direct link
                lower = f.url.lower()
                if lower.endswith(".txt"):
                    txt_url = f.url
                else:
                    html_url = f.url

            # Save HTML
            if html_url:
                html_bytes = _http_get(html_url)
                if html_bytes:
                    ok, reason = basic_html_check(html_bytes, cfg.min_html_size_bytes)
                    if ok and not dry_run:
                        res = storage.save_html(html_rel, html_bytes)
                        meta = {
                            "ticker": ticker,
                            "form": "DEF 14A",
                            "filing_date": f.filing_date,
                            "url": f.url,
                            "doc_url": html_url,
                            "doc_type": "html",
                            "saved_as": "html",
                            "html_ok": True,
                            "html_reason": "ok",
                            "size": res.size,
                            "sha256": res.sha256,
                        }
                        storage.write_meta(html_rel, meta)
                        saved_counts["html"] += 1
                        print(f"    Saved HTML -> {res.path}")
                    else:
                        print(f"    HTML invalid ({reason}); not saved")
                else:
                    print("    HTML not fetched")
            else:
                print("    No HTML doc link found")

            # Save TXT
            if txt_url:
                txt_bytes = _http_get(txt_url)
                if txt_bytes is not None:
                    try:
                        txt = txt_bytes.decode("utf-8", errors="ignore")
                    except Exception:
                        txt = txt_bytes.decode("latin-1", errors="ignore")
                    if not dry_run:
                        res2 = storage.save_text(txt_rel, txt)
                        meta2 = {
                            "ticker": ticker,
                            "form": "DEF 14A",
                            "filing_date": f.filing_date,
                            "url": f.url,
                            "doc_url": txt_url,
                            "doc_type": "txt",
                            "saved_as": "txt",
                            "size": res2.size,
                            "sha256": res2.sha256,
                        }
                        storage.write_meta(txt_rel, meta2)
                        saved_counts["txt"] += 1
                        print(f"    Saved TXT -> {res2.path}")
                else:
                    print("    TXT not fetched")
            else:
                print("    No TXT doc link found")

    print("\nSummary:")
    print(f"  HTML saved: {saved_counts['html']}")
    print(f"  TXT saved:  {saved_counts['txt']}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Trial resolver for DEF 14A (saves HTML and TXT side-by-side)")
    ap.add_argument("--tickers", default="", help="Comma-separated tickers")
    ap.add_argument("--tickers-file", default="", help="CSV/TSV/TXT with tickers (optional)")
    ap.add_argument("--base", default="./trial_data", help="Base folder (data saved under <base>/data)")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of tickers to process")
    ap.add_argument("--years", default="", help="Year range 'YYYY0:YYYY1' or single 'YYYY' (blank = all)")
    ap.add_argument("--dry-run", action="store_true", help="Resolve and print links without saving")
    args = ap.parse_args(argv)

    tickers: List[str] = []
    if args.tickers:
        tickers.extend([t.strip().upper() for t in args.tickers.split(",") if t.strip()])
    if args.tickers_file:
        try:
            tickers.extend(load_tickers_from_file(args.tickers_file))
        except Exception as e:
            print(f"Could not load tickers from file: {e}")
    # de-dupe preserve order
    seen = set()
    uniq: List[str] = []
    for t in tickers:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    tickers = uniq
    if not tickers:
        print("No tickers provided.")
        return 2

    base = Path(args.base).expanduser()
    years = _parse_years(args.years)
    run_trial(tickers, base, years, args.limit, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

