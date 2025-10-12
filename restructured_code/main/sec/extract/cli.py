from __future__ import annotations

import argparse
from typing import List, Optional

from .runner import extract_for_ticker, detect_tickers_with_form_htmls


def main(argv: List[str] = None) -> int:
    ap = argparse.ArgumentParser("SCT extractor (XPath-first)")
    ap.add_argument("--tickers", help="Comma-separated tickers, e.g., NVDA,MSFT")
    ap.add_argument("--all", action="store_true", help="Process all detected tickers that have HTML for the form")
    ap.add_argument("--form", default="DEF 14A", help="Form name (default: DEF 14A)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing CSVs")
    ap.add_argument("--limit", type=int, default=None, help="Process only first N HTML files per ticker")

    args = ap.parse_args(argv)

    if args.all:
        tickers = detect_tickers_with_form_htmls(form=args.form)
        if not tickers:
            print(f"No tickers with HTML found for form '{args.form}'.")
            return 1
        print(f"Detected {len(tickers)} tickers for form '{args.form}'.")
    else:
        if not args.tickers:
            ap.error("--tickers is required unless --all is specified")
            return 2
        tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()]

    for t in tickers:
        outs = extract_for_ticker(t, form=args.form, overwrite=args.overwrite, limit=args.limit)
        print(f"{t}: {len(outs)} files extracted")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
