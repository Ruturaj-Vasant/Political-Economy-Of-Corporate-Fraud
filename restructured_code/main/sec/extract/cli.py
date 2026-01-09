from __future__ import annotations

import argparse
import os
try:
    from tqdm import tqdm  # type: ignore
    _HAS_TQDM = True
except Exception:
    _HAS_TQDM = False
from typing import List, Optional
from pathlib import Path
import os
from ..config import load_config
from .run_log import RunFileLogger, TickerStats, new_run_id

from .runner import extract_for_ticker, extract_text_for_ticker, extract_for_ticker_all, detect_tickers_with_form_htmls


def main(argv: List[str] = None) -> int:
    ap = argparse.ArgumentParser("SCT extractor (HTML XPath + TXT regex)")
    ap.add_argument("--tickers", help="Comma-separated tickers, e.g., NVDA,MSFT")
    ap.add_argument("--base", default="", help="Data root folder (overrides SEC_DATA_ROOT for this run)")
    ap.add_argument("--all", action="store_true", help="Process all detected tickers that have HTML for the form")
    ap.add_argument("--form", default="DEF 14A", help="Form name (default: DEF 14A)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    ap.add_argument("--save-parquet", action="store_true", help="(No-op in HTML mode) kept for compatibility")
    ap.add_argument("--limit", type=int, default=None, help="Process only first N files per ticker")
    ap.add_argument("--text-only", action="store_true", help="Process only TXT files (regex snippet)")
    ap.add_argument("--extractor", choices=["xpath", "score", "both"], default="xpath", help="HTML extractor to use (default: xpath)")
    # Default behavior now processes both HTML and TXT; --include-txt no longer required
    ap.add_argument("--no-progress", action="store_true", help="Disable progress bar")

    args = ap.parse_args(argv)

    # Optionally override data root (use env at process start to avoid config cache issues)
    if args.base:
        os.environ["SEC_DATA_ROOT"] = args.base
        print(f"Using data root: {args.base}")

    # Prepare run-file logger (once per ticker)
    data_root = Path(os.environ.get("SEC_DATA_ROOT", "data"))
    run_id = new_run_id()
    run_logger = RunFileLogger(root=data_root, run_id=run_id, form=args.form)
    print(f"Run ID: {run_id}")
    print(f"Run log: {run_logger.path}")

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

    total_extracted = 0
    use_bar = (not args.no_progress) and _HAS_TQDM
    iterator = tqdm(total=len(tickers), desc="Tickers", unit="ticker") if use_bar else None
    try:
        for idx, t in enumerate(tickers, start=1):
            if not use_bar:
                print(f"[{idx}/{len(tickers)}] {t}")
            stats = TickerStats()
            if args.text_only:
                outs = extract_text_for_ticker(
                    t,
                    form=args.form,
                    overwrite=args.overwrite,
                    limit=args.limit,
                    index=None,
                    stats=stats,
                )
            else:
                # Default: process both HTML and TXT
                outs = extract_for_ticker_all(
                    t,
                    form=args.form,
                    overwrite=args.overwrite,
                    limit=args.limit,
                    include_txt=True,
                    save_parquet=args.save_parquet,
                    extractor=args.extractor,
                    index=None,
                    stats=stats,
                )
            # Write per-ticker summary to run log (idempotent within this run)
            run_logger.write_ticker(t, stats)
            total_extracted += len(outs)
            if not use_bar:
                print(f"  -> {len(outs)} files extracted")
            else:
                iterator.update(1)
    finally:
        if iterator is not None:
            iterator.close()
    print(f"Done. Total outputs: {total_extracted}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
