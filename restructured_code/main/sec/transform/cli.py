from __future__ import annotations

import argparse
import os
from pathlib import Path
from time import perf_counter
from typing import List

try:
    from tqdm import tqdm  # type: ignore
    _HAS_TQDM = True
except Exception:
    _HAS_TQDM = False

from ..config import load_config
from ..downloads.file_naming import normalize_form_for_fs
from .html_to_json import process_html_file_to_json


def iter_extracted_htmls(data_root: Path, ticker: str, form: str) -> List[Path]:
    t = ticker.upper()
    f_fs = normalize_form_for_fs(form)
    base = data_root / t / f_fs / "extracted"
    if not base.exists():
        return []
    return sorted(base.glob("*_SCT.html"))


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser("Convert extracted SCT HTMLs to JSON")
    ap.add_argument("--base", default="", help="Data root (overrides SEC_DATA_ROOT for this run)")
    ap.add_argument("--form", default="DEF 14A", help="Form name (default: DEF 14A)")
    ap.add_argument("--tickers", help="Comma-separated tickers (default: all detected under base/form/extracted)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing JSON files")
    ap.add_argument("--no-progress", action="store_true", help="Disable progress bar")
    args = ap.parse_args(argv)

    if args.base:
        os.environ["SEC_DATA_ROOT"] = args.base
        print(f"Using data root: {args.base}")

    cfg = load_config()
    data_root = Path(cfg.data_root)

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        # Detect tickers by presence of extracted HTMLs
        tickers = []
        f_fs = normalize_form_for_fs(args.form)
        for child in data_root.iterdir():
            if not child.is_dir() or child.name.startswith("."):
                continue
            extracted_dir = child / f_fs / "extracted"
            if extracted_dir.exists() and any(extracted_dir.glob("*_SCT.html")):
                tickers.append(child.name.upper())
        tickers = sorted(set(tickers))

    total = 0
    total_start = perf_counter()
    use_bar = _HAS_TQDM and not args.no_progress
    iterable = tqdm(tickers, desc="Tickers", unit="ticker") if use_bar else enumerate(tickers, start=1)
    for idx, t in (enumerate(iterable, start=1) if use_bar else iterable):
        start = perf_counter()
        htmls = iter_extracted_htmls(data_root, t, args.form)
        if not htmls:
            print(f"{t}: no extracted HTMLs found")
            continue
        written = 0
        for hp in htmls:
            out_json = data_root / t / normalize_form_for_fs(args.form) / "json" / (hp.stem.replace("_SCT", "_SCT") + ".json")
            if out_json.exists() and not args.overwrite:
                continue
            res = process_html_file_to_json(hp, form=args.form)
            if res:
                total += 1
                written += 1
        elapsed = perf_counter() - start
        elapsed_total = perf_counter() - total_start
        avg_per_ticker = elapsed_total / idx
        remaining = avg_per_ticker * (len(tickers) - idx)
        line = f"[{idx}/{len(tickers)}] {t}: processed {len(htmls)} HTML files, wrote {written} JSONs in {elapsed:.2f}s"
        if not use_bar:
            print(f"{line} | est remaining {remaining:.2f}s")
        else:
            iterable.set_postfix_str(f"{written} jsons, {elapsed:.2f}s")
    total_elapsed = perf_counter() - total_start
    if use_bar:
        iterable.close()
    print(f"Done. JSON files written: {total} in {total_elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
