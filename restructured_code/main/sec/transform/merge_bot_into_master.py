#!/usr/bin/env python3
"""
Merge BOT JSONs into the master company JSON under the 'beneficial_ownership' key.

Usage (in-place overwrite master):
  python -m restructured_code.main.sec.transform.merge_bot_into_master \
    --master restructured_code/json/sec_company_tickers.json \
    --data-root /path/to/data \
    --form "DEF 14A"

Limit to specific tickers (comma-separated) and write to a new file:
  python -m ...merge_bot_into_master \
    --tickers ABMD,ECL \
    --output /tmp/sec_company_tickers_with_bot.json
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from restructured_code.main.sec.transform.html_to_json import normalize_form_for_fs
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parents[4]))
    from restructured_code.main.sec.transform.html_to_json import normalize_form_for_fs  # type: ignore

from tqdm import tqdm


def load_master(master_path: Path) -> Dict:
    return json.loads(master_path.read_text())


def build_ticker_index(master: Dict) -> Dict[str, str]:
    idx: Dict[str, str] = {}
    for k, entry in master.items():
        ticker = entry.get("ticker")
        if ticker:
            idx[ticker.upper()] = k
    return idx


def discover_ticker_dirs(data_root: Path) -> List[str]:
    return sorted([p.name for p in data_root.iterdir() if p.is_dir()])


def load_bot_jsons(ticker: str, data_root: Path, form: str) -> List[Dict]:
    form_fs = normalize_form_for_fs(form)
    bot_files = sorted((data_root / ticker / form_fs / "json").glob(f"{ticker}_*_BOT*.json"))
    outs: List[Dict] = []
    for fp in bot_files:
        try:
            data = json.loads(fp.read_text())
        except Exception:
            continue
        bo = data.get("company", {}).get("beneficial_ownership")
        if bo:
            outs.extend(bo if isinstance(bo, list) else [bo])
    return outs


def attach_bot_to_master(master: Dict, ticker_index: Dict[str, str], ticker: str, bot_entries: List[Dict]) -> bool:
    key = ticker_index.get(ticker.upper())
    if key is None:
        return False
    entry = master[key]
    existing = entry.get("beneficial_ownership") or []
    # simple dedupe by report_date + table_kind
    seen = {(e.get("report_date"), e.get("table_kind")) for e in existing if isinstance(e, dict)}
    for bo in bot_entries:
        if not isinstance(bo, dict):
            continue
        rk = (bo.get("report_date"), bo.get("table_kind"))
        if rk in seen:
            continue
        existing.append(bo)
        seen.add(rk)
    entry["beneficial_ownership"] = existing
    return True


def safe_backup(path: Path) -> None:
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak-{ts}")
    shutil.copy2(path, bak)


def parse_tickers_arg(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    return [t.strip().upper() for t in raw.split(",") if t.strip()]


def merge_all(
    master_path: Path,
    data_root: Path,
    form: str,
    tickers: Optional[Iterable[str]],
    output_path: Optional[Path],
    no_backup: bool,
    use_progress: bool,
) -> Tuple[int, int, int, int]:
    master = load_master(master_path)
    ticker_index = build_ticker_index(master)

    target_tickers = [t.upper() for t in tickers] if tickers else discover_ticker_dirs(data_root)

    scanned = attached = missing_master = missing_bot = 0
    iterator = tqdm(target_tickers, desc="Tickers", unit="ticker") if use_progress else target_tickers

    for t in iterator:
        scanned += 1
        bot_entries = load_bot_jsons(t, data_root, form)
        if not bot_entries:
            missing_bot += 1
            continue
        ok = attach_bot_to_master(master, ticker_index, t, bot_entries)
        if ok:
            attached += 1
        else:
            missing_master += 1

    dest = output_path or master_path
    if dest == master_path and not no_backup:
        safe_backup(master_path)
    dest.write_text(json.dumps(master, indent=2))
    return scanned, attached, missing_master, missing_bot


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge BOT JSONs into master (beneficial_ownership field).")
    parser.add_argument("--master", required=True, help="Path to master JSON.")
    parser.add_argument("--data-root", required=True, help="Data root with ticker folders.")
    parser.add_argument("--form", default="DEF 14A", help='Form name (default: "DEF 14A").')
    parser.add_argument("--tickers", help="Comma-separated tickers to process (default: all ticker folders in data-root).")
    parser.add_argument("--output", help="Optional output path. If omitted, master is overwritten (backup created unless --no-backup).")
    parser.add_argument("--no-backup", action="store_true", help="Skip creating a .bak of the master when overwriting in place.")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar.")
    args = parser.parse_args()

    master_path = Path(args.master).expanduser().resolve()
    data_root = Path(args.data_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve() if args.output else None
    tickers = parse_tickers_arg(args.tickers)

    scanned, attached, missing_master, missing_bot = merge_all(
        master_path=master_path,
        data_root=data_root,
        form=args.form,
        tickers=tickers,
        output_path=output_path,
        no_backup=args.no_backup,
        use_progress=not args.no_progress,
    )

    print(f"Scanned tickers: {scanned}")
    print(f"Attached to master: {attached}")
    print(f"Missing in master: {missing_master}")
    print(f"Missing/empty BOT files: {missing_bot}")
    print(f"Master written to: {output_path or master_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
