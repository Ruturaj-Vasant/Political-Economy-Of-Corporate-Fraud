#!/usr/bin/env python3
"""
Merge SCT combined JSONs into the master company JSON.

Usage (in-place overwrite master):
  python -m restructured_code.main.sec.transform.merge_combined_into_master \
    --master restructured_code/json/sec_company_tickers.json \
    --data-root /path/to/data \
    --form "DEF 14A"

Limit to specific tickers (comma-separated) and write to a new file:
  python -m ...merge_combined_into_master \
    --tickers ABMD,ECL \
    --output /tmp/sec_company_tickers_with_sct.json
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# Support execution as a module or as a standalone script.
try:
    from restructured_code.main.sec.transform.html_to_json import normalize_form_for_fs
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parents[4]))
    from restructured_code.main.sec.transform.html_to_json import normalize_form_for_fs

from tqdm import tqdm


def load_master(master_path: Path) -> Dict:
    """Load the master company JSON."""
    return json.loads(master_path.read_text())


def build_ticker_index(master: Dict) -> Dict[str, str]:
    """
    Build a mapping from TICKER -> master key (master is keyed by '0','1',...).
    Keeps master untouched; keys are uppercased for stable lookup.
    """
    idx: Dict[str, str] = {}
    for k, entry in master.items():
        ticker = entry.get("ticker")
        if ticker:
            idx[ticker.upper()] = k
    return idx


def discover_ticker_dirs(data_root: Path) -> List[str]:
    """List top-level directory names under data_root (expected ticker folders)."""
    return sorted([p.name for p in data_root.iterdir() if p.is_dir()])


def load_combined_json(ticker: str, data_root: Path, form: str) -> Optional[Dict]:
    """
    Load {ticker}_SCT_combined.json if present and non-empty.
    Returns parsed dict or None if missing/empty.
    """
    form_fs = normalize_form_for_fs(form)
    path = data_root / ticker / form_fs / "json" / f"{ticker}_SCT_combined.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    sct = data.get("summary_compensation_table") or {}
    return data if sct else None


def attach_sct_to_master(master: Dict, ticker_index: Dict[str, str], ticker: str, combined: Dict) -> bool:
    """
    Attach combined SCT payload to the matching master entry.
    Returns True if attached; False if ticker missing in master.
    """
    key = ticker_index.get(ticker.upper())
    if key is None:
        return False
    entry = master[key]
    # Overwrite/insert SCT-related fields; leave the rest untouched.
    entry["summary_compensation_table"] = combined.get("summary_compensation_table", {})
    if "report_years" in combined:
        entry["summary_compensation_years"] = combined["report_years"]
    return True


def safe_backup(path: Path) -> None:
    """Create a timestamped backup alongside master."""
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak-{ts}")
    shutil.copy2(path, bak)


def parse_tickers_arg(raw: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated tickers; return None if not provided."""
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
    """
    Perform the merge.
    Returns counts: (scanned, attached, missing_in_master, missing_combined).
    """
    master = load_master(master_path)
    ticker_index = build_ticker_index(master)

    # Determine which tickers to process.
    if tickers:
        target_tickers = [t.upper() for t in tickers]
    else:
        target_tickers = discover_ticker_dirs(data_root)

    scanned = attached = missing_master = missing_combined = 0

    iterator = tqdm(target_tickers, desc="Tickers", unit="ticker") if use_progress else target_tickers

    for t in iterator:
        scanned += 1
        combined = load_combined_json(t, data_root, form)
        if not combined:
            missing_combined += 1
            continue
        ok = attach_sct_to_master(master, ticker_index, t, combined)
        if ok:
            attached += 1
        else:
            missing_master += 1

    # Write out (in-place or to a specified output).
    dest = output_path or master_path
    if dest == master_path and not no_backup:
        safe_backup(master_path)
    dest.write_text(json.dumps(master, indent=2))

    return scanned, attached, missing_master, missing_combined


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge SCT combined JSONs into master.")
    parser.add_argument("--master", required=True, help="Path to master JSON.")
    parser.add_argument("--data-root", required=True, help="Data root with ticker folders.")
    parser.add_argument("--form", default="DEF 14A", help='Form name (default: "DEF 14A").')
    parser.add_argument(
        "--tickers",
        help="Comma-separated tickers to process (default: all ticker folders in data-root).",
    )
    parser.add_argument(
        "--output",
        help="Optional output path. If omitted, master is overwritten (backup created unless --no-backup).",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating a .bak of the master when overwriting in place.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bar.",
    )
    args = parser.parse_args()

    master_path = Path(args.master).expanduser().resolve()
    data_root = Path(args.data_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve() if args.output else None
    tickers = parse_tickers_arg(args.tickers)

    scanned, attached, missing_master, missing_combined = merge_all(
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
    print(f"Missing/empty combined files: {missing_combined}")
    print(f"Master written to: {output_path or master_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
