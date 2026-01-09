from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

from ..config import load_config
from ..downloads.file_naming import normalize_form_for_fs


def load_master(path: Path) -> Dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def write_master(path: Path, data: Dict) -> None:
    path.write_text(json.dumps(data, indent=2))


def merge_one_ticker(
    ticker: str,
    form: str,
    data_root: Path,
    master_path: Path,
) -> bool:
    t = ticker.upper()
    form_fs = normalize_form_for_fs(form)
    combined_path = data_root / t / form_fs / "json" / f"{t}_SCT_combined.json"
    if not combined_path.exists():
        print(f"{t}: combined file not found at {combined_path}")
        return False
    try:
        combined = json.loads(combined_path.read_text())
    except Exception:
        print(f"{t}: failed to read combined file {combined_path}")
        return False

    master = load_master(master_path)
    # Normalize master to dict keyed by ticker if it's a list
    if isinstance(master, list):
        master_dict = {str(entry.get("ticker", "")).upper(): entry for entry in master}
    elif isinstance(master, dict):
        master_dict = {k.upper(): v for k, v in master.items()}
    else:
        master_dict = {}

    entry = master_dict.get(t, {"ticker": t})
    sct = combined.get("summary_compensation_table", {})
    if sct:
        entry["summary_compensation_table"] = sct
    ry = combined.get("report_years")
    if ry is not None:
        entry["sct_report_years"] = ry
    master_dict[t] = entry

    # Write back in the same shape as loaded
    if isinstance(master, list):
        master_out = list(master_dict.values())
    else:
        master_out = master_dict
    write_master(master_path, master_out)
    print(f"{t}: merged combined SCT into {master_path}")
    return True


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser("Merge combined SCT JSON into sec_company_tickers.json")
    ap.add_argument("--base", default="", help="Data root (overrides SEC_DATA_ROOT)")
    ap.add_argument("--form", default="DEF 14A", help="Form name (default: DEF 14A)")
    ap.add_argument("--tickers", required=True, help="Comma-separated tickers to merge")
    ap.add_argument("--master", default="restructured_code/json/sec_company_tickers.json", help="Path to master tickers JSON")
    args = ap.parse_args(argv)

    if args.base:
        # override SEC_DATA_ROOT for this run
        pass

    cfg = load_config()
    if args.base:
        data_root = Path(args.base)
    else:
        data_root = Path(cfg.data_root)

    master_path = Path(args.master)
    merged = 0
    for t in [x.strip().upper() for x in args.tickers.split(",") if x.strip()]:
        if merge_one_ticker(t, args.form, data_root, master_path):
            merged += 1
    print(f"Done. tickers merged: {merged}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
