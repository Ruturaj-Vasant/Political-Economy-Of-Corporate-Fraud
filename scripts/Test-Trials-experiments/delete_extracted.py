#!/usr/bin/env python3
"""
Delete all directories named "extracted" within the project `data` folder
under each ticker. Example target paths:

    data/ABCP/DEF_14A/extracted

Usage:
    python scripts/Test-Trials-experiments/small.py             # prompts before deleting
    python scripts/Test-Trials-experiments/small.py --yes       # delete without prompt
    python scripts/Test-Trials-experiments/small.py --dry-run   # show what would be deleted

Options:
    --data-root PATH  Root folder to search (default: ./data)
    --yes             Skip confirmation prompt
    --dry-run         Do not delete; only list targets
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import List


def find_extracted_dirs(data_root: Path) -> List[Path]:
    """Find candidate `extracted` directories under `data_root`.

    Only returns directories that are at least two levels deep relative to
    `data_root` (e.g., ticker/form/extracted) to avoid accidental matches like
    `data/extracted`.
    """
    if not data_root.exists() or not data_root.is_dir():
        return []

    results: List[Path] = []
    for p in data_root.rglob("*"):
        if not p.is_dir() or p.name not in ("extracted", "extracted_best_clean", "extracted_best_csv"):
            continue
        try:
            rel = p.relative_to(data_root)
        except Exception:
            # Shouldn't happen, but be defensive
            continue
        # Ensure it's nested inside a ticker folder (>= 2 parts before name)
        # e.g., ABCP/DEF_14A/extracted -> parts >= 3
        if len(rel.parts) >= 2 and p.name in ("extracted", "extracted_best_clean", "extracted_best_csv"):
            results.append(p)
    return sorted(results)


def delete_dirs(dirs: List[Path]) -> None:
    for d in dirs:
        # Use shutil.rmtree for recursive deletion
        shutil.rmtree(d, ignore_errors=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete all 'extracted' folders under data/<ticker>/*.")
    parser.add_argument("--data-root", default="data", type=str, help="Root directory to search (default: data)")
    parser.add_argument("--yes", action="store_true", help="Confirm deletion without prompt")
    parser.add_argument("--dry-run", action="store_true", help="List targets without deleting")
    args = parser.parse_args()

    data_root = Path(args.data_root).resolve()
    targets = find_extracted_dirs(data_root)

    if not targets:
        print(f"No 'extracted' directories found under {data_root}.")
        return

    print(f"Found {len(targets)} 'extracted' directorie(s) under {data_root}:")
    for p in targets:
        print(f"  - {p}")

    if args.dry_run:
        print("Dry-run mode: nothing deleted.")
        return

    if not args.yes:
        try:
            resp = input("Proceed to delete all listed directories? [y/N]: ").strip().lower()
        except EOFError:
            resp = "n"
        if resp not in ("y", "yes"):
            print("Aborted.")
            return

    deleted = 0
    for p in targets:
        try:
            delete_dirs([p])
            deleted += 1
            print(f"Deleted: {p}")
        except Exception as e:
            print(f"Failed:  {p} -> {e}")

    print(f"Done. Deleted {deleted}/{len(targets)} directorie(s).")


if __name__ == "__main__":
    main()
