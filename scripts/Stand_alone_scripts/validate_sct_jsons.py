#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Tuple


def find_per_report_jsons(base: Path) -> List[Path]:
    """List all per-report SCT JSON files (excluding combined files)."""
    return [
        fp
        for fp in base.rglob("*_SCT.json")
        if not fp.name.endswith("_SCT_combined.json")
    ]


def summarize_combined_jsons(base: Path) -> Tuple[int, List[Path], List[Tuple[Path, Exception]]]:
    """
    Count report_date entries across all combined SCT JSONs.
    Returns (total_entries, combined_files, errors).
    """
    total_entries = 0
    files: List[Path] = []
    errors: List[Tuple[Path, Exception]] = []
    for fp in base.rglob("*_SCT_combined.json"):
        files.append(fp)
        try:
            data = json.loads(fp.read_text())
            total_entries += len(data.get("summary_compensation_table", {}))
        except Exception as exc:
            errors.append((fp, exc))
    return total_entries, files, errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate SCT JSON counts.")
    parser.add_argument(
        "--base",
        default="data",
        help="Data root containing ticker/form/json folders (default: data)",
    )
    args = parser.parse_args()

    base = Path(args.base).expanduser().resolve()
    if not base.exists():
        print(f"Base path does not exist: {base}")
        return 1

    per_report = find_per_report_jsons(base)
    total_combined_entries, combined_files, errors = summarize_combined_jsons(base)

    print(f"Base: {base}")
    print(f"Per-report JSONs (validated HTMLs): {len(per_report):,}")
    print(f"Combined JSON files: {len(combined_files):,}")
    print(f"Report dates inside combined files (post-validation): {total_combined_entries:,}")

    if errors:
        print("\nFiles that could not be read/parsed:")
        for fp, exc in errors:
            print(f"- {fp}: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
