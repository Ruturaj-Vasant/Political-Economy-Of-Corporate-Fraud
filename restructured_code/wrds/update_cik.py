import argparse
import json
from pathlib import Path
from typing import Dict, Any
import pandas as pd

from .cik_enrich import enrich_cik


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r") as f:
        return json.load(f)


def write_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(data, f, indent=2)
    tmp.replace(path)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser("Fill missing CIKs in JSON using WRDS")
    ap.add_argument("--input", required=True, help="Input JSON path (inside restructured_code/json)")
    ap.add_argument("--output", help="Output JSON path; if omitted with --in-place, overwrites input")
    ap.add_argument("--in-place", action="store_true", help="Overwrite input file after backup .bak")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--changes-csv", help="Path to write changelog CSV", default=None)

    args = ap.parse_args(argv)
    inp = Path(args.input)
    if not inp.exists():
        raise SystemExit(f"Input not found: {inp}")

    data = load_json(inp)
    if not isinstance(data, dict):
        raise SystemExit("Unsupported JSON format: expected dict of records")

    updated, changes_df = enrich_cik(data, batch_size=args.batch_size, limit=args.limit, dry_run=args.dry_run)

    if args.dry_run:
        print(f"Dry run complete. {len(changes_df)} CIK changes would be applied.")
    else:
        if args.in_place:
            bak = inp.with_suffix(inp.suffix + ".bak")
            bak.write_text(inp.read_text())
            write_json_atomic(inp, updated)
            out_path = inp
        else:
            out_path = Path(args.output) if args.output else inp.with_name(inp.stem + ".cik_enriched.json")
            write_json_atomic(out_path, updated)
        print(f"Wrote updated JSON to {out_path}")

    if args.changes_csv:
        Path(args.changes_csv).parent.mkdir(parents=True, exist_ok=True)
        changes_df.to_csv(args.changes_csv, index=False)
        print(f"Wrote changes CSV to {args.changes_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

