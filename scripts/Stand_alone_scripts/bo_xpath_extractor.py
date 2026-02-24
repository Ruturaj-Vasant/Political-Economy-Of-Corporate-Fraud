#!/usr/bin/env python3
"""
Standalone Beneficial Ownership table extractor using XPath.

Extracts all tables matching a Beneficial Ownership-oriented XPath and saves
each table (as-is) into HTML stubs that preserve the original page styling
via head includes and base href.

Usage examples:
  # Single file
  python3 scripts/Stand_alone_scripts/bo_xpath_extractor.py \
    --html data/AA/DEF_14A/2017-03-17_DEF_14A.html

  # Multiple tickers (defaults: --data-root data --form "DEF 14A")
  python3 scripts/Stand_alone_scripts/bo_xpath_extractor.py \
    --tickers AA,ABCP
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Optional

from lxml import html as LH  # type: ignore

# XPath provided for Beneficial Ownership tables
XPATH_BO_TABLES = """
//table[
  .//tr[
    (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'beneficial')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'owner')]
    )
    or
    (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'address')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'beneficial')]
    )
  ]
]
""".strip()


# -------------------------
# Helpers
# -------------------------

def _collect_head_includes(tree) -> str:
    """Collect style/link tags from the original head to preserve styling."""
    nodes = tree.xpath(
        "//head/style | //head/link[translate(@rel, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='stylesheet']"
    )
    fragments: List[str] = []
    for n in nodes:
        fragments.append(LH.tostring(n, encoding="unicode", method="html"))
    return "\n".join(fragments)


def _html_stub(table_html: str, *, base_href: Optional[str], head_includes: str) -> str:
    head_parts = ['<meta charset="utf-8">']
    if base_href:
        head_parts.append(f'<base href="{base_href}">')
    if head_includes:
        head_parts.append(head_includes.strip())
    head = "\n".join(head_parts)
    return (
        "<!doctype html>\n"
        "<html><head>\n"
        f"{head}\n"
        "</head><body>\n"
        f"{table_html}\n"
        "</body></html>\n"
    )


def _derive_ticker_and_date(html_path: Path) -> tuple[str, str]:
    ticker = html_path.parent.parent.name.upper()
    m = re.search(r"\d{4}-\d{2}-\d{2}", html_path.name)
    report_date = m.group(0) if m else html_path.stem
    return ticker, report_date


def _output_path(html_path: Path, idx: int) -> Path:
    ticker, report_date = _derive_ticker_and_date(html_path)
    outdir = html_path.parent / "extracted"
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / f"{ticker}_{report_date}_BOT_{idx}.html"


def _table_shape_ok(tbl) -> bool:
    """Heuristic: require >=2 rows with >=2 meaningful columns to avoid stray cells."""
    rows = tbl.xpath(".//tr")
    if len(rows) < 2:
        return False

    meaningful_rows = 0
    for r in rows:
        cells = r.xpath("./td|./th")
        # Keep cells that have some text content (ignoring pure whitespace)
        texts = [("".join(c.itertext()) or "").strip() for c in cells]
        non_empty = [t for t in texts if t]
        if len(non_empty) >= 2:
            meaningful_rows += 1
    return meaningful_rows >= 2


# -------------------------
# Core extraction
# -------------------------

def extract_bo_tables_from_file(path: Path, max_tables: Optional[int] = None) -> List[str]:
    p = path.expanduser().resolve()
    try:
        html_bytes = p.read_bytes()
    except Exception as e:
        print(f"[ERR] Failed to read {p}: {e}")
        return []

    try:
        tree = LH.fromstring(html_bytes)
    except Exception as e:
        print(f"[ERR] Failed to parse HTML {p}: {e}")
        return []

    tables = tree.xpath(XPATH_BO_TABLES)
    if not tables:
        print(f"[SKIP] {p} (no BO tables found)")
        return []

    head_includes = _collect_head_includes(tree)
    base_href = p.parent.as_uri()
    stubs: List[str] = []

    limit = max_tables if max_tables is not None and max_tables > 0 else len(tables)
    kept = 0
    for tbl in tables:
        if not _table_shape_ok(tbl):
            continue
        kept += 1
        if kept > limit:
            break
        tbl_html = LH.tostring(tbl, encoding="unicode", method="html", with_tail=False)
        stub = _html_stub(tbl_html, base_href=base_href, head_includes=head_includes)
        stubs.append(stub)
    if not stubs:
        print(f"[SKIP] {p} (no BO tables passed shape check)")
    return stubs


def process_file(html_path: Path, max_tables: Optional[int]) -> None:
    stubs = extract_bo_tables_from_file(html_path, max_tables=max_tables)
    if not stubs:
        return
    for idx, stub in enumerate(stubs, start=1):
        out = _output_path(html_path, idx)
        out.write_text(stub, encoding="utf-8")
        print(f"[OK] {html_path} -> {out}")


# -------------------------
# CLI
# -------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract Beneficial Ownership tables via XPath (all matches).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--html", help="Path to a single source HTML file.")
    ap.add_argument(
        "--tickers",
        help="Comma-separated tickers to batch process (finds all *_DEF_14A.html under each).",
    )
    ap.add_argument(
        "--data-root",
        default="data",
        help="Data root containing ticker/form/extracted folders (used when --tickers is provided).",
    )
    ap.add_argument("--form", default="DEF 14A", help='Form name (default: "DEF 14A") for ticker discovery.')
    ap.add_argument(
        "--max-tables",
        type=int,
        default=0,
        help="Maximum tables to save per filing (0 = no limit).",
    )
    args = ap.parse_args()

    if not args.html and not args.tickers:
        ap.error("Provide either --html for a single file or --tickers for batch mode.")

    if args.html:
        html_path = Path(args.html).expanduser().resolve()
        if not html_path.exists():
            print(f"[ERR] HTML not found: {html_path}")
            return 1
        max_tables = args.max_tables if args.max_tables > 0 else None
        process_file(html_path, max_tables=max_tables)
        return 0

    # Batch mode
    data_root = Path(args.data_root).expanduser().resolve()
    form_fs = args.form.replace(" ", "_").upper()
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        ap.error("No tickers parsed from --tickers.")

    for t in tickers:
        base_dir = data_root / t / form_fs
        if not base_dir.exists():
            print(f"[WARN] Missing form dir for {t}: {base_dir}")
            continue
        html_files: Iterable[Path] = sorted(base_dir.glob("*_DEF_14A.html")) or sorted(base_dir.glob("*.html"))
        if not html_files:
            print(f"[WARN] No HTML files for {t} in {base_dir}")
            continue
        max_tables = args.max_tables if args.max_tables > 0 else None
        for hp in html_files:
            process_file(hp, max_tables=max_tables)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
