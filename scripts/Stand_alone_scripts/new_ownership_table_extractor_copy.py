"""Standalone Beneficial Ownership table extractor (DEF 14A / proxy HTML).

Goal
-----
Given an SEC filing HTML (often messy EDGAR HTML), find and extract the table(s)
corresponding to Beneficial Ownership disclosures. Headers vary widely across issuers,
so this script uses a robust signature-based scoring approach:
  - presence of share counts (comma numbers or large ints)
  - presence of percent values (including split '%' cells)
  - text-heavy first column (names/addresses)
  - ownership vocabulary (beneficial, ownership, shares, percent, directors, management)
  - penalties for equity-comp tables (vesting, option awards, exercise price, etc.)

Outputs
-------
Writes up to two HTML stubs containing the extracted tables:
  - beneficial_owner_table.html
  - management_table.html

Usage
-----
python new_ownership_table_extractor.py --input /path/to/filing.html --outdir /path/to/out

Notes
-----
- This script is intentionally standalone (no project imports).
- Designed to be tolerant of EDGAR "gutter" cells and layout tables.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from lxml import html as LH  # type: ignore


# -------------------------
# Text helpers
# -------------------------

def _normalize(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    return " ".join(s.lower().split())


def _node_text(node) -> str:
    return _normalize(" ".join(node.itertext()))


def _html_stub(table_html: str, title: str) -> str:
    return (
        "<!doctype html>\n"
        "<html>\n"
        "<head>\n"
        '  <meta charset=\"utf-8\">\n'
        f"  <title>{title}</title>\n"
        "  <style>\n"
        "    body { font-family: Arial, sans-serif; margin: 24px; }\n"
        "    table { border-collapse: collapse; }\n"
        "    td, th { vertical-align: top; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        f"{table_html}\n"
        "</body>\n"
        "</html>\n"
    )


# -------------------------
# Patterns
# -------------------------

# Comma-formatted share counts: 1,234 or 12,345,678
RE_COMMA_NUM = re.compile(r"\b\d{1,3}(?:,\d{3})+\b")

# Large plain numbers (fallback when commas are not used)
RE_LARGE_NUM = re.compile(r"\b\d{6,}\b")

# Percents like 7.12% or 7% or .37%
RE_PERCENT_INLINE = re.compile(r"\b\d{0,3}(?:\.\d+)?\s*%")

# A cell that is just '%'
RE_PERCENT_CELL = re.compile(r"^%$")

# A numeric value that might be followed by a separate '%' cell
RE_NUM_LIKE = re.compile(r"^\d{0,3}(?:\.\d+)?$")

# Stars for <= 1% in some filings
RE_STAR = re.compile(r"^\*$")

# Dashes / em dash / placeholders
RE_DASH = re.compile(r"^(?:-|—|–|—\s*|\s*—\s*)$")


OWNERSHIP_TOKENS = (
    "beneficial",
    "beneficially",
    "ownership",
    "owned",
    "shares",
    "common stock",
    "percent",
    "class",
    "outstanding",
    "undiluted",
    "fully diluted",
    "principal",
    "stockholder",
    "stockholders",
    "security ownership",
)

MANAGEMENT_TOKENS = (
    "management",
    "director",
    "directors",
    "executive officer",
    "executive officers",
    "named executive",
    "officers",
    "as a group",
)

# Strong negatives to avoid equity compensation tables
NEGATIVE_TOKENS = (
    "option awards",
    "exercise price",
    "vesting",
    "plan-based awards",
    "outstanding equity",
    "equity compensation plan",
    "securities authorized for issuance",
    "rsu vest",
)


# -------------------------
# Table analysis
# -------------------------

@dataclass
class TableFeatures:
    idx: int
    col_count_med: int
    row_count: int
    pct_rows: int
    share_rows: int
    star_rows: int
    total_rows: int
    addr_like_rows: int
    ownership_hits: int
    management_hits: int
    negative_hits: int
    first_col_avg_len: float
    other_cols_avg_len: float


def _meaningful_cell_text(cell) -> str:
    """Return normalized cell text.

    We consider "meaningful" even if the cell is only symbols like %, *, —.
    """
    txt = _node_text(cell)
    if txt:
        return txt

    # Sometimes the text is empty but there may be a standalone <sup> etc.
    # If completely empty, keep as empty.
    return ""


def _is_meaningful_token(txt: str) -> bool:
    if not txt:
        return False
    if RE_PERCENT_CELL.match(txt):
        return True
    if RE_STAR.match(txt):
        return True
    if RE_DASH.match(txt):
        return True
    if RE_COMMA_NUM.search(txt) or RE_LARGE_NUM.search(txt):
        return True
    if RE_PERCENT_INLINE.search(txt):
        return True
    # Plain text is meaningful
    return True


def _row_cells_compact(tr) -> List[str]:
    """Extract a compact list of meaningful cell texts from a row.

    EDGAR HTML often uses extra gutter <td> cells for borders/spacing.
    We drop empty/noise cells so the logical columns appear.
    """
    cells = tr.xpath("./td|./th")
    out: List[str] = []
    for c in cells:
        t = _meaningful_cell_text(c)
        if _is_meaningful_token(t):
            out.append(t)
    return out


def _iter_table_rows(table) -> Iterable:
    return table.xpath(".//tr")


def _detect_percent_in_row(compact_cells: Sequence[str]) -> bool:
    if not compact_cells:
        return False

    # Inline percent anywhere
    if any(RE_PERCENT_INLINE.search(c) for c in compact_cells):
        return True

    # Split percent: number cell followed by '%' cell
    for i in range(len(compact_cells) - 1):
        a, b = compact_cells[i].strip(), compact_cells[i + 1].strip()
        if RE_NUM_LIKE.match(a) and RE_PERCENT_CELL.match(b):
            return True

    # Some filings use '-' or '*', but a percent column is still present.
    # We count '*' as a weak percent indicator when it appears away from the first col.
    for i, c in enumerate(compact_cells[1:], start=1):
        if RE_STAR.match(c):
            return True

    return False


def _detect_shares_in_row(compact_cells: Sequence[str]) -> bool:
    if not compact_cells:
        return False
    # Shares are usually not in the first column; but allow anywhere.
    return any(RE_COMMA_NUM.search(c) or RE_LARGE_NUM.search(c) for c in compact_cells)


def _detect_total_row(row_text: str) -> bool:
    row_text = _normalize(row_text)
    return (
        "total" in row_text
        or "as a group" in row_text
        or "officers and directors" in row_text
        or "directors and executive officers" in row_text
    )


def _addr_like(text: str) -> bool:
    """Heuristic: address-like if it has commas, state abbreviations, or multiple line-ish parts."""
    t = _normalize(text)
    if not t:
        return False
    # Common address cues
    if "," in text:
        return True
    if re.search(r"\b[a-z]{2}\s+\d{5}\b", t):
        return True
    if re.search(r"\b(po box|suite|street|st\.|avenue|ave\.|road|rd\.|boulevard|blvd)\b", t):
        return True
    return False


def _table_features(table, idx: int) -> TableFeatures:
    rows = list(_iter_table_rows(table))
    row_count = len(rows)

    compact_rows: List[List[str]] = [_row_cells_compact(r) for r in rows]
    non_empty_rows = [r for r in compact_rows if any(c.strip() for c in r)]

    # Column count estimate: median of non-empty rows' lengths
    lengths = sorted(len(r) for r in non_empty_rows if len(r) > 0)
    if lengths:
        col_count_med = lengths[len(lengths) // 2]
    else:
        col_count_med = 0

    pct_rows = 0
    share_rows = 0
    star_rows = 0
    total_rows = 0
    addr_like_rows = 0

    first_col_lens: List[int] = []
    other_col_lens: List[int] = []

    for r_cells, r_node in zip(compact_rows, rows):
        if not r_cells:
            continue

        if _detect_percent_in_row(r_cells):
            pct_rows += 1

        if _detect_shares_in_row(r_cells):
            share_rows += 1

        # Star rows (common for <=1%)
        if any(RE_STAR.match(c.strip()) for c in r_cells[1:]):
            star_rows += 1

        r_text = _node_text(r_node)
        if _detect_total_row(r_text):
            total_rows += 1

        # First column text density
        first = r_cells[0]
        if first:
            first_col_lens.append(len(first))
            if _addr_like(first) or "address" in first:
                addr_like_rows += 1

        if len(r_cells) > 1:
            other = " ".join(r_cells[1:])
            if other:
                other_col_lens.append(len(other))

    first_col_avg_len = (sum(first_col_lens) / len(first_col_lens)) if first_col_lens else 0.0
    other_cols_avg_len = (sum(other_col_lens) / len(other_col_lens)) if other_col_lens else 0.0

    # Token hits
    table_text = _node_text(table)
    ownership_hits = sum(1 for t in OWNERSHIP_TOKENS if t in table_text)
    management_hits = sum(1 for t in MANAGEMENT_TOKENS if t in table_text)
    negative_hits = sum(1 for t in NEGATIVE_TOKENS if t in table_text)

    return TableFeatures(
        idx=idx,
        col_count_med=col_count_med,
        row_count=row_count,
        pct_rows=pct_rows,
        share_rows=share_rows,
        star_rows=star_rows,
        total_rows=total_rows,
        addr_like_rows=addr_like_rows,
        ownership_hits=ownership_hits,
        management_hits=management_hits,
        negative_hits=negative_hits,
        first_col_avg_len=first_col_avg_len,
        other_cols_avg_len=other_cols_avg_len,
    )


def _score_table(feat: TableFeatures) -> float:
    """Score a table for being a beneficial ownership table."""
    if feat.col_count_med < 3:
        # Most BO tables have >=3 logical columns.
        return -10.0

    # Normalize by row count (avoid huge layout tables dominating)
    denom = max(feat.row_count, 1)

    pct_rate = feat.pct_rows / denom
    share_rate = feat.share_rows / denom

    score = 0.0

    # Structural signals
    score += 12.0 * min(pct_rate, 0.4)  # percent presence is key
    score += 10.0 * min(share_rate, 0.4)  # shares presence

    # First column should be more text-heavy than the rest
    if feat.first_col_avg_len > 0:
        ratio = (feat.first_col_avg_len + 1.0) / (feat.other_cols_avg_len + 1.0)
        score += 6.0 * min(max(ratio - 1.0, 0.0), 3.0)  # cap contribution

    # Address-like content helps
    score += 2.0 * min(feat.addr_like_rows, 6)

    # Ownership vocabulary
    score += 2.0 * min(feat.ownership_hits, 8)

    # Totals / group rows are strong
    score += 4.0 * min(feat.total_rows, 2)

    # '*' rows (<=1%) are common in director tables
    score += 1.5 * min(feat.star_rows, 4)

    # Penalties
    score -= 8.0 * min(feat.negative_hits, 2)

    return score


def _classify_table(table_text: str) -> str:
    """Classify extracted table as management vs beneficial_owner when possible."""
    t = _normalize(table_text)

    # Strong title-like phrases
    if "security ownership of management" in t:
        return "management"
    if "stock ownership of directors" in t or "directors and executive officers" in t:
        return "management"

    if "security ownership of beneficial owner" in t:
        return "beneficial_owner"
    if "certain beneficial owners" in t:
        return "beneficial_owner"

    # Token-based fallback
    mgmt_hits = sum(1 for tok in MANAGEMENT_TOKENS if tok in t)
    own_hits = sum(1 for tok in OWNERSHIP_TOKENS if tok in t)

    if mgmt_hits >= 2 and mgmt_hits >= own_hits:
        return "management"

    return "beneficial_owner"


def extract_beneficial_ownership_tables(html_bytes: bytes, top_k: int = 3) -> List[Tuple[float, str, str]]:
    """Return list of (score, kind, table_html_stub) sorted by score desc."""
    try:
        tree = LH.fromstring(html_bytes)
    except Exception as e:
        raise RuntimeError(f"Failed to parse HTML: {e}")

    tables = tree.xpath("//table")
    scored: List[Tuple[float, str, str]] = []

    for idx, tbl in enumerate(tables):
        feat = _table_features(tbl, idx)
        score = _score_table(feat)
        if score <= 0:
            continue

        tbl_text = _node_text(tbl)
        kind = _classify_table(tbl_text)
        tbl_html = LH.tostring(tbl, encoding="unicode", method="html")
        stub = _html_stub(tbl_html, f"Beneficial Ownership Extract ({kind}) - table[{idx}] score={score:.2f}")
        scored.append((score, kind, stub))

    scored.sort(key=lambda x: (-x[0], x[1]))
    return scored[:top_k]


def pick_best_by_kind(scored: Sequence[Tuple[float, str, str]]) -> Tuple[Optional[str], Optional[str]]:
    """Pick best beneficial_owner and management tables from scored list."""
    best_bo: Optional[Tuple[float, str]] = None
    best_mgmt: Optional[Tuple[float, str]] = None

    for score, kind, stub in scored:
        if kind == "beneficial_owner":
            if best_bo is None or score > best_bo[0]:
                best_bo = (score, stub)
        elif kind == "management":
            if best_mgmt is None or score > best_mgmt[0]:
                best_mgmt = (score, stub)

    return (best_bo[1] if best_bo else None, best_mgmt[1] if best_mgmt else None)


# -------------------------
# CLI
# -------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Extract Beneficial Ownership table(s) from SEC HTML")
    ap.add_argument("--input", required=True, help="Path to filing HTML")
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument(
        "--top-k",
        type=int,
        default=6,
        help="How many candidate tables to score before choosing best by kind",
    )
    args = ap.parse_args()

    in_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    html_bytes = in_path.read_bytes()

    scored = extract_beneficial_ownership_tables(html_bytes, top_k=max(args.top_k, 2))
    if not scored:
        print("No Beneficial Ownership table candidates found.")
        return 2

    best_bo, best_mgmt = pick_best_by_kind(scored)

    # If one of the kinds is missing, fall back to top scores.
    if best_bo is None and scored:
        best_bo = scored[0][2]
    if best_mgmt is None and len(scored) > 1:
        best_mgmt = scored[1][2]

    if best_bo:
        (outdir / "beneficial_owner_table.html").write_text(best_bo, encoding="utf-8")
        print(f"Wrote: {outdir / 'beneficial_owner_table.html'}")

    if best_mgmt:
        (outdir / "management_table.html").write_text(best_mgmt, encoding="utf-8")
        print(f"Wrote: {outdir / 'management_table.html'}")

    # Also write a small debug report
    report_lines = ["score\tkind"]
    for score, kind, _ in scored:
        report_lines.append(f"{score:.2f}\t{kind}")
    (outdir / "candidates_report.tsv").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Wrote: {outdir / 'candidates_report.tsv'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
