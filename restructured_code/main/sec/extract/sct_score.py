"""Summary Compensation Table (SCT) extraction via scoring heuristics.

Port of the scoring logic from scripts/Test-Trials-experiments/Extract_best_html_table_2.py
adapted to return a single HTML stub of the best candidate table.

Entry points:
- extract_best_sct_html_from_bytes(html_bytes) -> str | None
- extract_best_sct_html_from_file(path) -> str | None
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Sequence, Tuple, Optional
import re

from lxml import html as LH  # type: ignore

# Keywords and penalties
COMP_KEYS: Tuple[str, ...] = (
    "salary",
    "bonus",
    "stock",
    "option",
    "non-equity",
    "non equity",
    "incentive",
    "pension",
    "all other",
    "total",
    "compensation",
    "awards",
    "cash",
    "year",
)
GRANT_KEYS = ("grant", "grant date", "estimated future payouts")

TITLE_XPATH = (
    "//*[contains(translate(normalize-space(string(.)), "
    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
    "'summary compensation table')]"
)


def normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").lower().split()) if text else ""


def html_stub(table_html: str) -> str:
    return (
        "<!doctype html>\n"
        "<html><head><meta charset=\"utf-8\"></head><body>\n"
        f"{table_html}\n"
        "</body></html>\n"
    )


def build_title_bonus_map(tree, tables: Sequence) -> set[int]:
    table_index_by_id = {id(t): idx for idx, t in enumerate(tables)}
    bonus: set[int] = set()
    for node in tree.xpath(TITLE_XPATH):
        # Table containing the title
        for anc in node.iterancestors():
            if anc.tag == "table":
                idx = table_index_by_id.get(id(anc))
                if idx is not None:
                    bonus.add(idx)
                break
        # Next table after the title
        nxt = node.xpath("following::table[1]")
        if nxt:
            idx = table_index_by_id.get(id(nxt[0]))
            if idx is not None:
                bonus.add(idx)
    return bonus


def extract_header_text(table) -> str:
    rows = table.xpath(".//tr")[:12]
    bits: List[str] = []
    for tr in rows:
        for cell in tr.xpath("./td|./th"):
            bits.append(normalize(" ".join(cell.itertext())))
    return " ".join(bits)


def score_table(table, title_bonus: set[int], idx: int) -> int:
    header = extract_header_text(table)
    full_text = normalize(" ".join(table.itertext()))
    score = 0

    # Name / position cues
    name_ok = "name" in header
    principal_ok = "principal" in header or "named executive" in header
    position_ok = "position" in header or "occupation" in header
    if name_ok and principal_ok and position_ok:
        score += 7
    elif name_ok and (principal_ok or position_ok):
        score += 5
    elif name_ok:
        score += 2

    # Compensation keywords
    comp_hits = {k for k in COMP_KEYS if k in header}
    score += min(len(comp_hits), 8)

    # Money / numeric cues
    if re.search(r"\$|\b\d{3,}\b", full_text):
        score += 2

    # Grant / plan penalty
    if any(g in header for g in GRANT_KEYS):
        score -= 4

    # Caption with SCT
    if table.xpath("./caption[contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'summary compensation table')]"):
        score += 4

    # Title proximity bonus
    if idx in title_bonus:
        score += 4

    return score


def parse_tables(tree) -> List:
    return tree.xpath("//table")


def extract_best_sct_html_from_bytes(html_bytes: bytes) -> Optional[str]:
    try:
        tree = LH.fromstring(html_bytes)
    except Exception:
        return None
    tables = parse_tables(tree)
    if not tables:
        return None
    title_bonus = build_title_bonus_map(tree, tables)
    scored = []
    for idx, table in enumerate(tables):
        scored.append((score_table(table, title_bonus, idx), idx, table))
    scored.sort(key=lambda x: (-x[0], x[1]))
    best = scored[0][2]
    best_html = LH.tostring(best, encoding="unicode", method="html")
    return html_stub(best_html)


def extract_best_sct_html_from_file(path: str | Path) -> Optional[str]:
    p = Path(path)
    try:
        html_bytes = p.read_bytes()
    except Exception:
        return None
    return extract_best_sct_html_from_bytes(html_bytes)
