import argparse
import glob
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any


@dataclass
class ExtractorConfig:
    window_after: int = 300
    header_lookahead: int = 60
    ascii_capture_max: int = 220
    score_threshold: int = 4
    print_samples: int = 0


COL_TOKENS = [
    "fiscal year",
    "year",
    "salary",
    "bonus",
    "stock",
    "option",
    "options",
    "sars",
    "award",
    "awards",
    "non-equity",
    "incentive",
    "pension",
    "all other",
    "total",
    "compensation",
]

STOP_SECTION_PATTERNS = [
    r"grants of plan[- ]based awards",
    r"outstanding equity awards",
    r"pension benefits",
    r"director compensation",
    r"change[- ]in[- ]control",
    r"severance pay plan",
    r"retirement plans",
]
STOP_SECTION_RE = re.compile("|".join(STOP_SECTION_PATTERNS), re.I)


def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def _is_new_heading(line: str) -> bool:
    s = line.strip()
    if len(s) < 6:
        return False
    letters = sum(ch.isalpha() for ch in s)
    upp = sum(ch.isupper() for ch in s if ch.isalpha())
    ratio = (upp / letters) if letters else 0.0
    return ratio > 0.7


def _sgml_tables(window_text: str) -> List[str]:
    # Collect all <table ...> ... </table> blocks (non-greedy), case-insensitive
    return re.findall(r"(?is)<\s*table\b.*?</\s*table\s*>", window_text, re.IGNORECASE | re.DOTALL)


def _header_type_from_text(txt_lower: str) -> str:
    # Infer header strength: npp > pp > pos
    if "name" in txt_lower and "principal" in txt_lower and "position" in txt_lower:
        return "npp"
    if "principal" in txt_lower and "position" in txt_lower:
        return "pp"
    if "position" in txt_lower:
        return "pos"
    return "none"


def _find_ascii_headers(lines: List[str], start_idx: int, end_idx: int, cfg: ExtractorConfig) -> List[Tuple[int, str]]:
    """Scan from start_idx to end_idx for candidate headers. Returns (header_line_index, header_type)."""
    out: List[Tuple[int, str]] = []
    lim = min(start_idx + cfg.header_lookahead, end_idx)
    for j in range(start_idx, lim):
        cur = lines[j]
        nxt = lines[j + 1] if j + 1 < end_idx else ""
        nnx = lines[j + 2] if j + 2 < end_idx else ""
        block = (cur + "\n" + nxt + "\n" + nnx).lower()

        # Strong: Name and Principal Position (allow wrap)
        if ("name" in block and "principal" in block and "position" in block):
            out.append((j, "npp"))
            continue

        # Moderate: Principal Position
        if ("principal" in block and "position" in block):
            out.append((j, "pp"))
            continue

        # Weak: Position only, but must look header-like
        # Require: (year|fiscal year) AND (one of salary/bonus/total/compensation/options/stock)
        # and evidence of multi-column spacing
        if "position" in block:
            has_year = ("year" in block) or ("fiscal year" in block)
            has_pay = any(k in block for k in ("salary", "bonus", "total", "compensation", "options", "stock"))
            has_cols = bool(re.search(r"\S\s{2,}\S", block))
            if has_year and has_pay and has_cols:
                out.append((j, "pos"))
                continue
    return out


def _capture_ascii(lines: List[str], hdr_idx: int, window_end: int, cfg: ExtractorConfig) -> str:
    start_cap = max(hdr_idx - 2, 0)
    out_lines: List[str] = []
    blanks = 0
    for k in range(start_cap, window_end):
        if len(out_lines) >= cfg.ascii_capture_max:
            break
        line = lines[k]
        out_lines.append(line)
        if not line.strip():
            blanks += 1
        else:
            blanks = 0

        if blanks >= 2 and len(out_lines) > 6:
            break

        if len(out_lines) > 10 and (_is_new_heading(line) or STOP_SECTION_RE.search(line or "")):
            break

    return "\n".join(out_lines).strip()


def _has_numbers_or_currency(s: str) -> bool:
    return bool(re.search(r"(\$|,|\d)", s))


def _row_like_count(s: str) -> int:
    cnt = 0
    for ln in s.splitlines():
        if re.search(r"\$?\s*\d", ln) and re.search(r"\s{2,}", ln):
            cnt += 1
    return cnt


def _score_candidate(snippet: str, kind: str) -> Tuple[int, str]:
    low = snippet.lower()
    header_type = _header_type_from_text(low)
    header_score = {"npp": 3, "pp": 2, "pos": 1, "none": 0}[header_type]
    col_score = sum(1 for t in COL_TOKENS if t in low)
    shape_score = 0

    if kind == "sgml":
        shape_score += 1
        if _has_numbers_or_currency(snippet):
            shape_score += 1
    else:
        if _row_like_count(snippet) >= 2:
            shape_score += 1
        if _has_numbers_or_currency(snippet):
            shape_score += 1

    return header_score + col_score + shape_score, header_type


def _collect_candidates_around(lines: List[str], anchor_idx: int, cfg: ExtractorConfig) -> List[Tuple[int, str, str, str]]:
    total = len(lines)
    win_start = max(0, anchor_idx - 10)
    win_end = min(total, anchor_idx + cfg.window_after)
    window_text = "\n".join(lines[win_start:win_end])

    candidates: List[Tuple[int, str, str, str]] = []

    # SGML candidates
    for tbl in _sgml_tables(window_text):
        score, htype = _score_candidate(tbl, "sgml")
        candidates.append((score, "sgml", htype, tbl))

    # ASCII header candidates
    for hdr_idx, htype in _find_ascii_headers(lines, anchor_idx, win_end, cfg):
        ascii_snip = _capture_ascii(lines, hdr_idx, win_end, cfg)
        if not ascii_snip:
            continue
        # Guardrails for weak headers: require at least 2 row-like lines
        if htype == "pos" and _row_like_count(ascii_snip) < 2:
            continue
        score, _ = _score_candidate(ascii_snip, "ascii")
        candidates.append((score, "ascii", htype, ascii_snip))

    return candidates


def extract_summary_table(text: str, cfg: ExtractorConfig) -> Tuple[Optional[str], Dict[str, Any]]:
    """Return best SCT snippet and metadata."""
    txt = _normalize(text)
    lines = txt.splitlines()
    meta: Dict[str, Any] = {"anchor_found": False}

    anchor_idxs = [i for i, ln in enumerate(lines) if ("summary" in ln.lower() and "compensation" in ln.lower() and "table" in ln.lower())]

    all_candidates: List[Tuple[int, str, str, str]] = []
    if anchor_idxs:
        meta["anchor_found"] = True
        for ai in anchor_idxs:
            all_candidates.extend(_collect_candidates_around(lines, ai, cfg))
    else:
        # Fallback: scan entire doc in strides for ASCII headers
        stride = max(20, cfg.header_lookahead // 2)
        for i in range(0, len(lines), stride):
            hdrs = _find_ascii_headers(lines, i, min(len(lines), i + cfg.header_lookahead), cfg)
            for (hdr_idx, htype) in hdrs:
                cap = _capture_ascii(lines, hdr_idx, min(len(lines), hdr_idx + cfg.window_after), cfg)
                if cap:
                    sc, _ = _score_candidate(cap, "ascii")
                    all_candidates.append((sc, "ascii", htype, cap))

    if not all_candidates:
        meta.update({"kind": None, "header_type": None, "score": 0})
        return None, meta

    best = max(all_candidates, key=lambda t: t[0])
    score, kind, htype, snippet = best
    meta.update({"kind": kind, "header_type": htype, "score": score})
    if score >= cfg.score_threshold:
        return snippet, meta
    return None, meta


def read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        try:
            with open(path, "r", encoding="latin-1", errors="ignore") as f:
                return f.read()
        except Exception:
            return ""


def enumerate_txt_files(root: str = "data") -> List[str]:
    files: List[str] = []
    for pat in [
        os.path.join(root, "**", "DEF_14A", "*.txt"),
        os.path.join(root, "**", "DEF 14A", "*.txt"),
    ]:
        files.extend(glob.glob(pat, recursive=True))
    files = [f for f in files if "/extracted/" not in f]
    return sorted(files)


def find_html_candidates(txt_path: str) -> List[str]:
    """Find .html siblings for a given .txt DEF 14A file."""
    folder = os.path.dirname(txt_path)
    # Only accept exact date-matched sibling HTML to avoid cross-year mismatches
    base = os.path.basename(txt_path)
    m = re.search(r"^(\d{4}-\d{2}-\d{2})_DEF[_ ]14A\.txt$", base)
    if m:
        prefer = os.path.join(folder, f"{m.group(1)}_DEF_14A.html")
        prefer2 = os.path.join(folder, f"{m.group(1)}_DEF 14A.html")
        ordered: List[str] = []
        for p in (prefer, prefer2):
            if os.path.exists(p):
                ordered.append(p)
        return ordered
    return []


def extract_html_summary_table(html_text: str, cfg: ExtractorConfig) -> Tuple[Optional[str], Dict[str, Any]]:
    """Regex-only HTML extractor: uses anchors and SGML <table> blocks with scoring."""
    txt = _normalize(html_text)
    lines = txt.splitlines()
    meta: Dict[str, Any] = {"anchor_found": False}

    anchor_idxs = [i for i, ln in enumerate(lines) if ("summary" in ln.lower() and "compensation" in ln.lower() and "table" in ln.lower())]
    candidates: List[Tuple[int, str, str, str]] = []

    def add_tables_from_window(ws: str):
        for tbl in _sgml_tables(ws):
            sc, htype = _score_candidate(tbl, "sgml")
            candidates.append((sc, "sgml", htype, tbl))

    if anchor_idxs:
        meta["anchor_found"] = True
        for ai in anchor_idxs:
            win_start = max(0, ai - 10)
            win_end = min(len(lines), ai + cfg.window_after)
            window_text = "\n".join(lines[win_start:win_end])
            add_tables_from_window(window_text)
    else:
        # Global scan for tables
        add_tables_from_window(txt)

    if not candidates:
        meta.update({"kind": None, "header_type": None, "score": 0})
        return None, meta

    best = max(candidates, key=lambda t: t[0])
    score, kind, htype, snippet = best
    meta.update({"kind": kind, "header_type": htype, "score": score})
    if score >= cfg.score_threshold:
        return snippet, meta
    return None, meta


def html_fallback(txt_path: str, cfg: ExtractorConfig) -> Tuple[Optional[str], Dict[str, Any]]:
    html_files = find_html_candidates(txt_path)
    best: Optional[Tuple[int, Dict[str, Any], str]] = None  # (score, meta, snippet)
    for hp in html_files:
        htxt = read_text(hp)
        if not htxt:
            continue
        snippet, meta = extract_html_summary_table(htxt, cfg)
        if snippet is None:
            continue
        sc = int(meta.get("score", 0))
        if best is None or sc > best[0]:
            best = (sc, {**meta, "html_path": hp}, snippet)
    if best is None:
        return None, {"kind": None, "header_type": None, "score": 0}
    return best[2], best[1]


def classify_file(path: str, cfg: ExtractorConfig) -> Tuple[str, Dict[str, Any]]:
    text = read_text(path)
    if not text:
        return "error", {"reason": "read_failed"}
    snippet, meta = extract_summary_table(text, cfg)

    # If text missed, try HTML fallback in same folder
    if snippet is None:
        # Determine anchor status before fallback for labeling
        anchor_found = bool(meta.get("anchor_found"))
        html_snip, html_meta = html_fallback(path, cfg)
        if html_snip is not None:
            html_meta = {**html_meta, "from_fallback": True}
            return "html_fallback", html_meta
        # Still missing -> categorize by anchor presence
        if anchor_found:
            return "anchor_but_missing", meta
        else:
            return "no_anchor", meta

    kind = meta.get("kind")
    if kind == "sgml":
        return "sgml", meta
    if kind == "ascii":
        h = meta.get("header_type") or "pos"
        if h == "npp":
            return "ascii_npp", meta
        if h == "pp":
            return "ascii_pp", meta
        return "ascii_pos_only", meta
    return "unknown", meta


def run_audit(root: str, cfg: ExtractorConfig) -> Dict[str, Any]:
    files = enumerate_txt_files(root)
    cats = [
        "sgml",
        "ascii_npp",
        "ascii_pp",
        "ascii_pos_only",
        "html_fallback",
        "anchor_but_missing",
        "no_anchor",
        "error",
    ]
    counts = {c: 0 for c in cats}
    samples: Dict[str, List[str]] = {c: [] for c in cats}

    for i, f in enumerate(files, 1):
        cat, meta = classify_file(f, cfg)
        if cat not in counts:
            counts[cat] = 0
            samples[cat] = []
        counts[cat] += 1
        if cfg.print_samples and len(samples[cat]) < cfg.print_samples:
            samples[cat].append(f)
        if i % 50 == 0:
            # progress output to stderr
            print(f"Processed {i}/{len(files)} files...", file=sys.stderr)

    total = len(files)
    extracted = (
        counts.get("sgml", 0)
        + counts.get("ascii_npp", 0)
        + counts.get("ascii_pp", 0)
        + counts.get("ascii_pos_only", 0)
        + counts.get("html_fallback", 0)
    )

    print(f"Total txt DEF14A files: {total}")
    print(f"Extractable (score >= {cfg.score_threshold}): {extracted}")
    for c in cats:
        print(f"{c}: {counts.get(c, 0)}")

    if cfg.print_samples:
        print("\nSamples per category:")
        for c in cats:
            if samples.get(c):
                print(f"\n[{c}] ({len(samples[c])} shown)")
                for s in samples[c]:
                    print(s)

    return {"counts": counts, "total": total, "extracted": extracted, "samples": samples}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit Summary Compensation Table extraction from DEF 14A text files.")
    p.add_argument("--root", default="data", help="Root folder containing ticker subfolders (default: data)")
    p.add_argument("--window-after", type=int, default=ExtractorConfig.window_after, help="Lines to scan after anchor (default: 300)")
    p.add_argument("--header-lookahead", type=int, default=ExtractorConfig.header_lookahead, help="Lines to look ahead for ASCII header (default: 60)")
    p.add_argument("--ascii-capture-max", type=int, default=ExtractorConfig.ascii_capture_max, help="Max lines to capture for ASCII block (default: 220)")
    p.add_argument("--score-threshold", type=int, default=ExtractorConfig.score_threshold, help="Min score to accept candidate (default: 4)")
    p.add_argument("--print-samples", type=int, default=0, help="Print up to N sample file paths per category")
    p.add_argument("--summary-csv", default=None, help="Optional path to write per-file classification summary CSV")
    p.add_argument("--dump-snippets-dir", default=None, help="Optional dir to write extracted snippets for inspection")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    cfg = ExtractorConfig(
        window_after=args.window_after,
        header_lookahead=args.header_lookahead,
        ascii_capture_max=args.ascii_capture_max,
        score_threshold=args.score_threshold,
        print_samples=args.print_samples,
    )
    result = run_audit(args.root, cfg)

    # Optional CSV summary and snippet dumps
    if args.summary_csv or args.dump_snippets_dir:
        import csv
        os.makedirs(args.dump_snippets_dir, exist_ok=True) if args.dump_snippets_dir else None
        rows = []
        files = enumerate_txt_files(args.root)
        for f in files:
            cat, meta = classify_file(f, cfg)
            score = meta.get("score", 0)
            html_path = meta.get("html_path") if isinstance(meta, dict) else None

            # Write snippet if requested
            snip = None
            text = read_text(f)
            snip, _meta_text = extract_summary_table(text, cfg)
            if snip is None:
                snip_html, _meta_html = html_fallback(f, cfg)
                snip = snip_html
            if args.dump_snippets_dir and snip is not None:
                # Write with safe filename
                safe = f.replace(os.sep, "__")
                outp = os.path.join(args.dump_snippets_dir, safe + (".html" if "<table" in snip.lower() else ".txt"))
                try:
                    with open(outp, "w", encoding="utf-8", errors="ignore") as wf:
                        wf.write(snip)
                except Exception:
                    pass

            rows.append({
                "file": f,
                "category": cat,
                "score": score,
                "html_fallback_path": html_path or "",
            })
        if args.summary_csv:
            try:
                with open(args.summary_csv, "w", newline="", encoding="utf-8") as cf:
                    w = csv.DictWriter(cf, fieldnames=["file", "category", "score", "html_fallback_path"])
                    w.writeheader()
                    w.writerows(rows)
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
