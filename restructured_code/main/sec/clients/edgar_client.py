"""Thin wrapper around the `edgar` Python library used in legacy scripts.

We keep this small and swappable so we can later replace it with official
SEC submissions/archives if needed, without changing caller code.
"""
from __future__ import annotations

from typing import Iterable, List, Optional
from dataclasses import dataclass

import json
import os
import urllib.request
from pathlib import Path

try:
    from edgar import set_identity, Company  # type: ignore
    HAS_EDGAR_LIB = True
except Exception:  # pragma: no cover
    set_identity = None
    Company = None
    HAS_EDGAR_LIB = False


@dataclass
class FilingRef:
    form: str
    filing_date: str  # YYYY-MM-DD
    url: str
    _filing_obj: object  # the `edgar` filing object (opaque to callers)


def init_identity(user_agent: str) -> None:
    """Set the EDGAR identity header (required by the edgar library)."""
    if HAS_EDGAR_LIB:
        set_identity(user_agent)
    else:
        # No-op for HTTP fallback; we pass UA per request
        return


def list_filings_for_ticker(ticker: str, form: str) -> List[FilingRef]:
    """Return a list of recent filings for a ticker and form using `edgar`.

    This mirrors the legacy approach but returns a light wrapper so callers
    aren't tightly coupled to the library API.
    """
    if HAS_EDGAR_LIB:
        c = Company(ticker)
        filings = c.get_filings(form=form)
        out: List[FilingRef] = []
        for f in filings:
            filing_date = getattr(f, "filing_date", None) or ""
            url = getattr(f, "url", "")
            out.append(FilingRef(form=form, filing_date=str(filing_date), url=str(url), _filing_obj=f))
        return out
    # HTTP fallback using official SEC submissions + archive URL
    cik = _resolve_cik_from_metadata(ticker)
    if not cik:
        return []
    subs = _get_company_submissions(cik)
    return _recent_filings_by_form(cik, subs, form)


def fetch_html(filing: FilingRef, timeout: int = 30) -> Optional[bytes]:
    """Fetch the HTML content for a filing. Returns None if not available."""
    if HAS_EDGAR_LIB and filing._filing_obj is not None:
        f = filing._filing_obj
        try:
            raw_html = f.html()
            if raw_html is None:
                return None
            if isinstance(raw_html, bytes):
                return raw_html
            return raw_html.encode("utf-8", errors="ignore")
        except Exception:
            return None
    # HTTP fallback
    try:
        req = urllib.request.Request(filing.url, headers={"User-Agent": os.getenv("SEC_USER_AGENT", "you@example.com")})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception:
        return None


def fetch_text(filing: FilingRef, timeout: int = 30) -> Optional[str]:
    """Fetch the plain-text content for a filing. Returns None on failure."""
    if HAS_EDGAR_LIB and filing._filing_obj is not None:
        f = filing._filing_obj
        try:
            txt = f.text()
            if txt is None:
                return None
            return str(txt)
        except Exception:
            return None
    # HTTP fallback: no standard text endpoint; return None
    return None


# ---------------------- HTTP fallback helpers ----------------------

def _resolve_cik_from_metadata(ticker: str) -> Optional[str]:
    """Resolve CIK using local metadata JSON under restructured_code/json/.

    Returns zero-padded 10-char CIK or None.
    """
    paths = [
        Path("restructured_code/json/sec_company_tickers.json"),
        Path("metadata/sec_company_tickers.json"),
    ]
    t = ticker.upper().strip()
    for p in paths:
        try:
            if p.exists():
                data = json.loads(p.read_text())
                if isinstance(data, dict):
                    for rec in data.values():
                        if isinstance(rec, dict) and str(rec.get("ticker", "")).upper() == t:
                            cik = rec.get("cik_str") or rec.get("cik")
                            if cik:
                                s = str(int(cik)).zfill(10)
                                return s
        except Exception:
            continue
    return None


def _submissions_url(cik: str) -> str:
    padded = str(int(cik)).zfill(10)
    return f"https://data.sec.gov/submissions/CIK{padded}.json"


def _get_company_submissions(cik: str) -> dict:
    req = urllib.request.Request(
        _submissions_url(cik), headers={"User-Agent": os.getenv("SEC_USER_AGENT", "you@example.com")}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _recent_filings_by_form(cik: str, subs: dict, form: str) -> List[FilingRef]:
    recent = subs.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    acc = recent.get("accessionNumber", [])
    docs = recent.get("primaryDocument", [])
    out: List[FilingRef] = []
    for i, f in enumerate(forms):
        if str(f).upper() == form.upper():
            filing_date = dates[i] if i < len(dates) else ""
            accession = acc[i] if i < len(acc) else ""
            primary = docs[i] if i < len(docs) else ""
            url = _archive_url(cik, accession, primary)
            out.append(FilingRef(form=form, filing_date=filing_date, url=url, _filing_obj=None))
    return out


def _archive_url(cik: str, accession: str, primary_doc: str) -> str:
    acc_no_dash = str(accession).replace("-", "")
    cik_unpadded = str(int(cik))
    return f"https://www.sec.gov/Archives/edgar/data/{cik_unpadded}/{acc_no_dash}/{primary_doc}"
