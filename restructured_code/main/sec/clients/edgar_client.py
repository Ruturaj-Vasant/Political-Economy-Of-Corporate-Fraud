"""Thin wrapper around the `edgar` Python library used in legacy scripts.

We keep this small and swappable so we can later replace it with official
SEC submissions/archives if needed, without changing caller code.
"""
from __future__ import annotations

from typing import Iterable, List, Optional
from dataclasses import dataclass

try:
    from edgar import set_identity, Company  # type: ignore
except Exception:  # pragma: no cover
    set_identity = None
    Company = None


@dataclass
class FilingRef:
    form: str
    filing_date: str  # YYYY-MM-DD
    url: str
    _filing_obj: object  # the `edgar` filing object (opaque to callers)


def init_identity(user_agent: str) -> None:
    """Set the EDGAR identity header (required by the edgar library)."""
    if set_identity is None:
        raise RuntimeError("edgar library not available; please install it")
    set_identity(user_agent)


def list_filings_for_ticker(ticker: str, form: str) -> List[FilingRef]:
    """Return a list of recent filings for a ticker and form using `edgar`.

    This mirrors the legacy approach but returns a light wrapper so callers
    aren't tightly coupled to the library API.
    """
    if Company is None:
        raise RuntimeError("edgar library not available; please install it")

    c = Company(ticker)
    filings = c.get_filings(form=form)
    out: List[FilingRef] = []
    for f in filings:
        filing_date = getattr(f, "filing_date", None) or ""
        url = getattr(f, "url", "")
        out.append(FilingRef(form=form, filing_date=str(filing_date), url=str(url), _filing_obj=f))
    return out


def fetch_html(filing: FilingRef, timeout: int = 30) -> Optional[bytes]:
    """Fetch the HTML content for a filing. Returns None if not available."""
    f = filing._filing_obj
    try:
        raw_html = f.html()
        if raw_html is None:
            return None
        # The edgar lib returns string HTML; encode to bytes for validators.
        if isinstance(raw_html, bytes):
            return raw_html
        return raw_html.encode("utf-8", errors="ignore")
    except Exception:
        return None


def fetch_text(filing: FilingRef, timeout: int = 30) -> Optional[str]:
    """Fetch the plain-text content for a filing. Returns None on failure."""
    f = filing._filing_obj
    try:
        txt = f.text()
        if txt is None:
            return None
        return str(txt)
    except Exception:
        return None

