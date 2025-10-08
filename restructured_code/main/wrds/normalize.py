from typing import Optional, Any, Dict


_EXCH_MAP = {
    1: "NYSE",
    2: "AMEX",
    3: "NASDAQ",
}


def to_int_or_none(x) -> Optional[int]:
    try:
        if x is None:
            return None
        xi = int(float(x)) if isinstance(x, str) and x.replace(".", "", 1).isdigit() else int(x)
        return xi
    except Exception:
        return None


def to_gvkey_str(x) -> Optional[str]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        if not s:
            return None
        # Numeric-like -> zero-pad 6
        if s.isdigit():
            return s.zfill(6)
        # Already has leading zeros or alphanumeric; return as is
        return s
    except Exception:
        return None


def map_exchange_code(code) -> Optional[str]:
    c = to_int_or_none(code)
    return _EXCH_MAP.get(c) if c is not None else None


def delisted_flag(dlstcd) -> Optional[bool]:
    if dlstcd is None:
        return False
    try:
        c = int(dlstcd)
    except Exception:
        return False
    # CRSP code 100 is not a true delist status; treat others as delisted
    return False if c == 100 else True


def compose_location(row: Dict[str, Any]) -> Optional[str]:
    parts = []
    for k in ("address", "city"):
        v = row.get(k)
        if v:
            parts.append(str(v).strip())
    state = row.get("state")
    zipc = row.get("zip")
    tail = " ".join([x for x in [state, zipc] if x])
    if tail:
        parts.append(tail.strip())
    if parts:
        return ", ".join(parts)
    return None

