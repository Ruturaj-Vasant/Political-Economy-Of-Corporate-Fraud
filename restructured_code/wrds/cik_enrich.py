from typing import Dict, Any, Iterable, Tuple
import time
import pandas as pd

from .client import get_conn, close_conn
from . import queries as Q


def _batch(it: Iterable, n: int):
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def enrich_cik(records: Dict[str, Any], batch_size: int = 500, limit: int = None, dry_run: bool = False) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """Fill missing cik_str for records using WRDS (ticker-only resolution).

    Returns updated records and a changes dataframe.
    """
    # worklist of ids with missing cik
    ids = [k for k in sorted(records.keys(), key=lambda x: int(x) if str(x).isdigit() else str(x))]
    work = []
    for k in ids:
        rec = records[k]
        if not isinstance(rec, dict):
            continue
        t = str(rec.get("ticker") or "").upper()
        if not t:
            continue
        if rec.get("cik_str") in (None, "", "null"):
            work.append(k)
    if limit is not None:
        work = work[: int(limit)]

    if not work:
        return records, pd.DataFrame(columns=["id","ticker","field","before","after","source"])

    changes = []
    conn = get_conn()
    try:
        for chunk_ids in _batch(work, batch_size):
            tickers = [str(records[k]["ticker"]).upper() for k in chunk_ids]
            # primary: compustat mapping
            df_comp = Q.cik_by_tickers(conn, tickers)
            cik_by_ticker = {r["ticker"]: r.get("cik") for _, r in df_comp.iterrows()}
            # secondary: via linktable path if we have permno
            df_perm = Q.permno_by_tickers(conn, tickers)
            perm_by_ticker = {r["ticker"]: r.get("permno") for _, r in df_perm.iterrows()}
            permnos = [int(p) for p in perm_by_ticker.values() if pd.notna(p)]
            df_link_cik = Q.cik_by_permnos(conn, permnos)
            cik_by_perm = {int(r["permno"]): r.get("cik") for _, r in df_link_cik.iterrows()}

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            for k in chunk_ids:
                rec = records[k]
                t = str(rec.get("ticker") or "").upper()
                before = rec.copy()

                if rec.get("cik_str") in (None, "", "null"):
                    cik = cik_by_ticker.get(t)
                    if not cik:
                        p = perm_by_ticker.get(t)
                        if p:
                            cik = cik_by_perm.get(int(p))
                    if cik:
                        rec["cik_str"] = str(int(cik)).zfill(10)
                        changes.append({"id": k, "ticker": t, "field": "cik_str", "before": before.get("cik_str"), "after": rec["cik_str"], "source": "WRDS.COMP"})
                        src = str(rec.get("source") or "").strip()
                        if "WRDS" not in src:
                            rec["source"] = (src + ", WRDS").strip(", ").replace(" ,", ",")
                            changes.append({"id": k, "ticker": t, "field": "source", "before": before.get("source"), "after": rec["source"], "source": "WRDS"})
                        rec["last_enriched"] = now_iso
    finally:
        close_conn(conn)

    return records, pd.DataFrame(changes, columns=["id","ticker","field","before","after","source"]).dropna(how="all")

