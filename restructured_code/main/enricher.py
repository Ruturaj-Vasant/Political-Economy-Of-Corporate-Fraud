from typing import Dict, Any, List, Tuple, Iterable
import time
import pandas as pd

from .wrds_client import get_conn, close_conn
from . import wrds_queries as Q
from .normalize import to_int_or_none, to_gvkey_str, map_exchange_code, delisted_flag, compose_location


def _batch(iterable: Iterable, n: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def enrich_records(records: Dict[str, Any], batch_size: int = 500, limit: int = None, dry_run: bool = False) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """Enrich records keyed by arbitrary ids containing a 'ticker'.

    Only fills missing permno/gvkey and enriches optional fields.

    Returns updated records and a change log dataframe.
    """
    # Build worklist of ids in deterministic order
    ids = [k for k in sorted(records.keys(), key=lambda x: int(x) if str(x).isdigit() else str(x))]
    work: List[str] = []
    for k in ids:
        rec = records[k]
        if not isinstance(rec, dict):
            continue
        t = str(rec.get("ticker") or "").upper()
        if not t:
            continue
        need_permno = rec.get("permno") in (None, "", "null")
        need_gvkey = rec.get("gvkey") in (None, "", "null")
        if need_permno or need_gvkey:
            work.append(k)
    if limit is not None:
        work = work[: int(limit)]

    changes: List[Dict[str, Any]] = []

    if not work:
        return records, pd.DataFrame(columns=["id","ticker","field","before","after","source"])

    conn = get_conn()
    try:
        for chunk_ids in _batch(work, batch_size):
            tickers = [str(records[k]["ticker"]).upper() for k in chunk_ids]

            # 1) Permno via CRSP stocknames
            df_perm = Q.permno_by_tickers(conn, tickers)
            perm_by_ticker = {r["ticker"]: to_int_or_none(r["permno"]) for _, r in df_perm.iterrows()}
            ncusip_by_ticker = {r["ticker"]: r.get("ncusip") for _, r in df_perm.iterrows()}

            # 2) GVKEY via linktable using permno
            permnos = [p for p in perm_by_ticker.values() if p is not None]
            df_link = Q.gvkey_by_permnos(conn, permnos)
            gvkey_by_permno = {int(r["permno"]): to_gvkey_str(r["gvkey"]) for _, r in df_link.iterrows()}

            # 3) Fallback Compustat mapping by ticker (gvkey + cusip)
            df_comp = Q.comp_mapping_by_tickers(conn, tickers)
            comp_gvkey_by_ticker = {r["ticker"]: to_gvkey_str(r["gvkey"]) for _, r in df_comp.iterrows()}
            cusip_by_ticker = {r["ticker"]: r.get("cusip") for _, r in df_comp.iterrows()}

            # 4) Exchange + Delist via permnos
            df_exch = Q.exchange_by_permnos(conn, permnos)
            exch_by_permno = {int(r["permno"]): map_exchange_code(r.get("exchcd")) for _, r in df_exch.iterrows()}
            df_delist = Q.delist_by_permnos(conn, permnos)
            delist_by_permno = {int(r["permno"]): delisted_flag(r.get("dlstcd")) for _, r in df_delist.iterrows()}

            # 5) Currency via gvkeys
            # Collect gvkeys from linktable first, else comp mapping
            gvkeys = set([g for g in gvkey_by_permno.values() if g]) | set([g for g in comp_gvkey_by_ticker.values() if g])
            df_cur = Q.currency_by_gvkeys(conn, sorted(gvkeys)) if gvkeys else pd.DataFrame(columns=["gvkey","curcd"])
            cur_by_gvkey = {str(r["gvkey"]): r.get("curcd") for _, r in df_cur.iterrows()}

            # 6) Execucomp descriptors per ticker (sector_alt/industry_alt and location)
            df_exec = Q.execucomp_desc_by_tickers(conn, tickers)
            exec_by_ticker = {r["ticker"]: r.to_dict() for _, r in df_exec.iterrows()}

            # Apply to in-memory records
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            for k in chunk_ids:
                rec = records[k]
                t = str(rec.get("ticker") or "").upper()
                before = rec.copy()

                # permno
                if rec.get("permno") in (None, "", "null"):
                    p = perm_by_ticker.get(t)
                    if p is not None:
                        rec["permno"] = p
                        changes.append({"id": k, "ticker": t, "field": "permno", "before": before.get("permno"), "after": p, "source": "WRDS.CRSP"})

                # gvkey via link, else comp
                if rec.get("gvkey") in (None, "", "null"):
                    p = to_int_or_none(rec.get("permno")) or perm_by_ticker.get(t)
                    g = gvkey_by_permno.get(p) if p is not None else None
                    if not g:
                        g = comp_gvkey_by_ticker.get(t)
                    if g:
                        rec["gvkey"] = g
                        changes.append({"id": k, "ticker": t, "field": "gvkey", "before": before.get("gvkey"), "after": g, "source": "WRDS.LINK/COMP"})

                # cusip: prefer comp.security; else crsp ncusip (8 chars)
                if rec.get("cusip") in (None, "", "null"):
                    c9 = cusip_by_ticker.get(t)
                    if c9:
                        rec["cusip"] = c9
                        changes.append({"id": k, "ticker": t, "field": "cusip", "before": before.get("cusip"), "after": c9, "source": "WRDS.COMPSEC"})
                    else:
                        c8 = ncusip_by_ticker.get(t)
                        if c8:
                            rec["cusip"] = c8
                            changes.append({"id": k, "ticker": t, "field": "cusip", "before": before.get("cusip"), "after": c8, "source": "WRDS.CRSP.NCUSIP"})

                # exchange via permno
                if rec.get("exchange") in (None, "", "null"):
                    p = to_int_or_none(rec.get("permno"))
                    ex = exch_by_permno.get(p) if p is not None else None
                    if ex:
                        rec["exchange"] = ex
                        changes.append({"id": k, "ticker": t, "field": "exchange", "before": before.get("exchange"), "after": ex, "source": "WRDS.CRSP.MSF"})

                # isDelisted via permno
                if rec.get("isDelisted") in (None, "", "null"):
                    p = to_int_or_none(rec.get("permno"))
                    dl = delist_by_permno.get(p) if p is not None else False
                    rec["isDelisted"] = bool(dl)
                    if rec["isDelisted"] != before.get("isDelisted"):
                        changes.append({"id": k, "ticker": t, "field": "isDelisted", "before": before.get("isDelisted"), "after": rec["isDelisted"], "source": "WRDS.CRSP.MSEDELIST"})

                # currency via gvkey
                if rec.get("currency") in (None, "", "null"):
                    g = to_gvkey_str(rec.get("gvkey"))
                    cur = cur_by_gvkey.get(g) if g else None
                    if cur:
                        rec["currency"] = cur
                        changes.append({"id": k, "ticker": t, "field": "currency", "before": before.get("currency"), "after": cur, "source": "WRDS.COMP.FUNDA"})

                # sector_alt / industry_alt via Execucomp
                exec_row = exec_by_ticker.get(t)
                if exec_row:
                    if not rec.get("sector_alt"):
                        naicsdesc = exec_row.get("naicsdesc")
                        if naicsdesc:
                            rec["sector_alt"] = naicsdesc
                            changes.append({"id": k, "ticker": t, "field": "sector_alt", "before": before.get("sector_alt"), "after": naicsdesc, "source": "WRDS.EXECUCOMP"})
                    if not rec.get("industry_alt"):
                        sicdesc = exec_row.get("sicdesc")
                        if sicdesc:
                            rec["industry_alt"] = sicdesc
                            changes.append({"id": k, "ticker": t, "field": "industry_alt", "before": before.get("industry_alt"), "after": sicdesc, "source": "WRDS.EXECUCOMP"})

                    # location
                    if not rec.get("location"):
                        loc = compose_location(exec_row)
                        if loc:
                            rec["location"] = loc
                            changes.append({"id": k, "ticker": t, "field": "location", "before": before.get("location"), "after": loc, "source": "WRDS.EXECUCOMP"})

                # source tag
                src = str(rec.get("source") or "").strip()
                if "WRDS" not in src:
                    rec["source"] = (src + ", WRDS").strip(", ").replace(" ,", ",")
                    if rec["source"] != before.get("source"):
                        changes.append({"id": k, "ticker": t, "field": "source", "before": before.get("source"), "after": rec["source"], "source": "WRDS"})

                rec["last_enriched"] = now_iso

    finally:
        close_conn(conn)

    changes_df = pd.DataFrame(changes, columns=["id","ticker","field","before","after","source"]).dropna(how="all")
    return records, changes_df

