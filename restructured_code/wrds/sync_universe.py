import argparse
import json
from pathlib import Path
from typing import Dict, Any
import time
import pandas as pd

from .universe import fetch_universes, union_universe
from .client import get_conn, close_conn
from . import queries as Q
from .normalize import map_exchange_code, to_gvkey_str
from .enricher import _batch  # reuse batching
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
    ap = argparse.ArgumentParser("Sync JSON with WRDS ticker universe (add missing)")
    ap.add_argument("--input", required=True, help="Input JSON path (inside restructured_code/json)")
    ap.add_argument("--preview-missing", action="store_true", help="Write CSV of missing tickers (no JSON change)")
    ap.add_argument("--add-missing", action="store_true", help="Append missing tickers to JSON (with .bak)")
    ap.add_argument("--out", help="Output CSV for preview or additions list", default="restructured_code/json/missing_tickers.csv")
    ap.add_argument("--changes-csv", help="CSV of appended records when --add-missing", default="restructured_code/json/missing_tickers_added.csv")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args(argv)

    if not (args.preview_missing or args.add_missing):
        raise SystemExit("Specify --preview-missing or --add-missing")

    inp = Path(args.input)
    data = load_json(inp)
    if not isinstance(data, dict):
        raise SystemExit("Unsupported JSON format: expected dict of records")

    # Build current ticker set
    current = set()
    for rec in data.values():
        if isinstance(rec, dict) and rec.get('ticker'):
            current.add(str(rec['ticker']).upper())

    # Fetch WRDS universes and union
    df_crsp, df_comp = fetch_universes(equity_only=True)
    universe = union_universe(df_crsp, df_comp)
    uni_tickers = set(universe['ticker'].dropna().astype(str).str.upper())

    missing = sorted(list(uni_tickers - current))
    if args.limit is not None:
        missing = missing[: int(args.limit)]

    if args.preview_missing:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        pd.Series(missing, name='ticker').to_csv(out, index=False)
        print(f"Wrote preview of {len(missing)} missing tickers to {out}")
        if not args.add_missing:
            return 0

    # Add missing records with best-available fields in batches
    conn = get_conn()
    added_rows = []
    try:
        for batch in _batch(missing, args.batch_size):
            # Core identifiers
            df_perm = Q.permno_by_tickers(conn, batch)
            perm_by_ticker = {r['ticker']: r.get('permno') for _, r in df_perm.iterrows()}
            ncusip_by_ticker = {r['ticker']: r.get('ncusip') for _, r in df_perm.iterrows()}

            df_compm = Q.comp_mapping_by_tickers(conn, batch)
            gvkey_by_ticker = {r['ticker']: to_gvkey_str(r.get('gvkey')) for _, r in df_compm.iterrows()}
            cusip_by_ticker = {r['ticker']: r.get('cusip') for _, r in df_compm.iterrows()}
            conm_by_ticker = {r['ticker']: r.get('conm') for _, r in df_compm.iterrows()}

            # Exchange, currency
            permnos = [int(p) for p in perm_by_ticker.values() if pd.notna(p)]
            df_exch = Q.exchange_by_permnos(conn, permnos)
            exch_by_permno = {int(r['permno']): map_exchange_code(r.get('exchcd')) for _, r in df_exch.iterrows()}
            df_del = Q.delist_by_permnos(conn, permnos)
            delist_by_permno = {int(r['permno']): True for _, r in df_del.iterrows() if pd.notna(r.get('dlstcd')) and int(r.get('dlstcd')) != 100}

            gvkeys = [g for g in gvkey_by_ticker.values() if g]
            df_cur = Q.currency_by_gvkeys(conn, gvkeys)
            cur_by_gvkey = {str(r['gvkey']): r.get('curcd') for _, r in df_cur.iterrows()}

            # Execucomp descriptors
            df_exec = Q.execucomp_desc_by_tickers(conn, batch)
            exec_by_ticker = {r['ticker']: r.to_dict() for _, r in df_exec.iterrows()}

            # CIKs
            df_cik_t = Q.cik_by_tickers(conn, batch)
            cik_by_ticker = {r['ticker']: r.get('cik') for _, r in df_cik_t.iterrows()}

            # Compose records for each ticker
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            for t in batch:
                rec = {
                    'cik_str': (str(int(cik_by_ticker.get(t))).zfill(10) if pd.notna(cik_by_ticker.get(t)) else None),
                    'ticker': t,
                    'title': (conm_by_ticker.get(t) if pd.notna(conm_by_ticker.get(t)) else None),
                    'permno': (int(perm_by_ticker.get(t)) if pd.notna(perm_by_ticker.get(t)) else None),
                    'gvkey': gvkey_by_ticker.get(t),
                    'cusip': (cusip_by_ticker.get(t) or ncusip_by_ticker.get(t)),
                    'exchange': (exch_by_permno.get(int(perm_by_ticker.get(t))) if pd.notna(perm_by_ticker.get(t)) else None),
                    'isDelisted': (bool(delist_by_permno.get(int(perm_by_ticker.get(t)))) if pd.notna(perm_by_ticker.get(t)) else False),
                    'currency': (cur_by_gvkey.get(gvkey_by_ticker.get(t)) if gvkey_by_ticker.get(t) else None),
                    'sector_alt': (exec_by_ticker.get(t, {}).get('naicsdesc')),
                    'industry_alt': (exec_by_ticker.get(t, {}).get('sicdesc')),
                    'location': None,
                    'source': 'WRDS',
                    'last_enriched': now_iso,
                }
                # location composition
                ex = exec_by_ticker.get(t)
                if ex:
                    addr = ex.get('address'); city = ex.get('city'); state = ex.get('state'); zipc = ex.get('zip')
                    parts = []
                    if addr: parts.append(str(addr).strip())
                    if city: parts.append(str(city).strip())
                    tail = " ".join([str(x).strip() for x in [state, zipc] if x])
                    if tail: parts.append(tail)
                    if parts:
                        rec['location'] = ", ".join(parts)

                added_rows.append(rec)
    finally:
        close_conn(conn)

    # Append to JSON data
    if args.add_missing:
        # Determine next numeric key
        keys = [int(k) for k in data.keys() if str(k).isdigit()]
        next_key = max(keys) + 1 if keys else 0
        for rec in added_rows:
            data[str(next_key)] = rec
            next_key += 1
        # Backup and write
        bak = inp.with_suffix(inp.suffix + '.bak')
        bak.write_text(inp.read_text())
        write_json_atomic(inp, data)
        # Write report
        outcsv = Path(args.changes_csv)
        outcsv.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(added_rows).to_csv(outcsv, index=False)
        print(f"Appended {len(added_rows)} tickers to {inp} and wrote {outcsv}")
    else:
        # just write preview CSV of missing tickers
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        pd.Series(missing, name='ticker').to_csv(out, index=False)
        print(f"Wrote preview of {len(missing)} missing tickers to {out}")

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

