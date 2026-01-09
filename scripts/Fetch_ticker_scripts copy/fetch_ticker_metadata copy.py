import json
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
# Import the WRDS runner from project modules
from modules.wrds_executor import WRDSQueryRunner

def exchcd_to_str(code):
    return {1: "NYSE", 2: "AMEX", 3: "NASDAQ"}.get(code, "Other")

def map_sic_to_sector(sic):
    return "Services" if sic and str(sic).startswith("73") else "Other"

def map_sic_to_industry(sic):
    return "Services-Prepackaged Software" if sic == 7372 else "Other"

def inferred_category(sic):
    return "Domestic Common Stock" if sic and 1000 <= int(sic) < 9999 else None

def fetch_wrds_ticker_metadata(wrds_runner, existing_keys=None, existing_data=None):
    """
    Fetch all tickers and corresponding identifiers using WRDSQueryRunner.
    """
    # Query for stocknames (CRSP)
    stocknames = wrds_runner.execute_raw_sql("""
        SELECT permno, ticker, comnam AS name, cusip, exchcd
        FROM crsp.stocknames
        WHERE namedt <= CURRENT_DATE AND (nameenddt >= CURRENT_DATE OR nameenddt IS NULL)
    """)

    # Query for Compustat gvkey info with proper join between comp.security and comp.company
    comp = wrds_runner.execute_raw_sql("""
        SELECT s.gvkey, s.tic AS ticker, c.conm AS name, c.sic, c.gsector, c.gind, c.loc AS currency, c.state, c.country
        FROM comp.security s
        LEFT JOIN comp.company c ON s.gvkey = c.gvkey
    """)

    # Delisted tickers
    delisted = wrds_runner.execute_raw_sql("""
        SELECT DISTINCT permno
        FROM crsp.dsedelist
    """)
    delisted_set = set(delisted['permno'])

    # Merge stocknames with Compustat data
    merged = pd.merge(stocknames, comp, how='left', on='ticker', suffixes=('', '_comp'))
    merged['gvkey'] = merged['gvkey'].fillna('')

    new_entries = {}
    for i, row in enumerate(merged.itertuples(), start=(max(existing_keys) + 1) if existing_keys else 1):
        is_del = row.permno in delisted_set
        cleaned_ticker = f"{row.ticker}-DELISTED" if is_del else row.ticker

        existing_entry = existing_data.get(row.ticker) if existing_data else {}
        existing_source = existing_entry.get("source", "SEC")
        new_source = existing_source + ", wrds" if "wrds" not in existing_source.lower() else existing_source

        new_entries[str(i)] = {
            "cik_str": existing_entry.get("cik_str"),
            "ticker": cleaned_ticker,
            "title": row.name,
            "permno": int(row.permno) if pd.notna(row.permno) else None,
            "gvkey": row.gvkey if row.gvkey else None,
            "cusip": row.cusip if row.cusip else None,
            "exchange": exchcd_to_str(row.exchcd),
            "isDelisted": is_del,
            "category": inferred_category(row.sic),
            "sector": row.gsector,
            "industry": row.gind,
            "sic": row.sic,
            "sicSector": map_sic_to_sector(row.sic),
            "sicIndustry": map_sic_to_industry(row.sic),
            "currency": row.currency,
            "location": f"{row.state}; {row.country}" if row.state and row.country else None,
            "sec_internal_id": existing_entry.get("sec_internal_id"),
            "source": new_source
        }

    return new_entries

def main():
    wrds_runner = WRDSQueryRunner()
    ticker_metadata = fetch_wrds_ticker_metadata(wrds_runner)

    # Print summary to terminal
    print(f"Total tickers fetched: {len(ticker_metadata)}")
    sample_keys = list(ticker_metadata.keys())[:5]
    for k in sample_keys:
        print(f"{k}: {ticker_metadata[k]}")

    # Optional: Save to file
    metadata_path = Path(__file__).resolve().parents[2] / "metadata/sec_company_tickers.json"
    with open(metadata_path, "w") as f:
        json.dump(ticker_metadata, f, indent=2)
    print(f"\nSaved full ticker metadata to: {metadata_path.resolve()}")

if __name__ == "__main__":
    main()