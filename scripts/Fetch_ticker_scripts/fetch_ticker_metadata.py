import json
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
# Import the WRDS runner from project modules
from modules.wrds_executor import WRDSQueryRunner

def fetch_wrds_ticker_metadata(wrds_runner, existing_keys=None):
    """
    Fetch all tickers and corresponding identifiers using WRDSQueryRunner.
    """
    # Query for stocknames (CRSP)
    stocknames = wrds_runner.execute_raw_sql("""
        SELECT permno, ticker, comnam AS name
        FROM crsp.stocknames
        WHERE namedt <= CURRENT_DATE AND (nameendt >= CURRENT_DATE OR nameendt IS NULL)
    """)

    # Query for Compustat gvkey info
    comp = wrds_runner.execute_raw_sql("""
        SELECT gvkey, tic AS ticker, conm AS name
        FROM comp.security
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

        new_entries[str(i)] = {
            "cik_str": None,
            "ticker": cleaned_ticker,
            "title": row.name,
            "permno": int(row.permno) if pd.notna(row.permno) else None,
            "gvkey": row.gvkey if row.gvkey != '' else None,
            "delisted": is_del,
            "source": "WRDS"
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
    output_path = Path(__file__).parent / "../metadata/ticker_metadata.json"
    with open(output_path, "w") as f:
        json.dump(ticker_metadata, f, indent=2)
    print(f"\nSaved full ticker metadata to: {output_path.resolve()}")

if __name__ == "__main__":
    main()