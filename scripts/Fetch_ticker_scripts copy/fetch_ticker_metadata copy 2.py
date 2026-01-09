import json
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
# Import the WRDS runner from project modules
from modules.wrds_executor import WRDSQueryRunner

def fetch_wrds_ticker_metadata(wrds_runner, existing_data=None):
    """
    Fetch all tickers and corresponding identifiers using WRDSQueryRunner.
    """
    # Query for stocknames (CRSP)
    stocknames = wrds_runner.execute_raw_sql("""
        SELECT permno, ticker, comnam AS name
        FROM crsp.stocknames
        WHERE namedt <= CURRENT_DATE AND (nameenddt >= CURRENT_DATE OR nameenddt IS NULL)
    """)

    # Query for Compustat gvkey info (extended fields) with join between comp.security and comp.company
    comp = wrds_runner.execute_raw_sql("""
        SELECT s.gvkey, s.tic AS ticker, c.conm AS name, c.sic, c.gsector, c.gind, c.loc AS currency, c.state, c.county
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
    for i, row in enumerate(merged.itertuples(), start=1):
        is_del = row.permno in delisted_set
        cleaned_ticker = f"{row.ticker}-DELISTED" if is_del else row.ticker

        source_value = "SEC, WRDS" if not existing_data or row.ticker not in existing_data else (
            existing_data[row.ticker].get("source", "SEC") + ", WRDS"
            if "wrds" not in existing_data[row.ticker].get("source", "").lower()
            else existing_data[row.ticker]["source"]
        )

        new_entries[str(i)] = {
            "cik_str": None,
            "ticker": cleaned_ticker,
            "title": row.name,
            "permno": int(row.permno) if pd.notna(row.permno) else None,
            "gvkey": row.gvkey if row.gvkey != '' else None,
            "delisted": is_del,
            "source": source_value
        }

    return new_entries

def main():
    metadata_path = Path(__file__).resolve().parents[1] / "metadata/sec_company_tickers.json"
    if metadata_path.exists():
        with open(metadata_path) as f:
            existing_data = json.load(f)
    else:
        existing_data = {}

    # Track next available index
    existing_keys = [int(k) for k in existing_data.keys()]
    max_index = max(existing_keys) if existing_keys else -1
    wrds_runner = WRDSQueryRunner()
    wrds_data = fetch_wrds_ticker_metadata(wrds_runner, existing_data=existing_data)

    # Merge WRDS data into existing JSON
    for key, entry in wrds_data.items():
        ticker = entry["ticker"]
        match_found = False
        for existing_key, existing_entry in existing_data.items():
            if existing_entry.get("ticker") == ticker:
                # Update existing entry without overwriting SEC-specific fields
                existing_entry.update({k: v for k, v in entry.items() if v is not None and k != "cik_str" and k != "sec_internal_id"})
                existing_entry["source"] = existing_entry.get("source", "SEC")
                if "wrds" not in existing_entry["source"].lower():
                    existing_entry["source"] += ", wrds"
                match_found = True
                break
        if not match_found:
            max_index += 1
            existing_data[str(max_index)] = entry

    # Save updated JSON
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(existing_data, f, indent=2)
    print(f"Saved full ticker metadata to: {metadata_path.resolve()}")

if __name__ == "__main__":
    main()