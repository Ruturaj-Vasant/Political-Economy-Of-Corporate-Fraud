# scripts/run_multiple_tickers.py

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from modules.wrds_executor import WRDSQueryRunner

def run_for_tickers(ticker_list, start_year=1992, end_year=2024):
    runner = WRDSQueryRunner()
    
    for ticker in ticker_list:
        try:
            print(f"\n🔍 Querying data for: {ticker}")
            df = runner.query_and_export_comp_execucomp(
                ticker=ticker,
                start_year=start_year,
                end_year=end_year
            )
            print(f"✅ {ticker}: {len(df)} rows retrieved and saved.")
        except Exception as e:
            print(f"❌ {ticker}: Error - {e}")

if __name__ == "__main__":
    tickers = [
        "XRX",       # Xerox
        "AAPL",      # Apple
        "ENRNQ",     # Enron (historical, may be missing)
        "CVS",       # CVS Health
        "LIGHT.AS",  # Signify (Euronext, may not exist in WRDS)
        "TYC"        # Tyco International (pre-merger)
    ]
    
    # run_for_tickers(tickers, start_year=1992, end_year=2024)
    runner = WRDSQueryRunner()
    for ticker in tickers:
        try:
            df = runner.get_comp_execucomp_auto_years(ticker)
            if not df.empty:
                print(f"✅ {ticker}: Retrieved {len(df)} rows")
                print(df.head(2))  # Show only 2 rows per ticker
            else:
                print(f"⚠️ {ticker}: No data found")
        except Exception as e:
            print(f"❌ {ticker}: Error - {e}")