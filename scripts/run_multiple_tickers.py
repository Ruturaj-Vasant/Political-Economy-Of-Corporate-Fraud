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
            df = runner.query_and_export_comp_execucomp_annual_compensation(
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
        print(f"\nProcessing {ticker}...")
        try:
            df = runner.query_and_export_comp_execucomp_annual_compensation(
                ticker, 1992, 2024
            )
            print(f"✅ Successfully processed {ticker}")
            print(f"Retrieved {len(df)} rows of data")
        except Exception as e:
            print(f"❌ Error processing {ticker}: {str(e)}")

    # Test auto years functionality for a single ticker
    ticker = "XRX"
    print(f"\nTesting auto years functionality for {ticker}...")
    try:
        df = runner.get_comp_execucomp_annual_compensation_auto_years(ticker)
        print(f"✅ Successfully retrieved data for {ticker}")
        print(f"Retrieved {len(df)} rows of data")
    except Exception as e:
        print(f"❌ Error processing {ticker}: {str(e)}")