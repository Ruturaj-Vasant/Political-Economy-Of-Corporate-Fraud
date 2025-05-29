# scripts/batch_download_comp_data.py

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import datetime
from modules.wrds_executor import WRDSQueryRunner
import json
import datetime
from pathlib import Path
from modules.wrds_executor import WRDSQueryRunner

def load_tickers_from_json(json_path):
    """Load tickers from a JSON file."""
    with open(json_path, 'r') as f:
        tickers_data = json.load(f)
    return [info['ticker'] for info in tickers_data.values() if info['ticker']]  # Skip empty ticker entries

def main():
    project_root = Path(__file__).parent.parent
    json_file = project_root / "metadata" / "sec_company_tickers.json"

    try:
        tickers = load_tickers_from_json(json_file)
    except Exception as e:
        print(f"Error loading tickers: {e}")
        return

    runner = WRDSQueryRunner()
    start_year = 1992
    end_year = datetime.datetime.now().year - 1

    for ticker in tickers:
        print(f"\nProcessing {ticker}...")

        try:
            runner.download_comp_execucomp_annual_compensation(ticker, start_year, end_year)
            runner.download_comp_director_compensation(ticker, start_year, end_year)
        except Exception as e:
            print(f"Failed to download data for {ticker}: {e}")

    print("\nBatch download completed.")

if __name__ == "__main__":
    main()