# scripts/test_single_ticker.
import sys
from pathlib import Path

# Adds the parent directory of "scripts" to the Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from modules.wrds_executor import WRDSQueryRunner

def test_single_ticker(ticker: str, start_year=1992, end_year=2024):
    runner = WRDSQueryRunner()

    # 1️⃣ Just get the DataFrame (no export)
    print(f"\n🔹 Running get_comp_execucomp for {ticker}")
    df = runner.get_comp_execucomp(ticker, start_year, end_year)
    print(f" Retrieved {len(df)} rows")
    print(df.head())

    # 2️⃣ Get and export to CSV via download_comp_execucomp
    print(f"\n🔹 Running download_comp_execucomp for {ticker}")
    runner.download_comp_execucomp(ticker, start_year, end_year)

    # 3️⃣ Get and export via unified query_and_export_comp_execucomp
    print(f"\n🔹 Running query_and_export_comp_execucomp for {ticker}")
    df_exported = runner.query_and_export_comp_execucomp(ticker, start_year, end_year)
    print(f" Exported {len(df_exported)} rows to CSV")

if __name__ == "__main__":
    # test_single_ticker("XRX", start_year=1992, end_year=2024)
    runner = WRDSQueryRunner()
    df = runner.get_comp_execucomp_auto_years("XRX")
    print(df.head())