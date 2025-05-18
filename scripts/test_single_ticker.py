# scripts/test_single_ticker.
import sys
from pathlib import Path
import pandas as pd
from config.wrds_config import get_wrds_connectio
# from modules.csv_exporter import CSVExporter

# Adds the parent directory of "scripts" to the Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from modules.wrds_executor import WRDSQueryRunner

def test_single_ticker(ticker: str, start_year=1992, end_year=2024):
    runner = WRDSQueryRunner()

    # 1️⃣ Get data via get_comp_execucomp_annual_compensation
    print(f"\n🔹 Running get_comp_execucomp_annual_compensation for {ticker}")
    df = runner.get_comp_execucomp_annual_compensation(ticker, start_year, end_year)
    print(f"✅ Retrieved {len(df)} rows of data")

    # 2️⃣ Get and export to CSV via download_comp_execucomp_annual_compensation
    print(f"\n🔹 Running download_comp_execucomp_annual_compensation for {ticker}")
    runner.download_comp_execucomp_annual_compensation(ticker, start_year, end_year)
    print("✅ Data downloaded and saved to CSV")

    # 3️⃣ Get and export via unified query_and_export_comp_execucomp_annual_compensation
    print(f"\n🔹 Running query_and_export_comp_execucomp_annual_compensation for {ticker}")
    df_exported = runner.query_and_export_comp_execucomp_annual_compensation(ticker, start_year, end_year)
    print(f"✅ Retrieved and exported {len(df_exported)} rows of data")

    # 4️⃣ Test auto years functionality
    print(f"\n🔹 Running get_comp_execucomp_annual_compensation_auto_years for {ticker}")
    # df = runner.get_comp_execucomp_annual_compensation_auto_years("XRX")

if __name__ == "__main__":
    #test_single_ticker("XRX", start_year=1992, end_year=2024)
    runner = WRDSQueryRunner()

    # df = runner.get_comp_execucomp_annual_compensation_auto_years("XRX")
    # print(df.head())


# Get column names from comp.funda
    query = """
    SELECT * 
    FROM comp.funda
    WHERE fyear = 2023
    LIMIT 1
    """

    df = pd.read_sql(query, conn)
    all_columns = df.columns.tolist()
    print(f"Total columns: {len(all_columns)}")
    print(all_columns)