# scripts/interactive_query.py

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from modules.wrds_executor import WRDSQueryRunner
import datetime

def main():
    runner = WRDSQueryRunner()
    
    # Ask for compensation type
    print("\nChoose data type:")
    print("1 - Annual Executive Compensation")
    print("2 - Director Compensation")
    print("3 - North America Annual Fundamentals")
    comp_type = input("Enter 1, 2, or 3: ").strip()
    
    if comp_type not in ["1", "2", "3"]:
        print("Invalid choice. Exiting.")
        return

    # Ask for ticker
    ticker = input("Enter company ticker (e.g., AAPL): ").strip().upper()

    # Ask for year range
    custom_years = input("Do you want to enter custom year range? (y/n): ").strip().lower()
    if custom_years == 'y':
        try:
            start_year = int(input("Enter start year (e.g., 2000): ").strip())
            end_year = int(input("Enter end year (e.g., 2024): ").strip())
        except ValueError:
            print("Invalid year input. Using default range 1992 to current year - 1.")
            start_year = 1992
            end_year = datetime.datetime.now().year - 1
    else:
        start_year = 1992
        end_year = datetime.datetime.now().year - 1

    # Ask for action type
    print("\nChoose an action:")
    print("1 - Download and save to CSV")
    print("2 - Download and display data")
    choice = input("Enter 1 or 2: ").strip()

    # Execute based on data type
    if comp_type == "1":  # Annual Executive Compensation
        if choice == "1":
            runner.download_comp_execucomp_annual_compensation(ticker, start_year, end_year)
        elif choice == "2":
            df = runner.get_comp_execucomp_annual_compensation(ticker, start_year, end_year)
            print(df.head())
        else:
            print("Invalid choice. Exiting.")
    elif comp_type == "2":  # Director Compensation
        if choice == "1":
            runner.download_comp_director_compensation(ticker, start_year, end_year)
        elif choice == "2":
            df = runner.get_comp_director_compensation(ticker, start_year, end_year)
            print(df.head())
        else:
            print("Invalid choice. Exiting.")
    else:  # North America Annual Fundamentals
        if choice == "1":
            runner.download_comp_northamerica_fundamentals_annual(ticker, start_year, end_year)
        elif choice == "2":
            df = runner.get_comp_northamerica_fundamentals_annual(ticker, start_year, end_year)
            print(df.head())
        else:
            print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()