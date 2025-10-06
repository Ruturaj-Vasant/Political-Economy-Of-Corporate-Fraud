# modules/wrds_executor.py

import pandas as pd
from config.wrds_config import get_wrds_connection
from modules.csv_exporter import CSVExporter
import datetime
from pathlib import Path

class WRDSQueryRunner:
    def __init__(self):
        self.conn = get_wrds_connection()

    def _check_existing_data(self, ticker: str, start_year: int, end_year: int, data_type: str, output_path: str = "data") -> bool:
        """
        Check if data already exists for the given parameters.
        
        Args:
            ticker (str): Stock ticker symbol
            start_year (int): Start year for data retrieval
            end_year (int): End year for data retrieval
            data_type (str): Type of data (e.g., 'comp_execucomp_anncomp', 'comp_director_compensation', 'comp_northamerica_fundamentals_annual')
            output_path (str): Base directory for data storage
            
        Returns:
            bool: True if data exists and covers the requested range, False otherwise
        """
        ticker_path = Path(output_path) / ticker
        if not ticker_path.exists():
            return False
            
        # Look for files matching the pattern
        pattern = f"{ticker}_*_{data_type}.csv"
        matching_files = list(ticker_path.glob(pattern))
        
        if not matching_files:
            return False
            
        # Check if any existing file covers our requested range
        for file in matching_files:
            try:
                # Extract year range from filename
                filename = file.stem
                parts = filename.split('_')
                file_start_year = int(parts[1])
                file_end_year = int(parts[2])
                
                # If existing file covers our requested range, we don't need to download
                if file_start_year <= start_year and file_end_year >= end_year:
                    print(f"✅ Data already exists for {ticker} ({data_type}) covering {start_year}-{end_year}")
                    return True
            except (IndexError, ValueError):
                continue
                
        return False

    def get_comp_execucomp_annual_compensation(self, ticker: str, start_year: int = 1992, end_year: int = 2024) -> pd.DataFrame:
        query = f"""
        SELECT
            comp_execucomp.anncomp.ticker,
            comp_execucomp.anncomp.year,
            comp_execucomp.anncomp.address,
            comp_execucomp.anncomp.city,
            comp_execucomp.anncomp.coname,
            comp_execucomp.anncomp.cusip,
            comp_execucomp.anncomp.exchange,
            comp_execucomp.anncomp.gvkey,
            comp_execucomp.anncomp.inddesc,
            comp_execucomp.anncomp.naics,
            comp_execucomp.anncomp.naicsdesc,
            comp_execucomp.anncomp.sic,
            comp_execucomp.anncomp.sicdesc,
            comp_execucomp.anncomp.spcode,
            comp_execucomp.anncomp.spindex,
            comp_execucomp.anncomp.state,
            comp_execucomp.anncomp.sub_tele,
            comp_execucomp.anncomp.tele,
            comp_execucomp.anncomp.zip,
            comp_execucomp.anncomp.execid,
            comp_execucomp.anncomp.exec_fname,
            comp_execucomp.anncomp.exec_fullname,
            comp_execucomp.anncomp.exec_lname,
            comp_execucomp.anncomp.exec_mname,
            comp_execucomp.anncomp.gender,
            comp_execucomp.anncomp.nameprefix,
            comp_execucomp.anncomp.page,
            comp_execucomp.anncomp.becameceo,
            comp_execucomp.anncomp.co_per_rol,
            comp_execucomp.anncomp.execrank,
            comp_execucomp.anncomp.joined_co,
            comp_execucomp.anncomp.leftco,
            comp_execucomp.anncomp.leftofc,
            comp_execucomp.anncomp.pceo,
            comp_execucomp.anncomp.pcfo,
            comp_execucomp.anncomp.reason,
            comp_execucomp.anncomp.rejoin,
            comp_execucomp.anncomp.releft,
            comp_execucomp.anncomp.title,
            comp_execucomp.anncomp.age,
            comp_execucomp.anncomp.allothpd,
            comp_execucomp.anncomp.allothtot,
            comp_execucomp.anncomp.bonus,
            comp_execucomp.anncomp.ceoann,
            comp_execucomp.anncomp.cfoann,
            comp_execucomp.anncomp.chg_ctrl_pymt,
            comp_execucomp.anncomp.comment,
            comp_execucomp.anncomp.defer_balance_tot,
            comp_execucomp.anncomp.defer_contrib_co_tot,
            comp_execucomp.anncomp.defer_contrib_exec_tot,
            comp_execucomp.anncomp.defer_earnings_tot,
            comp_execucomp.anncomp.defer_rpt_as_comp_tot,
            comp_execucomp.anncomp.defer_withdr_tot,
            comp_execucomp.anncomp.eip_unearn_num,
            comp_execucomp.anncomp.eip_unearn_val,
            comp_execucomp.anncomp.execdir,
            comp_execucomp.anncomp.execrankann,
            comp_execucomp.anncomp.interlock,
            comp_execucomp.anncomp.ltip,
            comp_execucomp.anncomp.noneq_incent,
            comp_execucomp.anncomp.old_datafmt_flag,
            comp_execucomp.anncomp.option_awards,
            comp_execucomp.anncomp.option_awards_blk_value,
            comp_execucomp.anncomp.option_awards_fv,
            comp_execucomp.anncomp.option_awards_num,
            comp_execucomp.anncomp.option_awards_rpt_value,
            comp_execucomp.anncomp.opt_exer_num,
            comp_execucomp.anncomp.opt_exer_val,
            comp_execucomp.anncomp.opt_unex_exer_est_val,
            comp_execucomp.anncomp.opt_unex_exer_num,
            comp_execucomp.anncomp.opt_unex_unexer_est_val,
            comp_execucomp.anncomp.opt_unex_unexer_num,
            comp_execucomp.anncomp.othann,
            comp_execucomp.anncomp.othcomp,
            comp_execucomp.anncomp.pension_chg,
            comp_execucomp.anncomp.pension_pymts_tot,
            comp_execucomp.anncomp.pension_value_tot,
            comp_execucomp.anncomp.reprice,
            comp_execucomp.anncomp.ret_yrs,
            comp_execucomp.anncomp.rstkgrnt,
            comp_execucomp.anncomp.rstkvyrs,
            comp_execucomp.anncomp.salary,
            comp_execucomp.anncomp.sal_pct,
            comp_execucomp.anncomp.shrown_excl_opts,
            comp_execucomp.anncomp.shrown_excl_opts_pct,
            comp_execucomp.anncomp.shrown_tot,
            comp_execucomp.anncomp.shrown_tot_pct,
            comp_execucomp.anncomp.shrs_vest_num,
            comp_execucomp.anncomp.shrs_vest_val,
            comp_execucomp.anncomp.stock_awards,
            comp_execucomp.anncomp.stock_awards_fv,
            comp_execucomp.anncomp.stock_unvest_num,
            comp_execucomp.anncomp.stock_unvest_val,
            comp_execucomp.anncomp.tdc1,
            comp_execucomp.anncomp.tdc1_pct,
            comp_execucomp.anncomp.tdc2,
            comp_execucomp.anncomp.tdc2_pct,
            comp_execucomp.anncomp.term_pymt,
            comp_execucomp.anncomp.titleann,
            comp_execucomp.anncomp.total_alt1,
            comp_execucomp.anncomp.total_alt1_pct,
            comp_execucomp.anncomp.total_alt2,
            comp_execucomp.anncomp.total_alt2_pct,
            comp_execucomp.anncomp.total_curr,
            comp_execucomp.anncomp.total_curr_pct,
            comp_execucomp.anncomp.total_sec,
            comp_execucomp.anncomp.total_sec_pct

        FROM comp_execucomp.anncomp

        WHERE comp_execucomp.anncomp.year BETWEEN
            {start_year} AND {end_year}
        AND comp_execucomp.anncomp.ticker IN (
            '{ticker}'
        )
        """
        return pd.read_sql(query, self.conn)

    def download_comp_execucomp_annual_compensation(self, ticker: str, start_year: int = 1992, end_year: int = 2024, output_path: str = "data"):
        # Check if data already exists
        if self._check_existing_data(ticker, start_year, end_year, "comp_execucomp_anncomp", output_path):
            return
            
        df = self.get_comp_execucomp_annual_compensation(ticker, start_year, end_year)
        if df is not None and not df.empty:
            # Create ticker-specific folder path
            ticker_path = f"{output_path}/{ticker}"
            CSVExporter.export(df, f"{ticker}_{start_year}_{end_year}_comp_execucomp_anncomp.csv", ticker_path)
        else:
            print(f"No executive compensation data found for {ticker} between {start_year} and {end_year}")

    def get_comp_director_compensation(self, ticker: str, start_year: int = 1992, end_year: int = 2024) -> pd.DataFrame:
        """
        Fetch director compensation data for a given ticker and year range.
        
        Args:
            ticker (str): Stock ticker symbol
            start_year (int): Start year for data retrieval
            end_year (int): End year for data retrieval
            
        Returns:
            pd.DataFrame: Director compensation data
        """
        query = f"""
        SELECT
            comp_execucomp.directorcomp.cusip,
            comp_execucomp.directorcomp.year,
            comp_execucomp.directorcomp.address,
            comp_execucomp.directorcomp.city,
            comp_execucomp.directorcomp.coname,
            comp_execucomp.directorcomp.exchange,
            comp_execucomp.directorcomp.gvkey,
            comp_execucomp.directorcomp.inddesc,
            comp_execucomp.directorcomp.naics,
            comp_execucomp.directorcomp.naicsdesc,
            comp_execucomp.directorcomp.sic,
            comp_execucomp.directorcomp.sicdesc,
            comp_execucomp.directorcomp.spcode,
            comp_execucomp.directorcomp.spindex,
            comp_execucomp.directorcomp.state,
            comp_execucomp.directorcomp.sub_tele,
            comp_execucomp.directorcomp.tele,
            comp_execucomp.directorcomp.ticker,
            comp_execucomp.directorcomp.zip,
            comp_execucomp.directorcomp.cash_fees,
            comp_execucomp.directorcomp.dirname,
            comp_execucomp.directorcomp.dirnbr,
            comp_execucomp.directorcomp.noneq_incent,
            comp_execucomp.directorcomp.option_awards,
            comp_execucomp.directorcomp.othcomp,
            comp_execucomp.directorcomp.pension_chg,
            comp_execucomp.directorcomp.stock_awards,
            comp_execucomp.directorcomp.total_sec

        FROM comp_execucomp.directorcomp

        WHERE comp_execucomp.directorcomp.year BETWEEN
            {start_year} AND {end_year}
        AND comp_execucomp.directorcomp.ticker IN (
            '{ticker}'
        )
        """
        return pd.read_sql(query, self.conn)

    def download_comp_director_compensation(self, ticker: str, start_year: int = 1992, end_year: int = 2024, output_path: str = "data"):
        """
        Download director compensation data and save it to a CSV file.
        
        Args:
            ticker (str): Stock ticker symbol
            start_year (int): Start year for data retrieval
            end_year (int): End year for data retrieval
            output_path (str): Directory to save the CSV file
        """
        # Check if data already exists
        if self._check_existing_data(ticker, start_year, end_year, "comp_director_compensation", output_path):
            return
            
        df = self.get_comp_director_compensation(ticker, start_year, end_year)
        if df is not None and not df.empty:
            # Create ticker-specific folder path
            ticker_path = f"{output_path}/{ticker}"
            CSVExporter.export(df, f"{ticker}_{start_year}_{end_year}_comp_director_compensation.csv", ticker_path)
        else:
            print(f"No director compensation data found for {ticker} between {start_year} and {end_year}")

    def get_comp_northamerica_fundamentals_annual(self, ticker: str, start_year: int = 1992, end_year: int = 2024) -> pd.DataFrame:
        """
        Fetch annual fundamentals data for a given ticker and year range.
        
        Args:
            ticker (str): Stock ticker symbol
            start_year (int): Start year for data retrieval
            end_year (int): End year for data retrieval
            
        Returns:
            pd.DataFrame: Annual fundamentals data
        """
        query = f"""
        SELECT *
        FROM comp.funda
        WHERE indfmt = 'INDL'
          AND datafmt = 'STD'
          AND popsrc = 'D'
          AND consol = 'C'
          AND fyear BETWEEN {start_year} AND {end_year}
          AND tic = '{ticker.upper()}'
        """
        return pd.read_sql(query, self.conn)

    def download_comp_northamerica_fundamentals_annual(self, ticker: str, start_year: int = 1992, end_year: int = 2024, output_path: str = "data"):
        """
        Download annual fundamentals data and save it to a CSV file.
        
        Args:
            ticker (str): Stock ticker symbol
            start_year (int): Start year for data retrieval
            end_year (int): End year for data retrieval
            output_path (str): Directory to save the CSV file
        """
        # Check if data already exists
        if self._check_existing_data(ticker, start_year, end_year, "comp_northamerica_fundamentals_annual", output_path):
            return
            
        df = self.get_comp_northamerica_fundamentals_annual(ticker, start_year, end_year)
        if df is not None and not df.empty:
            # Create ticker-specific folder path
            ticker_path = f"{output_path}/{ticker}"
            CSVExporter.export(df, f"{ticker}_{start_year}_{end_year}_comp_northamerica_fundamentals_annual.csv", ticker_path)
        else:
            print(f"No fundamentals data found for {ticker} between {start_year} and {end_year}")

    def execute_raw_sql(self, query: str) -> pd.DataFrame:
        """
        Execute a raw SQL query and return the result as a pandas DataFrame.
        
        Args:
            query (str): Raw SQL query to execute.
        
        Returns:
            pd.DataFrame: Query results as a DataFrame.
        """
        return pd.read_sql(query, self.conn)