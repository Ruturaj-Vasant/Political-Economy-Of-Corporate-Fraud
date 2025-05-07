# modules/wrds_executor.py

import pandas as pd
from config.wrds_config import get_wrds_connection
from modules.csv_exporter import CSVExporter
import datetime

class WRDSQueryRunner:
    def __init__(self):
        self.conn = get_wrds_connection()

    def get_comp_execucomp(self, ticker: str, start_year: int = 1992, end_year: int = 2024) -> pd.DataFrame:
        query = f"""
        SELECT
            codirfin.ticker,
            codirfin.year,
            codirfin.address,
            codirfin.city,
            codirfin.coname,
            codirfin.cusip,
            codirfin.exchange,
            codirfin.gvkey,
            codirfin.inddesc,
            codirfin.naics,
            codirfin.naicsdesc,
            codirfin.sic,
            codirfin.sicdesc,
            codirfin.spcode,
            codirfin.spindex,
            codirfin.state,
            codirfin.sub_tele,
            codirfin.tele,
            codirfin.zip,
            codirfin.ajex,
            codirfin.assetchg,
            codirfin.assets,
            codirfin.bs_volatility,
            codirfin.bs_yield,
            codirfin.commeq,
            codirfin.divyield,
            codirfin.empl,
            codirfin.epsex,
            codirfin.epsex3ls,
            codirfin.epsex5ls,
            codirfin.epsexchg,
            codirfin.epsin,
            codirfin.epsinchg,
            codirfin.fyr,
            codirfin.mktval,
            codirfin.ni,
            codirfin.ni3ls,
            codirfin.ni5ls,
            codirfin.niac,
            codirfin.nibex,
            codirfin.nichg,
            codirfin.nummtgs,
            codirfin.oibd,
            codirfin.oibd3ls,
            codirfin.oibd5ls,
            codirfin.oibdchg,
            codirfin.pcommfees,
            codirfin.prcc,
            codirfin.prccf,
            codirfin.pretax,
            codirfin.roa,
            codirfin.roeavg,
            codirfin.roeper,
            codirfin.sale3ls,
            codirfin.sale5ls,
            codirfin.salechg,
            codirfin.sales,
            codirfin.seq,
            codirfin.shrsout,
            codirfin.srcdate,
            codirfin.trs1yr,
            codirfin.trs3yr,
            codirfin.trs5yr,
            codirfin.anndirret,
            codirfin.dirmtgfee,
            codirfin.diropt,
            codirfin.diroptad,
            codirfin.dirstk,
            codirfin.dirstkad,
            codirfin.pdirpensn,
            codirfin.pexecdirpd
        FROM comp_execucomp.codirfin AS codirfin
        WHERE codirfin.year BETWEEN {start_year} AND {end_year}
        AND codirfin.ticker = '{ticker}'
        """
        return pd.read_sql(query, self.conn)

    def download_comp_execucomp(self, ticker: str, start_year: int = 1992, end_year: int = 2024, output_path: str = "data"):
        df = self.get_comp_execucomp(ticker, start_year, end_year)
        CSVExporter.export(df, f"{ticker}_{start_year}_{end_year}_codirfin.csv", output_path)

    def query_and_export_comp_execucomp(self, ticker: str, start_year: int = 1992, end_year: int = 2024, output_path: str = "data"):
        df = self.get_comp_execucomp(ticker, start_year, end_year)
        CSVExporter.export(df, f"{ticker}_{start_year}_{end_year}_codirfin.csv", output_path)
        return df  # Optional: return df for further use
    import datetime

    def get_comp_execucomp_auto_years(self, ticker: str) -> pd.DataFrame:
        start_year = 1992
        end_year = datetime.datetime.now().year - 1

        query = f"""
        SELECT
            codirfin.ticker,
            codirfin.year,
            codirfin.address,
            codirfin.city,
            codirfin.coname,
            codirfin.cusip,
            codirfin.exchange,
            codirfin.gvkey,
            codirfin.inddesc,
            codirfin.naics,
            codirfin.naicsdesc,
            codirfin.sic,
            codirfin.sicdesc,
            codirfin.spcode,
            codirfin.spindex,
            codirfin.state,
            codirfin.sub_tele,
            codirfin.tele,
            codirfin.zip,
            codirfin.ajex,
            codirfin.assetchg,
            codirfin.assets,
            codirfin.bs_volatility,
            codirfin.bs_yield,
            codirfin.commeq,
            codirfin.divyield,
            codirfin.empl,
            codirfin.epsex,
            codirfin.epsex3ls,
            codirfin.epsex5ls,
            codirfin.epsexchg,
            codirfin.epsin,
            codirfin.epsinchg,
            codirfin.fyr,
            codirfin.mktval,
            codirfin.ni,
            codirfin.ni3ls,
            codirfin.ni5ls,
            codirfin.niac,
            codirfin.nibex,
            codirfin.nichg,
            codirfin.nummtgs,
            codirfin.oibd,
            codirfin.oibd3ls,
            codirfin.oibd5ls,
            codirfin.oibdchg,
            codirfin.pcommfees,
            codirfin.prcc,
            codirfin.prccf,
            codirfin.pretax,
            codirfin.roa,
            codirfin.roeavg,
            codirfin.roeper,
            codirfin.sale3ls,
            codirfin.sale5ls,
            codirfin.salechg,
            codirfin.sales,
            codirfin.seq,
            codirfin.shrsout,
            codirfin.srcdate,
            codirfin.trs1yr,
            codirfin.trs3yr,
            codirfin.trs5yr,
            codirfin.anndirret,
            codirfin.dirmtgfee,
            codirfin.diropt,
            codirfin.diroptad,
            codirfin.dirstk,
            codirfin.dirstkad,
            codirfin.pdirpensn,
            codirfin.pexecdirpd
        FROM comp_execucomp.codirfin AS codirfin
        WHERE codirfin.year BETWEEN {start_year} AND {end_year}
        AND codirfin.ticker = '{ticker}'
        """
        return pd.read_sql(query, self.conn)