from typing import Tuple
import pandas as pd
from .wrds_client import get_conn, close_conn
from . import wrds_queries as Q


def fetch_universes(equity_only: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (CRSP_universe, Compustat_universe) dataframes."""
    conn = get_conn()
    try:
        df_crsp = Q.crsp_universe(conn, equity_only=equity_only)
        df_comp = Q.comp_universe(conn)
        return df_crsp, df_comp
    finally:
        close_conn(conn)


def union_universe(df_crsp: pd.DataFrame, df_comp: pd.DataFrame) -> pd.DataFrame:
    """Uppercase-deduplicated union of tickers with best-available fields.

    Returns columns: [ticker, permno?, gvkey?, cusip?, conm?, cik?]
    """
    # Normalize columns and uppercase tickers
    a = df_crsp.copy()
    b = df_comp.copy()
    for df in (a, b):
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str).str.upper()
    # Merge preferring CRSP permno/ncusip and Comp gvkey/cik/cusip/conm
    # Left union via set of tickers
    all_tickers = pd.Series(pd.concat([a['ticker'], b['ticker']]).dropna().unique(), name='ticker')
    out = all_tickers.to_frame()
    out = out.merge(a[['ticker','permno','ncusip']], on='ticker', how='left')
    out = out.merge(b[['ticker','gvkey','cusip','conm','cik']], on='ticker', how='left')
    return out

