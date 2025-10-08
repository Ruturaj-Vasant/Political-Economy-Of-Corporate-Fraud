from typing import Tuple
import pandas as pd
from .client import get_conn, close_conn
from . import queries as Q


def fetch_universes(equity_only: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    conn = get_conn()
    try:
        df_crsp = Q.crsp_universe(conn, equity_only=equity_only)
        df_comp = Q.comp_universe(conn)
        return df_crsp, df_comp
    finally:
        close_conn(conn)


def union_universe(df_crsp: pd.DataFrame, df_comp: pd.DataFrame) -> pd.DataFrame:
    a = df_crsp.copy()
    b = df_comp.copy()
    for df in (a, b):
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str).str.upper()
    all_tickers = pd.Series(pd.concat([a['ticker'], b['ticker']]).dropna().unique(), name='ticker')
    out = all_tickers.to_frame()
    out = out.merge(a[['ticker','permno','ncusip']], on='ticker', how='left')
    out = out.merge(b[['ticker','gvkey','cusip','conm','cik']], on='ticker', how='left')
    return out

