from typing import Iterable, Sequence
import pandas as pd


def _in_clause(items: Sequence[str]) -> str:
    quoted = ",".join(["'" + i.replace("'", "''") + "'" for i in items])
    return f"({quoted})"


def permno_by_tickers(conn, tickers: Sequence[str]) -> pd.DataFrame:
    """Return most recent CRSP permno per ticker.

    Columns: [ticker, permno, ncusip]
    """
    if not tickers:
        return pd.DataFrame(columns=["ticker", "permno", "ncusip"])
    inlist = _in_clause([t.upper() for t in tickers])
    sql = f"""
        SELECT DISTINCT ON (UPPER(ticker))
               UPPER(ticker) AS ticker,
               permno,
               ncusip
        FROM crsp.stocknames
        WHERE UPPER(ticker) IN {inlist}
        ORDER BY UPPER(ticker), COALESCE(nameenddt, DATE '5999-12-31') DESC
    """
    return pd.read_sql(sql, conn)


def gvkey_by_permnos(conn, permnos: Sequence[int]) -> pd.DataFrame:
    """Return best gvkey per permno via linktable.

    Columns: [permno, gvkey]
    """
    if not permnos:
        return pd.DataFrame(columns=["permno", "gvkey"])
    inlist = ",".join(str(int(p)) for p in permnos)
    sql = f"""
        SELECT DISTINCT ON (lpermno)
               lpermno AS permno,
               gvkey
        FROM crsp.ccmxpf_linktable
        WHERE lpermno IN ({inlist})
          AND linktype IN ('LC','LU','LS','LD','LN')
          AND linkprim IN ('P','C')
        ORDER BY lpermno, (linkenddt IS NULL) DESC, linkenddt DESC
    """
    return pd.read_sql(sql, conn)


def comp_mapping_by_tickers(conn, tickers: Sequence[str]) -> pd.DataFrame:
    """Fallback mapping from Compustat security by ticker.

    Columns: [ticker, gvkey, cusip, conm]
    """
    if not tickers:
        return pd.DataFrame(columns=["ticker", "gvkey", "cusip"])
    inlist = _in_clause([t.upper() for t in tickers])
    sql = f"""
        SELECT DISTINCT ON (UPPER(s.tic))
               UPPER(s.tic) AS ticker,
               c.gvkey,
               s.cusip,
               c.conm
        FROM comp.company c
        JOIN comp.security s USING (gvkey)
        WHERE UPPER(s.tic) IN {inlist}
        ORDER BY UPPER(s.tic), c.gvkey
    """
    return pd.read_sql(sql, conn)


def exchange_by_permnos(conn, permnos: Sequence[int]) -> pd.DataFrame:
    """Latest exchange code per permno from CRSP monthly file.

    Columns: [permno, exchcd]
    """
    if not permnos:
        return pd.DataFrame(columns=["permno", "exchcd"])
    inlist = ",".join(str(int(p)) for p in permnos)
    sql = f"""
        SELECT DISTINCT ON (permno)
               permno, hexcd AS exchcd
        FROM crsp.msf
        WHERE permno IN ({inlist})
        ORDER BY permno, date DESC
    """
    return pd.read_sql(sql, conn)


def delist_by_permnos(conn, permnos: Sequence[int]) -> pd.DataFrame:
    """Latest delist code per permno.

    Columns: [permno, dlstcd]
    """
    if not permnos:
        return pd.DataFrame(columns=["permno", "dlstcd"])
    inlist = ",".join(str(int(p)) for p in permnos)
    sql = f"""
        SELECT DISTINCT ON (permno) permno, dlstcd
        FROM crsp.msedelist
        WHERE permno IN ({inlist})
        ORDER BY permno, dlstdt DESC
    """
    return pd.read_sql(sql, conn)


def currency_by_gvkeys(conn, gvkeys: Sequence[str]) -> pd.DataFrame:
    """Latest currency per gvkey from Compustat funda.

    Columns: [gvkey, curcd]
    """
    if not gvkeys:
        return pd.DataFrame(columns=["gvkey", "curcd"])
    inlist = _in_clause([g for g in gvkeys])
    sql = f"""
        SELECT DISTINCT ON (gvkey)
               gvkey, curcd
        FROM comp.funda
        WHERE gvkey IN {inlist}
          AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'
        ORDER BY gvkey, fyear DESC
    """
    return pd.read_sql(sql, conn)


def execucomp_desc_by_tickers(conn, tickers: Sequence[str]) -> pd.DataFrame:
    """Latest Execucomp descriptors + location per ticker.

    Columns: [ticker, naicsdesc, sicdesc, address, city, state, zip]
    """
    if not tickers:
        return pd.DataFrame(columns=["ticker", "naicsdesc", "sicdesc", "address", "city", "state", "zip"])
    inlist = _in_clause([t.upper() for t in tickers])
    sql = f"""
        SELECT DISTINCT ON (UPPER(ticker))
               UPPER(ticker) AS ticker,
               naicsdesc, sicdesc, address, city, state, zip, year
        FROM comp_execucomp.anncomp
        WHERE UPPER(ticker) IN {inlist}
        ORDER BY UPPER(ticker), year DESC
    """
    df = pd.read_sql(sql, conn)
    if 'year' in df.columns:
        df = df.drop(columns=['year'])
    return df


def cik_by_tickers(conn, tickers: Sequence[str]) -> pd.DataFrame:
    """Map ticker -> cik via Compustat company/security.

    Columns: [ticker, cik]
    """
    if not tickers:
        return pd.DataFrame(columns=["ticker", "cik"])
    inlist = _in_clause([t.upper() for t in tickers])
    sql = f"""
        SELECT DISTINCT ON (UPPER(s.tic))
               UPPER(s.tic) AS ticker,
               c.cik
        FROM comp.company c
        JOIN comp.security s USING (gvkey)
        WHERE UPPER(s.tic) IN {inlist}
        ORDER BY UPPER(s.tic), c.gvkey
    """
    return pd.read_sql(sql, conn)


def cik_by_permnos(conn, permnos: Sequence[int]) -> pd.DataFrame:
    """Map permno -> cik via linktable -> Compustat.

    Columns: [permno, cik]
    """
    if not permnos:
        return pd.DataFrame(columns=["permno", "cik"])
    inlist = ",".join(str(int(p)) for p in permnos)
    sql = f"""
        SELECT DISTINCT ON (l.lpermno)
               l.lpermno AS permno,
               c.cik
        FROM crsp.ccmxpf_linktable l
        JOIN comp.company c USING (gvkey)
        WHERE l.lpermno IN ({inlist})
          AND l.linktype IN ('LC','LU','LS','LD','LN')
          AND l.linkprim IN ('P','C')
        ORDER BY l.lpermno, (l.linkenddt IS NULL) DESC, l.linkenddt DESC
    """
    return pd.read_sql(sql, conn)


def crsp_universe(conn, equity_only: bool = True) -> pd.DataFrame:
    """Return a CRSP-derived ticker universe.

    Columns: [ticker, permno, ncusip]
    """
    if equity_only:
        sql = """
            SELECT DISTINCT ON (UPPER(n.ticker))
                   UPPER(n.ticker) AS ticker,
                   n.permno,
                   n.ncusip
            FROM crsp.stocknames n
            WHERE n.shrcd IN (10,11)
              AND n.ticker IS NOT NULL
            ORDER BY UPPER(n.ticker), COALESCE(n.nameenddt, DATE '5999-12-31') DESC
        """
    else:
        sql = """
            SELECT DISTINCT ON (UPPER(ticker))
                   UPPER(ticker) AS ticker,
                   permno,
                   ncusip
            FROM crsp.stocknames
            WHERE ticker IS NOT NULL
            ORDER BY UPPER(ticker), COALESCE(nameenddt, DATE '5999-12-31') DESC
        """
    return pd.read_sql(sql, conn)


def comp_universe(conn) -> pd.DataFrame:
    """Return a Compustat-derived ticker universe.

    Columns: [ticker, gvkey, conm, cik, cusip]
    """
    sql = """
        SELECT DISTINCT ON (UPPER(s.tic))
               UPPER(s.tic) AS ticker,
               c.gvkey, c.conm, c.cik, s.cusip
        FROM comp.company c
        JOIN comp.security s USING (gvkey)
        WHERE s.tic IS NOT NULL
        ORDER BY UPPER(s.tic), c.gvkey
    """
    return pd.read_sql(sql, conn)
