from typing import Optional
import pandas as pd
from config.wrds_config import get_wrds_connection


def q(conn, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params)


def main():
    conn = get_wrds_connection()
    if conn is None:
        print("Connection failed; cannot run queries.")
        return 1

    T = "AAPL"

    print("-- stocknames by ticker --")
    df_names = q(
        conn,
        """
        SELECT permno, permco, UPPER(ticker) AS ticker, comnam, namedt, nameenddt, ncusip
        FROM crsp.stocknames
        WHERE UPPER(ticker) = %(tic)s
        ORDER BY COALESCE(nameenddt, DATE '5999-12-31') DESC
        LIMIT 5;
        """,
        {"tic": T},
    )
    print(df_names.head(5).to_string(index=False))

    permno = None
    if not df_names.empty:
        permno = int(df_names.iloc[0]["permno"])  # take most recent name
        print(f"Picked permno: {permno}")

    gvkey_from_link: Optional[str] = None
    if permno is not None:
        print("-- linktable by permno (preferred gvkey) --")
        df_link = q(
            conn,
            """
            SELECT gvkey, linkprim, linktype, linkdt, linkenddt
            FROM crsp.ccmxpf_linktable
            WHERE lpermno = %(permno)s
              AND linktype IN ('LC','LU','LS','LD','LN')
              AND linkprim IN ('P','C')
            ORDER BY (linkenddt IS NULL) DESC, linkenddt DESC
            LIMIT 5;
            """,
            {"permno": permno},
        )
        print(df_link.to_string(index=False))
        if not df_link.empty:
            gvkey_from_link = str(df_link.iloc[0]["gvkey"]).zfill(6)

        print("-- latest exchange code from dsf --")
        df_exch = q(
            conn,
            """
            SELECT d.permno, d.date, d.hexcd AS exchcd
            FROM crsp.dsf d
            WHERE d.permno = %(permno)s
            ORDER BY d.date DESC
            LIMIT 1;
            """,
            {"permno": permno},
        )
        print(df_exch.to_string(index=False))

        print("-- delisting info (msedelist) --")
        df_delist = q(
            conn,
            """
            SELECT permno, dlstdt, dlstcd, dlret
            FROM crsp.msedelist
            WHERE permno = %(permno)s
            ORDER BY dlstdt DESC
            LIMIT 1;
            """,
            {"permno": permno},
        )
        print(df_delist.to_string(index=False))

    print("-- comp.company x comp.security by ticker (fallback gvkey) --")
    df_comp = q(
        conn,
        """
        SELECT c.gvkey, UPPER(s.tic) AS tic, c.conm, s.cusip, c.naics, c.sic, c.fic
        FROM comp.company c
        JOIN comp.security s USING (gvkey)
        WHERE UPPER(s.tic) = %(tic)s
        LIMIT 5;
        """,
        {"tic": T},
    )
    print(df_comp.to_string(index=False))

    print("-- comp.funda latest currency --")
    if gvkey_from_link:
        df_cur = q(
            conn,
            """
            SELECT gvkey, fyear, curcd
            FROM comp.funda
            WHERE gvkey = %(gvkey)s
              AND indfmt = 'INDL' AND datafmt = 'STD' AND popsrc = 'D' AND consol = 'C'
            ORDER BY fyear DESC
            LIMIT 1;
            """,
            {"gvkey": gvkey_from_link},
        )
    else:
        df_cur = q(
            conn,
            """
            SELECT gvkey, UPPER(tic) AS tic, fyear, curcd
            FROM comp.funda
            WHERE UPPER(tic) = %(tic)s
              AND indfmt = 'INDL' AND datafmt = 'STD' AND popsrc = 'D' AND consol = 'C'
            ORDER BY fyear DESC
            LIMIT 1;
            """,
            {"tic": T},
        )
    print(df_cur.to_string(index=False))

    print("-- execucomp (anncomp) descriptors & location --")
    df_exec = q(
        conn,
        """
        SELECT ticker, coname, gvkey, cusip, exchange, naics, naicsdesc, sic, sicdesc, address, city, state, zip, year
        FROM comp_execucomp.anncomp
        WHERE UPPER(ticker) = %(tic)s
        ORDER BY year DESC
        LIMIT 1;
        """,
        {"tic": T},
    )
    print(df_exec.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

