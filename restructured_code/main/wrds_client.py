from typing import Optional
from config.wrds_config import get_wrds_connection


def get_conn():
    """Return a live WRDS connection or raise RuntimeError."""
    conn = get_wrds_connection()
    if conn is None:
        raise RuntimeError("WRDS connection failed. Check .pgpass and network.")
    return conn


def close_conn(conn) -> None:
    try:
        conn.close()
    except Exception:
        pass

