import psycopg2
import pandas as pd

def get_wrds_connection():
    """
    Connect to WRDS using the .pgpass file for authentication.
    Ensure the file is at ~/.pgpass with correct permissions.
    """
    try:
        conn = psycopg2.connect(
            host="wrds-pgdata.wharton.upenn.edu",
            port=9737,
            database="wrds",
            user="rvt2018",  # Username is still required
            sslmode="require"
            # No password needed; it's fetched from ~/.pgpass
        )
        print("✅ Connected to WRDS via .pgpass")
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

if __name__ == "__main__":
    conn = get_wrds_connection()
    if conn:
        df = pd.read_sql("SELECT current_date;", conn)
        print("🎉 WRDS test query result:")
        print(df)