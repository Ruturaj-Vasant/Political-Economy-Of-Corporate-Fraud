import sqlite3
import pandas as pd

def get_director_analysis(conn, ticker):
    """Get director analysis for a specific company."""
    query = """
    WITH director_counts AS (
        SELECT 
            year,
            MAX(dirnbr) as max_directors
        FROM directors
        WHERE ticker = ?
        GROUP BY year
    ),
    director_turnover AS (
        SELECT 
            d1.year,
            COUNT(DISTINCT CASE WHEN d2.dirname IS NULL THEN d1.dirname END) as new_directors,
            COUNT(DISTINCT CASE WHEN d1.dirname IS NULL THEN d2.dirname END) as departed_directors
        FROM directors d1
        LEFT JOIN directors d2 ON 
            d1.ticker = d2.ticker AND 
            d1.year = d2.year + 1 AND 
            d1.dirname = d2.dirname
        WHERE d1.ticker = ?
        GROUP BY d1.year
    )
    SELECT 
        dc.year,
        dc.max_directors,
        dt.new_directors,
        dt.departed_directors
    FROM director_counts dc
    LEFT JOIN director_turnover dt ON dc.year = dt.year
    ORDER BY dc.year
    """
    
    return pd.read_sql_query(query, conn, params=(ticker, ticker))

def get_ceo_analysis(conn, ticker):
    """Get CEO analysis for a specific company."""
    query = """
    SELECT 
        e.year,
        e.exec_fullname as ceo_name,
        e.becameceo,
        CASE 
            WHEN e.becameceo IS NOT NULL 
            THEN e.year - CAST(SUBSTR(e.becameceo, 1, 4) AS INTEGER)
            ELSE NULL 
        END as tenure,
        ec.total_sec as total_compensation
    FROM executives e
    LEFT JOIN executive_compensation ec ON 
        e.ticker = ec.ticker AND 
        e.year = ec.year AND 
        e.execid = ec.execid
    WHERE e.ticker = ? 
    AND e.title LIKE '%CEO%'
    ORDER BY e.year
    """
    
    return pd.read_sql_query(query, conn, params=(ticker,))

def get_combined_analysis(conn, ticker):
    """Get combined analysis of directors and CEO for a specific company."""
    query = """
    WITH director_counts AS (
        SELECT 
            year,
            MAX(dirnbr) as max_directors
        FROM directors
        WHERE ticker = ?
        GROUP BY year
    ),
    director_turnover AS (
        SELECT 
            d1.year,
            COUNT(DISTINCT CASE WHEN d2.dirname IS NULL THEN d1.dirname END) as new_directors,
            COUNT(DISTINCT CASE WHEN d1.dirname IS NULL THEN d2.dirname END) as departed_directors
        FROM directors d1
        LEFT JOIN directors d2 ON 
            d1.ticker = d2.ticker AND 
            d1.year = d2.year + 1 AND 
            d1.dirname = d2.dirname
        WHERE d1.ticker = ?
        GROUP BY d1.year
    ),
    ceo_info AS (
        SELECT 
            e.year,
            e.exec_fullname as ceo_name,
            e.becameceo,
            CASE 
                WHEN e.becameceo IS NOT NULL 
                THEN e.year - CAST(SUBSTR(e.becameceo, 1, 4) AS INTEGER)
                ELSE NULL 
            END as tenure
        FROM executives e
        WHERE e.ticker = ? 
        AND e.title LIKE '%CEO%'
    )
    SELECT 
        dc.year,
        dc.max_directors,
        dt.new_directors,
        dt.departed_directors,
        c.ceo_name,
        c.becameceo,
        c.tenure
    FROM director_counts dc
    LEFT JOIN director_turnover dt ON dc.year = dt.year
    LEFT JOIN ceo_info c ON dc.year = c.year
    ORDER BY dc.year
    """
    
    return pd.read_sql_query(query, conn, params=(ticker, ticker, ticker))

def analyze_company(conn, ticker):
    """Analyze a specific company and save results to CSV files."""
    # Get analyses
    director_analysis = get_director_analysis(conn, ticker)
    ceo_analysis = get_ceo_analysis(conn, ticker)
    combined_analysis = get_combined_analysis(conn, ticker)
    
    # Save to CSV files
    director_analysis.to_csv(f'data/{ticker}/{ticker}_director_analysis.csv', index=False)
    ceo_analysis.to_csv(f'data/{ticker}/{ticker}_ceo_analysis.csv', index=False)
    combined_analysis.to_csv(f'data/{ticker}/{ticker}_combined_analysis.csv', index=False)
    
    print(f"Created analysis files for {ticker}")

def main():
    # Connect to database
    conn = sqlite3.connect('corporate_fraud.db')
    
    # Get list of companies
    companies = pd.read_sql_query("SELECT ticker FROM companies", conn)
    
    # Analyze each company
    for ticker in companies['ticker']:
        analyze_company(conn, ticker)
    
    conn.close()

if __name__ == "__main__":
    main() 