import pandas as pd
import sqlite3
import os
from datetime import datetime

def create_database():
    """Create SQLite database with proper schema."""
    conn = sqlite3.connect('corporate_fraud.db')
    cursor = conn.cursor()
    
    # Create companies table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS companies (
        ticker TEXT PRIMARY KEY,
        name TEXT,
        industry TEXT,
        sector TEXT
    )
    ''')
    
    # Create directors table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS directors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        year INTEGER,
        dirnbr INTEGER,
        dirname TEXT,
        cash_fees REAL,
        stock_awards REAL,
        option_awards REAL,
        noneq_incent REAL,
        pension_chg REAL,
        othcomp REAL,
        total_sec REAL,
        FOREIGN KEY (ticker) REFERENCES companies(ticker),
        UNIQUE(ticker, year, dirnbr)
    )
    ''')
    
    # Create executives table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS executives (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        year INTEGER,
        execid TEXT,
        exec_fullname TEXT,
        title TEXT,
        becameceo TEXT,
        joined_co TEXT,
        leftco TEXT,
        age INTEGER,
        gender TEXT,
        FOREIGN KEY (ticker) REFERENCES companies(ticker),
        UNIQUE(ticker, year, execid)
    )
    ''')
    
    # Create executive_compensation table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS executive_compensation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        year INTEGER,
        execid TEXT,
        salary REAL,
        bonus REAL,
        stock_awards REAL,
        option_awards REAL,
        noneq_incent REAL,
        pension_chg REAL,
        othcomp REAL,
        total_sec REAL,
        FOREIGN KEY (ticker) REFERENCES companies(ticker),
        FOREIGN KEY (ticker, year, execid) REFERENCES executives(ticker, year, execid)
    )
    ''')
    
    conn.commit()
    return conn

def process_company_data(conn, company_dir):
    """Process company data and insert into SQL database."""
    company_code = os.path.basename(company_dir)
    cursor = conn.cursor()
    
    # Find the relevant files
    director_file = None
    exec_file = None
    
    for file in os.listdir(company_dir):
        if 'director_compensation' in file:
            director_file = os.path.join(company_dir, file)
        elif 'execucomp_anncomp' in file:
            exec_file = os.path.join(company_dir, file)
    
    if not (director_file and exec_file):
        print(f"Skipping {company_code}: Missing required files")
        return
    
    try:
        # Process director compensation data
        df_director = pd.read_csv(director_file)
        if not df_director.empty:
            # Insert company info
            company_info = df_director.iloc[0]
            cursor.execute('''
            INSERT OR IGNORE INTO companies (ticker, name, industry, sector)
            VALUES (?, ?, ?, ?)
            ''', (company_code, company_info['coname'], company_info['inddesc'], company_info['sicdesc']))
            
            # Insert director data
            for _, row in df_director.iterrows():
                cursor.execute('''
                INSERT OR REPLACE INTO directors 
                (ticker, year, dirnbr, dirname, cash_fees, stock_awards, option_awards, 
                noneq_incent, pension_chg, othcomp, total_sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    company_code, row['year'], row['dirnbr'], row['dirname'],
                    row['cash_fees'], row['stock_awards'], row['option_awards'],
                    row['noneq_incent'], row['pension_chg'], row['othcomp'], row['total_sec']
                ))
        
        # Process executive compensation data
        df_exec = pd.read_csv(exec_file)
        if not df_exec.empty:
            # Insert executive data
            for _, row in df_exec.iterrows():
                # Insert executive info
                cursor.execute('''
                INSERT OR REPLACE INTO executives 
                (ticker, year, execid, exec_fullname, title, becameceo, 
                joined_co, leftco, age, gender)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    company_code, row['year'], row['execid'], row['exec_fullname'],
                    row['title'], row['becameceo'], row['joined_co'], row['leftco'],
                    row['age'], row['gender']
                ))
                
                # Insert compensation data
                cursor.execute('''
                INSERT OR REPLACE INTO executive_compensation 
                (ticker, year, execid, salary, bonus, stock_awards, option_awards,
                noneq_incent, pension_chg, othcomp, total_sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    company_code, row['year'], row['execid'],
                    row.get('salary', 0), row.get('bonus', 0),
                    row.get('stock_awards', 0), row.get('option_awards', 0),
                    row.get('noneq_incent', 0), row.get('pension_chg', 0),
                    row.get('othcomp', 0), row.get('total_sec', 0)
                ))
        
        conn.commit()
        print(f"Processed data for {company_code}")
        
    except Exception as e:
        print(f"Error processing {company_code}: {str(e)}")
        conn.rollback()

def main():
    # Create database and tables
    conn = create_database()
    
    # Process all companies
    data_dir = 'data'
    for company_dir in os.listdir(data_dir):
        company_path = os.path.join(data_dir, company_dir)
        if os.path.isdir(company_path):
            process_company_data(conn, company_path)
    
    conn.close()

if __name__ == "__main__":
    main() 