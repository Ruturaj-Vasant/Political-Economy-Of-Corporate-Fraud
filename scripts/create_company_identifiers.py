import pandas as pd
import json
from pathlib import Path
from typing import Dict, List
import time
import sys

# Add project root to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from modules.wrds_executor import WRDSQueryRunner

def get_all_company_identifiers(wrds_runner) -> pd.DataFrame:
    """
    Get all company identifiers from WRDS.
    Combines data from Compustat, CRSP, and SEC.
    """
    # First, let's check the structure of comp.company
    check_query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'comp' 
    AND table_name = 'company'
    AND column_name LIKE '%cusip%'
    """
    
    print("Checking CUSIP column names in comp.company...")
    cusip_columns = pd.read_sql(check_query, wrds_runner.conn)
    print("Available CUSIP columns:", cusip_columns['column_name'].tolist())
    
    query = """
    WITH latest_names AS (
        SELECT 
            permno,
            ticker,
            comnam,
            namedt,
            nameenddt,
            ROW_NUMBER() OVER (PARTITION BY permno ORDER BY namedt DESC) as rn
        FROM crsp.stocknames
        WHERE ticker IS NOT NULL 
        AND ticker != ''
    ),
    compustat_data AS (
        SELECT 
            gvkey,
            conm as company_name,
            cusip as cusip,  -- Using the standard CUSIP column
            tic as ticker
        FROM comp.company
        WHERE tic IS NOT NULL 
        AND tic != ''
    ),
    crsp_data AS (
        SELECT 
            b.lpermno as permno,
            b.gvkey,
            c.ticker,
            c.comnam as company_name
        FROM 
            crsp.ccmxpf_lnkhist b
        JOIN
            latest_names c
        ON
            b.lpermno = c.permno
        WHERE 
            c.rn = 1  -- Get the most recent name for each PERMNO
            AND b.linktype = 'LC'  -- Primary link
            AND b.linkprim = 'P'   -- Primary link
    )
    SELECT DISTINCT
        COALESCE(c.gvkey, cd.gvkey) as gvkey,
        c.permno,
        COALESCE(c.ticker, cd.ticker) as ticker,
        COALESCE(c.company_name, cd.company_name) as company_name,
        cd.cusip
    FROM 
        crsp_data c
    FULL OUTER JOIN
        compustat_data cd
    ON
        c.gvkey = cd.gvkey
    WHERE
        COALESCE(c.ticker, cd.ticker) IS NOT NULL
    """
    
    print("Fetching all company identifiers from WRDS...")
    df = pd.read_sql(query, wrds_runner.conn)
    print(f"Found {len(df)} company mappings")
    return df

def load_sec_tickers() -> Dict:
    """Load the SEC company tickers data."""
    project_root = Path(__file__).parent.parent
    tickers_file = project_root / "metadata" / "sec_company_tickers.json"
    
    with open(tickers_file, 'r') as f:
        return json.load(f)

def create_comprehensive_mapping(wrds_data: pd.DataFrame, sec_data: Dict) -> Dict:
    """
    Create a comprehensive mapping of company identifiers.
    """
    # Initialize the mapping with SEC data
    comprehensive_mapping = {}
    
    # First, add all SEC data
    for cik, info in sec_data.items():
        comprehensive_mapping[cik] = {
            'cik': cik,
            'ticker': info['ticker'],
            'company_name': info['title'],
            'permno': None,
            'gvkey': None,
            'cusip': None
        }
    
    # Then, add WRDS data where we can match
    for _, row in wrds_data.iterrows():
        ticker = row['ticker'].upper()
        
        # Try to find matching CIK by ticker
        matching_ciks = [
            cik for cik, info in sec_data.items() 
            if info['ticker'].upper() == ticker
        ]
        
        if matching_ciks:
            # Update existing entry
            for cik in matching_ciks:
                comprehensive_mapping[cik].update({
                    'permno': str(row['permno']) if pd.notna(row['permno']) else None,
                    'gvkey': str(row['gvkey']) if pd.notna(row['gvkey']) else None,
                    'cusip': str(row['cusip']) if pd.notna(row['cusip']) else None
                })
        else:
            # Add new entry
            new_cik = f"WRDS_{row['gvkey']}" if pd.notna(row['gvkey']) else f"WRDS_{row['permno']}"
            comprehensive_mapping[new_cik] = {
                'cik': new_cik,
                'ticker': row['ticker'],
                'company_name': row['company_name'],
                'permno': str(row['permno']) if pd.notna(row['permno']) else None,
                'gvkey': str(row['gvkey']) if pd.notna(row['gvkey']) else None,
                'cusip': str(row['cusip']) if pd.notna(row['cusip']) else None
            }
    
    return comprehensive_mapping

def save_comprehensive_mapping(data: Dict):
    """Save the comprehensive mapping to a JSON file."""
    project_root = Path(__file__).parent.parent
    mapping_file = project_root / "metadata" / "company_identifiers.json"
    
    with open(mapping_file, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved comprehensive mapping to {mapping_file}")

def main():
    try:
        # Initialize WRDS runner
        wrds_runner = WRDSQueryRunner()
        
        # Get data from both sources
        wrds_data = get_all_company_identifiers(wrds_runner)
        sec_data = load_sec_tickers()
        
        # Create comprehensive mapping
        comprehensive_mapping = create_comprehensive_mapping(wrds_data, sec_data)
        save_comprehensive_mapping(comprehensive_mapping)
        
        # Print statistics
        total_companies = len(comprehensive_mapping)
        companies_with_permno = sum(1 for info in comprehensive_mapping.values() if info.get('permno'))
        companies_with_gvkey = sum(1 for info in comprehensive_mapping.values() if info.get('gvkey'))
        companies_with_cusip = sum(1 for info in comprehensive_mapping.values() if info.get('cusip'))
        
        print("\nMapping Statistics:")
        print(f"Total companies: {total_companies}")
        print(f"Companies with PERMNO: {companies_with_permno}")
        print(f"Companies with GVKEY: {companies_with_gvkey}")
        print(f"Companies with CUSIP: {companies_with_cusip}")
        print(f"PERMNO Coverage: {(companies_with_permno/total_companies)*100:.2f}%")
        print(f"GVKEY Coverage: {(companies_with_gvkey/total_companies)*100:.2f}%")
        print(f"CUSIP Coverage: {(companies_with_cusip/total_companies)*100:.2f}%")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 