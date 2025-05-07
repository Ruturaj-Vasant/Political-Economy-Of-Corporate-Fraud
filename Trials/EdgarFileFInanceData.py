import requests
import pandas as pd
import os
import time
from datetime import datetime

# Configuration
COMPANY_CIK = '0000320193'  # Apple's CIK
FILING_TYPES = ['10-K', '10-Q']  # Filing types to fetch
OUTPUT_DIR = 'sec_structured_data'
HEADERS = {'User-Agent': 'rvt2018@nyu.edu'}  # SEC requires your institutional email

def setup_output_directory():
    """Create output directory if it doesn't exist"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def get_filing_list(cik, filing_types, limit=10):
    """Get list of filings from SEC submissions API"""
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    
    try:
        response = requests.get(submissions_url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        # Extract recent filings
        filings = []
        for i in range(len(data['filings']['recent']['accessionNumber'])):
            if data['filings']['recent']['form'][i] in filing_types:
                filing = {
                    'accession_number': data['filings']['recent']['accessionNumber'][i],
                    'form': data['filings']['recent']['form'][i],
                    'filing_date': data['filings']['recent']['filingDate'][i],
                    'report_date': data['filings']['recent']['reportDate'][i],
                    'primary_document': data['filings']['recent']['primaryDocument'][i],
                    'url': f"https://www.sec.gov/Archives/edgar/data/{cik}/{data['filings']['recent']['accessionNumber'][i].replace('-', '')}/{data['filings']['recent']['primaryDocument'][i]}"
                }
                filings.append(filing)
                if len(filings) >= limit:
                    break
        
        return pd.DataFrame(filings)
    
    except Exception as e:
        print(f"Error fetching filing list: {e}")
        return pd.DataFrame()

def get_filing_facts(accession_number):
    """Get structured data from SEC's facts API"""
    facts_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{COMPANY_CIK}.json"
    
    try:
        response = requests.get(facts_url, headers=HEADERS)
        response.raise_for_status()
        facts_data = response.json()
        
        # Extract relevant facts for this filing
        filing_facts = {}
        for taxonomy in ['us-gaap', 'dei']:
            if taxonomy in facts_data['facts']:
                for fact_name, fact_data in facts_data['facts'][taxonomy].items():
                    for unit in fact_data['units']:
                        for entry in fact_data['units'][unit]:
                            if entry['accn'] == accession_number:
                                if fact_name not in filing_facts:
                                    filing_facts[fact_name] = []
                                filing_facts[fact_name].append({
                                    'value': entry['val'],
                                    'form': entry['form'],
                                    'fy': entry.get('fy'),
                                    'fp': entry.get('fp'),
                                    'unit': unit
                                })
        return filing_facts
    
    except Exception as e:
        print(f"Error fetching facts for {accession_number}: {e}")
        return {}

def get_filing_structured_data(accession_number):
    """Get complete structured data package for a filing"""
    accession_clean = accession_number.replace('-', '')
    structured_url = f"https://www.sec.gov/Archives/edgar/data/{int(COMPANY_CIK):010d}/{accession_clean}/{accession_clean}-index.json"
    
    try:
        response = requests.get(structured_url, headers=HEADERS)
        response.raise_for_status()
        index_data = response.json()
        
        # Get the XBRL JSON file if available
        xbrl_json = None
        for file in index_data['directory']['item']:
            if file['name'].endswith('.json') and 'xbrl' in file['name'].lower():
                xbrl_url = urljoin(structured_url, file['name'])
                xbrl_response = requests.get(xbrl_url, headers=HEADERS)
                xbrl_json = xbrl_response.json()
                break
        
        return {
            'index_data': index_data,
            'xbrl_data': xbrl_json,
            'facts': get_filing_facts(accession_number)
        }
    
    except Exception as e:
        print(f"Error fetching structured data for {accession_number}: {e}")
        return None

def save_structured_data(data, accession_number, form_type):
    """Save structured data to JSON file"""
    filename = f"{form_type}_{accession_number}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    return filepath

def main():
    setup_output_directory()
    
    print(f"Fetching {', '.join(FILING_TYPES)} filings for CIK {COMPANY_CIK}...")
    filings_df = get_filing_list(COMPANY_CIK, FILING_TYPES)
    
    if filings_df.empty:
        print("No filings found.")
        return
    
    print(f"\nFound {len(filings_df)} filings:")
    print(filings_df[['form', 'filing_date', 'report_date']])
    
    # Process each filing
    print("\nFetching structured data...")
    for idx, row in filings_df.iterrows():
        print(f"\nProcessing {row['form']} filed on {row['filing_date']}...")
        
        structured_data = get_filing_structured_data(row['accession_number'])
        if structured_data:
            saved_path = save_structured_data(
                structured_data,
                row['accession_number'],
                row['form']
            )
            print(f"Saved structured data to: {saved_path}")
        
        time.sleep(0.5)  # Respect SEC rate limits

if __name__ == "__main__":
    import json
    from urllib.parse import urljoin
    main()