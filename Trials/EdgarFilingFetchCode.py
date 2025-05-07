import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import time
from warnings import filterwarnings
from bs4 import XMLParsedAsHTMLWarning

# Suppress XML parser warning
filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Configuration
COMPANY_CIK = '0000320193'  # Apple's CIK
FILING_TYPES = ['10-K', '10-Q']  # Types of filings to retrieve
OUTPUT_DIR = 'sec_filings'
HEADERS = {'User-Agent': 'Your Name your.email@domain.com'}  # SEC requires this

def setup_output_directory():
    """Create output directory if it doesn't exist"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def get_filing_list(cik, filing_types, limit=20):
    """Retrieve list of filings from SEC EDGAR"""
    base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    
    filings = []
    for form_type in filing_types:
        params = {
            'action': 'getcompany',
            'CIK': cik,
            'type': form_type,
            'output': 'atom',
            'count': limit
        }
        
        try:
            response = requests.get(base_url, params=params, headers=HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, features='xml')
            
            for entry in soup.find_all('entry'):
                filing = {
                    'title': entry.title.text if entry.title else None,
                    'type': entry.category['term'] if entry.category else None,
                    'date': entry.updated.text if entry.updated else None,
                    'link': entry.link['href'] if entry.link else None,
                    'accession_number': entry.link['href'].split('/')[-2] if entry.link else None
                }
                filings.append(filing)
            
            time.sleep(0.1)  # Respect SEC rate limits
            
        except Exception as e:
            print(f"Error fetching {form_type} filings: {e}")
    
    return pd.DataFrame(filings)

def get_document_url(accession_number):
    """Get the URL of the actual filing document"""
    try:
        # First get the filing directory
        dir_url = f"https://www.sec.gov/Archives/edgar/data/{int(COMPANY_CIK):010d}/{accession_number}/index.json"
        response = requests.get(dir_url, headers=HEADERS)
        data = response.json()
        
        # Find the primary document (usually the first .htm or .html file)
        for item in data['directory']['item']:
            if item['name'].endswith(('.htm', '.html')):
                return urljoin(dir_url, item['name'])
        
        return None
    except Exception as e:
        print(f"Error getting document URL for {accession_number}: {e}")
        return None

def download_filing(document_url, output_path):
    """Download the actual filing document"""
    try:
        response = requests.get(document_url, headers=HEADERS)
        response.raise_for_status()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        return True
    except Exception as e:
        print(f"Error downloading {document_url}: {e}")
        return False

def main():
    setup_output_directory()
    
    print(f"Fetching {', '.join(FILING_TYPES)} filings for CIK {COMPANY_CIK}...")
    filings_df = get_filing_list(COMPANY_CIK, FILING_TYPES)
    
    if filings_df.empty:
        print("No filings found.")
        return
    
    print(f"\nFound {len(filings_df)} filings:")
    print(filings_df[['type', 'date', 'title']].head())
    
    # Download the actual documents
    print("\nDownloading filing documents...")
    for idx, row in filings_df.iterrows():
        accession = row['accession_number']
        doc_url = get_document_url(accession)
        
        if doc_url:
            filename = f"{row['type']}_{row['date'][:10]}_{accession}.html"
            output_path = os.path.join(OUTPUT_DIR, filename)
            
            print(f"Downloading {filename}...")
            if download_filing(doc_url, output_path):
                print(f"Saved to {output_path}")
            
            time.sleep(0.2)  # Respect SEC rate limits

if __name__ == "__main__":
    main()