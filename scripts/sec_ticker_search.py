import json
from pathlib import Path
from typing import Dict, List

class SECCompanySearch:
    def __init__(self):
        # Get the project root directory (parent of scripts directory)
        self.project_root = Path(__file__).parent.parent
        self.tickers_file = self.project_root / "metadata" / "sec_company_tickers.json"
        self.tickers_data = None
        self._load_data()

    def _load_data(self):
        """Load the tickers data from the JSON file."""
        if not self.tickers_file.exists():
            raise FileNotFoundError(f"Ticker file not found: {self.tickers_file}")
        with open(self.tickers_file, 'r') as f:
            self.tickers_data = json.load(f)

    def search_by_company_name(self, company_name: str) -> List[Dict]:
        """
        Search for companies by name (case-insensitive partial match).
        Args:
            company_name: The company name or part of it to search for
        Returns:
            List of dictionaries containing company information
        """
        if not self.tickers_data:
            raise ValueError("Tickers data not loaded")
        company_name = company_name.lower()
        results = []
        for cik, company_info in self.tickers_data.items():
            if company_name in company_info['title'].lower():
                results.append({
                    'cik': cik,
                    'ticker': company_info['ticker'],
                    'company_name': company_info['title']
                })
        return results

def main():
    searcher = SECCompanySearch()
    while True:
        company_name = input("\nEnter company name to search (or 'quit' to exit): ")
        if company_name.lower() == 'quit':
            break
        results = searcher.search_by_company_name(company_name)
        if results:
            print(f"\nFound {len(results)} matches:")
            for result in results:
                print(f"Company: {result['company_name']}")
                print(f"Ticker: {result['ticker']}")
                print(f"CIK: {result['cik']}")
                print("-" * 50)
        else:
            print("No matches found.")

if __name__ == "__main__":
    main() 