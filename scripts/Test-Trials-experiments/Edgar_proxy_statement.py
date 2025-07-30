# %%
import os
from edgar import *
# Set identity
set_identity('ruturaj@gmail.com')

# Choose the ticker
ticker = 'MSFT'
company = Company(ticker)
print(f"Company Name: {getattr(company, 'name', 'N/A')}, CIK: {getattr(company, 'cik', 'N/A')}")

# # Fetch all DEF 14A filings
# proxy_filings = company.get_filings(form='DEF 14A')
# print(f"Total DEF 14A Filings Found: {len(proxy_filings)}")

# Fetch all 10-K filings
proxy_filings = company.get_filings(form='10-K')
print(f"Total 10-K Filings Found: {len(proxy_filings)}")

print(proxy_filings)

# Create base directory for the company
base_path = f"data/{ticker.upper()}"
os.makedirs(base_path, exist_ok=True)
print(f"Directory created at: {base_path}")

# Loop through filings and save each as a text file
for i, filing in enumerate(proxy_filings):
    print(f"\nProcessing filing {i+1}/{len(proxy_filings)}")
    try:
        # Use filing_date if available, else fallback to index-based name
        filing_date = getattr(filing, "filing_date", None)
        if not filing_date:
            filing_date = f"filing_{i+1}"
        # filename = os.path.join(base_path, f"{filing_date}_DEF14A.txt")
        filename = os.path.join(base_path, f"{filing_date}_10K.txt")

        # Save filing text
        text = filing.text()
        with open(filename, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Saved: {filename}")
    except Exception as e:
        print(f"Error saving filing {i+1}: {e}")