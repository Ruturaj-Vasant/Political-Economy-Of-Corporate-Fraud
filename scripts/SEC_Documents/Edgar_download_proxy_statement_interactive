from bs4 import BeautifulSoup
import os
import time
from datetime import datetime
from edgar import *

start_time = time.time()
print(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

set_identity('PoliticalStudy1@gmail.com')

form_options = {
    "1": "10-K",
    "2": "DEF 14A",
    "3": "10-Q",
    "4": "13F-HR",
    "5": "8-K",
    "6": "3",
    "7": "4",
    "8": "5",
    "9": "NPORT-P",
    "10": "D",
    "11": "C",
    "12": "MA-I",
    "13": "144"
}

print("Select the form type:")
for key, value in form_options.items():
    print(f"{key}. {value}")
form_choice = input("Enter the number corresponding to the form type: ").strip()
form_code = form_options.get(form_choice)
if not form_code:
    print("Invalid form choice. Exiting.")
    exit()

tickers_input = input("Enter ticker(s), separated by commas if multiple: ").strip()
tickers = [ticker.strip().upper() for ticker in tickers_input.split(",")]

print("\nSelect the save method for each filing:")
print("1. Open in browser")
print("2. Save as text")
print("3. Save as HTML")
save_choice = input("Enter the number corresponding to the save method: ").strip()
if save_choice not in {"1", "2", "3"}:
    print("Invalid save method choice. Exiting.")
    exit()

for ticker in tickers:
    print("\n" + "="*40)
    print(f"Processing ticker: {ticker}")
    print("="*40)
    company = Company(ticker)
    print(f"Company: {company.name} ({company.cik})")
    filings = company.get_filings(form=form_code)
    if not filings:
        print(f"No filings found for {ticker} with form {form_code}. Skipping folder creation.")
        print("="*40 + "\n")
        continue
    print(f"Found {len(filings)} filings for form {form_code}.")
    base_path = f"data/{ticker.upper()}"
    if save_choice in {"2", "3"}:
        os.makedirs(base_path, exist_ok=True)
    for idx, filing in enumerate(filings):
        filing_date = getattr(filing, "filing_date", None)
        if filing_date is None:
            filing_date_str = f"index_{idx}"
        else:
            filing_date_str = filing_date
        print(f"Filing {idx + 1}: Date: {filing_date_str}, URL: {filing.url}")
        if save_choice == "1":
            try:
                filing.open()
                print(f"Opened filing {idx + 1} in browser.")
                time.sleep(2)  # Sleep to allow browser to open
            except Exception as e:
                print(f"Error opening filing {idx + 1} in browser: {e}")
        elif save_choice == "2":
            filename = os.path.join(base_path, f"{filing_date_str}_{form_code.replace(' ', '')}.txt")
            try:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(filing.text())
                print(f"Saved filing {idx + 1} to {filename}")
            except Exception as e:
                print(f"Error saving filing {idx + 1}: {e}")
        elif save_choice == "3":
            # First, try to get HTML, else fallback to text
            filename_html = os.path.join(base_path, f"{filing_date_str}_{form_code.replace(' ', '')}.html")
            filename_txt = os.path.join(base_path, f"{filing_date_str}_{form_code.replace(' ', '')}.txt")
            try:
                raw_html = filing.html()
                if raw_html is None:
                    print(f"Filing {idx + 1}: No HTML available, attempting to save as plain text instead.")
                    try:
                        filing_text = filing.text()
                        with open(filename_txt, "w", encoding="utf-8") as f:
                            f.write(filing_text)
                        print(f"Saved filing {idx + 1} as plain text to {filename_txt}")
                    except Exception as e2:
                        print(f"Error saving filing {idx + 1} as text (fallback): {e2}")
                else:
                    try:
                        soup = BeautifulSoup(raw_html, "html.parser")
                        pretty_html = soup.prettify()
                        with open(filename_html, "w", encoding="utf-8") as f:
                            f.write(pretty_html)
                        print(f"Saved filing {idx + 1} to {filename_html}")
                    except Exception as e3:
                        print(f"Error saving filing {idx + 1} as HTML: {e3}")
            except Exception as e:
                print(f"Error retrieving HTML for filing {idx + 1}: {e}")
    print(f"Completed processing {len(filings)} filings for ticker {ticker}.")
    print("="*40 + "\n")

end_time = time.time()
elapsed = end_time - start_time
print(f"Script completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total runtime: {elapsed:.2f} seconds")
