import requests
from bs4 import BeautifulSoup
import re
import json
import os

URL = "https://firstratedata.com/b/22/stock-complete-historical-intraday"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_ticker_list(url):
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.text

def parse_tickers(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    pattern = re.compile(r"\b([A-Z0-9\.\-]+) \(([^\)]+)\) Start Date:\d{4}-\d{2}-\d{2}")
    return pattern.findall(text)

def load_json_tickers(json_path):
    with open(json_path, "r") as f:
        data = json.load(f)
    return data, {entry["ticker"] for entry in data.values()}

def find_missing(parsed_tickers, json_ticker_set):
    return [(ticker, name) for ticker, name in parsed_tickers if ticker not in json_ticker_set]

def is_delisted(ticker):
    return ticker.endswith("-DELISTED")

if __name__ == "__main__":
    html = fetch_ticker_list(URL)
    parsed_tickers = parse_tickers(html)
    print(f"Found {len(parsed_tickers)} tickers from FirstRateData.")

    json_path = os.path.join(os.path.dirname(__file__), "../metadata/sec_company_tickers.json")
    if not os.path.exists(json_path):
        print("Metadata file not found. Please check the path:", json_path)
    else:
        json_data, json_tickers = load_json_tickers(json_path)
        missing = find_missing(parsed_tickers, json_tickers)

        print(f"Tickers not in metadata: {len(missing)}")
        for ticker, name in missing:
            print(f"{ticker}: {name}")

        # Determine next available key
        max_key = max(int(k) for k in json_data.keys())
        new_entries = {}
        for i, (ticker, name) in enumerate(missing, start=max_key + 1):
            is_del = is_delisted(ticker)
            cleaned_ticker = ticker.replace("-DELISTED", "") if is_del else ticker

            new_entries[str(i)] = {
                "cik_str": None,
                "ticker": cleaned_ticker,
                "title": name,
                "permno": None,
                "gvkey": None,
                "delisted": is_del,
                "source": "FirstRateData"
            }

        # Optionally append to the original metadata in memory
        json_data.update(new_entries)

        # Save updated metadata to the original file
        with open(json_path, "w") as f:
            json.dump(json_data, f, indent=2)