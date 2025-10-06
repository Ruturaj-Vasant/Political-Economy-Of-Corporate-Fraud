from sec_api import Form13FHoldingsApi, Form13FCoverPagesApi
import requests
import json
from pathlib import Path
import pandas as pd

class SECApiClient:
    API_URL_DIRECTORS = "https://api.sec-api.io/directors-and-board-members"
    API_KEY = "26f111c2df9db80652d262a01c08df57e363b732c35c98026e647bbe1b2a7af6"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or self.API_KEY
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
        self.form13FHoldingsApi = Form13FHoldingsApi(self.api_key)
        self.form13FCoverPagesApi = Form13FCoverPagesApi(self.api_key)


    def fetch_ncen_filings_SEC(self, ticker: str):
        info = self.get_cik_from_ticker_SEC(ticker)
        if not info:
            print(f"Failed to get CIK for {ticker}")
            return
        cik = info["cik"]
        output_dir = Path(f"data/{ticker}")
        output_dir.mkdir(parents=True, exist_ok=True)

        filings = []
        offset, size = 0, 50
        while True:
            body = {
                "query": f"cik:{cik}",
                "from": offset,
                "size": size,
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            resp = requests.post("https://api.sec-api.io/form-ncen",
                                 headers=self.headers, json=body)
            if resp.status_code != 200:
                print(f"Error fetching N-CEN filings for {ticker}: {resp.status_code}")
                return
            result = resp.json()
            data = result.get("data", [])
            total = result.get("total", {}).get("value", 0)
            print(f"API responded with {len(data)} records, total expected: {total}")
            if not data:
                print("No filings found in this batch.")
            filings.extend(data)
            if offset + len(data) >= total:
                break
            offset += size

        if filings:
            file = output_dir / f"{ticker}_SEC_ncen_filings.json"
            with open(file, "w") as f:
                json.dump(filings, f, indent=2)
            print(f"Saved {len(filings)} annual filings to {file}")
        else:
            print(f"No annual filings found for {ticker}. File not saved.")

    def get_cik_from_ticker_SEC(self, ticker: str) -> dict:
        print(f"Looking up CIK for ticker: {ticker}")
        metadata_file = Path("metadata/sec_company_tickers.json")
        metadata_file.parent.mkdir(parents=True, exist_ok=True)

        # Load existing metadata
        if metadata_file.exists():
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
            print("Metadata file loaded.")
        else:
            metadata = {}
            print("No metadata file found. Starting fresh.")

        # Check if ticker exists and only return if CIK is not None or "null"
        for entry in metadata.values():
            if entry["ticker"].upper() == ticker.upper():
                cik_value = entry.get("cik_str")
                if cik_value and cik_value != "null":
                    print(f"Found {ticker} in metadata with valid CIK: {cik_value}")
                    return {"ticker": entry["ticker"], "cik": cik_value}
                else:
                    print(f"Found {ticker} in metadata but CIK is null or missing. Refetching...")
                    break

        # Fetch from SEC API
        print(f"{ticker} not found in metadata. Fetching from SEC API...")
        url = f"https://api.sec-api.io/mapping/ticker/{ticker}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            result = response.json()
            if result and isinstance(result, list):
                company = result[0]
                print(f"Received data from SEC API for {ticker}: {company}")
                new_entry = {
                    "cik_str": company.get("cik"),
                    "ticker": company.get("ticker"),
                    "title": company.get("name"),
                    "cusip": company.get("cusip"),
                    "exchange": company.get("exchange"),
                    "isDelisted": company.get("isDelisted"),
                    "category": company.get("category"),
                    "sector": company.get("sector"),
                    "industry": company.get("industry"),
                    "sic": company.get("sic"),
                    "sicSector": company.get("sicSector"),
                    "sicIndustry": company.get("sicIndustry"),
                    "currency": company.get("currency"),
                    "location": company.get("location"),
                    "sec_internal_id": company.get("id"),
                    "permno": None,
                    "gvkey": None
                }
                updated = False
                for key, entry in metadata.items():
                    if entry["ticker"].upper() == ticker.upper():
                        # Merge new data
                        for k, v in new_entry.items():
                            entry[k] = v if v is not None else entry.get(k)
                        # Handle source tag
                        existing_source = entry.get("source", "")
                        if "SEC" not in existing_source:
                            entry["source"] = f"{existing_source}, SEC".strip(", ").replace(" ,", ",")
                        metadata[key] = entry
                        updated = True
                        break
                if not updated:
                    new_entry["source"] = "SEC"
                    next_index = str(max(map(int, metadata.keys())) + 1) if metadata else "0"
                    metadata[next_index] = new_entry

                with open(metadata_file, "w") as f:
                    json.dump(metadata, f, indent=2)
                print(f"Added new entry for {ticker} to metadata.")
                return {"ticker": new_entry["ticker"], "cik": new_entry["cik_str"]}
            else:
                print(f"No CIK found in API response for ticker: {ticker}")
                return {}
        else:
            print(f"Failed to fetch CIK from SEC: {response.status_code} {response.text}")
            return {}

    def fetch_director_data_SEC(self, ticker: str):
        query_body = {
            "query": f"ticker:{ticker}",
            "from": 0,
            "size": 50,
            "sort": [{"filedAt": {"order": "desc"}}]
        }

        response = requests.post(self.API_URL_DIRECTORS, headers=self.headers, data=json.dumps(query_body))
        resp = response.json()
        print(json.dumps(resp, indent=2))
        if response.status_code == 200:
            data = response.json().get("data", [])
            if not data:
                print(f"No data found for ticker: {ticker}")
                return

            records = []
            for entry in data:
                for director in entry.get("directors", []):
                    record = {
                        "id": entry.get("id"),
                        "accessionNo": entry.get("accessionNo"),
                        "cik": entry.get("cik"),
                        "ticker": entry.get("ticker"),
                        "company_name": entry.get("entityName"),
                        "filed_at": entry.get("filedAt"),
                        "name": director.get("name"),
                        "dirClass": director.get("directorClass"),
                        "electedIn": director.get("dateFirstElected"),
                        "position": director.get("position"),
                        "age": director.get("age"),
                        "is_independent": director.get("isIndependent"),
                        "committee_memberships": director.get("committeeMemberships"),
                        "qualifications": director.get("qualificationsAndExperience")
                    }
                    records.append(record)

            output_dir = Path(f"data/{ticker}")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{ticker}_SEC_directors.csv"
            df = pd.DataFrame(records)
            df.to_csv(output_file, index=False)
            print(f"Data exported to {output_file}")
        else:
            print(f"Failed to fetch data: {response.status_code} {response.text}")

    def download_bulk_ticker_cik_mapping(self):
        url = "https://api.sec-api.io/bulk/mapping/ticker-to-cik"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            output_dir = Path("data/sec_bulk")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "ticker_to_cik_mapping.json"
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Ticker to CIK mapping saved to {output_file}")
        else:
            print(f"Failed to download mapping: {response.status_code} {response.text}")

    def fetch_13f_holdings_SEC(self, ticker: str):
        print(f"Starting fetch of 13F holdings for {ticker} from SEC...")
        info = self.get_cik_from_ticker_SEC(ticker)
        if not info:
            print(f"[FAIL] Could not retrieve CIK for ticker {ticker}. Aborting fetch.")
            return
        cik = info["cik"]
        output_dir = Path(f"data/{ticker}")
        output_dir.mkdir(parents=True, exist_ok=True)

        search_params = {
            "query": f"holdings.ticker:{ticker}",
            "from": "0",
            "size": "50",
            "sort": [{"filedAt": {"order": "desc"}}],
        }

        try:
            print("Requesting holdings data from SEC API...")
            response = self.form13FHoldingsApi.get_data(search_params)
            filings = response.get("data", [])
            if not filings:
                print(f"[INFO] SEC API responded successfully, but no 13F holdings data found for {ticker}.")
                return

            holdings_json = filings[0].get("holdings", [])
            if not holdings_json:
                print(f"[INFO] No holdings listed in the latest 13F filing for {ticker}.")
                return

            output_file = output_dir / "13f_holdings.json"
            with open(output_file, "w") as f:
                json.dump(holdings_json, f, indent=2)
            print(f"[SUCCESS] Saved {len(holdings_json)} records of 13F holdings to {output_file}")

        except Exception as e:
            print(f"[ERROR] Failed to fetch 13F holdings for {ticker}: {e}")

    def fetch_13f_coverpages_SEC(self, ticker: str, period_of_report: str):
        print(f"Starting fetch of 13F cover pages for {ticker} from SEC...")
        info = self.get_cik_from_ticker_SEC(ticker)
        if not info:
            print(f"[FAIL] Could not retrieve CIK for ticker {ticker}. Aborting fetch.")
            return
        cik = info["cik"]
        output_dir = Path(f"data/{ticker}")
        output_dir.mkdir(parents=True, exist_ok=True)

        search_params = {
            "query": f"cik:{cik} AND periodOfReport:{period_of_report}",
            "from": "0",
            "size": "1",
            "sort": [{"periodOfReport": {"order": "desc"}}],
        }

        try:
            print("Requesting cover page data from SEC API...")
            response = self.form13FCoverPagesApi.get_data(search_params)
            data = response.get("data", [])
            if not data:
                print(f"[INFO] No 13F cover page data found for {ticker} for report period {period_of_report}.")
                return

            cover_page = data[0]
            output_file = output_dir / f"13f_coverpage_{period_of_report}.json"
            with open(output_file, "w") as f:
                json.dump(cover_page, f, indent=2)
            print(f"[SUCCESS] Saved 13F cover page record to {output_file}")
        except Exception as e:
            print(f"[ERROR] Failed to fetch 13F cover pages for {ticker}: {e}")

    def fetch_13f_coverpages_timespan_SEC(self, ticker: str, start_date: str, end_date: str):
        print(f"Starting 13F cover pages fetch from SEC for {ticker} from {start_date} to {end_date}...")
        info = self.get_cik_from_ticker_SEC(ticker)
        if not info:
            print(f"[FAIL] Could not retrieve CIK for ticker {ticker}. Aborting fetch.")
            return
        cik = info["cik"]
        output_dir = Path(f"data/{ticker}")
        output_dir.mkdir(parents=True, exist_ok=True)

        search_params = {
            "query": f"cik:{cik} AND periodOfReport:[{start_date} TO {end_date}]",
            "from": "0",
            "size": "1",
            "sort": [{"periodOfReport": {"order": "desc"}}],
        }

        try:
            print("Request sent to SEC API. Awaiting cover page data response...")
            response = self.form13FCoverPagesApi.get_data(search_params)
            data = response.get("data", [])
            print(f"Received response with {len(data)} cover page records.")
            if not data:
                print(f"[INFO] Connection successful, but no cover page data found for {ticker} between {start_date} and {end_date}.")
                return
            output_file = output_dir / f"13f_coverpages_{start_date}_to_{end_date}.json"
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            print(f"[SUCCESS] Saved {len(data)} 13F cover page records for {ticker} to {output_file}")
        except Exception as e:
            print(f"[ERROR] SEC API request failed or encountered an error for {ticker} in time span {start_date} to {end_date}: {e}")

    def fetch_13f_holdings_timespan_SEC(self, ticker: str, start_date: str, end_date: str):
        print(f"Starting 13F holdings fetch from SEC for {ticker} from {start_date} to {end_date}...")
        info = self.get_cik_from_ticker_SEC(ticker)
        if not info:
            print(f"Failed to get CIK for ticker {ticker}")
            return
        cik = info["cik"]
        output_dir = Path(f"data/{ticker}")
        output_dir.mkdir(parents=True, exist_ok=True)

        search_params = {
            "query": f"cik:{cik} AND periodOfReport:[{start_date} TO {end_date}]",
            "from": "0",
            "size": "4",
            "sort": [{"filedAt": {"order": "desc"}}],
        }

        try:
            print("Request sent to SEC API. Awaiting response...")
            response = self.form13FHoldingsApi.get_data(search_params)
            filings = response.get("data", [])
            print(f"Received response with {len(filings)} filings.")
            if not filings:
                print(f"Connection successful, but no holdings data found for {ticker} between {start_date} and {end_date}.")
                return

            output_file = output_dir / f"13f_holdings_{start_date}_to_{end_date}.json"
            with open(output_file, "w") as f:
                json.dump(filings, f, indent=2)
            print(f"Saved 13F holdings for {ticker} to {output_file}")
        except Exception as e:
            print(f"SEC API request failed or encountered an error for {ticker} in time span {start_date} to {end_date}: {e}")

if __name__ == "__main__":
    client = SECApiClient()
    # client.download_bulk_ticker_cik_mapping()
    # client.get_cik_from_ticker_SEC("MSFT")
    # client.fetch_ncen_filings_SEC("MSFT")
    # client.fetch_10k_filings_SEC("MSFT")
    # client.fetch_13f_holdings_SEC("TSLA")
    # client.fetch_13f_coverpages_SEC("TSLA", "2024-03-31")
    # client.fetch_13f_coverpages_timespan_SEC("TSLA", "1992-01-01", "2025-12-31")
    # client.fetch_13f_holdings_timespan_SEC("TSLA", "2023-01-01", "2023-12-31")