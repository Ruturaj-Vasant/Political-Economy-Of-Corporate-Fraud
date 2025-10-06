import os
import json
import subprocess
import re

import pandas as pd
from kor.nodes import Object, Number, Text


# -------------------------------
# Function 1: Read CSV
# -------------------------------
def read_csv_file(csv_path: str) -> pd.DataFrame:
    print("Step 1: Reading CSV file...")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    df = pd.read_csv(csv_path)
    print("Step 1: Finished reading CSV file.")
    print("DataFrame head:\n", df.head())
    return df


# -------------------------------
# Function 2: Map Columns
# -------------------------------
def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize columns. 
    First column always has both name and position.
    Extra columns are preserved.
    """
    print("Step 2: Mapping columns...")

    # Rename first column
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "name_position"})

    COLUMN_MAP = {
        "year": ["Year", "Fiscal Year"],
        "salary": ["Salary", "Salary ($)", "Annual Compensation Salary"],
        "bonus": ["Bonus", "Bonus ($)", "Annual Compensation Bonus"],
        "stock_awards": ["Stock Awards", "Stock awards ($)"],
        "non_equity_incentive_plan": ["Non-Equity Incentive Plan", "Non-equity incentive"],
        "all_other_compensation": ["All Other Compensation", "All other compensation", "All Other  Compensation(2)"],
        "total": ["Total", "Total ($)"],
    }

    normalized_cols = {}
    for col in df.columns:
        mapped = None
        for key, variants in COLUMN_MAP.items():
            if any(col.strip().lower() == v.strip().lower() for v in variants):
                mapped = key
                break
        normalized_cols[col] = mapped if mapped else col

    mapped_df = df.rename(columns=normalized_cols)

    print("Step 2: Finished mapping columns.")
    print("Mapped DataFrame head:\n", mapped_df.head())
    print("Final columns:", list(mapped_df.columns))
    return mapped_df


# -------------------------------
# Extract ticker and report_year from CSV path
# -------------------------------
def extract_ticker_and_year(csv_path: str):
    # Expecting path like .../MSFT/extracted/MSFT_2000_SCT.csv
    base_name = os.path.basename(csv_path)
    # e.g. MSFT_2000_SCT.csv
    parts = base_name.split('_')
    if len(parts) < 2:
        raise ValueError(f"Cannot extract ticker and year from filename: {base_name}")
    ticker = parts[0]
    report_year = parts[1]
    print(f"Extracted ticker: {ticker}, report_year: {report_year}")
    return ticker, report_year


# -------------------------------
# Function 3: Call DeepSeek with Kor
# -------------------------------
def call_deepseek_with_kor(
    df: pd.DataFrame,
    ticker: str,
    report_year: str,
    report_date: str,
    # model: str = "deepseek-r1:14b",
    model: str = "llama3:8b",
) -> dict:
    """
    Uses DeepSeek to enforce schema-based extraction with hierarchical JSON output.
    """
    print("Step 3: Starting DeepSeek extraction with Kor...")
    table_text = df.to_string(index=False)

    prompt = f"""
You are a data assistant.
Task: Convert the following SEC Summary Compensation Table into JSON.

Rules:
1. JSON structure must be hierarchical:
   {{
     "company": {{
       "ticker": "{ticker}",
       "report_year": "{report_year}",
       "reports": [
         {{
           "report_date": "{report_date}",
           "executives": {{
             "Executive Name": {{
               "YYYY": {{
                 "position": "...",
                 "salary": <number or null>,
                 "bonus": <number or null>,
                 "stock_awards": <number or null>,
                 "non_equity_incentive_plan": <number or null>,
                 "all_other_compensation": <number or null>,
                 "total": <number or null>,
                 "other_fields": {{
                   "colname1": value,
                   "colname2": value
                 }}
               }},
               "YYYY2": {{ ... }}
             }}
           }}
         }}
       ]
     }}
   }}

2. The first column 'name_position' contains both executive name and title. Split it into:
   - executive_name
   - position

3. Include ALL columns from the table.
   - If a column does not map to salary/bonus/etc., place it under "other_fields".
   - If a value is missing, set it to null.

4. The JSON must be valid and properly nested.

Here is the table extracted from SEC filing:
{table_text}
"""
    print("Step 3: Generated prompt (truncated to 500 chars):")
    print(prompt[:500] + ("..." if len(prompt) > 500 else ""))

    try:
        print("Step 3: Calling DeepSeek subprocess...")
        process = subprocess.Popen(
            ["ollama", "run", model],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout_lines = []

        # Write prompt and close stdin
        process.stdin.write(prompt)
        process.stdin.close()

        # Read stdout line by line
        while True:
            line = process.stdout.readline()
            if line == "" and process.poll() is not None:
                break
            if line:
                print(line, end="")
                stdout_lines.append(line)

        # Wait for process to terminate and get stderr
        _, err = process.communicate()

        response_text = ''.join(stdout_lines).strip()
        print("Step 3: Received response from DeepSeek subprocess.")

        if err:
            print("Step 3: stderr from DeepSeek subprocess:")
            print(err.strip())

    except Exception as e:
        print("Error running DeepSeek:", str(e))
        return {}

    print("Step 4: Attempting to parse DeepSeek response as JSON...")

    parsed_json = None
    try:
        # Remove markdown code fences if present
        cleaned = response_text
        cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE | re.MULTILINE)
        cleaned = re.sub(r"^```\s*", "", cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r"```$", "", cleaned, flags=re.MULTILINE)

        # Find first { and last }
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            json_substring = cleaned[start : end + 1]
            try:
                parsed_json = json.loads(json_substring)
                print("Step 4: JSON parsing successful (from substring).")
                if isinstance(parsed_json, dict):
                    keys_preview = list(parsed_json.keys())[:10]
                    print(f"Top-level keys in parsed JSON (up to 10): {keys_preview}")
                else:
                    print("Parsed JSON is not a dict, type:", type(parsed_json))
                return parsed_json
            except Exception as e2:
                print("Step 4: JSON substring parsing failed:", str(e2))
        else:
            print("Step 4: Could not find JSON substring in response.")
    except Exception as e:
        print("Step 4: Exception during JSON extraction:", str(e))

    print("Step 4: JSON parsing failed. Returning raw response.")
    return {"raw_response": response_text}


# -------------------------------
# Function 4: Save JSON Output
# -------------------------------
def save_json_output(data: dict, csv_path: str):
    print("Step 5: Saving JSON output to file...")
    json_path = os.path.splitext(csv_path)[0] + "_kor.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    file_size = os.path.getsize(json_path)
    print(f"✅ JSON saved to {json_path}, size: {file_size} bytes")


# -------------------------------
# Orchestrator Function
# -------------------------------
def process_csv_with_kor(csv_path: str, report_date: str):
    print("=== Starting CSV processing with Kor ===")
    df = read_csv_file(csv_path)
    df = map_columns(df)
    ticker, report_year = extract_ticker_and_year(csv_path)
    extracted_data = call_deepseek_with_kor(df, ticker, report_year, report_date)
    save_json_output(extracted_data, csv_path)
    print("=== Finished CSV processing with Kor ===")


# -------------------------------
# Example usage
# -------------------------------
if __name__ == "__main__":
    csv_file = (
        "/Users/ruturaj_vasant/Desktop/PersonalProjects/Political-Economy-Of-Corporate-Fraud/data/"
        "MSFT/extracted/MSFT_2000_SCT.csv"
    )
    process_csv_with_kor(csv_file, report_date="2000-10-01")