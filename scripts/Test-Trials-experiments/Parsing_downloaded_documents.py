import os
import re
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")

def extract_ownership_info_from_text(text):
    lines = text.splitlines()
    ownership_section = []
    in_section = False

    # Identify start and end of ownership section
    for line in lines:
        if re.search(r'(security ownership|beneficial ownership)', line, re.I):
            in_section = True
        if in_section:
            ownership_section.append(line)
            if re.search(r'(executive compensation|director compensation|audit committee)', line, re.I):
                break

    # Extract tabular info from the ownership section
    ownership_data = []
    for line in ownership_section:
        match = re.match(r"^(.*?)\s+([\d,]+)\s+(\d{1,2}\.\d{1,2}%?)", line.strip())
        if match:
            name = match.group(1).strip()
            shares = match.group(2).replace(",", "")
            percent = match.group(3).replace("%", "")
            ownership_data.append({
                "Name of Beneficial Owner": name,
                "Number of Shares": int(shares),
                "Ownership %": float(percent)
            })

    return pd.DataFrame(ownership_data)

def parse_all_ownership_data():
    output_dir = Path("extracted") / "ownership"
    output_dir.mkdir(parents=True, exist_ok=True)
    all_data = []

    for ticker_dir in DATA_DIR.iterdir():
        if not ticker_dir.is_dir():
            continue
        if ticker_dir.name != "XRX":
            continue  # Only process XRX for now
        print(f"Processing ticker directory: {ticker_dir}")
        for file in ticker_dir.glob("*DEF14A.txt"):
            print(f"Accessing file: {file}")
            with open(file, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
            df = extract_ownership_info_from_text(text)
            if not df.empty:
                print(f"Extracted data from {file}:\n", df, "\n")
                df["Ticker"] = ticker_dir.name
                df["Filing Date"] = file.stem.split("_")[0]
                all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        print("✅ Final extracted ownership data:\n", final_df)
    else:
        print("⚠️ No ownership data extracted.")

if __name__ == "__main__":
    parse_all_ownership_data()