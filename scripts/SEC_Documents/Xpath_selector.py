

import os
from lxml import html
import pandas as pd

def extract_sct_anchors_from_file(filepath):
    with open(filepath, "rb") as f:
        content = f.read()

    try:
        tree = html.fromstring(content)
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return []

    xpath_expr = "//a[starts-with(@href, '#') and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'summary compensation table')]"
    anchors = tree.xpath(xpath_expr)
    
    results = []
    for a in anchors:
        href = a.get("href")
        text = a.text_content().strip()
        results.append((href, text))
    return results

def process_ticker_directory(ticker):
    directory = os.path.join("data", ticker)
    for filename in os.listdir(directory):
        if filename.endswith(".html"):
            full_path = os.path.join(directory, filename)
            print(f"\nProcessing: {filename}")
            anchors = extract_sct_anchors_from_file(full_path)
            if anchors:
                for href, text in anchors:
                    print(f"Anchor: {href} | Text: {text}")
                    anchor_id = href.lstrip("#")
                    df = extract_table_after_anchor(full_path, anchor_id)
                    if df is not None:
                        year = filename.split("-")[0]
                        output_file = os.path.join(directory,"extracted", f"{ticker}_{year}_SCT.csv")
                        os.makedirs(os.path.dirname(output_file), exist_ok=True)
                        df.to_csv(output_file, index=False)
                        print(f"Saved table to {output_file}")
            else:
                print("No SCT anchors found.")


# Extract the first table after the anchor target (by id or name)
def extract_table_after_anchor(filepath, anchor_id):
    with open(filepath, "rb") as f:
        content = f.read()

    try:
        tree = html.fromstring(content)
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return None

    target = tree.xpath(f'//*[@id="{anchor_id}" or @name="{anchor_id}"]')
    if not target:
        print(f"Anchor target '{anchor_id}' not found.")
        return None

    table = target[0].xpath("following::table[1]")
    if not table:
        print("No table found after anchor.")
        return None

    try:
        df = pd.read_html(html.tostring(table[0]))[0]
        return df
    except Exception as e:
        print(f"Failed to read table: {e}")
        return None

if __name__ == "__main__":
    ticker = input("Enter the ticker symbol: ").strip().upper()
    process_ticker_directory(ticker)