from Extract_Summary_Compensation_Table import extract_summary_compensation_table
import os
from lxml import html
import pandas as pd
import ftfy
import unicodedata
import re

def clean_text(val):
    if pd.isna(val):
        return val
    # Convert to string
    text = str(val)
    # Fix text encoding issues
    text = ftfy.fix_text(text)
    # Normalize unicode characters
    text = unicodedata.normalize('NFKC', text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_dataframe(df):
    # Apply clean_text to every cell in the DataFrame
    return df.applymap(clean_text)

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
            # anchors = extract_sct_anchors_from_file(full_path)
            # df = None
            # if anchors:
            #     for href, text in anchors:
            #         print(f"Anchor: {href} | Text: {text}")
            #         anchor_id = href.lstrip("#")
            #         df = extract_table_after_anchor(full_path, anchor_id)
            #         break  # only use the first anchor

            # if not anchors:
            #     print("No SCT anchors found, trying XPath fallback...")
            #     df = extract_table_with_xpath(full_path)
            df = extract_table_with_xpath(full_path)
            if df is not None:
                year = filename.split("-")[0]
                output_file = os.path.join(directory, "extracted", f"{ticker}_{year}_SCT.csv")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                df.to_csv(output_file, index=False)
                print(f"Saved table to {output_file}")

# New function to process lxml table element into cleaned DataFrame
def process_extracted_table(table):
    df = pd.read_html(html.tostring(table))[0]
    # Flatten multi-index headers by joining non-"Unnamed" parts
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [' '.join([str(i) for i in col if 'Unnamed' not in str(i)]).strip() for col in df.columns.values]
    else:
        df.columns = [str(col).strip() for col in df.columns]
    # Strip whitespace from all cell values using map
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    # Drop columns that are completely empty
    df = df.dropna(axis=1, how='all')
    return df

# Encapsulate the XPath fallback logic in a new function
def extract_table_with_xpath(filepath):
    """
    Attempts to extract a table containing 'name and principal position' headers using XPath.
    Returns a pandas DataFrame if successful, otherwise None.
    """
    from lxml import etree
    with open(filepath, "rb") as f:
        content = f.read()
    try:
        tree = html.fromstring(content)
        xpath_expr = """
//tr[
  .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
  and (
    (
      .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
    or (
      following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
      and following-sibling::tr[1]//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
    )
  )
]
"""
        tr_nodes = tree.xpath(xpath_expr)
        if tr_nodes:
            table = tr_nodes[0].getparent()
            while table is not None and table.tag != "table":
                table = table.getparent()
            if table is not None:
                df = process_extracted_table(table)
                return df
        # If tr_nodes is empty, return None
        return None
    except Exception as e:
        print(f"XPath fallback failed: {e}")
        return None

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
        print("Reading table...")
        df = process_extracted_table(table[0])
        return df
    except Exception as e:
        print(f"Failed to read table: {e}")
        return None

if __name__ == "__main__":
    ticker = input("Enter the ticker symbol: ").strip().upper()
    process_ticker_directory(ticker)