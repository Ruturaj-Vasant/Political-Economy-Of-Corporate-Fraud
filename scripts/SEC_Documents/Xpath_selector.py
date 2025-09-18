from Extract_Summary_Compensation_Table import extract_summary_compensation_table
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
    print(f"Found anchor links: {anchors}")
    
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
            
            df = None
            if anchors:
                for href, text in anchors:
                    print(f"Anchor: {href} | Text: {text}")
                    anchor_id = href.lstrip("#")
                    df = extract_table_after_anchor(full_path, anchor_id)
                    break  # only use the first anchor

            if not anchors:
                print("No SCT anchors found, trying XPath fallback...")
                from lxml import etree
                with open(full_path, "rb") as f:
                    content = f.read()
                try:
                    tree = html.fromstring(content)
                    xpath_expr = """
                    //tr[
                        .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'name')]
                        and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'principal')]
                        and .//text()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'position')]
                    ]
                    """
                    tr_node = tree.xpath(xpath_expr)
                    if tr_node:
                        table = tr_node[0].getparent()
                        while table is not None and table.tag != "table":
                            table = table.getparent()
                        if table is not None:
                            df = pd.read_html(html.tostring(table), header=None)[0]
                            # Improved header detection logic
                            max_rows_to_check = 3
                            header_candidate = df.iloc[:max_rows_to_check].astype(str).fillna("").agg(" ".join, axis=0).str.lower()
                            keywords = ["name", "salary", "bonus", "option", "stock", "total"]
                            keyword_count = sum(any(k in cell for k in keywords) for cell in header_candidate)

                            if keyword_count >= 2:
                                df.columns = header_candidate
                                df = df[max_rows_to_check:].reset_index(drop=True)
                            else:
                                print("Could not find a suitable header row.")
                                df = None
                    # If tr_node is empty, df remains None
                except Exception as e:
                    print(f"XPath fallback failed: {e}")

            if df is not None:
                keywords = ["name", "salary", "bonus", "option", "total", "stock"]
                column_headers = [str(col).lower() for col in df.columns]
                keyword_count = sum(any(k in col for col in column_headers) for k in keywords)
                if keyword_count < 2:
                    print("Table does not contain enough relevant columns. Skipping file.")
                    continue
                year = filename.split("-")[0]
                output_file = os.path.join(directory, "extracted", f"{ticker}_{year}_SCT.csv")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                df.to_csv(output_file, index=False)
                print(f"Saved table to {output_file}")


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
        df = pd.read_html(html.tostring(table[0]), header=None)[0]
        keywords = ["name", "salary", "bonus", "option", "stock", "total"]
        max_rows_to_check = 10
        header_candidate = df.iloc[:max_rows_to_check].astype(str).fillna("").agg(" ".join, axis=0).str.lower()
        keyword_count = sum(any(k in cell for k in keywords) for cell in header_candidate)

        if keyword_count >= 2:
            df.columns = header_candidate
            df = df[max_rows_to_check:].reset_index(drop=True)
            return df
        else:
            print("Could not find a suitable header row.")
            return None
    except Exception as e:
        print(f"Failed to read table: {e}")
        return None

if __name__ == "__main__":
    ticker = input("Enter the ticker symbol: ").strip().upper()
    process_ticker_directory(ticker)