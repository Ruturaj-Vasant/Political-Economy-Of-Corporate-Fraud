import os
import re
import pandas as pd
from lxml import html
import ftfy

DATA_DIR = "data"

def clean_text(val):
    if isinstance(val, str):
        val = ftfy.fix_text(val)
        val = re.sub(r'[^\x00-\x7F]+',' ', val)
        val = re.sub(r'\s+', ' ', val).strip()
    return val


def score_table_keywords(rows, keywords, max_rows=3):
    flat_text = ' '.join(cell.lower() for row in rows[:max_rows] for cell in row)
    return sum(1 for kw in keywords if kw in flat_text)

def score_table(rows):
    text = " ".join(cell.lower() for row in rows for cell in row)
    score = 0
    if any(k in text for k in ["salary", "bonus", "stock", "total", "compensation"]):
        score += 3
    if re.search(r"[A-Z][a-z]+ [A-Z][a-z]+", text):
        score += 2
    if len(rows) >= 3 and any(re.search(r"\$?[0-9]{3,}", cell) for row in rows for cell in row):
        score += 2
    return score

def find_summary_table(tree, keywords):
    anchor_candidates = tree.xpath(
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'summary compensation table')]"
    )
    best_table = None
    best_score = 0
    for anchor in anchor_candidates:
        following_elements = anchor.xpath("following::*")
        for elem in following_elements[:15]:
            if elem.tag != "table":
                continue
            rows = extract_table_data(elem)
            score = score_table(rows)
            keyword_score = score_table_keywords(rows, keywords)
            total_score = score + keyword_score
            if total_score > best_score:
                best_score = total_score
                best_table = elem
            if total_score >= 8:
                break  # good enough match
        if best_table is not None:
            break
    return best_table

def extract_table_data(table_element):
    rows = []
    for tr in table_element.xpath(".//tr"):
        cells = [clean_text(td.text_content()) for td in tr.xpath("./td|./th")]
        if any(cell for cell in cells):
            rows.append(cells)
    return rows

def detect_header_row(table_rows, keywords, min_matches=4):
    for idx, row in enumerate(table_rows[:10]):
        match_count = sum(1 for kw in keywords if any(kw in cell.lower() for cell in row))
        if match_count >= min_matches:
            return idx
    return None

def build_dataframe_from_rows(table_rows, header_row_idx):
    # Merge up to 3 header rows if they contain year markers or notes
    header_rows = table_rows[header_row_idx:header_row_idx + 3]
    merged = header_rows[0]
    for row in header_rows[1:]:
        match_count = sum(1 for cell in row if re.search(r'\d{4}|\(.*?\)', cell))
        if match_count >= len(row) // 2:
            merged = [
                f"{a} {b}".strip() if b else a
                for a, b in zip(merged, row)
            ]
        else:
            break
    headers = [clean_text(cell) for cell in merged]
    data_rows = table_rows[header_row_idx + len(header_rows):]

    # Pad rows
    max_cols = max(len(r) for r in data_rows + [headers]) if data_rows else len(headers)
    padded_rows = [r + [''] * (max_cols - len(r)) for r in data_rows]
    headers = headers + [''] * (max_cols - len(headers))
    df = pd.DataFrame(padded_rows, columns=headers)
    return df

def process_html_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    tree = html.fromstring(content.encode("utf-8"))  # fix for encoding declaration
    header_keywords = [
        'name', 'position', 'title', 'year', 'salary', 'bonus', 'stock', 'option',
        'total', 'incentive', 'compensation', 'base pay', 'non-equity', 'pension', 'awards', 'cash'
    ]
    table_element = find_summary_table(tree, header_keywords)
    if table_element is None:
        print(f"Table not found in {filepath}")
        return None
    rows = extract_table_data(table_element)
    header_row_idx = detect_header_row(rows, header_keywords)
    if header_row_idx is None:
        print(f"Could not detect valid header in {filepath}, using row 0 as fallback.")
        header_row_idx = 0
    df = build_dataframe_from_rows(rows, header_row_idx)
    return df

def process_ticker(ticker):
    input_dir = os.path.join(DATA_DIR, ticker)
    output_dir = os.path.join(DATA_DIR, ticker, "extracted")
    os.makedirs(output_dir, exist_ok=True)
    for fname in os.listdir(input_dir):
        if not fname.endswith(".html"):
            continue
        path = os.path.join(input_dir, fname)
        print(f"Processing: {fname}")
        try:
            df = process_html_file(path)
            if df is not None:
                year = re.search(r"(\d{4})", fname)
                year_str = year.group(1) if year else "unknown"
                out_path = os.path.join(output_dir, f"{ticker}_{year_str}_SCT_cleaned.csv")
                df.to_csv(out_path, index=False)
                print(f"Saved cleaned CSV to: {out_path}")
        except Exception as e:
            print(f"Error processing {fname}: {e}")

if __name__ == "__main__":
    ticker = input("Enter the ticker symbol: ").strip().upper()
    process_ticker(ticker)