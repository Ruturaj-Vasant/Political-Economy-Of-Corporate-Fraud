import os
import re
from bs4 import BeautifulSoup
import pandas as pd
import io as StringIO

def clean_table(df):
    df.columns = [str(col).strip().lower() for col in df.columns]
    df = df.dropna(how='all', axis=1)
    df = df.dropna(how='all', axis=0)
    return df

def clean_and_fix_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").reset_index(drop=True)

    header_row_idx = None
    keywords = ["name", "salary", "bonus", "option", "total", "stock"]
    for i in range(min(len(df), 5)):
        row = df.iloc[i].astype(str).str.lower()
        keyword_count = sum(any(k in cell for k in keywords) for cell in row)
        if keyword_count >= 2:
            header_row_idx = i
            break

    if header_row_idx is not None:
        df.columns = df.iloc[header_row_idx]
        df = df.iloc[header_row_idx + 1:].reset_index(drop=True)
    else:
        df.columns = [str(c).strip() for c in df.columns]

    df = df.dropna(axis=1, how="all")
    df.columns = [re.sub(r"\s+", " ", str(col)).strip() for col in df.columns]
    df.columns = [col.replace("\n", " ").replace("\xa0", " ") for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def is_summary_comp_table(df: pd.DataFrame) -> bool:
    SALARY = re.compile(r"salary", re.I)
    if any(SALARY.search(str(col)) for col in df.columns):
        return True
    for col in df.columns:
        for cell in df[col]:
            if SALARY.search(str(cell)):
                return True
    return False

def extract_summary_compensation_table(filepath: str) -> pd.DataFrame:
    """
    Extracts the Summary Compensation Table (SCT) from a local DEF 14A HTML file.
    """
    TEXT_FIND = (
        r"(name\s*(and|/|&)?\s*(principal)?\s*position)"
        r"|principal\s*position"
        r"|name\s*[/&]"
        r"|named\s+executive\s+(officers?|positions?)"
    )
    SALARY = re.compile(r"salary", re.I)

    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "lxml")
    elements = soup.find_all(string=re.compile(TEXT_FIND, re.I))

    for el in elements:
        try:
            table = el.find_parent("table")
            if not table:
                continue

            table_html = str(table)
            df_list = pd.read_html(StringIO.StringIO(table_html), flavor="html5lib")

            if not df_list:
                continue

            df = df_list[0]

            if is_summary_comp_table(df):
                return clean_and_fix_table(df)

        except Exception:
            continue

    return None

def main():
    ticker = input("Enter the ticker symbol: ").strip().upper()
    filepath = os.path.join("data", ticker)

    for filename in os.listdir(filepath)[:2]:
        if filename.endswith(".html"):
            full_path = os.path.join(filepath, filename)
            print(f"Processing file: {filename}")
            # Updated logic according to instructions
            anchors = []  # This should be obtained from somewhere, placeholder here
            df = None
            for href, text in anchors:
                print(f"Anchor: {href} | Text: {text}")
                anchor_id = href.lstrip("#")
                df = extract_table_after_anchor(full_path, anchor_id)
                break  # only use the first anchor

            if not anchors:
                print("No SCT anchors found, using regex fallback...")
                df = extract_summary_compensation_table(full_path)

            if df is not None:
                keywords = ["name", "salary", "bonus", "option", "total", "stock"]
                column_headers = [str(col).lower() for col in df.columns]
                keyword_count = sum(any(k in col for col in column_headers) for k in keywords)
                if keyword_count < 2:
                    print("Table does not contain enough relevant columns. Skipping file.")
                    continue
                year = filename.split("-")[0]
                output_file = os.path.join(filepath, "extracted", f"{ticker}_{year}_SCT.csv")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                df.to_csv(output_file, index=False)
                print(f"Saved table to {output_file}")
            else:
                print("Regex extraction failed.")

if __name__ == "__main__":
    main()