import os
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd

# 1. User selects a form
form_options = {
    "1": "10-K",
    "2": "DEF14A",
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

print("Select the form type to extract tables from:")
for key, value in form_options.items():
    print(f"{key}. {value}")

choice = input("Enter the number corresponding to the form type: ").strip()
form_code = form_options.get(choice)
if not form_code:
    print("Invalid choice. Exiting.")
    exit()

data_dir = Path("data")
html_files = list(data_dir.glob(f"*/**/*_{form_code}.html"))
if not html_files:
    print(f"No HTML files found for form {form_code}. Exiting.")
    exit()
html_file = html_files[0]

# 3. Function to extract tables from a file
def extract_tables_with_titles(html_file):
    print(f"\nProcessing: {html_file}")
    print("=" * 80)
    
    html = html_file.read_text(encoding='utf-8', errors='ignore')
    ticker = html_file.parts[-2]
    output_dir = html_file.parent

    soup = BeautifulSoup(html, 'html.parser')
    
    tables = soup.find_all("table")

    all_tables = []

    for i, table in enumerate(tables, 1):
        caption = table.find("caption")
        if caption:
            title = caption.get_text(strip=True)
        else:
            title = None
            for prev in table.find_all_previous():
                if prev.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    title = prev.get_text(strip=True)
                    break
        title = title if title else f"Table_{i}"

        rows = table.find_all("tr")
        extracted_rows = []
        for row in rows:
            cells = row.find_all(["th", "td"])
            row_text = [cell.get_text(strip=True).replace('\xa0', ' ') for cell in cells]
            extracted_rows.append(row_text)

        if extracted_rows:
            df = pd.DataFrame(extracted_rows)
            all_tables.append((title, df))

    if all_tables:
        base_name = html_file.stem  # e.g., 2024-02-23_DEF14A
        output_filename = f"{base_name}_extracted_tables.txt"
        output_path = output_dir / output_filename

        with open(output_path, "w", encoding="utf-8") as out_file:
            for idx, (title, df) in enumerate(all_tables, 1):
                out_file.write(f"{title}\n")
                out_file.write("=" * len(title) + "\n")
                out_file.write(df.to_string(index=False, header=False))
                out_file.write("\n\n")

        print(f"\n✅ All tables saved to: {output_path}")
    else:
        print("No tables extracted.")

extract_tables_with_titles(html_file)