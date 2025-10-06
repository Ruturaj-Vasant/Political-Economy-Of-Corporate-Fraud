



from pathlib import Path
import pandas as pd
import re
import ftfy

BASE_DIR = Path(__file__).resolve().parent.parent.parent

def clean_sct_csv(input_path, output_path):
    # Read the raw CSV without headers
    df = pd.read_csv(input_path, header=None, dtype=str)

    import ftfy
    # Helper to clean any text (used for both headers and data)
    def clean_text(val):
        if isinstance(val, str):
            val = ftfy.fix_text(val)
            val = re.sub(r'[^\x00-\x7F]+',' ', val)
            val = re.sub(r'\s+', ' ', val).strip()
        return val

    df = df.applymap(clean_text)

    # Find the most likely header row: first row with multiple known keywords
    header_keywords = ['name', 'position', 'year', 'salary', 'bonus', 'stock', 'option', 'total', 'incentive', 'compensation']
    header_row_idx = None
    max_matches = 0

    for i, row in df.iterrows():
        row_text = ' '.join([str(x).lower() for x in row if pd.notna(x)])
        matches = sum(1 for kw in header_keywords if kw in row_text)
        if matches > max_matches:
            max_matches = matches
            header_row_idx = i
        if matches >= 5:
            break  # good enough match

    if header_row_idx is None:
        raise ValueError("Could not find a valid header row.")

    headers = [clean_text(x) for x in df.iloc[header_row_idx].fillna("")]
    data = df.iloc[header_row_idx + 1:].copy()
    data.columns = headers
    data = data.reset_index(drop=True)

    # Drop fully empty or zero-filled rows
    data = data[~data.apply(lambda row: all((pd.isna(x) or str(x).strip() in ["", "0", "0.0"]) for x in row), axis=1)]

    # Convert known numeric columns
    for col in data.columns:
        if any(k in col.lower() for k in header_keywords[2:]):  # skip name/position
            data[col] = (
                data[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.replace("nan", "0", regex=False)
                .str.strip()
            )
            data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

    data = data.dropna(axis=1, how='all')
    data.to_csv(output_path, index=False)
    print(f"Cleaned file saved to {output_path}")



if __name__ == "__main__":
    import sys
    import glob

    ticker = input("Enter the ticker symbol: ").strip().upper()
    input_dir = BASE_DIR / f"data/{ticker}/extracted"
    if not input_dir.exists():
        print(f"Directory not found: {input_dir}")
        sys.exit(1)

    csv_files = sorted(input_dir.glob(f"*{ticker}_*SCT.csv"))
    if not csv_files:
        print(f"No *_SCT.csv files found in {input_dir}")
        sys.exit(1)

    for csv_file in csv_files:
        output_file = csv_file.with_name(csv_file.stem + "_cleaned.csv")
        try:
            clean_sct_csv(csv_file, output_file)
        except Exception as e:
            print(f"Failed to clean {csv_file.name}: {e}")