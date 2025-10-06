import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup

# Regex patterns to identify SCT sections and salary data
TEXT_FIND = r"Name\s+and\s+Principal\s+Position|Name\s+and\s+Principal|Principal\s+Position|Name\s+and\s+Position|Name\s+\&|Name/Position"
SALARY = re.compile(r"salary", re.I)


def extract_summary_compensation_table(filepath: str) -> pd.DataFrame:
    """
    Extracts the Summary Compensation Table (SCT) from a local DEF 14A HTML file.

    Args:
        filepath (str): Path to the HTML file.

    Returns:
        pd.DataFrame: Cleaned SCT DataFrame, or None if not found.
    """
    # Load HTML content
    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    # Locate all elements likely containing SCT text
    elements = soup.find_all(text=re.compile(TEXT_FIND, re.I))
    for el in elements:
        try:
            # Get the parent <table> of the matched element
            table = el.find_parent("table")
            if not table:
                continue

            # Convert table to DataFrame
            df = pd.read_html(str(table), flavor="html5lib")[0]

            # Check if it's a compensation table
            if is_summary_comp_table(df):
                return clean_comp_table(df)
        except Exception:
            continue

    return None  # Return None if SCT not found


def is_summary_comp_table(df: pd.DataFrame) -> bool:
    """
    Heuristic check to determine if the table contains salary data.

    Args:
        df (pd.DataFrame): Candidate DataFrame.

    Returns:
        bool: True if likely an SCT.
    """
    if any(SALARY.search(str(col)) for col in df.columns):
        return True
    for col in df.columns:
        for cell in df[col]:
            if SALARY.search(str(cell)):
                return True
    return False


def clean_comp_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the Summary Compensation Table.

    Args:
        df (pd.DataFrame): Raw table.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    df = df.replace(r"\(\$\)", "", regex=True)
    df = df.replace(r"^\s*\$\s*$", np.nan, regex=True)
    df = df.replace(r"\([\d\#\*\w]*\)", "", regex=True)
    df = df.replace(r"\*", "", regex=True)
    df = df.replace(r"\$", "", regex=True)
    df = df.replace(r"^\x97+$", np.nan, regex=True)
    df = df.replace(r"\x92", "'", regex=True)
    df = df.replace(r"^\s*$", np.nan, regex=True)
    df = df.dropna(how="all", axis=1)
    df.columns = [re.sub(r"\s+", " ", str(col)).strip() for col in df.columns]
    return df