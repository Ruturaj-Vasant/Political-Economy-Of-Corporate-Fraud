"""CSV → JSON via Ollama (llama3:8b), modeled on Deepseek_Integration copy.py.

We keep the prompt structure and the kor import the same as the reference,
and add light logging + sidecar meta files. The JSON is written next to the
CSV (same folder), with suffix "_kor.json" to preserve the original behavior.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import pandas as pd
from kor.nodes import Object, Number, Text  # kept to mirror the original import


# -------------------------------
# Function 1: Read CSV
# -------------------------------
def read_csv_file(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    return pd.read_csv(csv_path)


# -------------------------------
# Function 2: Map Columns (same spirit as the original)
# -------------------------------
def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns.
    First column is renamed to name_position; other known columns are mapped,
    preserving unmapped columns.
    """
    if not len(df.columns):
        return df
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "name_position"})

    # Drop symbol-only columns (e.g., stray currency glyph columns like '$')
    def _is_symbol_only_col(s: pd.Series) -> bool:
        vals = s.dropna().astype(str).str.strip().str.replace("\u00a0", "", regex=False)
        if vals.empty:
            return False
        return bool((~vals.str.contains(r"[A-Za-z0-9]", regex=True)).all())

    protected = {"name_position", "year"}
    drop_cols = [c for c in df.columns if c not in protected and _is_symbol_only_col(df[c])]
    if drop_cols:
        df = df.drop(columns=drop_cols)

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
            if any(str(col).strip().lower() == v.strip().lower() for v in variants):
                mapped = key
                break
        normalized_cols[col] = mapped if mapped else col
    return df.rename(columns=normalized_cols)


# -------------------------------
# Extract ticker and report date/year from CSV name
# -------------------------------
def extract_ticker_and_date(csv_path: str) -> tuple[str, Optional[str]]:
    # Expect: <TICKER>_<DATE>_SCT.csv
    base = os.path.basename(csv_path)
    m = re.match(r"([A-Z0-9\.-]+)_(\d{4}-\d{2}-\d{2})_SCT\.csv$", base, flags=re.I)
    if not m:
        # Fallback: try the original year pattern <TICKER>_<YEAR>_SCT.csv
        m2 = re.match(r"([A-Z0-9\.-]+)_(\d{4})_SCT\.csv$", base, flags=re.I)
        if m2:
            return m2.group(1).upper(), m2.group(2)
        # As last resort, ticker only
        return base.split('_')[0].upper(), None
    return m.group(1).upper(), m.group(2)


# -------------------------------
# Utility: Discover local Ollama models (best-effort)
# -------------------------------
def list_ollama_models(max_items: int = 200) -> list[str]:
    """Return a list of locally installed Ollama models, best-effort.

    Tries `ollama list` first, then falls back to `ollama ls`.
    Returns an empty list on failure (callers should provide defaults).
    """
    cmds = [
        ["ollama", "list"],
        ["ollama", "ls"],
    ]
    for cmd in cmds:
        try:
            res = subprocess.run(cmd, text=True, capture_output=True, check=False, timeout=5)
        except Exception:
            continue
        if res.returncode != 0:
            continue
        lines = [ln.strip() for ln in (res.stdout or "").splitlines() if ln.strip()]
        out: list[str] = []
        for i, ln in enumerate(lines):
            # Skip header lines commonly containing NAME or REPOSITORY
            if i == 0 and ("NAME" in ln.upper() or "REPOSITORY" in ln.upper()):
                continue
            # Model name is typically the first whitespace-separated token
            name = ln.split()[0]
            if name and name not in out:
                out.append(name)
            if len(out) >= max_items:
                break
        if out:
            return out
    return []


# -------------------------------
# Function 3: Call Ollama (Deepseek-style prompt)
# -------------------------------
def call_deepseek_with_kor(
    df: pd.DataFrame,
    ticker: str,
    report_year: str,
    report_date: str,
    # model: str = "llama3:8b",
    model: str = "deepseek-r1:14b",
) -> tuple[dict, str, str, int]:
    # Keep prompt shape the same as in the reference script
    table_text = df.to_string(index=False)
#     prompt = f"""
# You are a data assistant.
# Task: Convert the following SEC Summary Compensation Table into JSON.

# Rules:
# 1. JSON structure must be hierarchical:
#    {{
#      "company": {{
#        "ticker": "{ticker}",
#        "report_year": "{report_year}",
#        "reports": [
#          {{
#            "report_date": "{report_date}",
#            "executives": {{
#              "Executive Name": {{
#                "YYYY": {{
#                  "position": "...",
#                  "salary": <number or null>,
#                  "bonus": <number or null>,
#                  "stock_awards": <number or null>,
#                  "non_equity_incentive_plan": <number or null>,
#                  "all_other_compensation": <number or null>,
#                  "total": <number or null>,
#                  "other_fields": {{
#                    "colname1": value,
#                    "colname2": value
#                  }}
#                }},
#                "YYYY2": {{ ... }}
#              }}
#            }}
#          }}
#        ]
#      }}
#    }}

# 2. The first column 'name_position' contains both executive name and title. Split it into:
#    - executive_name
#    - position

# 3. Include ALL columns from the table.
#    - If a column does not map to salary/bonus/etc., place it under "other_fields".
#    - If a value is missing, set it to null.

# 4. The JSON must be valid and properly nested.
# Respond should strictly be in JSON format, no extra text. start and end with curly braces.
# {table_text}
# """

    prompt = f"""
You are a data assistant. Your task is to convert the following SEC Summary Compensation Table
into a clean, hierarchical JSON structure suitable for data ingestion.

Rules and structure requirements:

1. The output must be **valid JSON**, starting with '{{' and ending with '}}'.
   No explanations, no markdown, no text outside the JSON.

2. JSON structure hierarchy:
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
                 "salary": <number>,
                 "bonus": <number>,
                 "stock_awards": <number>,
                 "non_equity_incentive_plan": <number>,
                 "all_other_compensation": <number>,
                 "total": <number>,
                 "<additional_columns_from_csv>": <number or string>
               }},
               "YYYY2": {{ ... }}
             }}
           }}
         }}
       ]
     }}
   }}

3. Column handling:
   - The first column 'name_position' combines executive name and title.
     Split it into two fields:
       - "executive_name"
       - "position"

   - Use **every column header** from the CSV as a JSON key at the same level
     as "salary" and "bonus".

   - The data is in multilevel header format. If a column name is empty, repeated, or invalid, **ignore it** completely.

   - Do NOT include keys whose values are null, blank, or NaN.

   - Normalize numeric values:
       - Remove "$", "," or "()" and convert to numbers where possible.
       - If parsing fails, keep the value as a string.

4. Output precision:
   - Preserve the order of executives and years as they appear.
   - Use consistent key naming (camel_case or underscores).
   - Ensure each executive has a single entry per year.

5. Response formatting:
   - Output only the JSON structure described above.
   - Do not add explanations, commentary, or reasoning.
   - Ensure the response starts with '{{' and ends with '}}'.

Overall, focus on clean, structured JSON output that captures all data

{table_text}
"""

    # Spawn Ollama subprocess
    try:
        # Simpler, robust invocation: send prompt as input and capture full stdout/stderr
        result = subprocess.run(
            ["ollama", "run", model],
            input=prompt,
            text=True,
            capture_output=True,
            check=False,
        )
        response_text = (result.stdout or "").strip()
        stderr_text = (result.stderr or "").strip()
        returncode = int(result.returncode)
    except Exception as e:
        # Return error payload and no stdout/stderr
        return {"error": f"ollama subprocess error: {e}"}, "", str(e), -1

    # Parse JSON from model output
    try:
        cleaned = response_text
        cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE | re.MULTILINE)
        cleaned = re.sub(r"^```\s*", "", cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r"```$", "", cleaned, flags=re.MULTILINE)
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            json_sub = cleaned[start : end + 1]
            parsed = json.loads(json_sub)
            return parsed, response_text, stderr_text, returncode
    except Exception as e:
        return {"raw_response": response_text, "parse_error": str(e)}, response_text, stderr_text, returncode
    return {"raw_response": response_text}, response_text, stderr_text, returncode


# -------------------------------
# Function 4: Save JSON Output + meta
# -------------------------------
def save_json_output(data: dict, csv_path: str) -> str:
    json_path = os.path.splitext(csv_path)[0] + "_kor.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return json_path


@dataclass
class AiMeta:
    csv_path: str
    json_path: str
    model: str
    rows: int
    cols: int
    prompt_chars: int
    status: str
    error: Optional[str] = None


def write_meta(meta: AiMeta) -> None:
    p = Path(meta.json_path).with_suffix(".ai_meta.json")
    p.write_text(json.dumps(asdict(meta), indent=2))


def process_csv(csv_path: str, model: str = "llama3:8b") -> Optional[str]:
    df = read_csv_file(csv_path)
    df = map_columns(df)
    ticker, report_date = extract_ticker_and_date(csv_path)
    report_year = (report_date or "")[:4] if report_date else ""

    # Build the prompt text size for meta (approximate)
    table_text = df.to_string(index=False)
    prompt_chars = len(table_text)

    data, stdout_text, stderr_text, returncode = call_deepseek_with_kor(
        df=df,
        ticker=ticker,
        report_year=report_year or "",
        report_date=report_date or "",
        model=model,
    )
    # Print detailed logs to console to aid debugging
    print(f"[AI] Ollama model: {model}")
    print(f"[AI] Return code: {returncode}")
    if stderr_text:
        print("[AI] stderr (first 500 chars):\n" + stderr_text[:500])
    if stdout_text:
        print("[AI] stdout (first 1000 chars):\n" + stdout_text[:1000])
    json_path = save_json_output(data, csv_path)

    # Status & meta
    status = "ok"
    err = None
    if isinstance(data, dict) and "error" in data:
        status = "error"
        err = str(data.get("error"))
    # Write sidecar meta and a full .ollama.log next to the JSON
    write_meta(
        AiMeta(
            csv_path=csv_path,
            json_path=json_path,
            model=model,
            rows=int(df.shape[0]),
            cols=int(df.shape[1]),
            prompt_chars=prompt_chars,
            status=status,
            error=err,
        )
    )
    # Full log file for inspection
    log_path = os.path.splitext(json_path)[0] + ".ollama.log"
    try:
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write("=== Model ===\n" + model + "\n\n")
            lf.write("=== Prompt (table text omitted for brevity in meta, included here) ===\n")
            lf.write(df.to_string(index=False) + "\n\n")
            lf.write(f"=== Return code ===\n{returncode}\n\n")
            lf.write("=== STDERR ===\n" + (stderr_text or "") + "\n\n")
            lf.write("=== STDOUT ===\n" + (stdout_text or "") + "\n")
    except Exception:
        pass
    return json_path
