Restructured sandbox for SEC/WRDS enrichment (safe, self-contained)

- Legacy: `restructured_code/legacy/` reference-only copy of earlier scripts
- Working JSON: `restructured_code/json/sec_company_tickers.json`
- Tools: `restructured_code/main/wrds/` modular enrichment + sync utilities

Run CLI locally without installation:

- Single ticker:
  `python restructured_code/secflow/cli.py run-one --ticker JPM --years 2023:2024`
  Optionally point to metadata:
  `--metadata metadata/sec_company_tickers.json`

Prerequisites
- Python 3.10+ with pandas and psycopg2-binary installed.
- WRDS access configured in `~/.pgpass` (already present on your machine).
- Run commands from the repo root; use `python3` explicitly.

What these tools do
- Enrich missing identifiers and descriptors in the JSON strictly via WRDS using ticker-only lookups.
- Fill: `permno`, `gvkey` (via CRSP↔Compustat link), `cusip`, `exchange` (NYSE/AMEX/NASDAQ), `isDelisted`, `currency`,
  `sector_alt`/`industry_alt` (from Execucomp), `location`, and `source`/`last_enriched`.
- Separate flows to update only `cik_str` and to sync (add) missing tickers from WRDS.

Update JSON with WRDS (permno/gvkey/etc.)

1) Optional: probe a ticker (AAPL) to verify access
   `PYTHONPATH=. python3 -m restructured_code.main.wrds.tools.probe_aapl`

2) Dry-run over a small sample (no file writes)
   `PYTHONPATH=. python3 -m restructured_code.main.wrds.update_json \
      --input restructured_code/json/sec_company_tickers.json \
      --limit 200 --dry-run --batch-size 500 \
      --changes-csv restructured_code/json/enrich_changes_sample.csv`

3) Write an enriched copy (safe, non-destructive)
   `PYTHONPATH=. python3 -m restructured_code.main.wrds.update_json \
      --input restructured_code/json/sec_company_tickers.json \
      --output restructured_code/json/sec_company_tickers.enriched.json \
      --batch-size 500 --changes-csv restructured_code/json/enrich_changes_full.csv`

4) Overwrite in place with automatic .bak
   `PYTHONPATH=. python3 -m restructured_code.main.wrds.update_json \
      --input restructured_code/json/sec_company_tickers.json \
      --in-place --batch-size 500 \
      --changes-csv restructured_code/json/enrich_changes_full.csv`

Update only CIKs (fill missing cik_str)
- Dry-run (no writes):
  `PYTHONPATH=. python3 -m restructured_code.main.wrds.update_cik \
     --input restructured_code/json/sec_company_tickers.json \
     --limit 200 --dry-run --batch-size 500 \
     --changes-csv restructured_code/json/cik_changes_sample.csv`
- In-place update (writes .bak + CSV log):
  `PYTHONPATH=. python3 -m restructured_code.main.wrds.update_cik \
     --input restructured_code/json/sec_company_tickers.json \
     --in-place --batch-size 500 \
     --changes-csv restructured_code/json/cik_changes_full.csv`

Sync (add) missing WRDS tickers into the JSON
- Preview only (no JSON changes):
  `PYTHONPATH=. python3 -m restructured_code.main.wrds.sync_universe \
     --input restructured_code/json/sec_company_tickers.json \
     --preview-missing --batch-size 1000 \
     --out restructured_code/json/missing_tickers.csv`
- Append all missing tickers (writes .bak + CSV of appended rows):
  `PYTHONPATH=. python3 -m restructured_code.main.wrds.sync_universe \
     --input restructured_code/json/sec_company_tickers.json \
     --add-missing --batch-size 1000 \
     --changes-csv restructured_code/json/missing_tickers_added.csv`

Notes
- All scripts run inside `restructured_code`; nothing outside is changed.
- Strictly ticker-only lookup for permno/gvkey (linktable preferred for gvkey).
- Additional enrichments: cusip (Compustat, fallback CRSP ncusip), exchange (CRSP hexcd→NYSE/AMEX/NASDAQ),
  isDelisted (CRSP msedelist), currency (Compustat funda), sector_alt/industry_alt (Execucomp NAICS/SIC descriptions),
  location (Execucomp address/city/state/zip).
- Safe merging: fills only when existing values are null/empty; appends `WRDS` to `source`; adds `last_enriched` timestamp.
- For large runs (16K+), the default batch sizes are safe. You can adjust `--batch-size` (e.g., 1000) based on WRDS responsiveness.

Planned next steps:
- Add checkpointing, resume, and per-batch parquet cache for ~16K tickers
- Schema validation for outputs and a summary report
SEC Downloads Web UI (browse JSON)
- Static web page to search companies by ticker, CUSIP, PERMNO, GVKEY, CIK, or title.
- Files live under `restructured_code/web/` and read the JSON you load (defaults to `../json/sec_company_tickers.json`).

Run locally:
- From repo root, start a static server:
  `python3 -m http.server 8000`
- Open `http://localhost:8000/restructured_code/web/index.html`
- The page will try to load `../json/sec_company_tickers.json`.
  - If it fails, paste a different path or upload a JSON file using the UI.

Notes:
- The UI does not modify your data; it only reads and displays it.
- Searches cap at 200 results for responsiveness; refine your query if needed.
- The layout adapts to whatever fields your JSON contains, so it remains useful as the schema grows.

SEC Downloads (new, modular under `restructured_code/main/sec/`)
- Output naming (default): `data/<TICKER>/<FORM_FS>/<FILING_DATE>_<FORM_FS>.<ext>`
  - Example: `data/AAPL/DEF_14A/2002-03-21_DEF_14A.html`
  - Keeps forms segregated to avoid clutter within each ticker directory.
- Form naming (filesystem‑safe): spaces are replaced with underscores in paths and filenames
  (e.g., `DEF 14A` → `DEF_14A`). The original form string is still stored in sidecar/meta.
- Forms supported now: 10-K, DEF 14A, 10-Q, 13F-HR, 8-K, 3/4/5, NPORT-P, D, C, MA-I, 144.
- Validation (on by default):
  - Non‑empty, minimum size (2KB), parseable by lxml/bs4, and no obvious block pages.
  - Optional smoke test (off by default): for DEF 14A, a quick scan for SCT anchors/phrases.
    - If enabled and it fails, meta includes `extract_smoke_ok: false` for manual review.
- Resume semantics:
  - If a file exists without `meta.json`, we compute checksum + meta and validate it.
  - If valid, it is treated as done; if not, it’s re‑downloaded once with backoff.

Interactive mode (terminal prompts)
- Run the interactive downloader:
  `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.interactive`
- Flow:
  1) Choose a base folder (blank = default). A `data/` subfolder is created inside your base.
  2) Select a form (e.g., DEF 14A)
  3) Enter tickers (comma-separated) or provide a tickers file (CSV/TSV/TXT)
  4) Optionally enter PERMNO(s) and/or provide a PERMNO file to resolve to tickers
  4) Choose save method:
     - Open in browser (local only)
     - Save as text (.txt)
     - Save as HTML (.html); if invalid/unavailable, falls back to text
  5) Optional dry run: lists what would be downloaded without fetching
- Files are written to `data/<TICKER>/<FORM_FS>/<DATE>_<FORM_FS>.<ext>` (FORM_FS = form with spaces replaced by underscores) with a sidecar meta.json.

Dataset index (avoid re-downloads)
- Every data folder maintains a root `metadata.json` index for quick membership checks and counts.
- Location: `<data_root>/metadata.json` (created/updated automatically during runs).
- Rebuild or scan from existing sidecars via CLI:
  - Rebuild: `python3 -m restructured_code.main.sec.downloads.data_index --root data --mode rebuild`
  - Scan/update: `python3 -m restructured_code.main.sec.downloads.data_index --root data --mode scan`
 - Quick rebuild (example external path):
   `python3 -m restructured_code.main.sec.downloads.data_index --root /Volumes/YourDrive/your-folder/data --mode rebuild`

Migration note (older paths with spaces)
- Older runs saved under `data/<TICKER>/<FORM with spaces>/...`. Current versions normalize to underscores.
- If you want to standardize paths, remove the old `data/` and redownload. The index (`metadata.json`) will rebuild on first run.

Bulk downloader CLI (all tickers from JSON)
- File: `restructured_code/main/sec/downloads/bulk.py`
- Source list: `restructured_code/json/sec_company_tickers.json`
- Examples:
  - Dry run all tickers (DEF 14A) into `./edgar_all/data`:
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all --dry-run`
  - Real run (download):
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all`
  - Multiple forms:
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --forms "10-K" --base ./edgar_all`
  - Restrict to tickers:
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --tickers AAPL,MSFT --forms "DEF 14A" --base ./edgar_all`
  - Resolve by PERMNO(s):
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --permnos 14593,10107 --forms "DEF 14A" --base ./edgar_all`
  - Provide lists from files:
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --tickers-file tickers.csv --permnos-file permnos.txt --forms "DEF 14A" --base ./edgar_all`
  - Limit for testing:
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --limit 50 --base ./edgar_all`
  - Year range (optional):
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --years 1994:2025 --base ./edgar_all`
  - Stop after saving N new files or M new tickers (real runs only):
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all --max-new-files 1000`  
    `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base ./edgar_all --max-new-tickers 4500`

Index maintenance with bulk (Seagate examples)
- Re-run without rebuild (recommended default):
  `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base "/Volumes/SeagateX/Political-Economy-Of-Corporate-Fraud"`
- Quick sync from sidecars, then run:
  `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base "/Volumes/SeagateX/Political-Economy-Of-Corporate-Fraud" --scan-index`
- Full rebuild before run (slower; only when needed):
  `PYTHONPATH=. python3 -m restructured_code.main.sec.downloads.bulk --forms "DEF 14A" --base "/Volumes/SeagateX/Political-Economy-Of-Corporate-Fraud" --rebuild-index`

Environment settings (`restructured_code/main/sec/config.py`)
- Set via environment variables before running (recommended):
  - `SEC_USER_AGENT` (required for politeness, e.g., `you@yourdomain`)
  - `SEC_MIN_INTERVAL` (seconds between requests; default `0.5`, e.g., `1.0`)
  - `SEC_MAX_RETRIES` (default `3`)
  - `SEC_MIN_HTML_SIZE` (default `2048` bytes)
  - `SEC_SMOKE_DEF14A` (set to `1` to enable quick DEF 14A smoke test)
  - `SEC_DATA_ROOT` (default `data`; interactive/bulk can override via base folder)
- One‑off in shell:
  `export SEC_USER_AGENT='you@yourdomain'; export SEC_MIN_INTERVAL=1.0`
- Permanent in venv: append the `export` lines to `wrds_env/bin/activate`.

Notes
- The EDGAR integration uses the `edgar` library when available; otherwise it falls back to the official SEC submissions JSON for listings and archive URLs.
- The dataset index prevents re‑downloading across runs; per‑file sidecar meta remain the detailed source of truth next to each saved file.
