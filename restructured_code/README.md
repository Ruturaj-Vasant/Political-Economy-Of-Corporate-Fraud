Restructured sandbox for SEC/WRDS enrichment (safe, self-contained)

- Legacy: `restructured_code/legacy/` reference-only copy of earlier scripts
- Working JSON: `restructured_code/json/sec_company_tickers.json`
- Tools: `restructured_code/main/` modular enrichment + sync utilities

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
   `PYTHONPATH=. python restructured_code/main/wrds_probe_aapl.py`

2) Dry-run over a small sample (no file writes)
   `PYTHONPATH=. python3 -m restructured_code.main.update_json \
      --input restructured_code/json/sec_company_tickers.json \
      --limit 200 --dry-run --batch-size 500 \
      --changes-csv restructured_code/json/enrich_changes_sample.csv`

3) Write an enriched copy (safe, non-destructive)
   `PYTHONPATH=. python3 -m restructured_code.main.update_json \
      --input restructured_code/json/sec_company_tickers.json \
      --output restructured_code/json/sec_company_tickers.enriched.json \
      --batch-size 500 --changes-csv restructured_code/json/enrich_changes_full.csv`

4) Overwrite in place with automatic .bak
   `PYTHONPATH=. python3 -m restructured_code.main.update_json \
      --input restructured_code/json/sec_company_tickers.json \
      --in-place --batch-size 500 \
      --changes-csv restructured_code/json/enrich_changes_full.csv`

Update only CIKs (fill missing cik_str)
- Dry-run (no writes):
  `PYTHONPATH=. python3 -m restructured_code.main.update_cik \
     --input restructured_code/json/sec_company_tickers.json \
     --limit 200 --dry-run --batch-size 500 \
     --changes-csv restructured_code/json/cik_changes_sample.csv`
- In-place update (writes .bak + CSV log):
  `PYTHONPATH=. python3 -m restructured_code.main.update_cik \
     --input restructured_code/json/sec_company_tickers.json \
     --in-place --batch-size 500 \
     --changes-csv restructured_code/json/cik_changes_full.csv`

Sync (add) missing WRDS tickers into the JSON
- Preview only (no JSON changes):
  `PYTHONPATH=. python3 -m restructured_code.main.sync_universe \
     --input restructured_code/json/sec_company_tickers.json \
     --preview-missing --batch-size 1000 \
     --out restructured_code/json/missing_tickers.csv`
- Append all missing tickers (writes .bak + CSV of appended rows):
  `PYTHONPATH=. python3 -m restructured_code.main.sync_universe \
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
- Files live under `web/` and read the JSON you load (defaults to `../restructured_code/json/sec_company_tickers.json`).

Run locally:
- From repo root, start a static server:
  `python3 -m http.server 8000`
- Open `http://localhost:8000/web/index.html`
- The page will try to load `../restructured_code/json/sec_company_tickers.json`.
  - If it fails, paste a different path or upload a JSON file using the UI.

Notes:
- The UI does not modify your data; it only reads and displays it.
- Searches cap at 200 results for responsiveness; refine your query if needed.
- The layout adapts to whatever fields your JSON contains, so it remains useful as the schema grows.
