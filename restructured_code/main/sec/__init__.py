"""
SEC tools (downloads + extraction) — modular, HPC/cloud‑ready.

Layout
- config.py: runtime knobs (user agent, rate limit, data root)
- clients/edgar_client.py: thin wrapper around the `edgar` library
- storage/backends.py: local storage (default) + commented S3 placeholder
- downloads/: file naming, validator (incl. smoke tests), planner, downloader
- extract/: anchor/XPath helpers + SCT heuristics (ported progressively)
- cli/download_docs.py: non‑interactive CLI for batch/HPC runs

Note: This package uses data/<TICKER>/<FORM>/<DATE>_<FORM>.<ext> naming.
"""

