"""Tiny local API to list and serve downloaded filings for the web UI.

Run:
  (wrds_env) python3 restructured_code/web/server.py

Endpoints:
  GET /api/forms -> { forms: [...] }
  GET /api/tickers -> { tickers: [...] }
  GET /api/filings?ticker=JPM&forms=DEF%2014A,10-K&years=2018:2024&latest=1
    -> { results: [ { path, ticker, form, filing_date, ext, size, meta? } ] }
  GET /api/file?path=relative_path -> serves the file
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from flask import Flask, jsonify, request, send_file, abort, make_response


DATA_ROOT = Path(os.getenv('SEC_DATA_ROOT', 'data')).resolve()
APP_ROOT = Path(__file__).resolve().parent.parent
TICKERS_JSON = (APP_ROOT / 'json' / 'sec_company_tickers.json').resolve()

FORMS = ['10-K','DEF 14A','10-Q','13F-HR','8-K','3','4','5','NPORT-P','D','C','MA-I','144']

app = Flask(__name__)


@app.after_request
def add_cors(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    return resp


@app.route('/api/forms')
def api_forms():
    return jsonify({ 'forms': FORMS })


@app.route('/api/tickers')
def api_tickers():
    tickers: List[str] = []
    try:
        data = json.loads(TICKERS_JSON.read_text())
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, dict) and v.get('ticker'):
                    tickers.append(str(v['ticker']).upper())
    except Exception:
        pass
    return jsonify({ 'tickers': sorted(list(set(tickers))) })


def _safe_rel(rel: str) -> Optional[Path]:
    # prevent path traversal
    p = (DATA_ROOT / rel).resolve()
    if str(p).startswith(str(DATA_ROOT)) and p.exists():
        return p
    return None


@app.route('/api/file')
def api_file():
    rel = request.args.get('path', '')
    p = _safe_rel(rel)
    if not p:
        abort(404)
    return send_file(p)


def _list_form_files(ticker: str, form: str) -> List[Path]:
    base = DATA_ROOT / ticker / form
    if not base.exists():
        return []
    # list both html and txt
    return sorted(list(base.glob('*.html')) + list(base.glob('*.txt')))


def _parse_entry(p: Path, ticker: str, form: str) -> Dict:
    name = p.name  # YYYY-MM-DD_FORM.ext
    filing_date = name.split('_', 1)[0] if '_' in name else ''
    ext = p.suffix.lstrip('.')
    size = p.stat().st_size
    rel = p.relative_to(DATA_ROOT)
    # sidecar meta if exists
    meta_p = p.with_suffix(p.suffix + '.meta.json')
    meta = None
    if meta_p.exists():
        try:
            meta = json.loads(meta_p.read_text())
        except Exception:
            meta = None
    return {
        'path': str(rel),
        'ticker': ticker,
        'form': form,
        'filing_date': filing_date,
        'ext': ext,
        'size': size,
        'meta': meta,
    }


def _filter_by_years(rows: List[Dict], years: Optional[Tuple[int,int]]) -> List[Dict]:
    if not years:
        return rows
    y0, y1 = years
    out = []
    for r in rows:
        try:
            y = int((r.get('filing_date') or '')[:4])
        except Exception:
            continue
        if y0 <= y <= y1:
            out.append(r)
    return out


def _latest_per_year(rows: List[Dict]) -> List[Dict]:
    best: Dict[Tuple[str,int], Dict] = {}
    for r in rows:
        try:
            y = int((r.get('filing_date') or '')[:4])
        except Exception:
            continue
        key = (r['form'], y)
        cur = best.get(key)
        if not cur or (r.get('filing_date') or '') > (cur.get('filing_date') or ''):
            best[key] = r
    return sorted(best.values(), key=lambda x: (x.get('filing_date') or ''), reverse=True)


@app.route('/api/filings')
def api_filings():
    """List filings from the local data/ folder.

    If `ticker` is provided, list only under that ticker. If omitted, list all
    tickers. If `forms` provided (comma-separated), filter to those forms; otherwise
    return all forms.
    """
    ticker = (request.args.get('ticker') or '').upper().strip()
    forms = [f for f in (request.args.get('forms') or '').split(',') if f]
    if not forms:
        forms = FORMS

    results: List[Dict] = []
    tickers: List[str] = []
    if ticker:
        tickers = [ticker]
    else:
        # Enumerate all ticker directories under DATA_ROOT
        if DATA_ROOT.exists():
            for p in sorted(DATA_ROOT.iterdir()):
                if p.is_dir():
                    tickers.append(p.name)

    for t in tickers:
        for form in forms:
            for p in _list_form_files(t, form):
                results.append(_parse_entry(p, t, form))

    # Sort by date desc
    results.sort(key=lambda r: (r.get('filing_date') or ''), reverse=True)
    return jsonify({ 'results': results })


@app.route('/api/download', methods=['POST','OPTIONS'])
def api_download():
    if request.method == 'OPTIONS':
        return ('', 204)
    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({ 'error': 'invalid JSON' }), 400
    ticker = (payload.get('ticker') or '').upper().strip()
    forms = payload.get('forms') or []
    if not ticker or not forms:
        return jsonify({ 'error': 'ticker and forms are required' }), 400
    try:
        from restructured_code.main.sec.downloads.downloader import download_filings_for_ticker
        download_filings_for_ticker(ticker, forms=forms, years=None)
        return jsonify({ 'status': 'ok', 'ticker': ticker, 'forms': forms })
    except Exception as e:
        return jsonify({ 'error': str(e) }), 500


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=False)
