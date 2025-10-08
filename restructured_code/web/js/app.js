(() => {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  const state = {
    raw: {},
    rows: [], // array of {id, ...fields}
    loadedPath: '',
    mode: 'simple',
  };

  const DEFAULT_JSON_PATH = '../json/sec_company_tickers.json';
  const API_BASE = 'http://127.0.0.1:5000';

  function setStatus(msg, ok = true) {
    const el = $('#loadStatus');
    el.textContent = msg || '';
    el.style.color = ok ? 'var(--muted)' : '#ef4444';
  }

  async function fetchJSON(path) {
    const res = await fetch(path, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  }

  function parseRawToRows(raw) {
    const rows = [];
    const keys = Object.keys(raw);
    for (const k of keys) {
      const rec = raw[k];
      if (!rec || typeof rec !== 'object') continue;
      rows.push({ id: k, ...rec });
    }
    return rows;
  }

  async function loadFromPath(path) {
    setStatus('Loading...');
    try {
      const raw = await fetchJSON(path);
      state.raw = raw;
      state.rows = parseRawToRows(raw);
      state.loadedPath = path;
      setStatus(`Loaded ${state.rows.length} records`);
      updateStats();
      renderResults([]);
    } catch (e) {
      console.error(e);
      setStatus(`Failed to load JSON from ${path}: ${e.message}`, false);
    }
  }

  function loadFromFile(file) {
    setStatus('Reading file...');
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const raw = JSON.parse(reader.result);
        state.raw = raw;
        state.rows = parseRawToRows(raw);
        state.loadedPath = file.name;
        setStatus(`Loaded ${state.rows.length} records from file`);
        updateStats();
        renderResults([]);
      } catch (e) {
        setStatus(`Invalid JSON: ${e.message}`, false);
      }
    };
    reader.onerror = () => setStatus('Failed to read file', false);
    reader.readAsText(file);
  }

  function normStr(v) {
    if (v === null || v === undefined) return '';
    return String(v).trim();
  }

  function normUpper(v) {
    return normStr(v).toUpperCase();
  }

  function isNumericStr(s) {
    return /^\d+$/.test(normStr(s));
  }

  function search(query, field) {
    if (!state.rows.length) return [];
    const q = normStr(query);
    if (!q) return [];
    const qUpper = q.toUpperCase();
    const out = [];
    for (const r of state.rows) {
      const push = () => out.push(r);
      switch (field) {
        case 'ticker':
          if (normUpper(r.ticker) === qUpper) push();
          break;
        case 'cusip':
          if (normUpper(r.cusip) === qUpper) push();
          break;
        case 'permno':
          if (String(r.permno) === q) push();
          break;
        case 'gvkey':
          if (normUpper(r.gvkey) === qUpper) push();
          break;
        case 'cik_str':
          if (normStr(r.cik_str) === q || normStr(r.cik_str).padStart(10, '0') === q) push();
          break;
        case 'title':
          if (normUpper(r.title).includes(qUpper)) push();
          break;
        case 'auto':
        default: {
          // If numeric, try permno/gvkey/cik; else ticker/cusip/title contains
          if (isNumericStr(q)) {
            if (
              String(r.permno) === q ||
              normStr(r.gvkey) === q ||
              normStr(r.cik_str) === q ||
              normStr(r.cik_str).padStart(10, '0') === q
            ) push();
          } else {
            const qU = qUpper;
            if (
              normUpper(r.ticker) === qU ||
              normUpper(r.cusip) === qU ||
              normUpper(r.title).includes(qU)
            ) push();
          }
          break;
        }
      }
      if (out.length >= 200) break; // hard cap to keep UI responsive
    }
    return out;
  }

  function updateStats() {
    const s = $('#stats');
    s.textContent = state.rows.length ? `Loaded ${state.rows.length} records` : '';
  }

  function renderResults(rows) {
    const c = $('#results');
    c.innerHTML = '';
    if (!rows.length) {
      c.innerHTML = '<div class="muted">No results</div>';
      $('#details').classList.add('hidden');
      return;
    }
    for (const r of rows) {
      const card = document.createElement('div');
      card.className = 'card';
      const line = document.createElement('div');
      line.className = 'row';
      const left = document.createElement('div');
      left.innerHTML = `<div><strong>${normUpper(r.ticker) || '(no ticker)'}</strong></div><div class="muted">${normStr(r.title) || ''}</div>`;
      const mid = document.createElement('div');
      mid.className = 'kv';
      mid.innerHTML = `
        <div class="key">CUSIP</div><div class="val">${normStr(r.cusip) || '-'}</div>
        <div class="key">PERMNO</div><div class="val">${normStr(r.permno) || '-'}</div>
        <div class="key">GVKEY</div><div class="val">${normStr(r.gvkey) || '-'}</div>
        <div class="key">CIK</div><div class="val">${normStr(r.cik_str) || '-'}</div>
        <div class="key">Exchange</div><div class="val">${normStr(r.exchange) || '-'}</div>
      `;
      const right = document.createElement('div');
      right.className = 'actions';
      const btn = document.createElement('button');
      btn.textContent = 'Details';
      btn.onclick = () => showDetails(r);
      right.appendChild(btn);
      line.append(left, mid, right);
      card.append(line);
      c.append(card);
    }
  }

  // ---------------- JSON Query mode -----------------
  function runJsonQuery() {
    const text = $('#jsonQuery').value.trim();
    if (!text) {
      setStatus('Enter a JSON query to run.', false);
      return renderResults([]);
    }
    let expr;
    try {
      expr = JSON.parse(text);
    } catch (e) {
      setStatus('Invalid JSON: ' + e.message, false);
      return renderResults([]);
    }
    const out = [];
    for (const r of state.rows) {
      if (evaluateExpr(expr, r)) {
        out.push(r);
        if (out.length >= 500) break;
      }
    }
    setStatus(`Query matched ${out.length} records`);
    renderResults(out);
  }

  function evaluateExpr(expr, row) {
    // Supports:
    // { field: value } -> equality (case-insensitive for strings)
    // { field: { $eq, $neq, $in, $contains, $gt, $lt } }
    // { $and: [ ... ] }, { $or: [ ... ] }
    if (!expr || typeof expr !== 'object') return false;
    if (Array.isArray(expr)) {
      // All must be true
      return expr.every((e) => evaluateExpr(e, row));
    }
    if ('$and' in expr) {
      const arr = expr.$and;
      return Array.isArray(arr) && arr.every((e) => evaluateExpr(e, row));
    }
    if ('$or' in expr) {
      const arr = expr.$or;
      return Array.isArray(arr) && arr.some((e) => evaluateExpr(e, row));
    }
    // field conditions
    for (const [k, v] of Object.entries(expr)) {
      if (!evaluateField(k, v, row)) return false;
    }
    return true;
  }

  function evaluateField(field, cond, row) {
    const val = row[field];
    if (cond && typeof cond === 'object' && !Array.isArray(cond)) {
      // operator object
      if ('$eq' in cond) return cmpEq(val, cond.$eq);
      if ('$neq' in cond) return !cmpEq(val, cond.$neq);
      if ('$in' in cond && Array.isArray(cond.$in)) return cond.$in.some((x) => cmpEq(val, x));
      if ('$contains' in cond) return contains(val, cond.$contains);
      if ('$gt' in cond) return num(val) > num(cond.$gt);
      if ('$lt' in cond) return num(val) < num(cond.$lt);
      return false;
    }
    // equality
    return cmpEq(val, cond);
  }

  function cmpEq(a, b) {
    const na = normStr(a);
    const nb = normStr(b);
    if (na === '' && nb === '') return true;
    // numeric compare
    if (/^[-]?\d+(?:\.\d+)?$/.test(na) && /^[-]?\d+(?:\.\d+)?$/.test(nb)) {
      return Number(na) === Number(nb);
    }
    return na.toUpperCase() === nb.toUpperCase();
  }

  function contains(a, b) {
    const na = normStr(a).toUpperCase();
    const nb = normStr(b).toUpperCase();
    if (!nb) return true;
    return na.includes(nb);
  }

  function num(x) {
    const n = Number(normStr(x));
    return isFinite(n) ? n : NaN;
  }

  function showDetails(rec) {
    const aside = $('#details');
    aside.classList.remove('hidden');
    const tbl = document.createElement('table');
    tbl.className = 'details-table';
    const fields = Object.keys(rec).sort();
    tbl.innerHTML = fields.map(k => `
      <tr><th>${k}</th><td>${escapeHtml(normStr(rec[k]))}</td></tr>
    `).join('');
    aside.innerHTML = '';
    const h = document.createElement('h3');
    h.textContent = normUpper(rec.ticker) || rec.id;
    const copyBtn = document.createElement('button');
    copyBtn.textContent = 'Copy JSON';
    copyBtn.onclick = () => navigator.clipboard.writeText(JSON.stringify(rec, null, 2));
    aside.append(h, copyBtn, tbl);
  }

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));
  }

  // wire up
  $('#loadBtn').addEventListener('click', () => loadFromPath($('#jsonPath').value || DEFAULT_JSON_PATH));
  $('#fileInput').addEventListener('change', (e) => {
    const f = e.target.files && e.target.files[0];
    if (f) loadFromFile(f);
  });
  $('#searchBtn').addEventListener('click', () => {
    const q = $('#query').value;
    const f = $('#fieldSelect').value;
    const rows = search(q, f);
    renderResults(rows);
  });
  $('#query').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      $('#searchBtn').click();
    }
  });

  // Mode switch
  $$("input[name='mode']").forEach((el) => {
    el.addEventListener('change', (e) => {
      state.mode = e.target.value;
      const simple = state.mode === 'simple';
      $('#simpleControls').classList.toggle('hidden', !simple);
      $('#jsonControls').classList.toggle('hidden', simple);
      renderResults([]);
      setStatus(simple ? 'Simple mode' : 'JSON query mode');
    });
  });

  // JSON query actions
  $('#runJsonBtn').addEventListener('click', runJsonQuery);
  $('#insertExample').addEventListener('click', (e) => {
    e.preventDefault();
    $('#jsonQuery').value = JSON.stringify({
      "$or": [
        { "ticker": "AAPL" },
        { "permno": 14593 }
      ]
    }, null, 2);
  });

  // attempt initial load
  loadFromPath(DEFAULT_JSON_PATH);
  
  // ---------------- Filings Download on Home -----------------
  function renderHomeForms(forms) {
    const box = document.getElementById('formsHomeBox');
    if (!box) return;
    box.innerHTML = '';
    forms.forEach((f) => {
      const id = 'homeform_' + f.replace(/\W+/g, '_');
      const w = document.createElement('label');
      w.style.display = 'inline-flex'; w.style.gap = '6px'; w.style.alignItems = 'center'; w.style.marginRight = '12px'; w.style.marginBottom = '6px';
      w.innerHTML = `<input type="checkbox" value="${f}" id="${id}"/> <span>${f}</span>`;
      box.appendChild(w);
    });
  }

  async function api(path, options) {
    const r = await fetch(API_BASE + path, Object.assign({ cache: 'no-store' }, options||{}));
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const ct = r.headers.get('content-type') || '';
    if (ct.includes('application/json')) return await r.json();
    return await r.text();
  }

  async function loadFormsHome() {
    try {
      const data = await api('/api/forms');
      renderHomeForms(data.forms || []);
    } catch {
      renderHomeForms(['10-K','DEF 14A','10-Q','13F-HR','8-K','3','4','5','NPORT-P','D','C','MA-I','144']);
    }
  }

  function selectedHomeForms() {
    return Array.from(document.querySelectorAll('#formsHomeBox input[type=checkbox]:checked')).map(el => el.value);
  }

  async function downloadSelected() {
    const tickerEl = document.getElementById('dlTicker');
    if (!tickerEl) return;
    const t = (tickerEl.value || '').trim().toUpperCase();
    if (!t) { document.getElementById('dlStatus').textContent = 'Enter a ticker'; return; }
    const forms = selectedHomeForms();
    if (!forms.length) { document.getElementById('dlStatus').textContent = 'Select at least one form'; return; }
    document.getElementById('dlStatus').textContent = 'Downloading…';
    try {
      await api('/api/download', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker: t, forms }) });
      document.getElementById('dlStatus').textContent = 'Done';
      const vf = document.getElementById('viewFilingsBtn');
      if (vf) vf.href = `filings.html?ticker=${encodeURIComponent(t)}&forms=${encodeURIComponent(forms.join(','))}`;
    } catch (e) {
      document.getElementById('dlStatus').textContent = 'Error: ' + e.message;
    }
  }

  async function hintTickers() {
    try {
      const data = await api('/api/tickers');
      const count = (data.tickers || []).length;
      document.getElementById('dlTickerHint').textContent = `Loaded ${count} tickers`;
    } catch { document.getElementById('dlTickerHint').textContent = 'Tickers not loaded'; }
  }

  if (document.getElementById('formsHomeBox')) {
    loadFormsHome();
    document.getElementById('downloadBtn').addEventListener('click', downloadSelected);
    document.getElementById('dlLoadTickersBtn').addEventListener('click', hintTickers);
  }
})();
