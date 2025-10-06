(() => {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  const state = {
    raw: {},
    rows: [], // array of {id, ...fields}
    loadedPath: '',
  };

  const DEFAULT_JSON_PATH = '../restructured_code/json/sec_company_tickers.json';

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

  // attempt initial load
  loadFromPath(DEFAULT_JSON_PATH);
})();

