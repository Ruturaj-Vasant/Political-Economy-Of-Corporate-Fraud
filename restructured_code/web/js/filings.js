(() => {
  const API_BASE = 'http://127.0.0.1:5000'; // simple local API
  const $ = (sel) => document.querySelector(sel);

  const FORMS_DEFAULT = [
    '10-K','DEF 14A','10-Q','13F-HR','8-K','3','4','5','NPORT-P','D','C','MA-I','144'
  ];

  function setStatus(msg, ok = true) {
    const el = $('#status');
    el.textContent = msg || '';
    el.style.color = ok ? 'var(--muted)' : '#ef4444';
  }

  async function api(path) {
    const r = await fetch(API_BASE + path, { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  }

  async function loadForms() {
    try {
      const data = await api('/api/forms');
      renderForms(data.forms || FORMS_DEFAULT);
    } catch {
      renderForms(FORMS_DEFAULT);
    }
  }

  function renderForms(forms) {
    const box = $('#formsBox');
    box.innerHTML = '';
    forms.forEach((f) => {
      const id = 'form_' + f.replace(/\W+/g, '_');
      const w = document.createElement('label');
      w.style.display = 'inline-flex'; w.style.gap = '6px'; w.style.alignItems = 'center'; w.style.marginRight = '12px'; w.style.marginBottom = '6px';
      w.innerHTML = `<input type="checkbox" value="${f}" id="${id}" checked/> <span>${f}</span>`;
      box.appendChild(w);
    });
  }

  async function loadTickersHint() {
    try {
      const data = await api('/api/tickers');
      const count = (data.tickers || []).length;
      $('#tickerHint').textContent = `Loaded ${count} tickers`;
    } catch (e) {
      $('#tickerHint').textContent = 'Tickers not loaded';
    }
  }

  function getSelectedForms() {
    return Array.from(document.querySelectorAll('#formsBox input[type=checkbox]:checked')).map(el => el.value);
  }

  async function search() {
    const ticker = ($('#ticker').value || '').trim().toUpperCase();
    if (!ticker) { setStatus('Enter a ticker', false); return; }
    const forms = getSelectedForms();
    const y0 = ($('#yearFrom').value || '').trim();
    const y1 = ($('#yearTo').value || '').trim();
    const latest = $('#latestPerYear').checked;
    const years = (y0 && y1) ? `${y0}:${y1}` : (y0 ? `${y0}:${y0}` : '');

    const qs = new URLSearchParams({ ticker, forms: forms.join(','), years, latest: latest ? '1' : '0' });
    setStatus('Loading…');
    try {
      const data = await api('/api/filings?' + qs.toString());
      renderResults(data.results || []);
      setStatus(`${(data.results||[]).length} filings`);
    } catch (e) {
      setStatus('Failed to load filings: ' + e.message, false);
    }
  }

  function renderResults(rows) {
    const c = $('#results');
    c.innerHTML = '';
    if (!rows.length) { c.innerHTML = '<div class="muted">No results</div>'; return; }
    rows.forEach((r) => {
      const card = document.createElement('div');
      card.className = 'card';
      const html = `
        <div class="row" style="grid-template-columns: 140px 120px 1fr 220px;">
          <div><strong>${r.ticker}</strong></div>
          <div>${r.form}</div>
          <div class="muted">${r.filing_date || ''} · ${r.ext || ''} · ${r.size || ''} bytes</div>
          <div class="actions">
            ${r.ext === 'html' ? `<button data-path="${r.path}" data-act="preview">Preview</button>` : ''}
            <a target="_blank" href="${API_BASE}/api/file?path=${encodeURIComponent(r.path)}"><button>Open</button></a>
            ${r.meta && r.meta.extract_smoke_ok === false ? '<span class="muted">smoke: fail</span>' : ''}
          </div>
        </div>`;
      card.innerHTML = html;
      card.addEventListener('click', (e) => {
        const t = e.target;
        if (t && t.matches('button[data-act=preview]')) {
          const p = t.getAttribute('data-path');
          $('#preview').src = `${API_BASE}/api/file?path=${encodeURIComponent(p)}`;
        }
      });
      c.appendChild(card);
    });
  }

  // wire up
  $('#searchBtn').addEventListener('click', search);
  $('#loadTickersBtn').addEventListener('click', loadTickersHint);
  loadForms();
})();

