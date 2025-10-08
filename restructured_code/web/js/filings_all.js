(() => {
  const API_BASE = 'http://127.0.0.1:5000';
  const $ = (sel) => document.querySelector(sel);

  async function api(path) {
    const r = await fetch(API_BASE + path, { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return await r.json();
  }

  async function loadAll() {
    setStatus('Loading…');
    try {
      const data = await api('/api/filings');
      renderList((data && data.results) ? data.results : []);
      setStatus(((data && data.results) ? data.results.length : 0) + ' filings');
    } catch (e) {
      setStatus('Failed: ' + e.message, false);
    }
  }

  function setStatus(msg, ok = true) {
    const el = $('#status');
    el.textContent = msg || '';
    el.style.color = ok ? 'var(--muted)' : '#ef4444';
  }

  function renderList(rows) {
    const ul = $('#fileList');
    ul.innerHTML = '';
    if (!rows.length) { ul.innerHTML = '<li class="muted" style="padding:8px;">No filings</li>'; return; }
    rows.forEach(function(r) {
      const li = document.createElement('li');
      li.style.padding = '8px 10px';
      li.style.borderBottom = '1px solid var(--border)';
      li.style.cursor = 'pointer';
      var label = '<div><strong>' + (r.ticker || '') + '</strong> · ' + (r.form || '') + ' · <span class="muted">' + (r.filing_date || '') + '</span></div>';
      li.innerHTML = label;
      li.addEventListener('click', function() {
        const iframe = $('#preview');
        iframe.src = API_BASE + '/api/file?path=' + encodeURIComponent(r.path);
      });
      ul.appendChild(li);
    });
  }

  var btn = document.getElementById('refreshBtn');
  if (btn) btn.addEventListener('click', loadAll);
  loadAll();
})();

