/* ── State ───────────────────────────────────────────────────────────── */
const state = {
  leads: [],
  filtered: [],
  selected: null,        // lead id
  currentDetailId: null,
  currentJobId: null,
  sortCol: 'updated_at',
  sortDir: 'desc',
  search: '',
  statusFilter: '',
  hasEmail: false,
  hasPhone: false,
};

/* ── API helpers ─────────────────────────────────────────────────────── */
async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error(await res.text());
  if (res.status === 204) return null;
  return res.json().catch(() => null);
}
const GET  = (p)    => api('GET', p);
const PUT  = (p, b) => api('PUT', p, b);
const DEL  = (p)    => api('DELETE', p);

/* ── Init ────────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  loadLeads();
  setInterval(loadStats, 10000);

  // Search
  const searchEl = document.getElementById('search');
  searchEl.addEventListener('input', debounce(() => {
    state.search = searchEl.value;
    applyFilters();
  }, 200));
  document.getElementById('clear-search').addEventListener('click', () => {
    searchEl.value = '';
    state.search = '';
    applyFilters();
  });

  // Status filter
  document.getElementById('status-filter').addEventListener('change', e => {
    state.statusFilter = e.target.value;
    loadLeads();
  });

  // Toggle buttons
  document.getElementById('toggle-email').addEventListener('click', function() {
    state.hasEmail = !state.hasEmail;
    this.dataset.active = state.hasEmail;
    this.classList.toggle('active', state.hasEmail);
    loadLeads();
  });
  document.getElementById('toggle-phone').addEventListener('click', function() {
    state.hasPhone = !state.hasPhone;
    this.dataset.active = state.hasPhone;
    this.classList.toggle('active', state.hasPhone);
    loadLeads();
  });

  // Sorting
  document.querySelectorAll('th[data-col]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (state.sortCol === col) state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
      else { state.sortCol = col; state.sortDir = 'asc'; }
      document.querySelectorAll('th').forEach(t => t.classList.remove('sorted'));
      th.classList.add('sorted');
      const icon = th.querySelector('.sort-icon');
      if (icon) icon.textContent = state.sortDir === 'asc' ? '↑' : '↓';
      applyFilters();
    });
  });

  // Exports
  document.getElementById('btn-export-csv').addEventListener('click', exportCsv);
  document.getElementById('btn-export-xlsx').addEventListener('click', exportExcel);
  document.getElementById('btn-export-json').addEventListener('click', exportJson);
  document.getElementById('btn-new-scrape').addEventListener('click', openScrapeModal);

  // Import
  document.getElementById('btn-import').addEventListener('click', openImportModal);
  document.getElementById('import-file').addEventListener('change', e => {
    if (e.target.files[0]) handleImportFile(e.target.files[0]);
  });
  const dropZone = document.getElementById('import-drop-zone');
  dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
  dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    if (e.dataTransfer.files[0]) handleImportFile(e.dataTransfer.files[0]);
  });

  // Modal scrape — enter key
  document.getElementById('scrape-niche').addEventListener('keydown', e => {
    if (e.key === 'Enter') startScrape();
  });

  // Keyboard shortcuts
  document.addEventListener('keydown', handleKeyboard);

  // Close modals on overlay click
  document.getElementById('scrape-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeScrapeModal();
  });
  document.getElementById('detail-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeDetailModal();
  });
  document.getElementById('import-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeImportModal();
  });
});

/* ── Data loading ────────────────────────────────────────────────────── */
async function loadLeads() {
  setLoading(true);
  try {
    const params = new URLSearchParams();
    if (state.statusFilter) params.set('status', state.statusFilter);
    if (state.hasEmail)     params.set('has_email', 'true');
    if (state.hasPhone)     params.set('has_phone', 'true');
    params.set('order_by', `${state.sortCol} ${state.sortDir}`);

    state.leads = await GET(`/api/leads?${params}`);
    await loadStats();
    applyFilters();
  } catch(e) {
    toast('Failed to load leads', 'error');
  } finally {
    setLoading(false);
  }
}

async function loadStats() {
  try {
    const s = await GET('/api/stats');
    renderStats(s);
  } catch {}
}

function applyFilters() {
  const q = state.search.toLowerCase().trim();
  state.filtered = state.leads.filter(l => {
    if (!q) return true;
    return (
      (l.company_name || '').toLowerCase().includes(q) ||
      (l.website      || '').toLowerCase().includes(q) ||
      (l.emails  || []).join(' ').toLowerCase().includes(q) ||
      (l.phones  || []).join(' ').toLowerCase().includes(q) ||
      (l.address || '').toLowerCase().includes(q) ||
      (l.niche   || '').toLowerCase().includes(q)
    );
  });

  // Sort
  const col = state.sortCol;
  state.filtered.sort((a, b) => {
    let va = a[col] ?? '';
    let vb = b[col] ?? '';
    if (typeof va === 'number') return state.sortDir === 'asc' ? va - vb : vb - va;
    va = String(va).toLowerCase();
    vb = String(vb).toLowerCase();
    return state.sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
  });

  renderTable();
  document.getElementById('result-count').textContent =
    `${state.filtered.length} of ${state.leads.length} leads`;
}

/* ── Render ──────────────────────────────────────────────────────────── */
function renderStats(s) {
  document.getElementById('stats').innerHTML = `
    <span class="stat-pill total">
      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>
      ${s.total ?? 0} total
    </span>
    <span class="stat-pill email">${s.with_email ?? 0} email</span>
    <span class="stat-pill phone">${s.with_phone ?? 0} phone</span>
    <span class="stat-pill both">${s.with_both ?? 0} both</span>
    ${s.status_new > 0 ? `<span class="stat-pill" style="background:var(--blue-dim);border-color:var(--blue);color:#60a5fa">${s.status_new} new</span>` : ''}
    ${s.status_contacted > 0 ? `<span class="stat-pill" style="background:var(--yellow-dim);border-color:var(--yellow);color:#fbbf24">${s.status_contacted} contacted</span>` : ''}
    ${s.status_qualified > 0 ? `<span class="stat-pill" style="background:var(--green-dim);border-color:var(--green);color:#4ade80">${s.status_qualified} qualified</span>` : ''}
  `;
}

function renderTable() {
  const tbody = document.getElementById('leads-body');
  const empty = document.getElementById('empty-state');

  if (!state.filtered.length) {
    tbody.innerHTML = '';
    empty.classList.add('visible');
    return;
  }
  empty.classList.remove('visible');

  tbody.innerHTML = state.filtered.map((l, i) => {
    const email  = (l.emails  || [])[0] || '';
    const phone  = (l.phones  || [])[0] || '';
    const name   = l.company_name || domainName(l.website);
    const web    = (l.website || '').replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '');
    const conf   = Math.round((l.confidence || 0) * 100);
    const confColor = conf >= 70 ? '#22c55e' : conf >= 40 ? '#f59e0b' : '#ef4444';
    const isSelected = state.selected === l.id;

    return `<tr data-id="${l.id}" class="${isSelected ? 'selected' : ''}" onclick="selectRow(${l.id})" ondblclick="openDetail(${l.id})">
      <td class="muted" style="font-size:11px">${i + 1}</td>
      <td title="${esc(l.company_name)}">${esc(name)}</td>
      <td class="mono" title="${esc(l.emails?.join(', '))}">${email ? `<span style="color:var(--blue)">${esc(email)}</span>` : '<span class="muted">—</span>'}</td>
      <td class="mono" style="color:var(--yellow)">${esc(phone) || '<span class="muted">—</span>'}</td>
      <td class="website" onclick="openWebsite(event,'${esc(l.website)}')" title="${esc(l.website)}">${esc(web) || '—'}</td>
      <td class="muted" title="${esc(l.niche)}">${esc(l.niche || '—')}</td>
      <td>
        <div class="conf-bar">
          <div class="conf-track"><div class="conf-fill" style="width:${conf}%;background:${confColor}"></div></div>
          <span class="conf-label" style="color:${confColor}">${conf}%</span>
        </div>
      </td>
      <td><span class="badge ${l.status || 'new'}">${(l.status || 'new').charAt(0).toUpperCase() + (l.status || 'new').slice(1)}</span></td>
      <td><button class="row-action" onclick="deleteRow(event,${l.id})" title="Delete">✕</button></td>
    </tr>`;
  }).join('');
}

/* ── Row selection ───────────────────────────────────────────────────── */
function selectRow(id) {
  state.selected = state.selected === id ? null : id;
  renderTable();
}

/* ── Detail Modal ────────────────────────────────────────────────────── */
async function openDetail(id) {
  try {
    const l = await GET(`/api/leads/${id}`);
    state.currentDetailId = id;

    document.getElementById('detail-title').textContent = l.company_name || domainName(l.website);

    const emailTags = (l.emails || []).map(e =>
      `<span class="tag email" onclick="copyText('${esc(e)}')" title="Click to copy" style="cursor:pointer">${esc(e)}</span>`
    ).join('') || '<span class="muted">—</span>';

    const phoneTags = (l.phones || []).map(p =>
      `<span class="tag phone">${esc(p)}</span>`
    ).join('') || '<span class="muted">—</span>';

    document.getElementById('detail-grid').innerHTML = `
      <div class="detail-field full">
        <label>Website</label>
        <div class="value link" onclick="window.open('${esc(l.website)}','_blank')">${esc(l.website) || '—'}</div>
      </div>
      <div class="detail-field full">
        <label>Emails</label>
        <div class="tag-list">${emailTags}</div>
      </div>
      <div class="detail-field full">
        <label>Phone Numbers</label>
        <div class="tag-list">${phoneTags}</div>
      </div>
      <div class="detail-field">
        <label>Company Name</label>
        <div class="value">${esc(l.company_name) || '—'}</div>
      </div>
      <div class="detail-field">
        <label>Niche</label>
        <div class="value">${esc(l.niche) || '—'}</div>
      </div>
      <div class="detail-field full">
        <label>Address</label>
        <div class="value">${esc(l.address) || '—'}</div>
      </div>
      <div class="detail-field">
        <label>Sources</label>
        <div class="value mono" style="font-size:11px">${esc((l.sources || []).join(', ')) || '—'}</div>
      </div>
      <div class="detail-field">
        <label>Confidence</label>
        <div class="value">${Math.round((l.confidence || 0) * 100)}%</div>
      </div>
      <div class="divider"></div>
      <div class="detail-field">
        <label>Status</label>
        <select class="status-select" id="detail-status">
          ${['new','contacted','qualified','rejected'].map(s =>
            `<option value="${s}" ${l.status === s ? 'selected' : ''}>${s.charAt(0).toUpperCase()+s.slice(1)}</option>`
          ).join('')}
        </select>
      </div>
      <div class="detail-field">
        <label>Added</label>
        <div class="value muted" style="font-size:11px">${formatDate(l.created_at)}</div>
      </div>
      <div class="detail-field full">
        <label>Notes</label>
        <textarea class="notes" id="detail-notes" placeholder="Add notes…">${esc(l.notes || '')}</textarea>
      </div>
    `;

    document.getElementById('detail-modal').classList.add('open');
  } catch(e) {
    toast('Failed to load lead', 'error');
  }
}

function closeDetailModal() {
  document.getElementById('detail-modal').classList.remove('open');
  state.currentDetailId = null;
}

async function saveDetail() {
  if (!state.currentDetailId) return;
  const status = document.getElementById('detail-status').value;
  const notes  = document.getElementById('detail-notes').value;
  try {
    await PUT(`/api/leads/${state.currentDetailId}`, { status, notes });
    toast('Saved', 'success');
    closeDetailModal();
    loadLeads();
  } catch {
    toast('Save failed', 'error');
  }
}

async function deleteCurrentLead() {
  if (!state.currentDetailId) return;
  await DEL(`/api/leads/${state.currentDetailId}`);
  toast('Deleted', 'success');
  closeDetailModal();
  loadLeads();
}

function openCurrentWebsite() {
  const l = state.leads.find(x => x.id === state.currentDetailId);
  if (l?.website) window.open(l.website, '_blank');
}

/* ── Row actions ─────────────────────────────────────────────────────── */
async function deleteRow(event, id) {
  event.stopPropagation();
  await DEL(`/api/leads/${id}`);
  toast('Deleted', 'success');
  loadLeads();
}

function openWebsite(event, url) {
  event.stopPropagation();
  if (url) window.open(url, '_blank');
}

/* ── Scrape Modal ────────────────────────────────────────────────────── */
function openScrapeModal() {
  resetScrapeModal();
  document.getElementById('scrape-modal').classList.add('open');
  document.getElementById('scrape-niche').focus();
}

function closeScrapeModal() {
  if (state.currentJobId) return; // block close during active scrape
  document.getElementById('scrape-modal').classList.remove('open');
  resetScrapeModal();
  loadLeads();
}

function resetScrapeModal() {
  document.getElementById('scrape-log').innerHTML = '';
  document.getElementById('scrape-log').classList.remove('visible');
  document.getElementById('scrape-progress-bar').classList.remove('visible');
  document.getElementById('scrape-stats').classList.remove('visible');
  document.getElementById('btn-scrape-start').disabled = false;
  document.getElementById('btn-scrape-start').style.display = '';
  document.getElementById('btn-scrape-stop').style.display = 'none';
  document.getElementById('btn-scrape-close').textContent = 'Cancel';
  state.currentJobId = null;
}

async function startScrape() {
  const niche    = document.getElementById('scrape-niche').value.trim();
  const location = document.getElementById('scrape-location').value.trim();
  const maxLeads = parseInt(document.getElementById('scrape-max').value) || 100;

  if (!niche) {
    document.getElementById('scrape-niche').focus();
    return;
  }

  // Show progress UI
  const log      = document.getElementById('scrape-log');
  const progBar  = document.getElementById('scrape-progress-bar');
  const stats    = document.getElementById('scrape-stats');
  log.innerHTML  = '';
  log.classList.add('visible');
  progBar.classList.add('visible');
  stats.classList.add('visible');

  document.getElementById('btn-scrape-start').style.display = 'none';
  document.getElementById('btn-scrape-stop').style.display = '';
  document.getElementById('btn-scrape-close').textContent = 'Running…';

  try {
    const { job_id } = await api('POST', '/api/scrape', { niche, location, max_leads: maxLeads });
    state.currentJobId = job_id;
    listenToJob(job_id);
  } catch(e) {
    appendLog('error', 'Failed to start scrape: ' + e.message);
  }
}

function listenToJob(jobId) {
  const es = new EventSource(`/api/scrape/${jobId}/stream`);

  es.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'ping') return;

    if (msg.type === 'log') {
      appendLog(msg.level, msg.msg);
    }

    if (msg.type === 'progress') {
      document.getElementById('stat-discovered').textContent = msg.discovered || 0;
      document.getElementById('stat-new').textContent        = msg.saved_new  || 0;
      document.getElementById('stat-merged').textContent     = msg.merged     || 0;
      const fill = document.getElementById('scrape-progress-fill');
      if (msg.discovered > 0) {
        fill.classList.remove('indeterminate');
        const pct = Math.min(100, Math.round(((msg.saved_new + msg.merged) / msg.discovered) * 100));
        fill.style.width = pct + '%';
      }
      // Refresh table live
      loadLeads();
    }

    if (msg.type === 'done') {
      es.close();
      state.currentJobId = null;
      document.getElementById('btn-scrape-stop').style.display = 'none';
      document.getElementById('btn-scrape-start').style.display = '';
      document.getElementById('btn-scrape-start').textContent = 'Scrape Again';
      document.getElementById('btn-scrape-close').textContent = 'Close';
      document.getElementById('scrape-progress-fill').style.width = '100%';
      document.getElementById('scrape-progress-fill').classList.remove('indeterminate');
      loadLeads();
      toast(`Done! +${msg.counts?.saved_new || 0} new leads`, 'success');
    }
  };

  es.onerror = () => {
    es.close();
    if (state.currentJobId === jobId) {
      state.currentJobId = null;
      document.getElementById('btn-scrape-stop').style.display = 'none';
      document.getElementById('btn-scrape-close').textContent = 'Close';
    }
  };
}

function appendLog(level, msg) {
  const log = document.getElementById('scrape-log');
  const div = document.createElement('div');
  div.className = `log-line ${level}`;
  div.textContent = msg;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}

async function stopScrape() {
  if (state.currentJobId) {
    await fetch(`/api/scrape/${state.currentJobId}/stop`);
    toast('Stopping after current request…', 'info');
    document.getElementById('btn-scrape-stop').disabled = true;
  }
}

/* ── Export ──────────────────────────────────────────────────────────── */
function buildExportParams() {
  const p = new URLSearchParams();
  if (state.statusFilter) p.set('status', state.statusFilter);
  if (state.hasEmail)     p.set('has_email', 'true');
  if (state.hasPhone)     p.set('has_phone', 'true');
  if (state.search)       p.set('search', state.search);
  return p.toString();
}

function exportCsv() {
  window.location = `/api/export/csv?${buildExportParams()}`;
  toast('Downloading CSV…', 'info');
}

function exportExcel() {
  window.location = `/api/export/excel?${buildExportParams()}`;
  toast('Downloading Excel…', 'info');
}

function exportJson() {
  window.location = `/api/export/json?${buildExportParams()}`;
  toast('Downloading JSON…', 'info');
}

/* ── Import ──────────────────────────────────────────────────────────── */
function openImportModal() {
  document.getElementById('import-status').style.display = 'none';
  document.getElementById('import-file').value = '';
  document.getElementById('import-modal').classList.add('open');
}

function closeImportModal() {
  document.getElementById('import-modal').classList.remove('open');
}

async function handleImportFile(file) {
  const ext = file.name.split('.').pop().toLowerCase();
  const endpoints = { csv: '/api/import/csv', xlsx: '/api/import/excel', json: '/api/import/json' };
  const endpoint = endpoints[ext];
  if (!endpoint) { toast('Unsupported file type. Use .csv, .xlsx, or .json', 'error'); return; }

  const statusEl = document.getElementById('import-status');
  statusEl.style.display = 'block';
  statusEl.className = '';
  statusEl.style.background = 'var(--surface-3)';
  statusEl.style.color = 'var(--text-muted)';
  statusEl.textContent = `Uploading ${file.name}…`;

  const formData = new FormData();
  formData.append('file', file);

  try {
    const res = await fetch(endpoint, { method: 'POST', body: formData });
    if (!res.ok) throw new Error(await res.text());
    const result = await res.json();
    statusEl.style.background = 'var(--green-dim)';
    statusEl.style.color = '#4ade80';
    statusEl.textContent = `Done! Added ${result.added} new leads, merged ${result.merged}, skipped ${result.skipped}.`;
    toast(`Imported: +${result.added} new, ${result.merged} merged`, 'success');
    loadLeads();
  } catch(e) {
    statusEl.style.background = 'rgba(239,68,68,0.1)';
    statusEl.style.color = '#f87171';
    statusEl.textContent = `Import failed: ${e.message}`;
    toast('Import failed', 'error');
  }
}

/* ── Keyboard shortcuts ──────────────────────────────────────────────── */
function handleKeyboard(e) {
  // Ignore when typing in inputs
  if (['INPUT','TEXTAREA','SELECT'].includes(e.target.tagName)) return;
  if (e.metaKey || e.ctrlKey) return;

  const detail = document.getElementById('detail-modal').classList.contains('open');
  const scrape = document.getElementById('scrape-modal').classList.contains('open');

  if (detail) {
    if (e.key === 'Escape') { closeDetailModal(); e.preventDefault(); }
    if (e.key === 'Enter')  { saveDetail(); e.preventDefault(); }
    return;
  }
  if (scrape) {
    if (e.key === 'Escape' && !state.currentJobId) { closeScrapeModal(); e.preventDefault(); }
    return;
  }

  switch(e.key.toLowerCase()) {
    case 'n': openScrapeModal(); e.preventDefault(); break;
    case '/': document.getElementById('search').focus(); e.preventDefault(); break;
    case 'e': exportCsv(); break;
    case 'escape': state.selected = null; renderTable(); break;
    case 'enter':
      if (state.selected) { openDetail(state.selected); e.preventDefault(); }
      break;
    case 'delete':
    case 'backspace':
      if (state.selected && e.key === 'Delete') {
        DEL(`/api/leads/${state.selected}`).then(() => { toast('Deleted','success'); loadLeads(); });
        e.preventDefault();
      }
      break;
    case 'o':
      if (state.selected) {
        const l = state.leads.find(x => x.id === state.selected);
        if (l?.website) window.open(l.website, '_blank');
      }
      break;
    case 'c':
      if (state.selected) {
        const l = state.leads.find(x => x.id === state.selected);
        if (l?.emails?.[0]) copyText(l.emails[0]);
      }
      break;
    case 't':
      if (state.selected) cycleStatus(state.selected);
      break;
    case 'arrowdown':
    case 'arrowup': {
      const ids = state.filtered.map(l => l.id);
      const idx = ids.indexOf(state.selected);
      const next = e.key === 'arrowdown' ? idx + 1 : idx - 1;
      if (next >= 0 && next < ids.length) {
        state.selected = ids[next];
        renderTable();
        document.querySelector(`tr[data-id="${state.selected}"]`)?.scrollIntoView({ block: 'nearest' });
      }
      e.preventDefault();
      break;
    }
  }
}

async function cycleStatus(id) {
  const order = ['new', 'contacted', 'qualified', 'rejected'];
  const l = state.leads.find(x => x.id === id);
  if (!l) return;
  const next = order[(order.indexOf(l.status) + 1) % order.length];
  await PUT(`/api/leads/${id}`, { status: next });
  toast(`Status → ${next}`, 'success');
  loadLeads();
}

/* ── Utilities ───────────────────────────────────────────────────────── */
function setLoading(v) {
  document.getElementById('loading').classList.toggle('visible', v);
}

function toast(msg, type = 'info', duration = 3000) {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => el.remove(), duration);
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    toast(`Copied: ${text}`, 'success');
  } catch {
    toast('Copy failed', 'error');
  }
}

function domainName(url) {
  if (!url) return '';
  try {
    return new URL(url).hostname.replace(/^www\./, '').split('.')[0];
  } catch { return url; }
}

function esc(s) {
  if (!s) return '';
  return String(s)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;')
    .replace(/'/g,'&#39;');
}

function formatDate(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso + 'Z').toLocaleDateString(undefined, {
      year:'numeric', month:'short', day:'numeric',
    });
  } catch { return iso; }
}

function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}
