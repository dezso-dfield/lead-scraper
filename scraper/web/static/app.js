/* ── State ───────────────────────────────────────────────────────────── */
const state = {
  leads: [],
  filtered: [],
  selected: null,          // row-highlight lead id
  selectedIds: new Set(),  // bulk-checkbox selection
  currentDetailId: null,
  currentJobId: null,
  currentEmailJobId: null,
  sortCol: 'updated_at',
  sortDir: 'desc',
  search: '',
  statusFilter: '',
  hasEmail: false,
  hasPhone: false,
  projects: [],
  activeProjectId: 'default',
  newProjectColor: '#6366f1',
};

const PROJECT_COLORS = [
  '#6366f1','#22c55e','#f59e0b','#ef4444',
  '#06b6d4','#a855f7','#ec4899','#14b8a6',
];

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
  loadProjects();
  loadLeads();
  setInterval(loadStats, 10000);

  // Close project dropdown when clicking outside
  document.addEventListener('click', e => {
    if (!document.getElementById('project-switcher').contains(e.target) &&
        !document.getElementById('project-dropdown').contains(e.target)) {
      closeProjectDropdown();
    }
  });

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

  // Settings
  document.getElementById('btn-settings').addEventListener('click', openSettingsModal);
  document.getElementById('settings-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeSettingsModal();
  });
  document.getElementById('email-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) { if (!state.currentEmailJobId) closeEmailModal(); }
  });
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
  document.getElementById('new-project-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeNewProjectModal();
  });
  document.getElementById('new-project-name').addEventListener('keydown', e => {
    if (e.key === 'Enter') createProject();
  });

  // History
  document.getElementById('btn-history').addEventListener('click', openHistoryModal);
  document.getElementById('history-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeHistoryModal();
  });

  // Calling modal
  document.getElementById('calling-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeCallingModal();
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
    const isChecked  = state.selectedIds.has(l.id);
    const emailed = l.last_emailed_at ? `title="Last emailed: ${formatDate(l.last_emailed_at)}"` : '';
    const called  = l.last_called_at  ? `title="Last called: ${formatDate(l.last_called_at)}"` : '';

    return `<tr data-id="${l.id}" class="${isSelected ? 'selected' : ''} ${isChecked ? 'checked' : ''}" onclick="selectRow(${l.id})" ondblclick="openDetail(${l.id})">
      <td class="col-check" onclick="event.stopPropagation()">
        <input type="checkbox" class="row-check" ${isChecked ? 'checked' : ''} onchange="toggleSelect(${l.id}, this.checked)">
      </td>
      <td class="muted" style="font-size:11px">${i + 1}</td>
      <td title="${esc(l.company_name)}">${esc(name)}${l.last_emailed_at ? ' <span class="emailed-dot" '+emailed+'></span>' : ''}${l.last_called_at ? ' <span class="called-dot" '+called+'></span>' : ''}</td>
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
  updateSelectionBar();
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
    case 'm': if (state.selectedIds.size > 0) openEmailModal(); break;
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

/* ── Bulk Selection ──────────────────────────────────────────────────── */
function toggleSelect(id, checked) {
  if (checked) state.selectedIds.add(id);
  else state.selectedIds.delete(id);
  updateSelectionBar();
  // Update header checkbox
  const all = state.filtered.every(l => state.selectedIds.has(l.id));
  const none = state.filtered.every(l => !state.selectedIds.has(l.id));
  const cb = document.getElementById('select-all');
  if (cb) { cb.checked = all; cb.indeterminate = !all && !none; }
}

function toggleSelectAll(checkbox) {
  if (checkbox.checked) {
    state.filtered.forEach(l => state.selectedIds.add(l.id));
  } else {
    state.filtered.forEach(l => state.selectedIds.delete(l.id));
  }
  renderTable();
}

function deselectAll() {
  state.selectedIds.clear();
  const cb = document.getElementById('select-all');
  if (cb) { cb.checked = false; cb.indeterminate = false; }
  renderTable();
}

function updateSelectionBar() {
  const n = state.selectedIds.size;
  const bar = document.getElementById('selection-bar');
  bar.classList.toggle('visible', n > 0);
  const withEmail = state.leads.filter(l => state.selectedIds.has(l.id) && l.emails?.length > 0).length;
  document.getElementById('selection-count').textContent =
    `${n} selected${withEmail < n ? ` (${withEmail} have email)` : ''}`;
}

/* ── Email Campaign ──────────────────────────────────────────────────── */
function openEmailModal() {
  const withEmail = state.leads.filter(l => state.selectedIds.has(l.id) && l.emails?.length > 0);
  document.getElementById('email-to-label').textContent =
    `To: ${state.selectedIds.size} selected leads (${withEmail.length} have email address)`;

  // Reset UI
  document.getElementById('email-compose-section').style.display = '';
  document.getElementById('email-progress-section').style.display = 'none';
  document.getElementById('btn-email-send').style.display = '';
  document.getElementById('btn-email-stop').style.display = 'none';
  document.getElementById('btn-email-close').textContent = 'Cancel';
  document.getElementById('email-log').innerHTML = '';

  document.getElementById('email-modal').classList.add('open');
  document.getElementById('email-subject').focus();
}

function closeEmailModal() {
  if (state.currentEmailJobId) return;
  document.getElementById('email-modal').classList.remove('open');
  state.currentEmailJobId = null;
}

async function sendEmailCampaign() {
  const subject = document.getElementById('email-subject').value.trim();
  const body    = document.getElementById('email-body').value.trim();
  if (!subject) { document.getElementById('email-subject').focus(); toast('Subject is required', 'error'); return; }
  if (!body)    { document.getElementById('email-body').focus(); toast('Message body is required', 'error'); return; }
  if (!state.selectedIds.size) { toast('No leads selected', 'error'); return; }

  const autoContacted = document.getElementById('email-auto-contacted').checked;
  const leadIds = [...state.selectedIds];

  // Switch to progress view
  document.getElementById('email-compose-section').style.display = 'none';
  document.getElementById('email-progress-section').style.display = '';
  document.getElementById('btn-email-send').style.display = 'none';
  document.getElementById('btn-email-stop').style.display = '';
  document.getElementById('btn-email-close').textContent = 'Running…';
  document.getElementById('email-progress-fill').style.width = '0%';
  ['email-stat-sent','email-stat-failed','email-stat-skipped'].forEach(id => {
    document.getElementById(id).textContent = '0';
  });

  try {
    const { job_id } = await api('POST', '/api/email/send', {
      lead_ids: leadIds, subject, body, auto_contacted: autoContacted
    });
    state.currentEmailJobId = job_id;
    listenToEmailJob(job_id, leadIds.length);
  } catch(e) {
    appendEmailLog('error', 'Failed to start: ' + e.message);
    document.getElementById('btn-email-close').textContent = 'Close';
    document.getElementById('btn-email-stop').style.display = 'none';
  }
}

function listenToEmailJob(jobId, total) {
  const es = new EventSource(`/api/email/jobs/${jobId}/stream`);
  es.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'ping') return;
    if (msg.type === 'log') {
      appendEmailLog(msg.level, msg.msg);
    }
    if (msg.type === 'progress') {
      document.getElementById('email-stat-sent').textContent    = msg.sent    || 0;
      document.getElementById('email-stat-failed').textContent  = msg.failed  || 0;
      document.getElementById('email-stat-skipped').textContent = msg.skipped || 0;
      const done = (msg.sent || 0) + (msg.failed || 0) + (msg.skipped || 0);
      const pct  = total > 0 ? Math.min(100, Math.round(done / total * 100)) : 0;
      document.getElementById('email-progress-fill').style.width = pct + '%';
    }
    if (msg.type === 'done') {
      es.close();
      state.currentEmailJobId = null;
      document.getElementById('btn-email-stop').style.display = 'none';
      document.getElementById('btn-email-close').textContent = 'Close';
      document.getElementById('email-progress-fill').style.width = '100%';
      deselectAll();
      loadLeads();
      toast(`Campaign done — ${msg.counts?.sent || 0} sent`, 'success');
    }
  };
  es.onerror = () => {
    es.close();
    state.currentEmailJobId = null;
    document.getElementById('btn-email-stop').style.display = 'none';
    document.getElementById('btn-email-close').textContent = 'Close';
  };
}

function appendEmailLog(level, msg) {
  const log = document.getElementById('email-log');
  const div = document.createElement('div');
  div.className = `log-line ${level}`;
  div.textContent = msg;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}

async function stopEmailCampaign() {
  if (state.currentEmailJobId) {
    await fetch(`/api/email/jobs/${state.currentEmailJobId}/stop`);
    toast('Stopping after current email…', 'info');
    document.getElementById('btn-email-stop').disabled = true;
  }
}

function insertMergeTag(tag) {
  const ta = document.getElementById('email-body');
  const start = ta.selectionStart;
  const end   = ta.selectionEnd;
  ta.value = ta.value.slice(0, start) + tag + ta.value.slice(end);
  ta.selectionStart = ta.selectionEnd = start + tag.length;
  ta.focus();
}

/* ── Settings ────────────────────────────────────────────────────────── */
async function openSettingsModal() {
  try {
    const [s, envData] = await Promise.all([
      GET(`/api/settings?project_id=${state.activeProjectId}`),
      GET(`/api/env?project_id=${state.activeProjectId}`),
    ]);

    document.getElementById('s-smtp-host').value   = s.smtp_host  || '';
    document.getElementById('s-smtp-port').value   = s.smtp_port  || 587;
    document.getElementById('s-smtp-ssl').checked  = !!s.smtp_ssl;
    document.getElementById('s-smtp-starttls').checked = s.smtp_starttls !== false;
    document.getElementById('s-smtp-user').value   = s.smtp_user  || '';
    document.getElementById('s-smtp-password').value = '';
    document.getElementById('s-from-name').value   = s.from_name  || '';
    document.getElementById('s-from-email').value  = s.from_email || '';
    document.getElementById('s-delay-min').value   = s.delay_min  ?? 5;
    document.getElementById('s-delay-max').value   = s.delay_max  ?? 15;
    document.getElementById('s-daily-limit').value = s.daily_limit ?? 500;
    document.getElementById('s-unsubscribe-footer').checked = s.unsubscribe_footer !== false;

    // Env files
    document.getElementById('s-global-env').value  = envData.global  || '';
    document.getElementById('s-project-env').value = envData.project || '';
    const isDefault = state.activeProjectId === 'default';
    document.getElementById('s-project-env-wrap').style.opacity = isDefault ? '0.4' : '1';
    document.getElementById('s-project-env-label').textContent =
      isDefault ? '(switch to a non-default project to set project vars)' :
      `(~/.scraper/projects/${state.activeProjectId}/.env)`;

    // Disable env-locked fields
    const locked = s._env_locked || [];
    const fieldMap = { smtp_host:'s-smtp-host', smtp_port:'s-smtp-port',
      smtp_user:'s-smtp-user', smtp_password:'s-smtp-password',
      from_name:'s-from-name', from_email:'s-from-email' };
    Object.values(fieldMap).forEach(id => {
      const el = document.getElementById(id);
      if (el) { el.disabled = false; el.title = ''; }
    });
    locked.forEach(key => {
      const el = document.getElementById(fieldMap[key]);
      if (el) { el.disabled = true; el.title = 'Set via environment variable'; }
    });
    document.getElementById('settings-env-notice').style.display = locked.length ? '' : 'none';
    document.getElementById('smtp-test-result').textContent = '';
  } catch(e) {
    toast('Failed to load settings', 'error');
  }
  document.querySelector('.settings-tab[data-tab="smtp"]').click();
  document.getElementById('settings-modal').classList.add('open');
}

function closeSettingsModal() {
  document.getElementById('settings-modal').classList.remove('open');
}

function switchTab(btn) {
  document.querySelectorAll('.settings-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.settings-tab-body').forEach(t => t.style.display = 'none');
  btn.classList.add('active');
  document.getElementById('tab-' + btn.dataset.tab).style.display = '';
}

async function saveSettings() {
  const pw = document.getElementById('s-smtp-password').value;
  const updates = {
    smtp_host:     document.getElementById('s-smtp-host').value.trim(),
    smtp_port:     parseInt(document.getElementById('s-smtp-port').value) || 587,
    smtp_ssl:      document.getElementById('s-smtp-ssl').checked,
    smtp_starttls: document.getElementById('s-smtp-starttls').checked,
    smtp_user:     document.getElementById('s-smtp-user').value.trim(),
    from_name:     document.getElementById('s-from-name').value.trim(),
    from_email:    document.getElementById('s-from-email').value.trim(),
    delay_min:     parseFloat(document.getElementById('s-delay-min').value) || 5,
    delay_max:     parseFloat(document.getElementById('s-delay-max').value) || 15,
    daily_limit:   parseInt(document.getElementById('s-daily-limit').value) || 500,
    unsubscribe_footer: document.getElementById('s-unsubscribe-footer').checked,
    _scope: 'global',
  };
  if (pw && !pw.includes('•')) updates.smtp_password = pw;

  const globalEnv  = document.getElementById('s-global-env').value;
  const projectEnv = document.getElementById('s-project-env').value;
  const isDefault  = state.activeProjectId === 'default';

  try {
    await Promise.all([
      api('PUT', '/api/settings', updates),
      api('PUT', '/api/env', { content: globalEnv, project_id: null }),
      ...(isDefault ? [] : [api('PUT', '/api/env', { content: projectEnv, project_id: state.activeProjectId })]),
    ]);
    toast('Settings saved', 'success');
    closeSettingsModal();
  } catch(e) {
    toast('Failed to save: ' + e.message, 'error');
  }
}

/* ── Projects ────────────────────────────────────────────────────────── */
async function loadProjects() {
  try {
    state.projects = await GET('/api/projects');
    const active = state.projects.find(p => p.active) || state.projects[0];
    if (active) {
      state.activeProjectId = active.id;
      document.getElementById('project-name').textContent = active.name;
      document.getElementById('project-dot').style.background = active.color;
    }
    renderProjectList();
  } catch(e) {}
}

function renderProjectList() {
  const list = document.getElementById('project-list');
  list.innerHTML = state.projects.map(p => `
    <div class="project-item ${p.active ? 'active' : ''}" onclick="switchProject('${esc(p.id)}')">
      <span class="project-dot" style="background:${esc(p.color)}"></span>
      <div class="project-item-info">
        <span class="project-item-name">${esc(p.name)}</span>
        <span class="project-item-count">${p.lead_count ?? 0} leads</span>
      </div>
      ${p.id !== 'default' ? `<button class="project-delete" onclick="deleteProject(event,'${esc(p.id)}')" title="Delete project">✕</button>` : ''}
    </div>
  `).join('');
}

async function switchProject(projectId) {
  if (projectId === state.activeProjectId) { closeProjectDropdown(); return; }
  try {
    await api('POST', `/api/projects/${projectId}/activate`, {});
    state.activeProjectId = projectId;
    closeProjectDropdown();
    state.selectedIds.clear();
    state.selected = null;
    await loadProjects();
    await loadLeads();
    toast(`Switched to project`, 'success');
  } catch(e) {
    toast('Failed to switch project', 'error');
  }
}

async function deleteProject(event, projectId) {
  event.stopPropagation();
  if (!confirm(`Delete project? The leads database will be kept on disk.`)) return;
  try {
    await DEL(`/api/projects/${projectId}`);
    toast('Project deleted', 'success');
    await loadProjects();
    if (state.activeProjectId === projectId) {
      state.activeProjectId = 'default';
      await loadLeads();
    }
  } catch(e) {
    toast('Failed to delete project: ' + e.message, 'error');
  }
}

function toggleProjectDropdown(event) {
  event.stopPropagation();
  const dd = document.getElementById('project-dropdown');
  dd.classList.toggle('open');
}

function closeProjectDropdown() {
  document.getElementById('project-dropdown').classList.remove('open');
}

function openNewProjectModal() {
  state.newProjectColor = PROJECT_COLORS[state.projects.length % PROJECT_COLORS.length];
  document.getElementById('new-project-name').value = '';
  // Render color picker
  const cp = document.getElementById('color-picker');
  cp.innerHTML = PROJECT_COLORS.map(c =>
    `<button class="color-swatch ${c === state.newProjectColor ? 'selected' : ''}"
       style="background:${c}" onclick="selectProjectColor('${c}')" title="${c}"></button>`
  ).join('');
  document.getElementById('new-project-modal').classList.add('open');
  setTimeout(() => document.getElementById('new-project-name').focus(), 50);
}

function closeNewProjectModal() {
  document.getElementById('new-project-modal').classList.remove('open');
}

function selectProjectColor(color) {
  state.newProjectColor = color;
  document.querySelectorAll('.color-swatch').forEach(b => {
    b.classList.toggle('selected', b.style.background === color || b.style.backgroundColor === color);
  });
}

async function createProject() {
  const name = document.getElementById('new-project-name').value.trim();
  if (!name) { document.getElementById('new-project-name').focus(); return; }
  try {
    await api('POST', '/api/projects', { name, color: state.newProjectColor });
    closeNewProjectModal();
    toast(`Project "${name}" created`, 'success');
    state.selectedIds.clear();
    state.selected = null;
    await loadProjects();
    await loadLeads();
  } catch(e) {
    toast('Failed to create project: ' + e.message, 'error');
  }
}

/* ── Settings — env tab ──────────────────────────────────────────────── */
async function testSmtp() {
  const btn = document.getElementById('btn-test-smtp');
  const result = document.getElementById('smtp-test-result');
  btn.disabled = true;
  result.textContent = 'Testing…';
  result.style.color = 'var(--text-muted)';
  try {
    // Save first so we test current values
    const pw = document.getElementById('s-smtp-password').value;
    const updates = {
      smtp_host:     document.getElementById('s-smtp-host').value.trim(),
      smtp_port:     parseInt(document.getElementById('s-smtp-port').value) || 587,
      smtp_ssl:      document.getElementById('s-smtp-ssl').checked,
      smtp_starttls: document.getElementById('s-smtp-starttls').checked,
      smtp_user:     document.getElementById('s-smtp-user').value.trim(),
      from_email:    document.getElementById('s-from-email').value.trim(),
    };
    if (pw && !pw.includes('•')) updates.smtp_password = pw;
    await api('PUT', '/api/settings', updates);
    const r = await api('POST', '/api/settings/test-smtp', {});
    if (r.ok) {
      result.textContent = '✓ Connected successfully';
      result.style.color = 'var(--green)';
    } else {
      result.textContent = '✗ ' + r.error;
      result.style.color = 'var(--red)';
    }
  } catch(e) {
    result.textContent = '✗ ' + e.message;
    result.style.color = 'var(--red)';
  } finally {
    btn.disabled = false;
  }
}

/* ══════════════════════════════════════════════════════════════════════
   CALLING MODAL
   ══════════════════════════════════════════════════════════════════════ */

const callingState = {
  queue: [],      // [{id, company_name, phones, website, city, niche, last_called_at}]
  cursor: 0,
  done: 0,
};

function openCallingModal() {
  // Build queue from selected leads that have phones
  const leadsWithPhone = state.filtered.filter(
    l => state.selectedIds.has(l.id) && l.phones && l.phones.length > 0
  );
  if (!leadsWithPhone.length) {
    toast('No selected leads with phone numbers', 'error');
    return;
  }
  callingState.queue  = leadsWithPhone;
  callingState.cursor = 0;
  callingState.done   = 0;
  document.getElementById('call-notes').value = '';
  document.getElementById('calling-modal').classList.add('open');
  renderCallCard();
}

function closeCallingModal() {
  document.getElementById('calling-modal').classList.remove('open');
  if (callingState.done > 0) loadLeads();
}

function renderCallCard() {
  const { queue, cursor } = callingState;
  const total = queue.length;
  const pct   = total ? Math.round((cursor / total) * 100) : 0;

  document.getElementById('call-progress-bar').style.width  = pct + '%';
  document.getElementById('call-progress-label').textContent = `${cursor + 1} / ${total}`;
  document.getElementById('call-done-count').textContent     = callingState.done
    ? `${callingState.done} logged`
    : '';
  document.getElementById('btn-call-prev').disabled = cursor === 0;

  if (cursor >= total) {
    document.getElementById('call-company').textContent = 'All done!';
    document.getElementById('call-phone').textContent   = '';
    document.getElementById('call-meta').textContent    = '';
    document.getElementById('call-progress-bar').style.width = '100%';
    document.getElementById('call-progress-label').textContent = `${total} / ${total}`;
    document.querySelectorAll('.outcome-btn').forEach(b => b.disabled = true);
    document.getElementById('btn-call-skip').disabled = true;
    return;
  }

  const lead = queue[cursor];
  document.getElementById('call-company').textContent = lead.company_name || domainName(lead.website) || '—';
  document.getElementById('call-phone').textContent   = (lead.phones || [])[0] || '—';
  document.getElementById('call-meta').innerHTML      = [
    lead.city   ? `<span>${esc(lead.city)}</span>`  : '',
    lead.niche  ? `<span>${esc(lead.niche)}</span>` : '',
    (lead.phones || []).length > 1
      ? `<span>${lead.phones.length} numbers</span>`
      : '',
  ].filter(Boolean).join(' · ');

  document.querySelectorAll('.outcome-btn').forEach(b => b.disabled = false);
  document.getElementById('btn-call-skip').disabled = false;
  document.getElementById('call-notes').value = '';
}

async function logCall(outcome) {
  const { queue, cursor } = callingState;
  if (cursor >= queue.length) return;
  const lead  = queue[cursor];
  const notes = document.getElementById('call-notes').value.trim();

  // Map outcome class names to DB values (no-answer → no_answer, etc.)
  const outcomeDb = outcome;  // already underscored from HTML onclick

  try {
    await api('POST', `/api/leads/${lead.id}/activity`, {
      activity_type: 'call',
      outcome: outcomeDb,
      notes,
      update_status: ['answered', 'interested'].includes(outcomeDb) ? 'qualified' : '',
    });
    callingState.done++;
    callingState.cursor++;
    renderCallCard();
  } catch(e) {
    toast('Failed to log call: ' + e.message, 'error');
  }
}

function callSkip() {
  if (callingState.cursor < callingState.queue.length) {
    callingState.cursor++;
    renderCallCard();
  }
}

function callPrev() {
  if (callingState.cursor > 0) {
    callingState.cursor--;
    renderCallCard();
  }
}

function copyCallPhone() {
  const phone = document.getElementById('call-phone').textContent.trim();
  if (!phone || phone === '—') return;
  navigator.clipboard.writeText(phone).then(() => toast('Phone copied', 'success'));
}


/* ══════════════════════════════════════════════════════════════════════
   ACTIVITY HISTORY MODAL
   ══════════════════════════════════════════════════════════════════════ */

const histState = { filter: '' };

function openHistoryModal() {
  histState.filter = '';
  document.querySelectorAll('.hist-filter-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('hist-filter-all').classList.add('active');
  document.getElementById('history-modal').classList.add('open');
  loadHistory();
}

function closeHistoryModal() {
  document.getElementById('history-modal').classList.remove('open');
}

function setHistFilter(type) {
  histState.filter = type;
  document.querySelectorAll('.hist-filter-btn').forEach(b => b.classList.remove('active'));
  const id = type === '' ? 'hist-filter-all' : type === 'call' ? 'hist-filter-call' : 'hist-filter-email';
  document.getElementById(id).classList.add('active');
  loadHistory();
}

async function loadHistory() {
  const list = document.getElementById('history-list');
  list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px">Loading…</div>';
  try {
    let url = '/api/activity?limit=200';
    if (histState.filter) url += '&activity_type=' + histState.filter;
    const data = await GET(url);
    renderHistoryList(data);
  } catch(e) {
    list.innerHTML = `<div style="padding:20px;text-align:center;color:var(--red);font-size:13px">Failed to load: ${esc(e.message)}</div>`;
  }
}

function renderHistoryList(items) {
  const list = document.getElementById('history-list');
  if (!items.length) {
    list.innerHTML = '<div style="padding:30px;text-align:center;color:var(--text-muted);font-size:13px">No activity yet</div>';
    return;
  }

  const callIcon  = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 13a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3.6 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg>`;
  const emailIcon = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>`;
  const noteIcon  = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>`;

  list.innerHTML = items.map(item => {
    const isCall  = item.activity_type === 'call';
    const isEmail = item.activity_type === 'email';
    const icon    = isCall ? callIcon : isEmail ? emailIcon : noteIcon;
    const iconCls = isCall ? 'call' : isEmail ? 'email' : 'note';
    const outcome = item.outcome
      ? `<span class="hist-outcome ${item.outcome}">${item.outcome.replace(/_/g, ' ')}</span>`
      : '';
    const company = esc(item.company_name || '—');
    const subject = item.subject
      ? `<div class="hist-meta">${esc(item.subject)}</div>`
      : '';
    const notes   = item.notes
      ? `<div class="hist-notes">"${esc(item.notes)}"</div>`
      : '';
    const time = item.created_at ? formatDate(item.created_at) : '';

    return `<div class="hist-item">
      <div class="hist-icon ${iconCls}">${icon}</div>
      <div class="hist-body">
        <div class="hist-company">${company}${outcome}</div>
        ${subject}${notes}
      </div>
      <div class="hist-time">${time}</div>
    </div>`;
  }).join('');
}
