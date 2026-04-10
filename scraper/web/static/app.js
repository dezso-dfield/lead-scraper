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
  emailedFilter: '',  // '' | 'never' | '1' | '3' | '7' | '30' | '90'
  calledFilter:  '',
  tagFilter: '',
  callbackOverdue: false,
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
  _updateSortIcons();
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
    _updateFilterDot();
    loadLeads();
  });

  // Toggle buttons
  document.getElementById('toggle-email').addEventListener('click', function() {
    state.hasEmail = !state.hasEmail;
    this.dataset.active = state.hasEmail;
    this.classList.toggle('active', state.hasEmail);
    _updateFilterDot();
    loadLeads();
  });
  document.getElementById('toggle-phone').addEventListener('click', function() {
    state.hasPhone = !state.hasPhone;
    this.dataset.active = state.hasPhone;
    this.classList.toggle('active', state.hasPhone);
    _updateFilterDot();
    loadLeads();
  });

  document.getElementById('filter-emailed').addEventListener('change', e => {
    state.emailedFilter = e.target.value;
    _updateFilterDot();
    applyFilters();
  });
  document.getElementById('filter-called').addEventListener('change', e => {
    state.calledFilter = e.target.value;
    _updateFilterDot();
    applyFilters();
  });

  // Tag filter
  const tagInput = document.getElementById('tag-filter');
  if (tagInput) {
    tagInput.addEventListener('input', debounce(e => {
      state.tagFilter = e.target.value.trim();
      _updateFilterDot();
      loadLeads();
    }, 300));
  }

  // Callbacks due toggle
  const overdueBtn = document.getElementById('toggle-overdue');
  if (overdueBtn) {
    overdueBtn.addEventListener('click', function() {
      state.callbackOverdue = !state.callbackOverdue;
      this.dataset.active = state.callbackOverdue;
      this.classList.toggle('active', state.callbackOverdue);
      _updateFilterDot();
      loadLeads();
    });
  }

  // Re-render on resize (switch table ↔ cards)
  window.addEventListener('resize', debounce(() => {
    renderTable();
  }, 150));

  // Sorting is handled via onclick="sortBy(col)" on each <th data-col="...">

  // Exports (inside more menu — close menu after action)
  document.getElementById('btn-export-csv').addEventListener('click', () => { closeMoreMenu(); exportCsv(); });
  document.getElementById('btn-export-xlsx').addEventListener('click', () => { closeMoreMenu(); exportExcel(); });
  document.getElementById('btn-export-json').addEventListener('click', () => { closeMoreMenu(); exportJson(); });
  document.getElementById('btn-new-scrape').addEventListener('click', openScrapeModal);

  // Import
  document.getElementById('btn-import').addEventListener('click', () => { closeMoreMenu(); openImportModal(); });

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

  // Update modal
  document.getElementById('update-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeUpdateModal();
  });

  // Silent update check on load — just lights the dot if behind
  checkUpdateSilent();
});

/* ── Data loading ────────────────────────────────────────────────────── */
async function loadLeads() {
  setLoading(true);
  try {
    const params = new URLSearchParams();
    if (state.statusFilter)   params.set('status', state.statusFilter);
    if (state.hasEmail)       params.set('has_email', 'true');
    if (state.hasPhone)       params.set('has_phone', 'true');
    if (state.tagFilter)      params.set('tag', state.tagFilter);
    if (state.callbackOverdue) params.set('callback_overdue', 'true');
    // Only pass DB-sortable columns to the server; computed cols (score) are sorted client-side
    const DB_SORT_COLS = new Set(['company_name','website','status','created_at','updated_at','niche','city']);
    const serverCol = DB_SORT_COLS.has(state.sortCol) ? state.sortCol : 'updated_at';
    const serverDir = DB_SORT_COLS.has(state.sortCol) ? state.sortDir : 'desc';
    params.set('order_by', `${serverCol} ${serverDir}`);

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

function _daysAgo(isoStr) {
  if (!isoStr) return null;
  const ms = Date.now() - new Date(isoStr).getTime();
  return ms / 86400000;  // fractional days
}

function applyFilters() {
  const q = state.search.toLowerCase().trim();
  state.filtered = state.leads.filter(l => {
    // text search
    if (q && !(
      (l.company_name || '').toLowerCase().includes(q) ||
      (l.website      || '').toLowerCase().includes(q) ||
      (l.emails  || []).join(' ').toLowerCase().includes(q) ||
      (l.phones  || []).join(' ').toLowerCase().includes(q) ||
      (l.address || '').toLowerCase().includes(q) ||
      (l.niche   || '').toLowerCase().includes(q)
    )) return false;

    // emailed date filter
    if (state.emailedFilter) {
      if (state.emailedFilter === 'never') {
        if (l.last_emailed_at) return false;
      } else {
        const days = _daysAgo(l.last_emailed_at);
        if (days === null || days > parseInt(state.emailedFilter)) return false;
      }
    }

    // called date filter
    if (state.calledFilter) {
      if (state.calledFilter === 'never') {
        if (l.last_called_at) return false;
      } else {
        const days = _daysAgo(l.last_called_at);
        if (days === null || days > parseInt(state.calledFilter)) return false;
      }
    }

    return true;
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
  const parts = [];
  if (s.with_email  > 0) parts.push(`<span class="stat-chip stat-email">${s.with_email} email</span>`);
  if (s.with_phone  > 0) parts.push(`<span class="stat-chip stat-phone">${s.with_phone} phone</span>`);
  if (s.status_qualified > 0) parts.push(`<span class="stat-chip stat-qual">${s.status_qualified} qualified</span>`);
  if (s.overdue_callbacks > 0) parts.push(`<span class="stat-chip stat-due">⚠ ${s.overdue_callbacks} due</span>`);
  const sep = `<span class="stat-sep">·</span>`;
  document.getElementById('stats').innerHTML =
    `<span class="stat-total">${s.total ?? 0} leads</span>` +
    (parts.length ? sep + parts.join(sep) : '');
}

function isMobile() { return window.innerWidth <= 768; }

function renderTable() {
  renderCards();
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
    const isSelected = state.selected === l.id;
    const isChecked  = state.selectedIds.has(l.id);
    const emailed = l.last_emailed_at ? `title="Last emailed: ${formatDate(l.last_emailed_at)}"` : '';
    const called  = l.last_called_at  ? `title="Last called: ${formatDate(l.last_called_at)}"` : '';
    const score   = l.score ?? 0;
    const scoreClass = score >= 65 ? 'score-high' : score >= 35 ? 'score-mid' : 'score-low';
    const isOverdue = l.callback_at && new Date(l.callback_at) <= new Date();
    const rowClass = [isSelected ? 'selected' : '', isChecked ? 'checked' : '', isOverdue ? 'overdue-row' : ''].filter(Boolean).join(' ');
    const callbackTip = l.callback_at ? ` title="Callback: ${new Date(l.callback_at).toLocaleString()}"` : '';

    const statusVal = l.status || 'new';
    return `<tr data-id="${l.id}" class="${rowClass}" onclick="openDetail(${l.id})"${callbackTip}>
      <td class="col-check" onclick="toggleSelectRow(event,${l.id})">
        <input type="checkbox" class="row-check" ${isChecked ? 'checked' : ''} onchange="toggleSelect(${l.id}, this.checked)" onclick="event.stopPropagation()">
      </td>
      <td class="col-company" title="${esc(l.company_name || l.website)}">
        <div class="lead-name">${esc(name)}</div>
        <div class="lead-meta">
          ${l.niche ? `<span class="meta-niche">${esc(l.niche)}</span>` : ''}
          ${(l.tags||[]).map(t => `<span class="tag-pill" onclick="filterByTag(event,'${esc(t)}')">${esc(t)}</span>`).join('')}
          ${l.last_emailed_at ? `<span class="meta-dot emailed-dot" ${emailed}></span>` : ''}
          ${l.last_called_at  ? `<span class="meta-dot called-dot"  ${called}></span>`  : ''}
        </div>
      </td>
      <td onclick="event.stopPropagation();${email ? `copyCell(event,'${esc(email)}')` : ''}" title="${esc(l.emails?.join(', '))}">
        ${email
          ? `<span class="ci ci-email"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>${esc(email)}</span>`
          : `<span class="ci ci-empty"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg></span>`}
      </td>
      <td onclick="event.stopPropagation();${phone ? `copyCell(event,'${esc(phone)}')` : ''}" title="${esc(phone)}">
        ${phone
          ? `<span class="ci ci-phone"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 13a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3.6 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg>${esc(phone)}</span>`
          : `<span class="ci ci-empty"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 13a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3.6 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg></span>`}
      </td>
      <td class="website" onclick="openWebsite(event,'${esc(l.website)}')" title="${esc(l.website)}">${esc(web) || '<span class="muted">—</span>'}</td>
      <td style="text-align:center">
        <div class="score-bar-wrap">
          <div class="score-bar"><div class="score-bar-fill ${scoreClass}" style="width:${score}%"></div></div>
          <span class="score-num ${scoreClass}">${score}</span>
        </div>
      </td>
      <td onclick="event.stopPropagation()">
        <div class="status-dot-wrap clickable-badge" onclick="cycleStatus(${l.id})" title="Click to cycle status">
          <span class="status-dot ${statusVal}"></span>
          <span>${statusVal.charAt(0).toUpperCase() + statusVal.slice(1)}</span>
        </div>
      </td>
      <td><button class="row-action" onclick="deleteRow(event,${l.id})" title="Delete">✕</button></td>
    </tr>`;
  }).join('');
  updateSelectionBar();
  _updateSortIcons();
}

/* ── Row selection ───────────────────────────────────────────────────── */
let _lastCheckedIdx = -1;

function selectRow(id) {
  state.selected = id;
  // Highlight without re-rendering the whole table
  document.querySelectorAll('#leads-table tbody tr').forEach(tr => {
    tr.classList.toggle('selected', parseInt(tr.dataset.id) === id);
  });
}

function toggleSelectRow(e, id) {
  e.stopPropagation();
  const idx = state.filtered.findIndex(l => l.id === id);
  if (e.shiftKey && _lastCheckedIdx >= 0 && idx >= 0) {
    // Range select
    const lo = Math.min(_lastCheckedIdx, idx);
    const hi = Math.max(_lastCheckedIdx, idx);
    const addOrRemove = !state.selectedIds.has(id);
    for (let i = lo; i <= hi; i++) {
      if (addOrRemove) state.selectedIds.add(state.filtered[i].id);
      else state.selectedIds.delete(state.filtered[i].id);
    }
  } else {
    if (state.selectedIds.has(id)) state.selectedIds.delete(id);
    else state.selectedIds.add(id);
    _lastCheckedIdx = idx;
  }
  updateSelectionBar();
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
      `<span class="tag phone" onclick="copyText('${esc(p)}')" title="Click to copy" style="cursor:pointer">${esc(p)}</span>`
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
        <input type="text" id="detail-company-name" value="${esc(l.company_name || '')}" placeholder="Company name" style="width:100%">
      </div>
      <div class="detail-field">
        <label>Niche</label>
        <input type="text" id="detail-niche" value="${esc(l.niche || '')}" placeholder="e.g. restaurant" style="width:100%">
      </div>
      <div class="detail-field full">
        <label>Address</label>
        <div class="value">${esc(l.address) || '—'}</div>
      </div>
      <div class="detail-field">
        <label>Contact Name</label>
        <input type="text" id="detail-contact-name" value="${esc(l.contact_name || '')}" placeholder="e.g. John Smith" style="width:100%">
      </div>
      <div class="detail-field">
        <label>Contact Title</label>
        <input type="text" id="detail-contact-title" value="${esc(l.contact_title || '')}" placeholder="e.g. CEO" style="width:100%">
      </div>
      <div class="detail-field">
        <label>Sources</label>
        <div class="value mono" style="font-size:11px">${esc((l.sources || []).join(', ')) || '—'}</div>
      </div>
      <div class="detail-field">
        <label>Score</label>
        <div class="value">${l.score ?? 0} / 100</div>
      </div>
      <div class="divider"></div>
      <div class="detail-field">
        <label>Status</label>
        <select class="status-select" id="detail-status">
          ${['new','contacted','warm','qualified','rejected'].map(s =>
            `<option value="${s}" ${l.status === s ? 'selected' : ''}>${s.charAt(0).toUpperCase()+s.slice(1)}</option>`
          ).join('')}
        </select>
      </div>
      <div class="detail-field">
        <label>Callback Date</label>
        <input type="datetime-local" id="detail-callback-at" value="${l.callback_at ? l.callback_at.slice(0,16) : ''}" style="width:100%">
      </div>
      <div class="detail-field">
        <label>Added</label>
        <div class="value muted" style="font-size:11px">${formatDate(l.created_at)}</div>
      </div>
      <div class="detail-field full">
        <label>Tags</label>
        <input type="text" id="detail-tags" value="${esc((l.tags || []).join(', '))}" placeholder="tag1, tag2, tag3" style="width:100%">
      </div>
      <div class="detail-field full">
        <label>Notes</label>
        <textarea class="notes" id="detail-notes" placeholder="Add notes…">${esc(l.notes || '')}</textarea>
      </div>
    `;

    document.getElementById('detail-modal').classList.add('open');

    // Cross-project duplicate check (non-blocking)
    GET(`/api/leads/${id}/duplicates`).then(dupes => {
      if (!dupes || !dupes.length) return;
      const banner = document.createElement('div');
      banner.className = 'dupe-banner';
      banner.innerHTML = `
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
        Found in ${dupes.length} other project${dupes.length > 1 ? 's' : ''}:
        ${dupes.map(d => `<span class="dupe-pill" style="border-color:${esc(d.project_color)};color:${esc(d.project_color)}">${esc(d.project_name)}</span>`).join('')}
      `;
      const grid = document.getElementById('detail-grid');
      if (grid) grid.parentNode.insertBefore(banner, grid);
    }).catch(() => {});
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
  const tagsRaw = document.getElementById('detail-tags').value;
  const tags = tagsRaw.split(',').map(t => t.trim()).filter(Boolean);
  const callbackRaw = document.getElementById('detail-callback-at').value;
  const callback_at = callbackRaw ? callbackRaw.replace('T', ' ') + ':00' : '';
  const contact_name  = document.getElementById('detail-contact-name')?.value.trim()  || '';
  const contact_title = document.getElementById('detail-contact-title')?.value.trim() || '';
  const company_name  = document.getElementById('detail-company-name')?.value.trim()  || '';
  const niche         = document.getElementById('detail-niche')?.value.trim()         || '';
  try {
    await PUT(`/api/leads/${state.currentDetailId}`, { status, notes, tags, callback_at, contact_name, contact_title, company_name, niche });
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
async function openScrapeModal() {
  resetScrapeModal();
  document.getElementById('scrape-modal').classList.add('open');
  document.getElementById('scrape-niche').focus();
  // Check AI and Maps key availability
  try {
    const s = await GET('/api/settings');
    const aiWrap   = document.getElementById('ai-option-wrap');
    const aiStatus = document.getElementById('ai-option-status');
    const aiCheck  = document.getElementById('scrape-use-ai');
    if (aiWrap)   { aiWrap.style.opacity = s._has_anthropic ? '1' : '0.5'; }
    if (aiStatus) { aiStatus.textContent = s._has_anthropic ? '✓ ready' : '(configure in Settings → AI)'; }
    if (aiCheck)  { aiCheck.disabled = !s._has_anthropic; }

    const mapsWrap   = document.getElementById('maps-option-wrap');
    const mapsStatus = document.getElementById('maps-option-status');
    const mapsCheck  = document.getElementById('scrape-maps');
    if (mapsStatus) { mapsStatus.textContent = s._has_maps ? '✓ API key set' : '(DDG fallback)'; }
    if (mapsWrap)   { mapsWrap.style.opacity = '1'; }
    if (mapsCheck)  { mapsCheck.disabled = false; }
  } catch(e) {}
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
  const useAi    = document.getElementById('scrape-use-ai')?.checked || false;
  const social   = document.getElementById('scrape-social')?.checked || false;
  const inclMaps = document.getElementById('scrape-maps')?.checked || false;

  if (!niche) {
    document.getElementById('scrape-niche').focus();
    return;
  }

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
    const { job_id } = await api('POST', '/api/scrape', {
      niche, location, max_leads: maxLeads,
      use_ai: useAi, include_social: social, include_maps: inclMaps,
    });
    state.currentJobId = job_id;
    listenToJob(job_id);
  } catch(e) {
    appendLog('error', 'Failed to start scrape: ' + e.message);
    document.getElementById('btn-scrape-start').style.display = '';
    document.getElementById('btn-scrape-stop').style.display = 'none';
    document.getElementById('btn-scrape-close').textContent = 'Close';
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
    case 'escape':
      if (state.selectedIds.size) { deselectAll(); }
      else { state.selected = null; document.querySelectorAll('#leads-table tbody tr.selected').forEach(tr => tr.classList.remove('selected')); }
      break;
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
      const idx = ids.indexOf(state.selected ?? -1);
      const next = e.key === 'arrowdown' ? (idx < 0 ? 0 : idx + 1) : (idx <= 0 ? 0 : idx - 1);
      if (next >= 0 && next < ids.length) {
        selectRow(ids[next]);
        document.querySelector(`tr[data-id="${state.selected}"]`)?.scrollIntoView({ block: 'nearest' });
      }
      e.preventDefault();
      break;
    }
  }
}

async function cycleStatus(id) {
  const order = ['new', 'contacted', 'warm', 'qualified', 'rejected'];
  const l = state.leads.find(x => x.id === id);
  if (!l) return;
  const next = order[(order.indexOf(l.status) + 1) % order.length];
  await PUT(`/api/leads/${id}`, { status: next });
  toast(`Status → ${next}`, 'success');
  loadLeads();
}

/* ── Sidebar toggle ──────────────────────────────────────────────── */
function toggleSidebar() {
  document.getElementById('sidebar').classList.toggle('sb-collapsed');
}

/* ── Filter panel toggle ─────────────────────────────────────────────── */
let _filtersOpen = false;

function toggleFilters() {
  _filtersOpen = !_filtersOpen;
  const panel = document.getElementById('filter-panel');
  const btn   = document.getElementById('btn-filter-toggle');
  if (panel) panel.classList.toggle('open', _filtersOpen);
  if (btn)   btn.classList.toggle('active', _filtersOpen);
}

function resetFilters() {
  state.statusFilter  = '';
  state.hasEmail      = false;
  state.hasPhone      = false;
  state.emailedFilter = '';
  state.calledFilter  = '';
  state.tagFilter     = '';
  state.callbackOverdue = false;
  const els = {
    'status-filter':   v => { const e = document.getElementById(v); if(e) e.value = ''; },
    'filter-emailed':  v => { const e = document.getElementById(v); if(e) e.value = ''; },
    'filter-called':   v => { const e = document.getElementById(v); if(e) e.value = ''; },
    'tag-filter':      v => { const e = document.getElementById(v); if(e) e.value = ''; },
  };
  Object.keys(els).forEach(k => els[k](k));
  ['toggle-email','toggle-phone','toggle-overdue'].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.dataset.active = 'false'; el.classList.remove('active'); }
  });
  _updateFilterDot();
  loadLeads();
}

/* ── Column sorting ──────────────────────────────────────────────────── */
function sortBy(col) {
  if (!col) return;
  if (state.sortCol === col) {
    state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
  } else {
    state.sortCol = col;
    // score: highest first; company_name: A-Z; everything else: latest first
    state.sortDir = col === 'company_name' ? 'asc' : 'desc';
  }
  _updateSortIcons();
  loadLeads();
}

function _updateSortIcons() {
  document.querySelectorAll('#leads-table thead th[data-col]').forEach(th => {
    const icon = th.querySelector('.sort-icon');
    if (!icon) return;
    if (th.dataset.col === state.sortCol) {
      icon.textContent = state.sortDir === 'asc' ? '↑' : '↓';
      icon.classList.add('active');
    } else {
      icon.textContent = '↕';
      icon.classList.remove('active');
    }
  });
}

/* ── More Menu ───────────────────────────────────────────────────────── */
function toggleMoreMenu(e) {
  e.stopPropagation();
  document.getElementById('more-menu').classList.toggle('open');
}
function closeMoreMenu() {
  document.getElementById('more-menu').classList.remove('open');
}
document.addEventListener('click', () => closeMoreMenu());

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

async function copyCell(e, text) {
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
    // Brief flash on the cell
    const cell = e.currentTarget || e.target.closest('td');
    if (cell) {
      cell.classList.add('copied-flash');
      setTimeout(() => cell.classList.remove('copied-flash'), 600);
    }
    toast(`Copied: ${text}`, 'success', 1800);
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

async function bulkUpdateStatus(status) {
  const ids = [...state.selectedIds];
  if (!ids.length) return;
  try {
    const r = await api('POST', '/api/leads/bulk-status', { lead_ids: ids, status });
    toast(`Updated ${r.updated} leads to "${status}"`, 'success');
    deselectAll();
    loadLeads();
  } catch {
    toast('Bulk update failed', 'error');
  }
}

async function bulkDelete() {
  const ids = [...state.selectedIds];
  if (!ids.length) return;
  if (!confirm(`Delete ${ids.length} lead${ids.length > 1 ? 's' : ''}? This cannot be undone.`)) return;
  try {
    await api('DELETE', '/api/leads?' + ids.map(id => `ids=${id}`).join('&'));
    toast(`Deleted ${ids.length} lead${ids.length > 1 ? 's' : ''}`, 'success');
    deselectAll();
    loadLeads();
  } catch {
    toast('Delete failed', 'error');
  }
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
  const parts = [`${n} lead${n !== 1 ? 's' : ''} selected`];
  if (withEmail < n && withEmail > 0) parts.push(`${withEmail} with email`);
  else if (withEmail === 0 && n > 0) parts.push('none have email');
  document.getElementById('selection-count').textContent = parts.join(' · ');
}

/* ── Card view (mobile) ──────────────────────────────────────────────── */
function renderCards() {
  const cardView = document.getElementById('card-view');
  const table    = document.getElementById('leads-table');
  if (!cardView) return;

  if (!isMobile()) {
    cardView.style.display = 'none';
    table.style.display = '';
    return;
  }

  table.style.display = 'none';
  cardView.style.display = '';

  if (!state.filtered.length) {
    cardView.innerHTML = '';
    return;
  }

  cardView.innerHTML = state.filtered.map(l => {
    const email   = (l.emails  || [])[0] || '';
    const phone   = (l.phones  || [])[0] || '';
    const name    = l.company_name || domainName(l.website) || '—';
    const web     = (l.website || '').replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '');
    const conf    = Math.round((l.confidence || 0) * 100);
    const confColor = conf >= 70 ? '#22c55e' : conf >= 40 ? '#f59e0b' : '#ef4444';
    const isChecked = state.selectedIds.has(l.id);
    const status    = l.status || 'new';
    const emailed   = l.last_emailed_at ? `title="Emailed: ${formatDate(l.last_emailed_at)}"` : '';
    const called    = l.last_called_at  ? `title="Called: ${formatDate(l.last_called_at)}"` : '';

    return `<div class="lead-card ${isChecked ? 'checked' : ''}" data-id="${l.id}" onclick="cardTap(event, ${l.id})">
      <div class="lead-card-check">
        <input type="checkbox" ${isChecked ? 'checked' : ''} onchange="toggleSelect(${l.id}, this.checked)" onclick="event.stopPropagation()">
      </div>
      <div class="lead-card-body">
        <div class="lead-card-title">
          ${esc(name)}
          ${l.last_emailed_at ? `<span class="emailed-dot" ${emailed}></span>` : ''}
          ${l.last_called_at  ? `<span class="called-dot"  ${called}></span>`  : ''}
        </div>
        <div class="lead-card-meta">
          ${email ? `<a class="lead-card-email" href="mailto:${esc(email)}" onclick="event.stopPropagation()">${esc(email)}</a>` : '<span class="lead-card-none">no email</span>'}
          ${phone ? `<a class="lead-card-phone" href="tel:${esc(phone)}" onclick="event.stopPropagation()">${esc(phone)}</a>` : ''}
        </div>
        ${web ? `<div class="lead-card-web">${esc(web)}</div>` : ''}
      </div>
      <div class="lead-card-right">
        <span class="badge ${status}">${status.charAt(0).toUpperCase() + status.slice(1)}</span>
        <span class="lead-card-conf" style="color:${confColor}">${conf}%</span>
      </div>
    </div>`;
  }).join('');

  // Update select-all checkbox state
  const all = document.getElementById('select-all');
  if (all) all.checked = state.selectedIds.size > 0 &&
    state.filtered.every(l => state.selectedIds.has(l.id));
}

function cardTap(event, id) {
  if (event.target.tagName === 'INPUT' || event.target.tagName === 'A') return;
  openDetail(id);
}

/* ── Mobile filter toggle ────────────────────────────────────────────── */
let _mobileFiltersOpen = false;

function toggleMobileFilters() {
  _mobileFiltersOpen = !_mobileFiltersOpen;
  const fg  = document.getElementById('filter-group');
  const btn = document.getElementById('btn-filter-toggle');
  fg.classList.toggle('open', _mobileFiltersOpen);
  btn.classList.toggle('active', _mobileFiltersOpen);
}

function _updateFilterDot() {
  const active = !!(state.statusFilter || state.hasEmail || state.hasPhone ||
    state.emailedFilter || state.calledFilter || state.tagFilter || state.callbackOverdue);
  const dot = document.getElementById('filter-active-dot');
  if (dot) dot.style.display = active ? '' : 'none';
  const btn = document.getElementById('btn-filter-toggle');
  if (btn) btn.classList.toggle('active', active || _filtersOpen);
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
  loadScriptsList();
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
  const validateEmails = document.getElementById('email-validate')?.checked !== false;
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
      lead_ids: leadIds, subject, body, auto_contacted: autoContacted, validate_emails: validateEmails
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
    const isDefault = state.activeProjectId === 'default';
    const [globalS, projS, envData] = await Promise.all([
      GET('/api/settings'),
      isDefault ? Promise.resolve({}) : GET(`/api/settings?project_id=${state.activeProjectId}`),
      GET(`/api/env?project_id=${state.activeProjectId}`),
    ]);

    // Global SMTP tab
    document.getElementById('s-smtp-host').value   = globalS.smtp_host  || '';
    document.getElementById('s-smtp-port').value   = globalS.smtp_port  || 587;
    document.getElementById('s-smtp-ssl').checked  = !!globalS.smtp_ssl;
    document.getElementById('s-smtp-starttls').checked = globalS.smtp_starttls !== false;
    document.getElementById('s-smtp-user').value   = globalS.smtp_user  || '';
    document.getElementById('s-smtp-password').value = '';
    document.getElementById('s-from-name').value   = globalS.from_name  || '';
    document.getElementById('s-from-email').value  = globalS.from_email || '';

    // Project SMTP tab
    const proj = state.projects.find(p => p.id === state.activeProjectId);
    document.getElementById('proj-smtp-name').textContent = proj?.name || 'this project';
    document.getElementById('ps-smtp-host').value   = projS.smtp_host  || '';
    document.getElementById('ps-smtp-port').value   = projS.smtp_port  || '';
    document.getElementById('ps-smtp-ssl').checked  = !!projS.smtp_ssl;
    document.getElementById('ps-smtp-starttls').checked = !!projS.smtp_starttls;
    document.getElementById('ps-smtp-user').value   = projS.smtp_user  || '';
    document.getElementById('ps-smtp-password').value = '';
    document.getElementById('ps-from-name').value   = projS.from_name  || '';
    document.getElementById('ps-from-email').value  = projS.from_email || '';
    document.getElementById('proj-smtp-test-result').textContent = '';
    // Hide project SMTP tab for default project
    document.querySelector('.settings-tab[data-tab="proj-smtp"]').style.display =
      isDefault ? 'none' : '';

    // Sending
    document.getElementById('s-delay-min').value   = globalS.delay_min  ?? 5;
    document.getElementById('s-delay-max').value   = globalS.delay_max  ?? 15;
    document.getElementById('s-daily-limit').value = globalS.daily_limit ?? 500;
    document.getElementById('s-unsubscribe-footer').checked = globalS.unsubscribe_footer !== false;

    // Env files
    document.getElementById('s-global-env').value  = envData.global  || '';
    document.getElementById('s-project-env').value = envData.project || '';
    document.getElementById('s-project-env-wrap').style.opacity = isDefault ? '0.4' : '1';
    document.getElementById('s-project-env-label').textContent =
      isDefault ? '(switch to a non-default project to set project vars)' :
      `(~/.scraper/projects/${state.activeProjectId}/.env)`;

    // Env-locked fields (global tab)
    const locked = globalS._env_locked || [];
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

    // Load scripts + sequences
    await loadScriptsList();
    await loadSequences();
    // AI key status
    const aiStatus = document.getElementById('ai-option-status');
    if (aiStatus) aiStatus.textContent = globalS._has_anthropic ? '✓ configured' : '(no key set)';
    document.getElementById('s-anthropic-key').value = '';

    // Maps key + Base URL
    const mapsKeyEl = document.getElementById('s-maps-key');
    if (mapsKeyEl) mapsKeyEl.value = globalS._has_maps ? '••••••••' : '';
    const baseUrlEl = document.getElementById('s-base-url');
    if (baseUrlEl) baseUrlEl.value = globalS.base_url || '';

    // IMAP settings
    const iEl = k => document.getElementById(k);
    if (iEl('s-imap-host'))     iEl('s-imap-host').value     = globalS.imap_host     || '';
    if (iEl('s-imap-port'))     iEl('s-imap-port').value     = globalS.imap_port     || 993;
    if (iEl('s-imap-ssl'))      iEl('s-imap-ssl').checked    = globalS.imap_ssl      !== false;
    if (iEl('s-imap-user'))     iEl('s-imap-user').value     = globalS.imap_user     || '';
    if (iEl('s-imap-password')) iEl('s-imap-password').value = '';
    if (iEl('s-imap-folder'))   iEl('s-imap-folder').value   = globalS.imap_folder   || 'INBOX';
    if (iEl('s-imap-interval')) iEl('s-imap-interval').value = globalS.imap_interval || 10;
    // IMAP status
    try {
      const imapSt = await GET('/api/imap/status');
      const lbl = document.getElementById('imap-status-label');
      if (lbl) lbl.textContent = imapSt.running ? `Running — last check: ${imapSt.last_check || 'never'}` : 'Not running';
    } catch {}

    // Webhooks list
    await loadWebhooks();
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
  const isDefault = state.activeProjectId === 'default';

  // Global SMTP + sending
  const pw = document.getElementById('s-smtp-password').value;
  const globalUpdates = {
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
  if (pw && !pw.includes('•')) globalUpdates.smtp_password = pw;

  // Project SMTP (non-default only)
  const calls = [
    api('PUT', '/api/settings', globalUpdates),
    api('PUT', '/api/env', { content: document.getElementById('s-global-env').value, project_id: null }),
  ];
  if (!isDefault) {
    const ppw = document.getElementById('ps-smtp-password').value;
    const projPort = parseInt(document.getElementById('ps-smtp-port').value);
    const projUpdates = {
      smtp_host:     document.getElementById('ps-smtp-host').value.trim(),
      smtp_ssl:      document.getElementById('ps-smtp-ssl').checked,
      smtp_starttls: document.getElementById('ps-smtp-starttls').checked,
      smtp_user:     document.getElementById('ps-smtp-user').value.trim(),
      from_name:     document.getElementById('ps-from-name').value.trim(),
      from_email:    document.getElementById('ps-from-email').value.trim(),
      _scope: 'project',
    };
    if (projPort) projUpdates.smtp_port = projPort;
    if (ppw && !ppw.includes('•')) projUpdates.smtp_password = ppw;
    calls.push(api('PUT', `/api/settings?project_id=${state.activeProjectId}`, projUpdates));
    calls.push(api('PUT', '/api/env', {
      content: document.getElementById('s-project-env').value,
      project_id: state.activeProjectId,
    }));
  }

  // Save Anthropic API key if provided
  const anthropicKey = document.getElementById('s-anthropic-key')?.value.trim();
  if (anthropicKey && !anthropicKey.includes('•')) {
    calls.push(api('PUT', '/api/settings', { anthropic_api_key: anthropicKey, _scope: 'global' }));
  }

  // Save Maps key if provided
  const mapsKey = document.getElementById('s-maps-key')?.value.trim();
  if (mapsKey && !mapsKey.includes('•')) {
    calls.push(api('PUT', '/api/settings', { google_maps_api_key: mapsKey, _scope: 'global' }));
  }

  // Save Base URL
  const baseUrl = document.getElementById('s-base-url')?.value.trim();
  if (baseUrl !== undefined) {
    calls.push(api('PUT', '/api/settings', { base_url: baseUrl, _scope: 'global' }));
  }

  // Save IMAP settings
  const imapUpdates = { _scope: 'global' };
  const imapHost = document.getElementById('s-imap-host')?.value.trim();
  const imapUser = document.getElementById('s-imap-user')?.value.trim();
  const imapPass = document.getElementById('s-imap-password')?.value;
  if (imapHost !== undefined) imapUpdates.imap_host = imapHost;
  if (imapUser !== undefined) imapUpdates.imap_user = imapUser;
  if (imapPass && !imapPass.includes('•')) imapUpdates.imap_password = imapPass;
  imapUpdates.imap_port     = parseInt(document.getElementById('s-imap-port')?.value)     || 993;
  imapUpdates.imap_ssl      = document.getElementById('s-imap-ssl')?.checked              !== false;
  imapUpdates.imap_folder   = document.getElementById('s-imap-folder')?.value.trim()      || 'INBOX';
  imapUpdates.imap_interval = parseInt(document.getElementById('s-imap-interval')?.value) || 10;
  calls.push(api('PUT', '/api/settings', imapUpdates));

  try {
    await Promise.all(calls);
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
async function testSmtp(scope = 'global') {
  const isProject = scope === 'project';
  const btnId     = isProject ? 'btn-test-proj-smtp' : 'btn-test-smtp';
  const resultId  = isProject ? 'proj-smtp-test-result' : 'smtp-test-result';
  const prefix    = isProject ? 'ps' : 's';

  const btn    = document.getElementById(btnId);
  const result = document.getElementById(resultId);
  btn.disabled = true;
  result.textContent = 'Testing…';
  result.style.color = 'var(--text-muted)';
  try {
    // Save first so we test current values
    const pw = document.getElementById(`${prefix}-smtp-password`).value;
    const updates = {
      smtp_host:     document.getElementById(`${prefix}-smtp-host`).value.trim(),
      smtp_port:     parseInt(document.getElementById(`${prefix}-smtp-port`).value) || 587,
      smtp_ssl:      document.getElementById(`${prefix}-smtp-ssl`).checked,
      smtp_starttls: document.getElementById(`${prefix}-smtp-starttls`).checked,
      smtp_user:     document.getElementById(`${prefix}-smtp-user`).value.trim(),
      from_email:    document.getElementById(`${prefix}-from-email`).value.trim(),
      _scope: isProject ? 'project' : 'global',
    };
    if (pw && !pw.includes('•')) updates.smtp_password = pw;
    const saveUrl = isProject
      ? `/api/settings?project_id=${state.activeProjectId}`
      : '/api/settings';
    await api('PUT', saveUrl, updates);
    const r = await api('POST', `/api/settings/test-smtp?scope=${scope}`, {});
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

/* ── Scripts ──────────────────────────────────────────────────────────── */
let _scripts = [];
let _editingScriptId = null;

async function loadScriptsList() {
  try {
    _scripts = await GET('/api/scripts');
    renderScriptsList();
    renderEmailScriptCards();
    // Keep hidden select in sync for compatibility
    const picker = document.getElementById('script-picker');
    if (picker) {
      picker.innerHTML = '<option value="">— Select script —</option>' +
        _scripts.map(s => `<option value="${s.id}">${esc(s.name)}</option>`).join('');
    }
  } catch(e) {}
}

function renderEmailScriptCards() {
  const el = document.getElementById('email-script-cards');
  if (!el) return;
  if (!_scripts || !_scripts.length) {
    el.innerHTML = '<div class="script-card-empty">No scripts yet — save one in Settings → Scripts</div>';
    return;
  }
  el.innerHTML = _scripts.map(s => `
    <div class="script-card" data-script-id="${s.id}" onclick="loadScriptCard(${s.id})" title="${esc(s.subject || '')}">
      <div class="script-card-name">${esc(s.name)}</div>
      <div class="script-card-subject">${esc(s.subject || '(no subject)')}</div>
    </div>
  `).join('');
}

function loadScriptCard(scriptId) {
  const s = _scripts.find(x => x.id === scriptId);
  if (!s) return;
  document.getElementById('email-subject').value = s.subject || '';
  document.getElementById('email-body').value    = s.body    || '';
  // Mark active
  document.querySelectorAll('.script-card').forEach(c => {
    c.classList.toggle('active', parseInt(c.dataset.scriptId) === scriptId);
  });
}

function renderScriptsList() {
  const el = document.getElementById('scripts-list');
  if (!_scripts.length) {
    el.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px">No scripts yet. Click New Script to create one.</div>';
    return;
  }
  el.innerHTML = _scripts.map(s => `
    <div class="script-item">
      <div class="script-item-info">
        <div class="script-item-name">${esc(s.name)}</div>
        <div class="script-item-subject">${esc(s.subject || '(no subject)')}</div>
      </div>
      <div class="script-item-actions">
        <button class="btn" onclick="openScriptEditor(${s.id})">Edit</button>
        <button class="btn" onclick="deleteScript(${s.id})" title="Delete">✕</button>
      </div>
    </div>`).join('');
}

function openScriptEditor(scriptId) {
  _editingScriptId = scriptId || null;
  const editor = document.getElementById('script-editor');
  const title  = document.getElementById('script-editor-title');
  if (scriptId) {
    const s = _scripts.find(x => x.id === scriptId);
    if (!s) return;
    document.getElementById('se-name').value    = s.name;
    document.getElementById('se-subject').value = s.subject;
    document.getElementById('se-body').value    = s.body;
    title.textContent = 'Edit Script';
  } else {
    document.getElementById('se-name').value    = '';
    document.getElementById('se-subject').value = '';
    document.getElementById('se-body').value    = '';
    title.textContent = 'New Script';
  }
  editor.style.display = '';
  document.getElementById('se-name').focus();
}

function closeScriptEditor() {
  document.getElementById('script-editor').style.display = 'none';
  _editingScriptId = null;
}

async function saveScript() {
  const name    = document.getElementById('se-name').value.trim();
  const subject = document.getElementById('se-subject').value.trim();
  const body    = document.getElementById('se-body').value;
  if (!name) { document.getElementById('se-name').focus(); return; }
  try {
    if (_editingScriptId) {
      await api('PUT', `/api/scripts/${_editingScriptId}`, { name, subject, body });
    } else {
      await api('POST', '/api/scripts', { name, subject, body });
    }
    closeScriptEditor();
    await loadScriptsList();
    toast('Script saved', 'success');
  } catch(e) {
    toast('Failed to save script: ' + e.message, 'error');
  }
}

async function deleteScript(scriptId) {
  if (!confirm('Delete this script?')) return;
  try {
    await DEL(`/api/scripts/${scriptId}`);
    await loadScriptsList();
    toast('Script deleted', 'success');
  } catch(e) {
    toast('Failed to delete script', 'error');
  }
}

function loadScript(scriptId) {
  if (!scriptId) return;
  const s = _scripts.find(x => x.id === parseInt(scriptId));
  if (!s) return;
  if (s.subject) document.getElementById('email-subject').value = s.subject;
  if (s.body)    document.getElementById('email-body').value    = s.body;
  document.getElementById('script-picker').value = '';
  toast(`Loaded "${s.name}"`, 'success');
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

  // Contact chips (email + website)
  const contactRow = document.getElementById('call-contact-row');
  const emailBtn   = document.getElementById('call-email-btn');
  const webBtn     = document.getElementById('call-website-btn');
  if (contactRow && lead) {
    const email = (lead.emails || [])[0];
    const web   = lead.website;
    contactRow.style.display = (email || web) ? '' : 'none';
    if (emailBtn) { emailBtn.textContent = email ? `✉ ${email}` : ''; emailBtn.style.display = email ? '' : 'none'; }
    if (webBtn)   { webBtn.textContent = web ? `↗ ${web.replace(/^https?:\/\/(www\.)?/, '').split('/')[0]}` : ''; webBtn.style.display = web ? '' : 'none'; }
  }

  // History row
  const histRow = document.getElementById('call-history-row');
  if (histRow && lead) {
    const parts = [];
    if (lead.last_called_at)  parts.push(`Last called: ${formatDate(lead.last_called_at)}`);
    if (lead.last_emailed_at) parts.push(`Last emailed: ${formatDate(lead.last_emailed_at)}`);
    histRow.textContent = parts.join(' · ');
  }

  // Hide sequence suggestion
  const seqSuggest = document.getElementById('call-seq-suggest');
  if (seqSuggest) seqSuggest.style.display = 'none';
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

    // Check for matching auto-trigger sequence
    const trigger  = `call:${outcomeDb}`;
    const matchSeq = _sequences.find(s => s.trigger === trigger && s.active);
    callingState._lastOutcome  = outcomeDb;
    callingState._lastLeadId   = lead.id;
    callingState._suggestSeqId = matchSeq ? matchSeq.id : null;

    callingState.cursor++;
    renderCallCard();

    // Show sequence suggestion on the new card position (before advancing)
    if (matchSeq) {
      const sugg = document.getElementById('call-seq-suggest');
      const text = document.getElementById('call-seq-suggest-text');
      if (sugg && text) {
        text.textContent = `Enroll in "${matchSeq.name}"?`;
        sugg.style.display = '';
      }
    }
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

/* ══════════════════════════════════════════════════════════════════════
   UPDATE MODAL
   ══════════════════════════════════════════════════════════════════════ */

async function checkUpdateSilent() {
  try {
    const data = await GET('/api/update/check');
    if (!data.up_to_date) {
      document.getElementById('update-dot').style.display = 'block';
    }
  } catch (_) { /* silently ignore — no git, no network, etc. */ }
}

async function openUpdateModal() {
  document.getElementById('update-modal').classList.add('open');
  document.getElementById('update-checking').style.display = 'block';
  document.getElementById('update-content').style.display  = 'none';
  document.getElementById('update-error').style.display    = 'none';
  document.getElementById('btn-do-update').style.display   = 'none';
  document.getElementById('update-log-wrap').style.display = 'none';
  document.getElementById('update-log').innerHTML = '';

  try {
    const data = await GET('/api/update/check');
    document.getElementById('update-checking').style.display = 'none';
    document.getElementById('update-content').style.display  = 'block';

    const statusText    = document.getElementById('update-status-text');
    const versionText   = document.getElementById('update-version-text');
    const statusIcon    = document.getElementById('update-status-icon');
    const changelogWrap = document.getElementById('update-changelog-wrap');
    const changelogList = document.getElementById('update-changelog');

    if (data.up_to_date) {
      statusIcon.innerHTML = `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#22c55e" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>`;
      statusText.textContent = 'You\'re up to date';
      statusText.style.color = 'var(--green)';
      versionText.textContent = data.current_version ? `Version ${data.current_version}` : '';
      changelogWrap.style.display = 'none';
      document.getElementById('btn-do-update').style.display = 'none';
      document.getElementById('update-dot').style.display = 'none';
    } else {
      statusIcon.innerHTML = `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#f59e0b" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`;
      statusText.textContent = `${data.commits_behind} update${data.commits_behind !== 1 ? 's' : ''} available`;
      statusText.style.color = 'var(--yellow)';
      versionText.textContent = data.current_version ? `Current: ${data.current_version}` : '';
      document.getElementById('update-dot').style.display = 'block';

      if (data.changelog && data.changelog.length) {
        changelogWrap.style.display = 'block';
        changelogList.innerHTML = data.changelog.map(line => {
          const hash = line.slice(0, 7);
          const msg  = esc(line.slice(8));
          return `<li style="display:flex;gap:8px;align-items:baseline">
            <span style="font-family:var(--mono);font-size:11px;color:var(--text-muted);flex-shrink:0">${hash}</span>
            <span style="font-size:13px">${msg}</span>
          </li>`;
        }).join('');
      }
      document.getElementById('btn-do-update').style.display = 'flex';
    }
  } catch(e) {
    document.getElementById('update-checking').style.display = 'none';
    document.getElementById('update-error').style.display    = 'block';
    document.getElementById('update-error').textContent = 'Could not check for updates: ' + e.message;
  }
}

function closeUpdateModal() {
  document.getElementById('update-modal').classList.remove('open');
}

function runUpdate() {
  const logWrap = document.getElementById('update-log-wrap');
  const logEl   = document.getElementById('update-log');
  const btn     = document.getElementById('btn-do-update');

  logWrap.style.display = 'block';
  logEl.innerHTML = '';
  btn.disabled = true;
  btn.textContent = 'Updating…';

  const appendLine = (text, cls = '') => {
    const d = document.createElement('div');
    d.className = 'update-log-line' + (cls ? ' ' + cls : '');
    d.textContent = text;
    logEl.appendChild(d);
    logEl.scrollTop = logEl.scrollHeight;
  };

  const es = new EventSource('/api/update/run');
  es.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.line) {
      const cls = msg.line.startsWith('ERROR') ? 'err'
                : msg.line.startsWith('Done') || msg.line.startsWith('✓') ? 'ok'
                : '';
      appendLine(msg.line, cls);
    }
    if (msg.done) {
      es.close();
      btn.disabled = false;
      if (msg.success) {
        btn.textContent = 'Restart to apply';
        btn.onclick = () => { closeUpdateModal(); toast('Restart the server to apply the update.', 'success'); };
        document.getElementById('update-dot').style.display = 'none';
        appendLine('Restart the server to apply changes.', 'ok');
      } else {
        btn.textContent = 'Retry';
        btn.onclick = runUpdate;
      }
    }
  };
  es.onerror = () => {
    es.close();
    appendLine('Connection lost.', 'err');
    btn.disabled = false;
    btn.textContent = 'Retry';
    btn.onclick = runUpdate;
  };
}

/* ══════════════════════════════════════════════════════════════════════
   ADD LEAD MODAL
   ══════════════════════════════════════════════════════════════════════ */

function openAddLeadModal() {
  ['al-name','al-website','al-email','al-phone','al-niche','al-city','al-country','al-notes'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  document.getElementById('al-status').value = 'new';
  document.getElementById('add-lead-modal').classList.add('open');
  setTimeout(() => document.getElementById('al-name').focus(), 50);
}

function closeAddLeadModal() {
  document.getElementById('add-lead-modal').classList.remove('open');
}

async function saveNewLead() {
  const name    = document.getElementById('al-name').value.trim();
  const website = document.getElementById('al-website').value.trim();
  if (!name && !website) {
    toast('Company name or website required', 'error');
    document.getElementById('al-name').focus();
    return;
  }
  try {
    await api('POST', '/api/leads', {
      company_name: name,
      website:      website,
      email:        document.getElementById('al-email').value.trim(),
      phone:        document.getElementById('al-phone').value.trim(),
      niche:        document.getElementById('al-niche').value.trim(),
      city:         document.getElementById('al-city').value.trim(),
      country:      document.getElementById('al-country').value.trim(),
      status:       document.getElementById('al-status').value,
      notes:        document.getElementById('al-notes').value.trim(),
    });
    toast(`Lead added: ${name || website}`, 'success');
    closeAddLeadModal();
    loadLeads();
  } catch(e) {
    toast('Failed to add lead: ' + e.message, 'error');
  }
}

/* ══════════════════════════════════════════════════════════════════════
   RESET
   ══════════════════════════════════════════════════════════════════════ */

async function resetAllLeads() {
  const confirmed = confirm(
    'Delete ALL leads, email logs, and activity for the current project?\n\nThis cannot be undone.'
  );
  if (!confirmed) return;
  try {
    const r = await api('DELETE', '/api/leads/all');
    toast(`Cleared ${r.deleted} leads`, 'success');
    closeSettingsModal();
    loadLeads();
  } catch(e) {
    toast('Failed to reset: ' + e.message, 'error');
  }
}

/* ══════════════════════════════════════════════════════════════════════
   SEQUENCES
   ══════════════════════════════════════════════════════════════════════ */

let _sequences  = [];
let _seqTriggers = [];
let _editingSeqId = null;
let _seqSteps     = [];

async function loadSequences() {
  try {
    [_sequences, _seqTriggers] = await Promise.all([
      GET('/api/sequences'),
      GET('/api/sequences/triggers'),
    ]);
    renderSequencesList();
    _populateTriggerDropdown();
  } catch(e) {}
}

function renderSequencesList() {
  const el = document.getElementById('sequences-list');
  if (!el) return;
  if (!_sequences.length) {
    el.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px">No sequences yet. Click New Sequence to create one.</div>';
    return;
  }
  el.innerHTML = _sequences.map(s => {
    const trigLabel = (_seqTriggers.find(t => t.value === s.trigger) || {}).label || s.trigger;
    const steps = s.steps || [];
    return `<div class="script-item">
      <div class="script-item-info">
        <div class="script-item-name" style="display:flex;align-items:center;gap:8px">
          ${esc(s.name)}
          ${s.active ? '' : '<span style="font-size:10px;background:rgba(239,68,68,.15);color:#f87171;padding:2px 6px;border-radius:99px">paused</span>'}
        </div>
        <div class="script-item-subject">${esc(trigLabel)} · ${steps.length} step${steps.length !== 1 ? 's' : ''}</div>
      </div>
      <div class="script-item-actions">
        <button class="btn" onclick="openSeqEditor(${s.id})">Edit</button>
        <button class="btn" onclick="deleteSeq(${s.id})" title="Delete">✕</button>
      </div>
    </div>`;
  }).join('');
}

function _populateTriggerDropdown() {
  const sel = document.getElementById('seq-trigger');
  if (!sel) return;
  sel.innerHTML = _seqTriggers.map(t =>
    `<option value="${esc(t.value)}">${esc(t.label)}</option>`
  ).join('');
}

function openSeqEditor(seqId) {
  _editingSeqId = seqId || null;
  const editor = document.getElementById('seq-editor');
  const title  = document.getElementById('seq-editor-title');
  _populateTriggerDropdown();
  if (seqId) {
    const s = _sequences.find(x => x.id === seqId);
    if (!s) return;
    document.getElementById('seq-name').value = s.name;
    document.getElementById('seq-trigger').value = s.trigger;
    _seqSteps = (s.steps || []).map(st => ({...st}));
    title.textContent = 'Edit Sequence';
  } else {
    document.getElementById('seq-name').value = '';
    document.getElementById('seq-trigger').value = 'manual';
    _seqSteps = [];
    title.textContent = 'New Sequence';
  }
  renderSeqSteps();
  editor.style.display = '';
  document.getElementById('seq-name').focus();
}

function closeSeqEditor() {
  document.getElementById('seq-editor').style.display = 'none';
  _editingSeqId = null;
  _seqSteps = [];
}

function addSeqStep() {
  _seqSteps.push({ delay_days: 3, subject: '', body: '' });
  renderSeqSteps();
}

function removeSeqStep(idx) {
  _seqSteps.splice(idx, 1);
  renderSeqSteps();
}

function renderSeqSteps() {
  const el = document.getElementById('seq-steps');
  if (!el) return;
  if (!_seqSteps.length) {
    el.innerHTML = '<div style="font-size:12px;color:var(--text-muted);padding:8px 0">No steps yet. Click "Add Step" to add an email step.</div>';
    return;
  }
  el.innerHTML = _seqSteps.map((step, i) => `
    <div class="seq-step" data-idx="${i}">
      <div class="seq-step-header">
        <span style="font-size:12px;font-weight:600;color:var(--text-dim)">Step ${i+1}</span>
        <div style="display:flex;align-items:center;gap:8px">
          <label style="font-size:12px;color:var(--text-muted);white-space:nowrap">Wait</label>
          <input type="number" class="seq-delay" min="0" step="0.5" value="${step.delay_days}"
            style="width:60px;padding:4px 6px;font-size:12px"
            oninput="_seqSteps[${i}].delay_days=parseFloat(this.value)||0">
          <label style="font-size:12px;color:var(--text-muted)">days, then send:</label>
          <button class="btn" style="font-size:11px;padding:3px 8px" onclick="removeSeqStep(${i})">✕</button>
        </div>
      </div>
      <div class="field full" style="margin-top:8px">
        <input type="text" class="seq-subject" placeholder="Subject line…" value="${esc(step.subject)}"
          oninput="_seqSteps[${i}].subject=this.value">
      </div>
      <div class="field full" style="margin-top:6px">
        <textarea class="seq-body email-textarea" style="min-height:80px" placeholder="Email body… (merge tags: {{company_name}}, {{first_name}}, etc.)"
          oninput="_seqSteps[${i}].body=this.value">${esc(step.body)}</textarea>
      </div>
    </div>
  `).join('');
}

async function saveSeq() {
  const name    = document.getElementById('seq-name').value.trim();
  const trigger = document.getElementById('seq-trigger').value;
  if (!name) { document.getElementById('seq-name').focus(); return; }
  try {
    const payload = { name, trigger, steps: _seqSteps, active: true };
    if (_editingSeqId) {
      await api('PUT', `/api/sequences/${_editingSeqId}`, payload);
    } else {
      await api('POST', '/api/sequences', payload);
    }
    closeSeqEditor();
    await loadSequences();
    toast('Sequence saved', 'success');
  } catch(e) {
    toast('Failed to save: ' + e.message, 'error');
  }
}

async function deleteSeq(seqId) {
  if (!confirm('Delete this sequence? Enrolled leads will be unenrolled.')) return;
  try {
    await DEL(`/api/sequences/${seqId}`);
    await loadSequences();
    toast('Sequence deleted', 'success');
  } catch(e) {
    toast('Failed to delete', 'error');
  }
}

/* ── Process due sequences ─────────────────────────────────────────── */
async function processSequences() {
  const btn    = document.getElementById('btn-process-sequences');
  const result = document.getElementById('seq-process-result');
  btn.disabled = true;
  result.textContent = 'Processing…';
  try {
    const { job_id } = await api('POST', '/api/sequences/process', {});
    const es = new EventSource(`/api/email/jobs/${job_id}/stream`);
    es.onmessage = e => {
      const msg = JSON.parse(e.data);
      if (msg.type === 'done') {
        es.close();
        btn.disabled = false;
        const c = msg.counts || {};
        result.textContent = `Done — ${c.sent||0} sent, ${c.failed||0} failed, ${c.skipped||0} skipped`;
        result.style.color = 'var(--green)';
        loadLeads();
      }
    };
    es.onerror = () => {
      es.close();
      btn.disabled = false;
      result.textContent = 'Error processing sequences';
      result.style.color = 'var(--red)';
    };
  } catch(e) {
    btn.disabled = false;
    result.textContent = 'Error: ' + e.message;
    result.style.color = 'var(--red)';
  }
}

/* ── Enroll lead in sequence (from detail modal) ───────────────────── */
function openEnrollModal() {
  if (!state.currentDetailId) return;
  const lead = state.leads.find(l => l.id === state.currentDetailId);
  if (!lead) return;
  document.getElementById('enroll-lead-name').textContent =
    lead.company_name || domainName(lead.website);

  const picker = document.getElementById('enroll-seq-picker');
  picker.innerHTML = _sequences.length
    ? _sequences.map(s => `<option value="${s.id}">${esc(s.name)}</option>`).join('')
    : '<option value="">No sequences yet — create one in Settings</option>';

  document.getElementById('enroll-seq-preview').textContent = '';
  picker.onchange = () => _showSeqPreview(parseInt(picker.value));
  _showSeqPreview(parseInt(picker.value));

  document.getElementById('enroll-modal').classList.add('open');
}

function closeEnrollModal() {
  document.getElementById('enroll-modal').classList.remove('open');
}

function _showSeqPreview(seqId) {
  const seq = _sequences.find(s => s.id === seqId);
  const el  = document.getElementById('enroll-seq-preview');
  if (!seq || !el) return;
  const steps = seq.steps || [];
  el.innerHTML = steps.map((s, i) =>
    `Step ${i+1}: wait ${s.delay_days}d → "${esc(s.subject || '(no subject)')}"`
  ).join('<br>');
}

async function confirmEnroll() {
  if (!state.currentDetailId) return;
  const seqId = parseInt(document.getElementById('enroll-seq-picker').value);
  if (!seqId) { toast('No sequence selected', 'error'); return; }
  try {
    await api('POST', `/api/leads/${state.currentDetailId}/enroll`, { sequence_id: seqId });
    const seqName = (_sequences.find(s => s.id === seqId) || {}).name || 'sequence';
    toast(`Enrolled in "${seqName}"`, 'success');
    closeEnrollModal();
  } catch(e) {
    if (e.message.includes('409') || e.message.includes('already enrolled')) {
      toast('Already enrolled in this sequence', 'error');
    } else {
      toast('Failed to enroll: ' + e.message, 'error');
    }
  }
}

/* ══════════════════════════════════════════════════════════════════════
   CALLING — sequence enrollment tracking state
   ══════════════════════════════════════════════════════════════════════ */

callingState._lastLeadId   = null;
callingState._suggestSeqId = null;

async function enrollFromCall() {
  const { _lastLeadId, _suggestSeqId } = callingState;
  if (!_lastLeadId || !_suggestSeqId) return;
  try {
    await api('POST', `/api/leads/${_lastLeadId}/enroll`, { sequence_id: _suggestSeqId });
    const seqName = (_sequences.find(s => s.id === _suggestSeqId) || {}).name || 'sequence';
    toast(`Enrolled in "${seqName}"`, 'success');
  } catch(e) {
    toast('Already enrolled or error', 'error');
  }
  document.getElementById('call-seq-suggest').style.display = 'none';
}

function callQuickEmail() {
  const { queue, cursor } = callingState;
  if (cursor >= queue.length) return;
  const lead = queue[cursor];
  if (!lead.emails?.length) { toast('No email for this lead', 'error'); return; }
  // Close call modal, select lead, open email
  state.selectedIds.clear();
  state.selectedIds.add(lead.id);
  closeCallingModal();
  openEmailModal();
}

function callOpenWebsite() {
  const { queue, cursor } = callingState;
  if (cursor >= queue.length) return;
  const lead = queue[cursor];
  if (lead.website) window.open(lead.website, '_blank');
}

/* ══════════════════════════════════════════════════════════════════════
   TAG FILTER
   ══════════════════════════════════════════════════════════════════════ */

function filterByTag(e, tag) {
  e.stopPropagation();
  const input = document.getElementById('tag-filter');
  if (input) {
    input.value = tag;
    state.tagFilter = tag;
    _updateFilterDot();
    loadLeads();
  }
}

/* ══════════════════════════════════════════════════════════════════════
   DASHBOARD
   ══════════════════════════════════════════════════════════════════════ */

let _dashboardVisible = false;

function toggleDashboard() {
  _dashboardVisible = !_dashboardVisible;
  const panel = document.getElementById('dashboard-panel');
  const btn   = document.getElementById('btn-dashboard');
  panel.style.display = _dashboardVisible ? '' : 'none';
  btn.classList.toggle('active', _dashboardVisible);
  if (_dashboardVisible) loadDashboard();
}

async function loadDashboard() {
  try {
    const d = await GET('/api/dashboard');
    renderDashboard(d);
  } catch(e) {}
}

function renderDashboard(d) {
  // Funnel
  const funnel = d.funnel || {};
  const funnelOrder = ['new', 'contacted', 'warm', 'qualified', 'rejected'];
  const funnelColors = {
    new: 'var(--primary)', contacted: 'var(--blue)', warm: '#fb923c',
    qualified: 'var(--green)', rejected: 'var(--red)'
  };
  const maxVal = Math.max(1, ...funnelOrder.map(k => funnel[k] || 0));
  document.getElementById('dash-funnel').innerHTML = funnelOrder.map(k => {
    const val = funnel[k] || 0;
    const pct = Math.round((val / maxVal) * 100);
    return `<div class="funnel-row">
      <span class="funnel-label">${k}</span>
      <div class="funnel-bar-wrap"><div class="funnel-bar" style="width:${pct}%;background:${funnelColors[k]}"></div></div>
      <span class="funnel-count">${val}</span>
    </div>`;
  }).join('');

  // Email stats
  const em = d.emails || {};
  const openRate = em.sent > 0 ? Math.round((em.opened / em.sent) * 100) : 0;
  document.getElementById('dash-email-stats').innerHTML = `
    <div class="dash-stat-row"><span>Emails sent</span><span class="dash-stat-val">${em.sent || 0}</span></div>
    <div class="dash-stat-row"><span>Opens tracked</span><span class="dash-stat-val">${em.opened || 0}</span></div>
    <div class="dash-stat-row"><span>Open rate</span><span class="dash-stat-val" style="color:var(--green)">${openRate}%</span></div>
    <div class="dash-stat-row"><span>Failed</span><span class="dash-stat-val" style="color:var(--red)">${em.failed || 0}</span></div>
  `;

  // Callbacks
  const due = d.callbacks_due || 0;
  document.getElementById('dash-callbacks').innerHTML = due > 0
    ? `<div style="font-size:32px;font-weight:700;color:var(--red);margin-bottom:6px">${due}</div>
       <div style="font-size:12.5px;color:var(--text-muted)">leads need a callback now</div>
       <button class="btn" style="margin-top:10px" onclick="document.getElementById('toggle-overdue').click()">Show overdue</button>`
    : `<div class="dash-empty">No callbacks due</div>`;
}

/* ── IMAP ────────────────────────────────────────────────────────── */
async function imapStart() {
  const cfg = {
    imap_host:     document.getElementById('s-imap-host')?.value.trim()      || '',
    imap_port:     parseInt(document.getElementById('s-imap-port')?.value)    || 993,
    imap_ssl:      document.getElementById('s-imap-ssl')?.checked             !== false,
    imap_user:     document.getElementById('s-imap-user')?.value.trim()       || '',
    imap_password: document.getElementById('s-imap-password')?.value          || '',
    imap_folder:   document.getElementById('s-imap-folder')?.value.trim()     || 'INBOX',
    imap_interval: parseInt(document.getElementById('s-imap-interval')?.value)|| 10,
  };
  if (!cfg.imap_host || !cfg.imap_user) {
    toast('Enter IMAP host and username first', 'error');
    return;
  }
  try {
    await api('POST', '/api/imap/start', cfg);
    toast('IMAP polling started', 'success');
    const lbl = document.getElementById('imap-status-label');
    if (lbl) lbl.textContent = 'Running…';
  } catch(e) {
    toast('Failed to start IMAP: ' + e.message, 'error');
  }
}

async function imapStop() {
  try {
    await api('POST', '/api/imap/stop', {});
    toast('IMAP polling stopped', 'success');
    const lbl = document.getElementById('imap-status-label');
    if (lbl) lbl.textContent = 'Not running';
  } catch(e) {
    toast('Failed to stop IMAP: ' + e.message, 'error');
  }
}

/* ── Webhooks ────────────────────────────────────────────────────── */
let _whEditId = null;

async function loadWebhooks() {
  try {
    const hooks = await GET('/api/webhooks');
    renderWebhooksList(hooks);
  } catch {}
}

function renderWebhooksList(hooks) {
  const el = document.getElementById('webhooks-list');
  if (!el) return;
  if (!hooks || hooks.length === 0) {
    el.innerHTML = '<div class="dash-empty" style="padding:20px 0">No webhooks configured</div>';
    return;
  }
  el.innerHTML = hooks.map(h => `
    <div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)">
      <div style="flex:1;min-width:0">
        <div style="font-size:13px;font-weight:500;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(h.url)}">${esc(h.url)}</div>
        <div style="font-size:11px;color:var(--text-muted);margin-top:2px">${esc(h.event)}</div>
      </div>
      <button class="btn" style="padding:4px 10px;font-size:11px" onclick="openWebhookEditor(${h.id})">Edit</button>
      <button class="btn danger" style="padding:4px 10px;font-size:11px" onclick="deleteWebhook(${h.id})">✕</button>
    </div>
  `).join('');
}

function openWebhookEditor(id) {
  _whEditId = id || null;
  const editor = document.getElementById('webhook-editor');
  if (!editor) return;
  editor.style.display = '';
  if (id) {
    // Populate from existing hook in list — re-fetch for simplicity
    GET('/api/webhooks').then(hooks => {
      const h = hooks.find(x => x.id === id);
      if (h) {
        document.getElementById('wh-url').value   = h.url   || '';
        document.getElementById('wh-event').value = h.event || 'status_changed';
      }
    }).catch(() => {});
  } else {
    document.getElementById('wh-url').value   = '';
    document.getElementById('wh-event').value = 'status_changed';
  }
  document.getElementById('wh-url').focus();
}

function closeWebhookEditor() {
  _whEditId = null;
  const editor = document.getElementById('webhook-editor');
  if (editor) editor.style.display = 'none';
}

async function saveWebhook() {
  const url   = document.getElementById('wh-url')?.value.trim();
  const event = document.getElementById('wh-event')?.value || 'status_changed';
  if (!url) { toast('Enter a URL', 'error'); return; }
  try {
    if (_whEditId) {
      await api('PUT', `/api/webhooks/${_whEditId}`, { url, event });
    } else {
      await api('POST', '/api/webhooks', { url, event });
    }
    closeWebhookEditor();
    await loadWebhooks();
    toast('Webhook saved', 'success');
  } catch(e) {
    toast('Failed to save webhook: ' + e.message, 'error');
  }
}

async function deleteWebhook(id) {
  try {
    await DEL(`/api/webhooks/${id}`);
    await loadWebhooks();
    toast('Webhook deleted', 'success');
  } catch(e) {
    toast('Failed to delete webhook', 'error');
  }
}

/* ── Load sequences silently on startup (needed for calling modal) ─── */
window.addEventListener('load', () => { loadSequences(); });
