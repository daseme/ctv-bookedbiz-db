/* AE My Accounts — CRM page client logic */

let allAccounts = [];
let currentSort = { key: 'health_score', dir: 'asc' };
let currentEntity = null;
let revenueChart = null;

// ── Init ──────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadAccounts();
    loadSignalQueue();
    loadActionItems();
    loadRecentActivity();

    document.getElementById('account-search')
        .addEventListener('input', filterAccounts);

    document.querySelectorAll('.accounts-table th[data-sort]')
        .forEach(th => th.addEventListener('click', () => {
            sortAccounts(th.dataset.sort);
        }));

    document.addEventListener('keydown', e => {
        if (e.key === 'Escape') {
            const guide = document.getElementById('guide-modal');
            if (guide.classList.contains('active')) {
                guide.classList.remove('active');
            } else {
                closeDetail();
            }
        }
    });

    document.getElementById('guide-modal')
        .addEventListener('click', e => {
            if (e.target.classList.contains('modal-overlay')) {
                e.target.classList.remove('active');
            }
        });

    // Restore last open panel
    const saved = sessionStorage.getItem('crm_open_entity');
    if (saved) {
        try {
            const { type, id } = JSON.parse(saved);
            openDetail(type, id);
        } catch { /* ignore */ }
    }
});

// ── AE Selector ───────────────────────────────────────────────────
function switchAe(ae) {
    const url = new URL(window.location);
    if (ae) {
        url.searchParams.set('ae', ae);
    } else {
        url.searchParams.delete('ae');
    }
    window.location = url;
}

// ── Stats ─────────────────────────────────────────────────────────
async function loadStats() {
    try {
        const resp = await fetch('/api/ae/my-accounts/stats' +
            window.location.search);
        const stats = await resp.json();
        document.getElementById('stat-accounts')
            .textContent = stats.account_count;
        document.getElementById('stat-revenue')
            .textContent = '$' + Math.round(
                stats.trailing_revenue).toLocaleString();
        document.getElementById('stat-signals')
            .textContent = stats.signal_count;
        document.getElementById('stat-followups')
            .textContent = stats.follow_up_count;
        document.getElementById('stat-at-risk').textContent =
            '$' + (stats.revenue_at_risk || 0).toLocaleString();
        if (stats.overdue_count > 0) {
            document.getElementById('stat-overdue')
                .textContent = stats.overdue_count;
            document.getElementById('stat-overdue-label')
                .style.display = 'inline';
            document.getElementById('card-followups')
                .classList.add('has-warning');
        }
    } catch (err) {
        console.error('Failed to load stats:', err);
    }
}

// ── Accounts ──────────────────────────────────────────────────────
async function loadAccounts() {
    try {
        const qs = window.location.search;
        const resp = await fetch('/api/ae/my-accounts' + qs);
        allAccounts = await resp.json();
        renderAccounts(allAccounts);

        // Load health data in background — don't block table render
        loadHealthData(qs);
    } catch (err) {
        console.error('Failed to load accounts:', err);
    }
}

async function loadHealthData(qs) {
    try {
        const resp = await fetch('/api/ae/my-accounts/health' + qs);
        const healthData = await resp.json();

        const healthMap = {};
        healthData.forEach(h => {
            healthMap[`${h.entity_type}:${h.entity_id}`] = h;
        });

        let withinCadence = 0;
        allAccounts.forEach(a => {
            const h = healthMap[`${a.entity_type}:${a.entity_id}`];
            if (h) {
                a.health_score = h.health_score;
                a.health_color = h.health_color;
                a.tier = h.tier;
                a.tier_cadence_days = h.tier_cadence_days;
                a.days_since_touch = h.days_since_touch;
                a.touch_status = h.touch_status;
                if (h.touch_status === 'green' || h.touch_status === 'yellow')
                    withinCadence++;
            }
        });

        renderAccounts(allAccounts);

        // Update touch compliance stat
        const pct = allAccounts.length
            ? Math.round(withinCadence / allAccounts.length * 100) : 0;
        document.getElementById('stat-compliance').textContent = pct + '%';
    } catch (err) {
        console.error('Failed to load health data:', err);
    }
}

function renderAccounts(accounts) {
    const tbody = document.getElementById('accounts-tbody');
    const empty = document.getElementById('accounts-empty');
    const count = document.getElementById('accounts-count');
    count.textContent = `(${accounts.length})`;

    if (accounts.length === 0) {
        tbody.innerHTML = '';
        empty.style.display = 'block';
        return;
    }
    empty.style.display = 'none';

    tbody.innerHTML = accounts.map(a => `
        <tr>
            <td>
                <span class="account-name"
                      onclick="openDetail('${a.entity_type}', ${a.entity_id})">
                    ${esc(a.entity_name)}
                </span>
            </td>
            <td><span class="badge badge-${a.entity_type}">
                ${a.entity_type}</span></td>
            <td style="text-align:center;">
                ${a.health_score != null
                    ? `<span class="health-dot ${a.health_color}"
                             title="Health: ${a.health_score}/100">
                           ${a.health_score}
                       </span>`
                    : '&mdash;'}
            </td>
            <td style="text-align:center;">
                ${a.tier
                    ? `<span class="tier-badge tier-${a.tier}">${a.tier}</span>`
                    : '&mdash;'}
            </td>
            <td>${a.signal_label
                ? `<span class="badge badge-${a.signal_type}">${esc(a.signal_label)}</span>`
                : '<span style="color:#cbd5e0;">&mdash;</span>'}</td>
            <td style="text-align:right;">${a.trailing_revenue
                ? '$' + Math.round(a.trailing_revenue).toLocaleString()
                : '&mdash;'}</td>
            <td class="touch-status ${a.touch_status ? 'touch-' + a.touch_status : ''}">
                ${a.days_since_touch != null
                    ? a.days_since_touch + 'd'
                    : 'Never'}
            </td>
            <td>${a.next_follow_up_date
                ? `${formatDate(a.next_follow_up_date)}`
                : '&mdash;'}</td>
            <td>
                <button class="badge badge-call"
                        style="cursor:pointer; border:none;"
                        onclick="openDetail('${a.entity_type}', ${a.entity_id})">
                    + Log
                </button>
            </td>
        </tr>
    `).join('');
}

function filterAccounts() {
    const q = document.getElementById('account-search')
        .value.toLowerCase();
    const filtered = allAccounts.filter(
        a => a.entity_name.toLowerCase().includes(q)
    );
    renderAccounts(filtered);
}

function sortAccounts(key) {
    if (currentSort.key === key) {
        currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort = { key, dir: 'asc' };
    }
    allAccounts.sort((a, b) => {
        let va = a[key], vb = b[key];
        const numKeys = ['signal_priority', 'health_score', 'days_since_touch'];
        if (va == null) va = numKeys.includes(key) ? 999 : '';
        if (vb == null) vb = numKeys.includes(key) ? 999 : '';
        if (va < vb) return currentSort.dir === 'asc' ? -1 : 1;
        if (va > vb) return currentSort.dir === 'asc' ? 1 : -1;
        return 0;
    });
    renderAccounts(allAccounts);
}

// ── Action Items ──────────────────────────────────────────────────
async function loadActionItems() {
    try {
        let url = '/api/address-book/follow-ups';
        if (CRM_AE_NAME && CRM_AE_NAME !== 'All AEs') {
            url += `?ae=${encodeURIComponent(CRM_AE_NAME)}`;
        }

        const resp = await fetch(url);
        const items = await resp.json();
        const today = todayISO();
        const actionable = items.filter(
            i => !i.is_completed &&
                 i.due_date && i.due_date <= today
        ).map(i => ({
            ...i,
            urgency: i.due_date < today ? 'overdue' : 'due-today'
        }));

        const container = document.getElementById('action-items');
        const list = document.getElementById('action-items-list');

        if (actionable.length === 0) {
            container.style.display = 'none';
            return;
        }
        container.style.display = 'block';

        list.innerHTML = actionable.map(item => `
            <div class="action-item-row">
                <span class="urgency-${item.urgency}">
                    ${item.urgency === 'overdue' ? 'OVERDUE' : 'TODAY'}
                </span>
                <span class="account-name"
                      onclick="openDetail('${item.entity_type}', ${item.entity_id})">
                    ${esc(item.entity_name || 'Unknown')}
                </span>
                <span style="flex:1; color:#4a5568;">
                    ${esc(item.description || '')}
                </span>
                <span style="color:#718096; font-size:13px;">
                    ${item.due_date}
                </span>
                <button class="badge badge-email"
                        style="cursor:pointer; border:none;"
                        onclick="completeFollowUp(${item.activity_id}, this)">
                    Complete
                </button>
            </div>
        `).join('');
    } catch (err) {
        console.error('Failed to load action items:', err);
    }
}

async function completeFollowUp(activityId, btn) {
    try {
        const resp = await fetch(
            `/api/address-book/activities/${activityId}/complete`,
            { method: 'POST' }
        );
        if (resp.ok) {
            btn.closest('.action-item-row').remove();
            loadStats();
            if (currentEntity) loadActivityTab();
        }
    } catch (err) {
        console.error('Failed to complete follow-up:', err);
    }
}

// ── Signal Queue ──────────────────────────────────────────────────
async function loadSignalQueue() {
    try {
        const qs = window.location.search;
        const resp = await fetch(
            `/api/ae/my-accounts/signal-queue${qs}`);
        const items = await resp.json();
        const container = document.getElementById('signal-queue');
        const list = document.getElementById('signal-queue-list');
        const count = document.getElementById('signal-queue-count');

        if (!items.length) {
            container.style.display = 'none';
            return;
        }

        container.style.display = '';
        count.textContent = `(${items.length})`;
        list.innerHTML = items.map(renderSignalItem).join('');
    } catch (err) {
        console.error('Failed to load signal queue:', err);
    }
}

function agingClass(days) {
    if (days >= 8) return 'critical';
    if (days >= 4) return 'warning';
    return 'normal';
}

function renderSignalItem(item) {
    return `
        <div class="signal-item">
            <span class="signal-aging ${agingClass(item.days_aging)}">
                ${item.days_aging}d
            </span>
            <span class="signal-badge ${item.signal_type}">
                ${esc(item.signal_label)}
            </span>
            <div class="signal-info">
                <span class="entity-link"
                      onclick="openDetail('${item.entity_type}', ${item.entity_id})">
                    ${esc(item.entity_name)}
                </span>
                <div class="signal-label">
                    ${esc(item.signal_detail || '')}
                </div>
            </div>
            <div class="signal-actions">
                <button onclick="snoozeSignal(${item.action_id})">
                    Snooze
                </button>
                <button onclick="dismissSignal(${item.action_id})">
                    Dismiss
                </button>
            </div>
        </div>
        <div class="snooze-form" id="snooze-form-${item.action_id}">
            <input type="date" id="snooze-date-${item.action_id}">
            <textarea id="snooze-reason-${item.action_id}"
                      placeholder="Reason (e.g., seasonal pattern)">
            </textarea>
            <div class="snooze-form-actions">
                <button onclick="submitSnooze(${item.action_id})">
                    Confirm Snooze
                </button>
                <button onclick="snoozeSignal(${item.action_id})">
                    Cancel
                </button>
            </div>
        </div>`;
}

function snoozeSignal(actionId) {
    const form = document.getElementById(`snooze-form-${actionId}`);
    form.style.display = form.style.display === 'none'
        ? 'block' : 'none';
}

async function submitSnooze(actionId) {
    const reason = document.getElementById(
        `snooze-reason-${actionId}`).value.trim();
    const snoozeUntil = document.getElementById(
        `snooze-date-${actionId}`).value;
    if (!snoozeUntil) return;

    try {
        await fetch(
            `/api/ae/my-accounts/signal-queue/${actionId}/snooze`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    reason,
                    snooze_until: snoozeUntil,
                }),
            }
        );
        loadSignalQueue();
    } catch (err) {
        console.error('Failed to snooze signal:', err);
    }
}

async function dismissSignal(actionId) {
    const reason = prompt('Why is this not actionable?');
    if (reason === null) return;

    try {
        await fetch(
            `/api/ae/my-accounts/signal-queue/${actionId}/dismiss`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ reason }),
            }
        );
        loadSignalQueue();
    } catch (err) {
        console.error('Failed to dismiss signal:', err);
    }
}

// ── Detail Panel ──────────────────────────────────────────────────
function openDetail(entityType, entityId) {
    currentEntity = { type: entityType, id: entityId };
    sessionStorage.setItem('crm_open_entity',
        JSON.stringify(currentEntity));

    const account = allAccounts.find(
        a => a.entity_type === entityType && a.entity_id === entityId
    );

    document.getElementById('detail-name')
        .textContent = account ? account.entity_name : '';

    const badges = document.getElementById('detail-badges');
    let badgeHtml = `<span class="badge badge-${entityType}">
        ${entityType}</span> `;
    if (account && account.signal_label) {
        badgeHtml += `<span class="badge badge-${account.signal_type}">
            ${esc(account.signal_label)}</span>`;
    }
    badges.innerHTML = badgeHtml;

    const link = document.getElementById('detail-link');
    if (entityType === 'customer') {
        link.href = `/reports/customer-detail/${entityId}`;
        link.style.display = 'inline';
    } else {
        link.style.display = 'none';
    }

    document.getElementById('detail-panel').classList.add('open');
    switchTab('activity');
}

function closeDetail() {
    document.getElementById('detail-panel').classList.remove('open');
    currentEntity = null;
    sessionStorage.removeItem('crm_open_entity');
}

function switchTab(tab) {
    document.querySelectorAll('.detail-tab').forEach(t => {
        t.classList.toggle('active', t.dataset.tab === tab);
    });
    if (tab === 'activity') loadActivityTab();
    else if (tab === 'info') loadInfoTab();
    else if (tab === 'revenue') loadRevenueTab();
}

// ── Activity Tab ──────────────────────────────────────────────────
async function loadActivityTab() {
    if (!currentEntity) return;
    const body = document.getElementById('detail-body');
    const { type, id } = currentEntity;

    body.innerHTML = `
        <div class="activity-form" id="activity-form">
            <select id="act-type">
                <option value="note">Note</option>
                <option value="call">Call</option>
                <option value="email">Email</option>
                <option value="meeting">Meeting</option>
            </select>
            <input type="text" id="act-desc"
                   placeholder="What happened?">
            <button onclick="submitActivity()">Log</button>
            <div class="follow-up-toggle"
                 onclick="toggleFollowUpField()">
                + Add follow-up date
            </div>
            <input type="date" id="act-due" style="display:none;">
        </div>
        <div id="activity-timeline">Loading...</div>
    `;

    try {
        const resp = await fetch(
            `/api/address-book/${type}/${id}/activities?limit=50`
        );
        const activities = await resp.json();
        const timeline = document.getElementById('activity-timeline');

        if (activities.length === 0) {
            timeline.innerHTML = `<div class="empty-state">
                <p>No activity yet. Log your first note or call.</p>
            </div>`;
            return;
        }

        timeline.innerHTML = activities.map(a => `
            <div class="timeline-entry">
                <div class="timeline-meta">
                    <span class="badge badge-${a.activity_type}">
                        ${a.activity_type}</span>
                    ${formatDate(a.activity_date)}
                    ${a.created_by
                        ? `&middot; ${esc(a.created_by)}` : ''}
                    ${a.activity_type === 'follow_up' && a.due_date
                        ? `&middot; Due: ${a.due_date}
                           ${a.is_completed
                               ? '<span style="color:#38a169;">&#10003;</span>'
                               : ''}` : ''}
                </div>
                <div class="timeline-desc">
                    ${esc(a.description || '')}
                </div>
            </div>
        `).join('');
    } catch (err) {
        document.getElementById('activity-timeline')
            .innerHTML = '<p style="color:#e53e3e;">Failed to load.</p>';
    }
}

function toggleFollowUpField() {
    const input = document.getElementById('act-due');
    input.style.display = input.style.display === 'none'
        ? 'inline-block' : 'none';
}

async function submitActivity() {
    if (!currentEntity) return;
    const { type, id } = currentEntity;
    const actType = document.getElementById('act-type').value;
    const desc = document.getElementById('act-desc').value.trim();
    if (!desc) return;

    const dueDate = document.getElementById('act-due').value || null;
    const finalType = dueDate ? 'follow_up' : actType;

    try {
        const resp = await fetch(
            `/api/address-book/${type}/${id}/activities`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    activity_type: finalType,
                    description: desc,
                    due_date: dueDate,
                }),
            }
        );
        if (resp.ok) {
            document.getElementById('act-desc').value = '';
            document.getElementById('act-due').value = '';
            document.getElementById('act-due').style.display = 'none';
            loadActivityTab();
            loadRecentActivity();
            loadStats();
            loadAccounts();
        }
    } catch (err) {
        console.error('Failed to submit activity:', err);
    }
}

// ── Info Tab ──────────────────────────────────────────────────────
async function loadInfoTab() {
    if (!currentEntity) return;
    const body = document.getElementById('detail-body');
    const { type, id } = currentEntity;

    body.innerHTML = 'Loading...';
    try {
        const resp = await fetch(`/api/address-book/${type}/${id}`);
        const data = await resp.json();

        let html = '<div style="font-size:14px;">';

        // Primary contact
        const pc = (data.contacts || []).find(c => c.is_primary);
        if (pc) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Primary Contact</strong><br>
                ${esc(pc.contact_name || '')}
                ${pc.phone ? `<br><a href="tel:${pc.phone}">${esc(pc.phone)}</a>` : ''}
                ${pc.email ? `<br><a href="mailto:${pc.email}">${esc(pc.email)}</a>` : ''}
            </div>`;
        }

        // Sectors
        if (data.sectors && data.sectors.length) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Sectors</strong><br>
                ${data.sectors.map(s => esc(s.sector_name || s)).join(', ')}
            </div>`;
        }

        // Agency
        if (data.agency_name) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Agency</strong><br>
                ${esc(data.agency_name)}
            </div>`;
        }

        // Markets
        if (data.markets) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Markets</strong><br>
                ${esc(data.markets)}
            </div>`;
        }

        // AE
        if (data.assigned_ae) {
            html += `<div>
                <strong style="color:#718096;">Assigned AE</strong><br>
                ${esc(data.assigned_ae)}
            </div>`;
        }

        html += '</div>';
        body.innerHTML = html;
    } catch (err) {
        body.innerHTML = '<p style="color:#e53e3e;">Failed to load.</p>';
    }
}

// ── Revenue Tab ───────────────────────────────────────────────────
async function loadRevenueTab() {
    if (!currentEntity) return;
    const body = document.getElementById('detail-body');
    const { type, id } = currentEntity;

    body.innerHTML = `<div class="revenue-chart-container">
        <canvas id="revenue-canvas"></canvas>
    </div>`;

    try {
        const resp = await fetch(
            `/api/ae/my-accounts/${type}/${id}/revenue-trend`
        );
        const data = await resp.json();

        if (data.length === 0) {
            body.innerHTML = `<div class="empty-state">
                <p>No revenue data yet.</p></div>`;
            return;
        }

        const ctx = document.getElementById('revenue-canvas')
            .getContext('2d');
        if (revenueChart) revenueChart.destroy();

        revenueChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.broadcast_month),
                datasets: [{
                    label: 'Revenue',
                    data: data.map(d => d.revenue),
                    backgroundColor: '#4299e1',
                    borderRadius: 3,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: ctx =>
                                '$' + ctx.parsed.y.toLocaleString(),
                        },
                    },
                },
                scales: {
                    y: {
                        ticks: {
                            callback: v =>
                                '$' + (v / 1000).toFixed(0) + 'k',
                        },
                    },
                },
            },
        });
    } catch (err) {
        body.innerHTML = '<p style="color:#e53e3e;">Failed to load.</p>';
    }
}

// ── Recent Activity Feed ──────────────────────────────────────────
async function loadRecentActivity() {
    const qs = new URLSearchParams(window.location.search);
    const ae = qs.get('ae') || '';
    let url = '/api/ae/my-accounts/recent-activity' +
        window.location.search;

    try {
        const resp = await fetch(url);
        const activities = await resp.json();
        const container = document.getElementById('recent-activity-list');

        if (activities.length === 0) {
            container.innerHTML = `<div class="empty-state">
                <p>No activity yet. Log your first note or call.</p>
            </div>`;
            return;
        }

        container.innerHTML = activities.map(a => `
            <div class="timeline-entry">
                <div class="timeline-meta">
                    <span class="badge badge-${a.activity_type}">
                        ${a.activity_type}</span>
                    ${formatDate(a.activity_date)}
                    &middot;
                    <span class="account-name"
                          onclick="openDetail('${a.entity_type}', ${a.entity_id})">
                        ${esc(a.entity_name || 'Unknown')}
                    </span>
                </div>
                <div class="timeline-desc">
                    ${esc(a.description || '')}
                </div>
            </div>
        `).join('');
    } catch (err) {
        console.error('Failed to load recent activity:', err);
    }
}

// ── Utilities ─────────────────────────────────────────────────────
function esc(str) {
    const div = document.createElement('div');
    div.textContent = str || '';
    return div.innerHTML;
}

function formatDate(dt) {
    if (!dt) return '';
    const d = new Date(dt);
    if (isNaN(d)) return dt;
    return d.toLocaleDateString('en-US', {
        month: 'short', day: 'numeric',
    });
}

function todayISO() {
    return new Date().toISOString().split('T')[0];
}
