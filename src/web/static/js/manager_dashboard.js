// Manager Dashboard -- loads fast data first, health async.

(function () {
    'use strict';

    let scoreboardData = {};
    let attentionItems = [];

    // --- Formatting helpers ---
    function fmtDollars(n) {
        if (n == null) return '$0';
        return '$' + Number(n).toLocaleString('en-US', {
            minimumFractionDigits: 0, maximumFractionDigits: 0
        });
    }

    function entityUrl(item) {
        return '/ae/my-accounts?ae='
            + encodeURIComponent(item.assigned_ae);
    }

    // --- Scoreboard ---
    function renderScoreboard() {
        var el = document.getElementById('scoreboard-container');
        if (!AE_LIST.length) {
            el.innerHTML = '<p class="zero-state">No AEs found.</p>';
            return;
        }
        var metrics = [
            { key: 'account_count', label: 'Active Accounts' },
            { key: 'revenue_at_risk', label: 'Revenue at Risk', fmt: fmtDollars },
            { key: 'unworked_signals_7d', label: 'Unworked Signals (>7d)' },
            { key: 'open_followups', label: 'Open Follow-ups' },
            { key: 'overdue_followups', label: 'Overdue Follow-ups' },
            { key: 'avg_health', label: 'Avg Health Score', async: true },
            {
                key: 'touch_compliance', label: 'Touch Compliance', async: true,
                fmt: function (v) { return v != null ? v + '%' : ''; }
            },
        ];

        var html = '<table class="scoreboard-table"><thead><tr>';
        html += '<th>Metric</th>';
        AE_LIST.forEach(function (ae) {
            html += '<th class="ae-col">' + ae + '</th>';
        });
        html += '</tr></thead><tbody>';

        metrics.forEach(function (m) {
            html += '<tr><td class="metric-label">' + m.label + '</td>';
            AE_LIST.forEach(function (ae) {
                var stats = scoreboardData[ae] || {};
                var val = stats[m.key];
                var display;
                if (m.async && val == null) {
                    display = '<span class="loading-placeholder">...</span>';
                } else {
                    display = m.fmt ? m.fmt(val) : (val != null ? val : '0');
                }
                var cls = 'ae-value';
                if ((m.key === 'overdue_followups' || m.key === 'unworked_signals_7d')
                    && val > 0) {
                    cls += ' text-red';
                }
                html += '<td class="' + cls + '">' + display + '</td>';
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        el.innerHTML = html;
    }

    // --- Attention Required ---
    function renderAttention() {
        var el = document.getElementById('attention-container');
        if (!attentionItems.length) {
            el.innerHTML = '<p class="zero-state">No items requiring attention.</p>';
            return;
        }
        var html = '<table class="attention-table"><thead><tr>';
        html += '<th>Type</th><th>Account</th><th>Signal</th>';
        html += '<th>Amount</th><th>Days</th><th>AE</th>';
        html += '</tr></thead><tbody>';

        attentionItems.forEach(function (item) {
            var badge, daysLabel, daysVal;
            if (item.item_type === 'unworked_signal') {
                badge = '<span class="badge-signal badge-unworked">Unworked</span>';
                daysLabel = item.days_aging + 'd aging';
                daysVal = item.days_aging;
            } else if (item.item_type === 'renewal_gap_stale') {
                badge = '<span class="badge-signal badge-renewal">Renewal Gap</span>';
                daysLabel = 'No touch 14d+';
                daysVal = 14;
            } else if (item.item_type === 'a_tier_overdue') {
                badge = '<span class="badge-signal badge-atier">A-Tier Overdue</span>';
                daysLabel = (item.days_since_touch || '?') + 'd since touch';
                daysVal = item.days_since_touch || 0;
            } else {
                badge = '<span class="badge-signal">' + item.item_type + '</span>';
                daysLabel = '';
                daysVal = 0;
            }

            var daysClass = daysVal > 7 ? 'text-red' : (daysVal > 3 ? 'text-yellow' : '');

            html += '<tr>';
            html += '<td>' + badge + '</td>';
            html += '<td><a class="entity-link" href="' + entityUrl(item) + '">'
                + (item.entity_name || 'Unknown') + '</a></td>';
            html += '<td>' + (item.signal_type || item.signal_label || '-') + '</td>';
            html += '<td>' + fmtDollars(item.trailing_revenue) + '</td>';
            html += '<td class="' + daysClass + '">' + daysLabel + '</td>';
            html += '<td>' + (item.assigned_ae || '-') + '</td>';
            html += '</tr>';
        });

        html += '</tbody></table>';
        el.innerHTML = html;
    }

    // --- Weekly Activity ---
    function renderActivity(data) {
        var el = document.getElementById('activity-container');
        if (!AE_LIST.length) {
            el.innerHTML = '<p class="zero-state">No AEs found.</p>';
            return;
        }
        var types = ['call', 'email', 'meeting', 'note', 'follow_up', 'total'];
        var typeLabels = {
            call: 'Calls', email: 'Emails', meeting: 'Meetings',
            note: 'Notes', follow_up: 'Follow-ups', total: 'Total'
        };
        var html = '<table class="activity-table"><thead><tr>';
        html += '<th>AE</th>';
        types.forEach(function (t) {
            html += '<th>' + typeLabels[t] + '</th>';
        });
        html += '</tr></thead><tbody>';

        AE_LIST.forEach(function (ae) {
            var counts = data[ae] || {};
            html += '<tr><td style="font-weight:600;">' + ae + '</td>';
            types.forEach(function (t) {
                var v = counts[t] || 0;
                var cls = '';
                if (t === 'total' && v === 0) cls = 'text-red';
                html += '<td class="' + cls + '">' + v + '</td>';
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        el.innerHTML = html;
    }

    // --- Data loading ---
    async function loadFastData() {
        try {
            var responses = await Promise.all([
                fetch('/api/manager/scoreboard'),
                fetch('/api/manager/attention'),
                fetch('/api/manager/weekly-activity'),
            ]);
            scoreboardData = await responses[0].json();
            attentionItems = await responses[1].json();
            var activityData = await responses[2].json();

            renderScoreboard();
            renderAttention();
            renderActivity(activityData);

            loadHealthData();
        } catch (err) {
            console.error('Failed to load dashboard data:', err);
        }
    }

    async function loadHealthData() {
        try {
            var resp = await fetch('/api/manager/health-summary');
            var data = await resp.json();

            AE_LIST.forEach(function (ae) {
                if (data.ae_health && data.ae_health[ae]) {
                    scoreboardData[ae] = scoreboardData[ae] || {};
                    scoreboardData[ae].avg_health = data.ae_health[ae].avg_health;
                    scoreboardData[ae].touch_compliance = data.ae_health[ae].touch_compliance;
                }
            });
            renderScoreboard();

            if (data.a_tier_overdue && data.a_tier_overdue.length) {
                attentionItems = attentionItems.concat(data.a_tier_overdue);
                attentionItems.sort(function (a, b) {
                    return (b.trailing_revenue || 0) - (a.trailing_revenue || 0);
                });
                renderAttention();
            }
        } catch (err) {
            console.error('Failed to load health data:', err);
        }
    }

    // --- Init ---
    document.addEventListener('DOMContentLoaded', loadFastData);
})();
