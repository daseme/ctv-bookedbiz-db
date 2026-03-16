(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const fmt = (n) => '$' + Math.round(n).toLocaleString();

  const WORLDLINK_ID = 5;
  const CLASS_CYCLE = ['regular', 'irregular', 'political'];
  const CLASS_LABELS = { regular: 'Regular', irregular: 'Irregular', political: 'Political' };

  let chart = null;
  let allCustomers = [];
  let allSectors = [];
  let sortCol = 'name';
  let sortAsc = true;

  const yearSel = $('#year-select');
  const sectorSel = $('#sector-filter');
  const aeSel = $('#ae-filter');
  const classSel = $('#class-filter');
  const searchInput = $('#search-input');

  function currentYear() {
    return parseInt(yearSel.value, 10);
  }

  function filterParams() {
    const p = new URLSearchParams({ year: currentYear() });
    if (sectorSel.value) p.set('sector_id', sectorSel.value);
    if (aeSel.value) p.set('ae', aeSel.value);
    if (classSel.value) p.set('classification', classSel.value);
    return p.toString();
  }

  async function loadData() {
    const [summaryRes, customersRes] = await Promise.all([
      fetch('/api/revenue-classification/summary?' + filterParams()),
      fetch('/api/revenue-classification/customers?' + filterParams()),
    ]);

    const summary = await summaryRes.json();
    const customers = await customersRes.json();

    renderSummary(summary);
    renderChart(summary.monthly);
    populateYears(summary.available_years);
    if (summary.sectors) allSectors = summary.sectors;
    allCustomers = customers;
    populateFilterDropdowns(customers);
    renderTable();
  }

  function renderSummary(s) {
    $('#regular-total').textContent = fmt(s.regular_total);
    $('#irregular-total').textContent = fmt(s.irregular_total);
    $('#political-total').textContent = fmt(s.political_total);
    $('#regular-pct').textContent = s.regular_pct.toFixed(1) + '%';
    $('#unclassified-count').textContent = s.unclassified_count;
  }

  function populateYears(years) {
    const isInitial = yearSel.options.length <= 1 || yearSel.options[0].text === 'Loading...';
    const cur = isInitial ? null : parseInt(yearSel.value, 10);
    if (isInitial) {
      yearSel.innerHTML = '';
      const now = new Date().getFullYear();
      const defaultYear = years.includes(now) ? now : years[years.length - 1];
      years.forEach((y) => {
        const opt = new Option(y, y);
        if (y === defaultYear) opt.selected = true;
        yearSel.appendChild(opt);
      });
    } else if (cur && years.includes(cur)) {
      yearSel.value = cur;
    }
    const yr = currentYear();
    const thCur = $('#th-current-year');
    const thPrior = $('#th-prior-year');
    if (thCur) thCur.firstChild.textContent = yr + ' Revenue ';
    if (thPrior) thPrior.firstChild.textContent = (yr - 1) + ' Revenue ';
  }

  function populateFilterDropdowns(customers) {
    const curSector = sectorSel.value;
    const curAe = aeSel.value;

    const aes = [...new Set(customers.map((c) => c.assigned_ae).filter(Boolean))].sort();

    if (sectorSel.options.length <= 1 && allSectors.length) {
      const groupOrder = ['Commercial','Financial','Healthcare','Outreach','Political','Other'];
      const groups = {};
      allSectors.forEach((s) => {
        const g = s.sector_group || 'Other';
        if (!groups[g]) groups[g] = [];
        groups[g].push(s);
      });
      groupOrder.forEach((g) => {
        if (!groups[g] || groups[g].length === 0) return;
        const og = document.createElement('optgroup');
        og.label = g;
        groups[g].forEach((s) => og.appendChild(new Option(s.sector_name, s.sector_id)));
        sectorSel.appendChild(og);
      });
    }
    if (aeSel.options.length <= 1) {
      aes.forEach((a) => aeSel.appendChild(new Option(a, a)));
    }

    if (curSector) sectorSel.value = curSector;
    if (curAe) aeSel.value = curAe;
  }

  function sectorOptions(selectedId) {
    const groupOrder = ['Commercial','Financial','Healthcare','Outreach','Political','Other'];
    const groups = {};
    allSectors.forEach((s) => {
      const g = s.sector_group || 'Other';
      if (!groups[g]) groups[g] = [];
      groups[g].push(s);
    });
    let html = '<option value="">Unassigned</option>';
    groupOrder.forEach((g) => {
      if (!groups[g] || groups[g].length === 0) return;
      html += `<optgroup label="${g}">`;
      groups[g].forEach((s) => {
        const sel = s.sector_id === selectedId ? ' selected' : '';
        html += `<option value="${s.sector_id}"${sel}>${esc(s.sector_name)}</option>`;
      });
      html += '</optgroup>';
    });
    return html;
  }

  function renderChart(monthly) {
    const ctx = $('#revenue-chart');
    if (chart) chart.destroy();

    chart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: monthly.map((m) => m.month),
        datasets: [
          {
            label: 'Regular',
            data: monthly.map((m) => m.regular),
            backgroundColor: '#3b82f6',
            borderRadius: 4,
          },
          {
            label: 'Irregular',
            data: monthly.map((m) => m.irregular),
            backgroundColor: '#f59e0b',
            borderRadius: 4,
          },
          {
            label: 'Political',
            data: monthly.map((m) => m.political),
            backgroundColor: '#9333ea',
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: {
            callbacks: {
              label: (ctx) => ctx.dataset.label + ': ' + fmt(ctx.raw),
            },
          },
        },
        scales: {
          y: {
            ticks: {
              callback: (v) => '$' + (v / 1000).toFixed(0) + 'k',
            },
          },
        },
      },
    });
  }

  function renderTable() {
    const search = searchInput.value.toLowerCase();
    let filtered = allCustomers;

    if (search) {
      filtered = filtered.filter((c) => c.name.toLowerCase().includes(search));
    }

    filtered.sort((a, b) => {
      let va = a[sortCol];
      let vb = b[sortCol];
      if (typeof va === 'string') {
        va = (va || '').toLowerCase();
        vb = (vb || '').toLowerCase();
      }
      if (va == null) va = -Infinity;
      if (vb == null) vb = -Infinity;
      if (va < vb) return sortAsc ? -1 : 1;
      if (va > vb) return sortAsc ? 1 : -1;
      return 0;
    });

    const tbody = $('#customer-tbody');
    if (filtered.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" class="loading-msg">No customers found</td></tr>';
      return;
    }

    tbody.innerHTML = filtered
      .map(
        (c) => {
          const isWL = c.customer_id === WORLDLINK_ID && c.name === 'WorldLink (Agency)';
          const nameCell = isWL
            ? `<span class="customer-link" style="cursor:default">${esc(c.name)}</span>`
            : `<a href="/reports/customer/${c.customer_id}" class="customer-link">${esc(c.name)}</a>`;
          const sectorCell = isWL
            ? '<span style="color:#94a3b8;font-size:12px">—</span>'
            : `<select class="sector-select" data-id="${c.customer_id}" data-prev="${c.sector_id || ''}" onchange="window._rcmSector(this)">${sectorOptions(c.sector_id)}</select>`;
          return `
      <tr>
        <td>${nameCell}</td>
        <td>${sectorCell}</td>
        <td>
          <button class="cls-toggle ${c.revenue_class}"
            data-id="${c.customer_id}"
            data-cls="${c.revenue_class}"
            onclick="window._rcmToggle(this)">
            ${CLASS_LABELS[c.revenue_class] || c.revenue_class}
          </button>
        </td>
        <td>${esc(c.assigned_ae)}</td>
        <td>${fmt(c.current_year_revenue)}</td>
        <td>${fmt(c.prior_year_revenue)}</td>
        <td>${yoyCell(c)}</td>
      </tr>`;
        }
      )
      .join('');
  }

  function yoyCell(c) {
    if (c.yoy_pct === null || c.yoy_pct === undefined) {
      if (c.current_year_revenue > 0) {
        return '<span class="yoy-new">New</span>';
      }
      return '<span class="yoy-new">-</span>';
    }
    const cls = c.yoy_dollar >= 0 ? 'yoy-positive' : 'yoy-negative';
    const sign = c.yoy_dollar >= 0 ? '+' : '';
    return `<span class="${cls}">${sign}${fmt(c.yoy_dollar)} (${sign}${c.yoy_pct.toFixed(1)}%)</span>`;
  }

  function esc(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
  }

  function flashRow(el, type) {
    const tr = el.closest('tr');
    if (!tr) return;
    tr.classList.remove('flash-saved', 'flash-error');
    void tr.offsetWidth;
    tr.classList.add(type === 'error' ? 'flash-error' : 'flash-saved');
  }

  window._rcmToggle = async function (btn) {
    const id = btn.dataset.id;
    const oldCls = btn.dataset.cls;
    const idx = CLASS_CYCLE.indexOf(oldCls);
    const newCls = CLASS_CYCLE[(idx + 1) % CLASS_CYCLE.length];

    btn.textContent = CLASS_LABELS[newCls];
    btn.className = 'cls-toggle ' + newCls;
    btn.dataset.cls = newCls;

    const cust = allCustomers.find((c) => c.customer_id === parseInt(id, 10));
    if (cust) cust.revenue_class = newCls;

    try {
      const res = await fetch('/api/revenue-classification/' + id, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ revenue_class: newCls }),
      });
      if (!res.ok) throw new Error('Save failed');
      flashRow(btn, 'saved');

      const summaryRes = await fetch(
        '/api/revenue-classification/summary?' + filterParams()
      );
      const summary = await summaryRes.json();
      renderSummary(summary);
      renderChart(summary.monthly);
    } catch (e) {
      btn.textContent = CLASS_LABELS[oldCls];
      btn.className = 'cls-toggle ' + oldCls;
      btn.dataset.cls = oldCls;
      if (cust) cust.revenue_class = oldCls;
      flashRow(btn, 'error');
      console.error('Classification update failed:', e);
    }
  };

  window._rcmSector = async function (sel) {
    const id = sel.dataset.id;
    const newSectorId = sel.value ? parseInt(sel.value, 10) : null;
    const oldSectorId = sel.dataset.prev || '';

    sel.dataset.prev = sel.value;

    const cust = allCustomers.find((c) => c.customer_id === parseInt(id, 10));
    if (cust) {
      cust.sector_id = newSectorId;
      const sec = allSectors.find((s) => s.sector_id === newSectorId);
      cust.sector_name = sec ? sec.sector_name : '';
    }

    try {
      const res = await fetch('/api/revenue-classification/' + id, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sector_id: newSectorId }),
      });
      if (!res.ok) throw new Error('Save failed');
      flashRow(sel, 'saved');
    } catch (e) {
      sel.value = oldSectorId;
      if (cust) {
        cust.sector_id = oldSectorId ? parseInt(oldSectorId, 10) : null;
        const sec = allSectors.find((s) => s.sector_id === cust.sector_id);
        cust.sector_name = sec ? sec.sector_name : '';
      }
      flashRow(sel, 'error');
      console.error('Sector update failed:', e);
    }
  };

  // Sorting
  document.querySelectorAll('.rcm-table th[data-col]').forEach((th) => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (sortCol === col) {
        sortAsc = !sortAsc;
      } else {
        sortCol = col;
        sortAsc = col === 'name' || col === 'sector_name' || col === 'assigned_ae';
      }

      document.querySelectorAll('.rcm-table th').forEach((h) => h.classList.remove('sorted'));
      th.classList.add('sorted');
      th.querySelector('.sort-arrow').textContent = sortAsc ? '\u25B2' : '\u25BC';

      renderTable();
    });
  });

  // Filter events
  yearSel.addEventListener('change', loadData);
  sectorSel.addEventListener('change', loadData);
  aeSel.addEventListener('change', loadData);
  classSel.addEventListener('change', loadData);
  searchInput.addEventListener('input', renderTable);

  // Initial load
  loadData();
})();
