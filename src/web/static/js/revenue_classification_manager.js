(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const fmt = (n) => '$' + Math.round(n).toLocaleString();

  let chart = null;
  let allCustomers = [];
  let sortCol = 'current_year_revenue';
  let sortAsc = false;

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
    allCustomers = customers;
    populateFilterDropdowns(customers);
    renderTable();
  }

  function renderSummary(s) {
    $('#regular-total').textContent = fmt(s.regular_total);
    $('#irregular-total').textContent = fmt(s.irregular_total);
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

    // Build unique sector (id, name) pairs and AE names
    const sectorMap = new Map();
    customers.forEach((c) => {
      if (c.sector_id && c.sector_name) sectorMap.set(c.sector_id, c.sector_name);
    });
    const aes = [...new Set(customers.map((c) => c.assigned_ae).filter(Boolean))].sort();

    if (sectorSel.options.length <= 1) {
      [...sectorMap.entries()]
        .sort((a, b) => a[1].localeCompare(b[1]))
        .forEach(([id, name]) => sectorSel.appendChild(new Option(name, id)));
    }
    if (aeSel.options.length <= 1) {
      aes.forEach((a) => aeSel.appendChild(new Option(a, a)));
    }

    if (curSector) sectorSel.value = curSector;
    if (curAe) aeSel.value = curAe;
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
        (c) => `
      <tr>
        <td><a href="/address-book/customer/${c.customer_id}" class="customer-link">${esc(c.name)}</a></td>
        <td>${esc(c.sector_name)}</td>
        <td>
          <button class="cls-toggle ${c.revenue_class}"
            data-id="${c.customer_id}"
            data-cls="${c.revenue_class}"
            onclick="window._rcmToggle(this)">
            ${c.revenue_class === 'regular' ? 'Regular' : 'Irregular'}
          </button>
        </td>
        <td>${esc(c.assigned_ae)}</td>
        <td>${fmt(c.current_year_revenue)}</td>
        <td>${fmt(c.prior_year_revenue)}</td>
        <td>${yoyCell(c)}</td>
      </tr>`
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

  window._rcmToggle = async function (btn) {
    const id = btn.dataset.id;
    const oldCls = btn.dataset.cls;
    const newCls = oldCls === 'regular' ? 'irregular' : 'regular';

    btn.textContent = newCls === 'regular' ? 'Regular' : 'Irregular';
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

      const [summaryRes] = await Promise.all([
        fetch('/api/revenue-classification/summary?' + filterParams()),
      ]);
      const summary = await summaryRes.json();
      renderSummary(summary);
      renderChart(summary.monthly);
    } catch (e) {
      btn.textContent = oldCls === 'regular' ? 'Regular' : 'Irregular';
      btn.className = 'cls-toggle ' + oldCls;
      btn.dataset.cls = oldCls;
      if (cust) cust.revenue_class = oldCls;
      console.error('Classification update failed:', e);
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
