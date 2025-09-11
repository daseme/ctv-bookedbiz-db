// static/js/customer-normalization-manager.js
(() => {
  const qs = (id) => document.getElementById(id);

  const state = {
    page: 1,
    size: 25,
    q: "",
    status: "all",
    rev: "all",
    sort: "raw_az",
    total: 0,
    items: []
  };

  const buildUrl = () => {
    const p = new URLSearchParams({
      page: String(state.page),
      size: String(state.size),
      q: state.q,
      status: state.status,
      rev: state.rev,
      sort: state.sort
    });
    return `/api/customer-normalization?${p.toString()}`;
    };

  const fetchStats = async () => {
    const r = await fetch(`/api/customer-normalization/stats`);
    const j = await r.json();
    qs("s-total").textContent = j.total;
    qs("s-exists").textContent = j.in_customers;
    qs("s-conflicts").textContent = j.conflicts;
    qs("s-ias").textContent = j.seen_internal_ad_sales;
    qs("s-bc").textContent = j.seen_branded_content;
  };

  const fetchPage = async () => {
    const r = await fetch(buildUrl());
    const j = await r.json();
    state.total = j.total;
    state.items = j.items;
    renderTable();
    renderPager();
  };

  const badge = (ok, label, cls) => `<span class="badge ${cls}">${label}</span>`;

  const renderRow = (x) => {
    const statusCells = [];
    if (x.exists_in_customers) statusCells.push(badge(true, "in customers", "ok"));
    else statusCells.push(badge(false, "missing", "warn"));
    if (x.has_alias) statusCells.push(badge(true, "alias", "ok"));
    if (x.alias_conflict) statusCells.push(badge(false, "alias conflict", "err"));
    const custId = x.customer_id ?? "";
    return `
      <tr>
        <td class="mono">${escapeHtml(x.raw_text)}</td>
        <td class="mono">${escapeHtml(x.normalized_name)}</td>
        <td>${escapeHtml(x.customer || "")}</td>
        <td>${escapeHtml(x.agency1 || "")}</td>
        <td>${escapeHtml(x.agency2 || "")}</td>
        <td>${escapeHtml(x.revenue_types_seen || "")}</td>
        <td>${statusCells.join(" ")}</td>
        <td>${custId ? `<span class="mono">${custId}</span>` : ""}</td>
        <td class="dim">${escapeHtml(x.customer_created_date || "")}</td>
      </tr>`;
  };

  const renderTable = () => {
    const tb = qs("tbody");
    if (!state.items.length) {
      tb.innerHTML = `<tr><td colspan="9" class="dim">No rows</td></tr>`;
      return;
    }
    tb.innerHTML = state.items.map(renderRow).join("");
  };

  const renderPager = () => {
    const start = (state.page - 1) * state.size + 1;
    const end = Math.min(state.page * state.size, state.total);
    qs("page-info").textContent = state.total
      ? `Showing ${start}–${end} of ${state.total}`
      : "No results";
  };

  const escapeHtml = (s) =>
    String(s).replace(/[&<>"'`=\/]/g, (c) => (
      { "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;","/":"&#x2F;","`":"&#x60;","=":"&#x3D;" }[c]
    ));

  // Wire controls
  const initControls = () => {
    qs("q").addEventListener("input", debounce(() => { state.q = qs("q").value.trim(); state.page=1; fetchPage(); }, 250));
    qs("status").addEventListener("change", () => { state.status = qs("status").value; state.page=1; fetchPage(); });
    qs("rev").addEventListener("change", () => { state.rev = qs("rev").value; state.page=1; fetchPage(); });
    qs("sort").addEventListener("change", () => { state.sort = qs("sort").value; fetchPage(); });
    qs("size").addEventListener("change", () => { state.size = parseInt(qs("size").value,10); state.page=1; fetchPage(); });
    qs("btn-refresh").addEventListener("click", fetchPage);
    qs("prev").addEventListener("click", () => { if (state.page>1){ state.page--; fetchPage(); }});
    qs("next").addEventListener("click", () => {
      const maxPage = Math.max(1, Math.ceil(state.total / state.size));
      if (state.page < maxPage){ state.page++; fetchPage(); }
    });
    qs("btn-export").addEventListener("click", exportCsv);
  };

  const exportCsv = async () => {
    // Fetch full current filter set without pagination (capped to 50k)
    const p = new URLSearchParams({
      page: "1", size: "50000", q: state.q, status: state.status, rev: state.rev, sort: state.sort
    });
    const r = await fetch(`/api/customer-normalization?${p.toString()}`);
    const j = await r.json();
    const headers = ["raw_text","normalized_name","customer","agency1","agency2","revenue_types_seen","exists_in_customers","has_alias","alias_conflict","customer_id","customer_created_date"];
    const rows = [headers.join(",")].concat(
      j.items.map(x => headers.map(h => csvCell(x[h])).join(","))
    );
    const blob = new Blob([rows.join("\n")], {type:"text/csv"});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = "customer_normalization_audit.csv"; a.click();
    URL.revokeObjectURL(url);
  };

  const csvCell = (v) => {
    const s = String(v ?? "");
    const needs = /[",\n]/.test(s);
    return needs ? `"${s.replace(/"/g,'""')}"` : s;
  };

  const debounce = (fn, ms) => {
    let t = null; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
  };

  // Boot
  window.addEventListener("DOMContentLoaded", async () => {
    initControls();
    await fetchStats();
    await fetchPage();
  });
})();
