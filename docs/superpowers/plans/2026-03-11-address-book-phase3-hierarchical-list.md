# Address Book Phase 3: Hierarchical Entity List Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Display address-book entities hierarchically — agencies expand to show their linked customers, direct advertisers appear at the top level — in both card and table views.

**Architecture:** Add `agency_id` and `agency_name` to the flat list API response for customers, plus `client_count` on agencies. All hierarchical grouping and expand/collapse behavior happens client-side in JS, keeping the API flat and backward-compatible. A "Group by Agency" toggle controls whether the list renders flat or hierarchical. (Note: the v2 design spec mentions a `?view=hierarchical` API parameter — we deliberately skip this in favor of client-side grouping since all data is already loaded into `allData` for client-side filtering. The API stays flat and backward-compatible.)

**Tech Stack:** Python (service layer), vanilla JS (client-side grouping + rendering), CSS (expand/collapse UI)

---

## Context

### Current State

- `entity_service.list_entities()` returns a flat array of agencies and customers
- Customers do NOT include `agency_id` or `agency_name` in the list response
- Agencies do NOT include `client_count` in the list response
- All filtering, sorting, and rendering happens client-side in `allData` → `applyFilters()` → `renderGrid()`
- Card view (`renderCard`) and table view (`renderTable`) both iterate `currentData` flat
- Customers with `:` in their name or who are "agency-booked" (all spots via an agency) are already excluded from the list (lines 161-164 of `entity_service.py`)

### Target State

- The flat API response includes `agency_id` and `agency_name` on each customer
- Agencies include `client_count` in their list response
- A "Group by Agency" toggle in the toolbar enables hierarchical rendering
- When grouped: agencies are expandable containers with client cards/rows nested inside, direct advertisers (no `agency_id`) appear at the top level
- When flat: current behavior unchanged
- Filtering: when a filter matches a client but not its parent agency, the agency appears muted as a container
- Bulk view: stays flat (the group toggle is ignored)

### File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/services/entity_service.py:143-188` | Modify | Add `agency_id`, `agency_name` to customers; `client_count` to agencies |
| `tests/services/test_entity_service.py` | Modify | Add tests for new fields |
| `src/web/templates/address_book.html` | Modify | Group toggle, hierarchical card/table rendering, expand/collapse UI, filter adjustments |

---

## Chunk 1: Service Layer — Add Agency Data to List

### Task 1: Add `agency_id`, `agency_name`, and `client_count` to Entity List

**Files:**
- Modify: `src/services/entity_service.py:99-188`
- Modify: `tests/services/test_entity_service.py`

The customer SQL needs to JOIN agencies to get `agency_id` and `agency_name`. Agencies need a batch count of active customers per agency.

- [ ] **Step 1: Write the failing tests**

Add to `tests/services/test_entity_service.py`:

```python
class TestListEntitiesAgencyFields:
    """Test agency-related fields in list_entities response."""

    def test_customer_includes_agency_id(self, service, conn):
        """Customers linked to an agency include agency_id."""
        conn.executescript("""
            INSERT OR IGNORE INTO agencies (agency_id, agency_name, is_active)
            VALUES (100, 'Test Agency', 1);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (200, 'Agency Client', 1, 100);
        """)
        results = service.list_entities(conn)
        client = next(
            (r for r in results
             if r["entity_type"] == "customer"
             and r["entity_id"] == 200),
            None,
        )
        assert client is not None
        assert client["agency_id"] == 100
        assert client["agency_name"] == "Test Agency"

    def test_direct_advertiser_has_null_agency(self, service, conn):
        """Direct advertisers have agency_id=None."""
        conn.executescript("""
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (201, 'Direct Advertiser', 1, NULL);
        """)
        results = service.list_entities(conn)
        direct = next(
            (r for r in results
             if r["entity_type"] == "customer"
             and r["entity_id"] == 201),
            None,
        )
        assert direct is not None
        assert direct["agency_id"] is None
        assert direct["agency_name"] is None

    def test_agency_includes_client_count(self, service, conn):
        """Agencies include a count of active linked customers."""
        conn.executescript("""
            INSERT OR IGNORE INTO agencies (agency_id, agency_name, is_active)
            VALUES (100, 'Test Agency', 1);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (200, 'Client A', 1, 100);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (201, 'Client B', 1, 100);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (202, 'Inactive Client', 0, 100);
        """)
        results = service.list_entities(conn)
        agency = next(
            (r for r in results
             if r["entity_type"] == "agency"
             and r["entity_id"] == 100),
            None,
        )
        assert agency is not None
        assert agency["client_count"] == 2
```

Note: The existing test fixture already has `agency_id INTEGER` on the `customers` table and a full `agencies` table with `agency_id`, `agency_name`, `is_active`. Read `tests/services/test_entity_service.py` to confirm the `service` and `conn` fixture setup before adding tests.

- [ ] **Step 2: Run tests to verify they fail**

```bash
source .venv/bin/activate && pytest tests/services/test_entity_service.py::TestListEntitiesAgencyFields -v
```

Expected: FAIL — `agency_id` key not in results, `client_count` key not in results.

- [ ] **Step 3: Modify the customer SQL in `list_entities`**

In `src/services/entity_service.py`, modify the customer query (lines 143-157) to JOIN agencies:

**Current:**
```python
        customers = conn.execute(f"""
            SELECT
                c.customer_id as entity_id,
                'customer' as entity_type,
                c.normalized_name as entity_name,
                c.address, c.city, c.state, c.zip,
                c.notes, c.assigned_ae, c.is_active,
                c.sector_id,
                s.sector_name,
                s.sector_code
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            {active_clause}
            ORDER BY c.normalized_name
        """).fetchall()
```

**New:**
```python
        customers = conn.execute(f"""
            SELECT
                c.customer_id as entity_id,
                'customer' as entity_type,
                c.normalized_name as entity_name,
                c.address, c.city, c.state, c.zip,
                c.notes, c.assigned_ae, c.is_active,
                c.sector_id,
                s.sector_name,
                s.sector_code,
                c.agency_id,
                ag.agency_name
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            LEFT JOIN agencies ag ON c.agency_id = ag.agency_id
            {active_clause}
            ORDER BY c.normalized_name
        """).fetchall()
```

Then in the customer loop (lines 159-186), add the agency fields to each row dict. After line 186 (`results.append(row)`), the `agency_id` and `agency_name` columns are already in the `dict(c)` since they're in the SELECT. No extra code needed — they flow through automatically.

- [ ] **Step 4: Add `client_count` batch query for agencies**

Add a batch query after the existing sector_counts batch (around line 54) to count active customers per agency:

```python
        # Batch: client count per agency
        client_counts = {}
        for row in conn.execute("""
            SELECT agency_id, COUNT(*) as cnt
            FROM customers
            WHERE is_active = 1 AND agency_id IS NOT NULL
            GROUP BY agency_id
        """).fetchall():
            client_counts[row["agency_id"]] = row["cnt"]
```

Then in the agency loop (around line 118-137), add:
```python
            row["client_count"] = client_counts.get(
                row["entity_id"], 0
            )
```

Place it after the existing `row["sector_ids"] = ""` line (around line 126).

- [ ] **Step 5: Run tests**

```bash
source .venv/bin/activate && pytest tests/services/test_entity_service.py -v
```

Expected: All tests PASS including the 3 new ones.

- [ ] **Step 6: Commit**

```bash
git add src/services/entity_service.py tests/services/test_entity_service.py
git commit -m "feat: include agency_id, agency_name, and client_count in entity list"
```

---

## Chunk 2: Client-Side Hierarchical Rendering

### Task 2: Add "Group by Agency" Toggle and Hierarchical Grouping Logic

**Files:**
- Modify: `src/web/templates/address_book.html`

Add a toggle button to the toolbar and the JS function that transforms the flat `currentData` into a hierarchical structure for rendering.

- [ ] **Step 1: Add the toggle button HTML**

Find the view toggle buttons in the template toolbar area. They are inside a `<div class="view-toggle">` and look like:
```html
<button id="view-cards" class="active" title="Card view">▤</button>
<button id="view-table" title="Table view">☰</button>
```

Note: These buttons do NOT have a `view-btn` class. After these buttons (but still inside the `view-toggle` div), add:
```html
<span style="margin:0 4px;color:#cbd5e1;">|</span>
<button id="group-toggle" title="Group by agency">⊞</button>
```

- [ ] **Step 2: Add CSS for the group toggle and hierarchical elements**

Add these styles to the `<style>` section:

```css
/* Group toggle */
#group-toggle.grouped {
    background: var(--nord8, #88c0d0);
    color: white;
}

/* Hierarchical card styles */
.agency-group {
    margin-bottom: 16px;
}
.agency-group-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    background: #f1f5f9;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 500;
    font-size: 14px;
}
.agency-group-header:hover {
    background: #e2e8f0;
}
.agency-group-header .expand-icon {
    transition: transform 0.2s;
    font-size: 12px;
}
.agency-group-header.expanded .expand-icon {
    transform: rotate(90deg);
}
.agency-group-header .client-count {
    font-size: 12px;
    color: #64748b;
    font-weight: normal;
}
.agency-group-header .muted {
    opacity: 0.5;
}
.agency-group-clients {
    display: none;
    padding-left: 24px;
    margin-top: 8px;
}
.agency-group-clients.expanded {
    display: block;
}

/* Table hierarchy styles */
tr.agency-parent-row {
    background: #f8fafc;
    font-weight: 500;
}
tr.agency-parent-row td.name-cell {
    cursor: pointer;
}
tr.agency-parent-row .expand-icon {
    margin-right: 4px;
    font-size: 11px;
    display: inline-block;
    transition: transform 0.2s;
    cursor: pointer;
    padding: 2px 4px;
}
tr.agency-parent-row.expanded .expand-icon {
    transform: rotate(90deg);
}
tr.agency-child-row {
    display: none;
}
tr.agency-child-row.visible {
    display: table-row;
}
tr.agency-child-row td.name-cell {
    padding-left: 32px;
}
```

- [ ] **Step 3: Add the JS grouping function and toggle handler**

Add after the existing view toggle handlers (around line 1496):

```javascript
  // Group by agency toggle
  let groupByAgency = localStorage.getItem('addressBookGroup') === 'true';
  const groupBtn = document.getElementById('group-toggle');
  if (groupByAgency) groupBtn.classList.add('grouped');

  groupBtn.addEventListener('click', () => {
    groupByAgency = !groupByAgency;
    localStorage.setItem('addressBookGroup', groupByAgency);
    groupBtn.classList.toggle('grouped', groupByAgency);
    renderGrid();
  });

  function buildHierarchy(data) {
    // Group customers by agency_id
    const agencyMap = {};   // agency_id -> { agency: entity, clients: [] }
    const topLevel = [];    // direct advertisers + agencies without clients in data

    // First pass: index agencies
    for (const e of data) {
      if (e.entity_type === 'agency') {
        agencyMap[e.entity_id] = { agency: e, clients: [], matched: true };
      }
    }

    // Second pass: assign customers to agencies or top level
    for (const e of data) {
      if (e.entity_type !== 'customer') continue;
      if (e.agency_id && agencyMap[e.agency_id]) {
        agencyMap[e.agency_id].clients.push(e);
      } else if (e.agency_id) {
        // Agency exists but wasn't in filtered data — create a muted placeholder
        // Look it up from allData
        const parentAgency = allData.find(
          a => a.entity_type === 'agency' && a.entity_id === e.agency_id
        );
        if (parentAgency) {
          agencyMap[e.agency_id] = {
            agency: parentAgency,
            clients: [e],
            matched: false,  // agency didn't match filters
          };
        } else {
          topLevel.push(e);  // orphan — show at top level
        }
      } else {
        topLevel.push(e);  // direct advertiser
      }
    }

    // Build ordered result
    const groups = [];
    // Agencies with clients (sorted by agency name)
    const agencyGroups = Object.values(agencyMap)
      .filter(g => g.clients.length > 0)
      .sort((a, b) =>
        a.agency.entity_name.toLowerCase()
          .localeCompare(b.agency.entity_name.toLowerCase())
      );
    for (const g of agencyGroups) {
      groups.push({
        type: 'agency-group',
        agency: g.agency,
        clients: g.clients,
        matched: g.matched,
      });
    }

    // Agencies that matched the filter but have no clients in filtered data:
    // Pull their clients from allData so they appear as expandable groups
    for (const e of data) {
      if (e.entity_type === 'agency' && agencyMap[e.entity_id]
          && agencyMap[e.entity_id].clients.length === 0) {
        // Find clients from the full dataset
        const agencyClients = allData.filter(
          c => c.entity_type === 'customer' && c.agency_id === e.entity_id
        );
        if (agencyClients.length > 0) {
          agencyMap[e.entity_id].clients = agencyClients;
          // Don't add to groups yet — the groups loop above already
          // skips empty-client entries. Re-add this agency group.
          groups.push({
            type: 'agency-group',
            agency: e,
            clients: agencyClients,
            matched: true,
          });
        } else {
          topLevel.push(e);  // agency with truly no clients
        }
      }
    }

    return { groups, topLevel };
  }
```

- [ ] **Step 4: Commit**

```bash
git add src/web/templates/address_book.html
git commit -m "feat: add Group by Agency toggle and hierarchical grouping logic"
```

---

### Task 3: Hierarchical Card Rendering

**Files:**
- Modify: `src/web/templates/address_book.html`

Update `renderGrid()` to use the hierarchy when `groupByAgency` is true for card view.

- [ ] **Step 1: Modify `renderGrid()` for hierarchical card rendering**

Replace the current card rendering block in `renderGrid()` (around lines 1404-1408):

**Current:**
```javascript
    // Render card view
    grid.innerHTML = currentData.map(renderCard).join('');
    document.querySelectorAll('.entity-card').forEach(card => {
      card.addEventListener('click', () => openDetail(card.dataset.type, card.dataset.id));
    });
```

**New:**
```javascript
    // Render card view
    if (groupByAgency) {
      const { groups, topLevel } = buildHierarchy(currentData);
      let html = '';

      for (const g of groups) {
        const clientCount = g.clients.length;
        const mutedClass = g.matched ? '' : ' muted';
        html += `<div class="agency-group" data-agency-id="${g.agency.entity_id}">
          <div class="agency-group-header${mutedClass}">
            <span class="expand-icon">▶</span>
            <span>${esc(g.agency.entity_name)}</span>
            <span class="client-count">(${clientCount} client${clientCount !== 1 ? 's' : ''})</span>
            ${g.agency.assigned_ae ? `<span class="ae-badge">${esc(g.agency.assigned_ae)}</span>` : ''}
          </div>
          <div class="agency-group-clients">
            ${g.clients.map(renderCard).join('')}
          </div>
        </div>`;
      }

      html += topLevel.map(renderCard).join('');
      grid.innerHTML = html;

      // Attach expand/collapse handlers via addEventListener (no inline onclick)
      document.querySelectorAll('.agency-group-header').forEach(header => {
        header.addEventListener('click', () => {
          header.classList.toggle('expanded');
          header.nextElementSibling.classList.toggle('expanded');
        });
      });
    } else {
      grid.innerHTML = currentData.map(renderCard).join('');
    }

    document.querySelectorAll('.entity-card').forEach(card => {
      card.addEventListener('click', () => openDetail(card.dataset.type, card.dataset.id));
    });
```

- [ ] **Step 2: Verify manually**

Restart the server. Go to `/address-book`. Click the `⊞` group toggle.

Expected:
- Agencies with linked customers appear as expandable groups with client count
- Clicking the group header expands to reveal client cards
- Direct advertisers appear at the top level alongside agency groups
- Toggling off returns to flat view
- State persists in localStorage

- [ ] **Step 3: Commit**

```bash
git add src/web/templates/address_book.html
git commit -m "feat: hierarchical card rendering with expandable agency groups"
```

---

### Task 4: Hierarchical Table Rendering

**Files:**
- Modify: `src/web/templates/address_book.html`

Update `renderTable()` to use tree-table pattern when `groupByAgency` is true.

- [ ] **Step 1: Modify `renderTable()` for hierarchical rows**

The current `renderTable()` function (around line 1417) maps `currentData` to rows. Wrap it in a conditional:

**Replace the body of `renderTable()` with:**

```javascript
  function renderTable() {
    const tbody = document.getElementById('table-body');

    if (groupByAgency) {
      const { groups, topLevel } = buildHierarchy(currentData);
      let html = '';

      for (const g of groups) {
        const a = g.agency;
        const revenue = a.total_revenue || 0;
        const revenueStr = revenue >= 1000000 ? `$${(revenue/1000000).toFixed(1)}M` :
                           revenue >= 1000 ? `$${(revenue/1000).toFixed(0)}K` :
                           `$${revenue.toFixed(0)}`;
        const mutedStyle = g.matched ? '' : ' style="opacity:0.5"';
        html += `
          <tr class="agency-parent-row" data-agency-id="${a.entity_id}"
              data-type="agency" data-id="${a.entity_id}"${mutedStyle}>
            <td class="checkbox-cell"></td>
            <td class="name-cell">
              <span class="expand-icon">▶</span>
              ${esc(a.entity_name)}
              <span class="client-count">(${g.clients.length})</span>
            </td>
            <td class="type-cell"><span class="entity-type agency">Agency</span></td>
            <td>-</td>
            <td>${a.assigned_ae ? `<span class="ae-badge">${esc(a.assigned_ae)}</span>` : '-'}</td>
            <td>${a.signals?.length > 0 ? `<span class="signal-badge ${a.signals[0].signal_type}">${SIGNAL_NAMES[a.signals[0].signal_type]}</span>` : '-'}</td>
            <td>${esc(a.primary_contact || '-')}</td>
            <td>${a.last_active ? formatDate(a.last_active) : '-'}</td>
            <td class="revenue-cell">${revenueStr}</td>
          </tr>`;

        for (const c of g.clients) {
          html += renderTableRow(c, true);
        }
      }

      for (const e of topLevel) {
        html += renderTableRow(e, false);
      }

      tbody.innerHTML = html;

      // Agency row: expand icon toggles children, name text opens detail
      tbody.querySelectorAll('.agency-parent-row').forEach(row => {
        const expandIcon = row.querySelector('.expand-icon');
        const nameCell = row.querySelector('.name-cell');

        // Expand icon click: toggle children
        expandIcon.addEventListener('click', (e) => {
          e.stopPropagation();
          row.classList.toggle('expanded');
          const agencyId = row.dataset.agencyId;
          tbody.querySelectorAll(
            `.agency-child-row[data-parent-agency="${agencyId}"]`
          ).forEach(child => child.classList.toggle('visible'));
        });

        // Name cell click: open detail modal
        nameCell.addEventListener('click', () => {
          openDetail(row.dataset.type, row.dataset.id);
        });
      });

    } else {
      tbody.innerHTML = currentData.map(e => renderTableRow(e, false)).join('');
    }

    // Attach click handlers for non-agency-parent name cells
    tbody.querySelectorAll('tr:not(.agency-parent-row) .name-cell').forEach(cell => {
      cell.addEventListener('click', () => {
        const row = cell.closest('tr');
        openDetail(row.dataset.type, row.dataset.id);
      });
    });

    // Checkbox handlers
    tbody.querySelectorAll('.row-select').forEach(cb => {
      cb.addEventListener('change', (e) => {
        e.stopPropagation();
        const row = cb.closest('tr');
        const key = row.dataset.key;
        if (cb.checked) {
          selectedEntities.add(key);
          row.classList.add('selected');
        } else {
          selectedEntities.delete(key);
          row.classList.remove('selected');
        }
        updateBulkToolbar();
      });
    });
  }
```

- [ ] **Step 2: Extract a `renderTableRow` helper**

Add this function before `renderTable()`:

```javascript
  function renderTableRow(e, isChild) {
    const key = `${e.entity_type}:${e.entity_id}`;
    const isSelected = selectedEntities.has(key);
    const revenue = e.total_revenue || 0;
    const revenueStr = revenue >= 1000000 ? `$${(revenue/1000000).toFixed(1)}M` :
                       revenue >= 1000 ? `$${(revenue/1000).toFixed(0)}K` :
                       `$${revenue.toFixed(0)}`;
    const lastActive = e.last_active ? formatDate(e.last_active) : '-';
    const inactiveTag = e.is_active === 0 ? '<span class="badge-inactive">Inactive</span>' : '';
    const childClass = isChild ? ` agency-child-row` : '';
    const parentAttr = isChild && e.agency_id ? ` data-parent-agency="${e.agency_id}"` : '';
    return `
      <tr class="${isSelected ? 'selected' : ''}${childClass}" data-key="${key}"
          data-type="${e.entity_type}" data-id="${e.entity_id}"${parentAttr}>
        <td class="checkbox-cell"><input type="checkbox" class="row-select" ${isSelected ? 'checked' : ''}></td>
        <td class="name-cell">${esc(e.entity_name)}${inactiveTag}</td>
        <td class="type-cell"><span class="entity-type ${e.entity_type}">${e.entity_type === 'agency' ? 'Agency' : 'Advertiser'}</span></td>
        <td>${e.sector_name ? esc(e.sector_name) + ((e.sector_count || 0) > 1 ? ` <span class="extra-count">+${e.sector_count - 1}</span>` : '') : '-'}</td>
        <td>${e.assigned_ae ? `<span class="ae-badge">${esc(e.assigned_ae)}</span>` : '-'}</td>
        <td>${e.signals?.length > 0 ? `<span class="signal-badge ${e.signals[0].signal_type}" title="${esc(e.signals[0].signal_label)}">${SIGNAL_NAMES[e.signals[0].signal_type]}</span>` : '-'}</td>
        <td>${esc(e.primary_contact || '-')}</td>
        <td>${lastActive}</td>
        <td class="revenue-cell">${revenueStr}</td>
      </tr>`;
  }
```

- [ ] **Step 3: Verify manually**

Switch to table view in the address book. Enable "Group by Agency".

Expected:
- Agency rows appear with expand icon `▶` and client count
- Clicking the `▶` expand icon toggles child row visibility
- Clicking the agency name text opens the agency detail modal
- Direct advertisers appear as normal rows at the top level
- Checkbox selection still works on child rows
- Clicking a child row name opens the detail modal

- [ ] **Step 4: Commit**

```bash
git add src/web/templates/address_book.html
git commit -m "feat: hierarchical table rendering with expandable agency rows"
```

---

### Task 5: Filter Behavior with Hierarchy

**Files:**
- Modify: `src/web/templates/address_book.html`

When grouped, the search filter should also match `agency_name` on customers (so searching for an agency name shows its clients). The `buildHierarchy` function already handles the muted-parent case. We just need to include `agency_name` in the search filter.

- [ ] **Step 1: Update the search filter in `applyFilters()`**

Find the search filter block (around lines 1262-1269):

**Current:**
```javascript
    if (search) {
      filtered = filtered.filter(r =>
        (r.entity_name || '').toLowerCase().includes(search) ||
        (r.notes || '').toLowerCase().includes(search) ||
        (r.sector_name || '').toLowerCase().includes(search) ||
        (r.sector_code || '').toLowerCase().includes(search) ||
        (r.primary_contact || '').toLowerCase().includes(search)
      );
    }
```

**New:**
```javascript
    if (search) {
      filtered = filtered.filter(r =>
        (r.entity_name || '').toLowerCase().includes(search) ||
        (r.notes || '').toLowerCase().includes(search) ||
        (r.sector_name || '').toLowerCase().includes(search) ||
        (r.sector_code || '').toLowerCase().includes(search) ||
        (r.primary_contact || '').toLowerCase().includes(search) ||
        (r.agency_name || '').toLowerCase().includes(search)
      );
    }
```

- [ ] **Step 2: Update stats to show hierarchy info when grouped**

In `updateStats()` (around line 1380), add a "Groups" stat when grouped:

**After the existing stats HTML (line 1389), add:**
```javascript
    if (groupByAgency) {
      const { groups } = buildHierarchy(currentData);
      const agencyClients = currentData.filter(d => d.entity_type === 'customer' && d.agency_id).length;
      document.getElementById('stats').innerHTML += `
        <div class="stat"><div class="val">${groups.length}</div><div class="lbl">Agency Groups</div></div>
        <div class="stat"><div class="val">${agencyClients}</div><div class="lbl">Agency Clients</div></div>`;
    }
```

- [ ] **Step 3: Verify manually**

With grouping enabled:
1. Search for an agency name — its clients should appear even if they don't match
2. Search for a client name — the parent agency should appear muted as a container
3. Apply a sector filter — agencies with matching clients appear, non-matching agencies are hidden
4. Stats show "Agency Groups" and "Agency Clients" counts

- [ ] **Step 4: Commit**

```bash
git add src/web/templates/address_book.html
git commit -m "feat: search includes agency_name, hierarchy-aware stats"
```

---

### Task 6: Disable Grouping in Bulk Operations

**Files:**
- Modify: `src/web/templates/address_book.html`

When bulk operations are in progress (items selected), the group toggle should be visually disabled and grouping should be off to prevent confusion with multi-select.

- [ ] **Step 1: Update `updateBulkToolbar()` to disable group toggle**

Find `updateBulkToolbar()` (around line 1516). Add group toggle handling:

**After the existing `toolbar.classList.add('active')` / `remove('active')` logic, add:**

```javascript
    // Disable grouping when bulk selecting
    const groupBtn = document.getElementById('group-toggle');
    if (selectedEntities.size > 0) {
      groupBtn.disabled = true;
      groupBtn.title = 'Grouping disabled during bulk selection';
    } else {
      groupBtn.disabled = false;
      groupBtn.title = 'Group by agency';
    }
```

- [ ] **Step 2: Add disabled style for the group button**

```css
#group-toggle:disabled {
    opacity: 0.4;
    cursor: not-allowed;
}
```

- [ ] **Step 3: Verify manually**

1. Enable grouping, select a checkbox on a row → grouping button becomes disabled
2. Deselect all → grouping button re-enables
3. When items are selected and grouping is on, the list stays in its current state (doesn't force re-render)

- [ ] **Step 4: Commit**

```bash
git add src/web/templates/address_book.html
git commit -m "feat: disable group toggle during bulk selection"
```

---

## Summary

| Task | Description | Files Changed |
|------|-------------|---------------|
| 1 | Add `agency_id`, `agency_name`, `client_count` to entity list API | 2 modified |
| 2 | Group toggle button, CSS, and `buildHierarchy()` JS function | 1 modified |
| 3 | Hierarchical card rendering with expandable agency groups | 1 modified |
| 4 | Hierarchical table rendering with tree-table pattern | 1 modified |
| 5 | Search filter includes `agency_name`, hierarchy-aware stats | 1 modified |
| 6 | Disable grouping during bulk selection | 1 modified |

**Total:** 2 modified files, 1 test file updated. Service layer change is minimal (add JOIN + batch count). Most work is in the template JS.
