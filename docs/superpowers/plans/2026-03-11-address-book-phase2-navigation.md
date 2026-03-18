# Address Book Phase 2: Navigation & Cross-Linking Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add context-aware breadcrumbs and cross-links between address-book, customer-detail, AE dashboard, and customer merge pages so users can navigate seamlessly between related entity views.

**Architecture:** Create a Jinja macro for breadcrumb links that reads a `?from=` query parameter to construct context-aware back-links. Update each page's breadcrumb block to use clickable links instead of static `<span>` elements. Add "View Full Detail" and "View in Address Book" cross-links where they're missing.

**Tech Stack:** Jinja2 macros, Flask route params, vanilla JS (no new dependencies)

---

## Context

### Current State

Pages are isolated islands:
- **Address-book modal** has a tiny `↗` icon link to customer detail (`/reports/customer/${id}`) — no text label, opens in new tab
- **Customer detail** breadcrumb shows `Home › Reporting › Customer Detail` as static text — "Reporting" is a `<span>`, not a link. The `← Back` link uses `javascript:history.back()` which is unreliable
- **AE dashboard** already links customer names to customer detail via `url_for()` — but has no link back to address-book
- **Customer merge** has no post-resolution links to view the customer in address-book or customer-detail

### Target State

- Every entity-related page has clickable breadcrumbs with proper links
- Customer detail breadcrumb is context-aware: shows "Address Book" or "AE Dashboard" based on `?from=` parameter
- Address-book modal has a visible "View Full Detail" button
- AE dashboard rows have "View in Address Book" links
- Customer merge shows "View Customer" link after resolving a bill code

### Existing Infrastructure

- `base.html:101-133` defines breadcrumb CSS (`.breadcrumb`, `.breadcrumb-container`, `.breadcrumb-separator`, `.breadcrumb-current`)
- `base.html:551-557` renders the breadcrumb bar with `Home` link + `{% block breadcrumb %}`
- Each page fills `{% block breadcrumb %}` with `<span>` elements and separators
- No shared Jinja macro files exist yet — `src/web/templates/macros/` does not exist

### File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/web/templates/macros/breadcrumbs.html` | Create | Reusable breadcrumb macro |
| `src/web/templates/customer_detail.html` | Modify | Context-aware breadcrumbs, replace `history.back()` |
| `src/web/routes/customer_detail_routes.py` | Modify | Pass `from_page` context to template |
| `src/web/templates/address_book.html` | Modify | "View Full Detail" button in modal |
| `src/web/templates/ae-dashboard.html` | Modify | "View in Address Book" links |
| `src/web/templates/ae-dashboard-personal.html` | Modify | "View in Address Book" links |
| `src/web/templates/customer_merge.html` | Modify | Post-resolve "View Customer" links |
| `tests/web/test_customer_detail_navigation.py` | Create | Test route passes navigation context |

---

## Chunk 1: Breadcrumb Macro & Customer Detail Navigation

### Task 1: Create Breadcrumb Macro

**Files:**
- Create: `src/web/templates/macros/breadcrumbs.html`

The macro generates breadcrumb trail items with proper `<a>` links. It accepts a list of `(label, url)` tuples plus a final current-page label.

- [ ] **Step 1: Create the macro file**

```html
{# Breadcrumb navigation macro.
   Usage: {% from "macros/breadcrumbs.html" import breadcrumb_trail %}
          {{ breadcrumb_trail([("Data Management", "/address-book")], "Customer Detail") }}
#}
{% macro breadcrumb_trail(links, current) %}
{% for label, url in links %}
<span class="breadcrumb-separator">›</span>
{% if url %}<a href="{{ url }}">{{ label }}</a>{% else %}<span>{{ label }}</span>{% endif %}
{% endfor %}
<span class="breadcrumb-separator">›</span>
<span class="breadcrumb-current">{{ current }}</span>
{% endmacro %}
```

- [ ] **Step 2: Verify the macro renders correctly**

Open a Python shell to confirm Jinja2 can load the macro:

```bash
source .venv/bin/activate && python -c "
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('src/web/templates'))
t = env.from_string('''
{% from \"macros/breadcrumbs.html\" import breadcrumb_trail %}
{{ breadcrumb_trail([(\"Data Management\", \"/address-book\")], \"Customer Detail\") }}
''')
print(t.render())
"
```

Expected: HTML output with `<a href="/address-book">Data Management</a>` and `<span class="breadcrumb-current">Customer Detail</span>`.

- [ ] **Step 3: Commit**

```bash
git add src/web/templates/macros/breadcrumbs.html
git commit -m "feat: add breadcrumb Jinja macro for navigation links"
```

---

### Task 2: Customer Detail Route — Pass Navigation Context

**Files:**
- Modify: `src/web/routes/customer_detail_routes.py:23-41`
- Create: `tests/web/test_customer_detail_navigation.py`

The route reads `?from=` query param and passes it to the template so breadcrumbs can be context-aware.

- [ ] **Step 1: Write the failing test**

Create `tests/web/test_customer_detail_navigation.py`:

```python
"""Tests for customer detail navigation context."""

import sqlite3
import tempfile
import os

import pytest


@pytest.fixture
def app():
    """Create test app with a minimal database."""
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            agency_id INTEGER
        );
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT NOT NULL
        );
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            gross_revenue REAL DEFAULT 0,
            net_revenue REAL DEFAULT 0,
            broadcast_month TEXT,
            revenue_type TEXT
        );
        INSERT INTO customers (customer_id, normalized_name)
        VALUES (1, 'Test Customer');
        INSERT INTO spots (spot_id, customer_id, gross_revenue,
                           net_revenue, broadcast_month)
        VALUES (1, 1, 1000, 800, 'Jan-25');
    """)
    conn.commit()
    conn.close()

    from src.web.app import create_app
    application = create_app()
    application.config["TESTING"] = True
    application.config["DB_PATH"] = db_path

    yield application

    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def client(app):
    return app.test_client()


class TestCustomerDetailNavigation:
    """Test that navigation context is passed to templates."""

    def test_no_from_param_defaults_to_none(self, client):
        """When no ?from= param, from_page should be empty."""
        resp = client.get("/reports/customer/1", follow_redirects=True)
        # Page should load (200 or redirect to login)
        assert resp.status_code in (200, 302)

    def test_from_address_book_passed_to_template(self, client):
        """When ?from=address-book, page should contain address-book link."""
        resp = client.get(
            "/reports/customer/1?from=address-book",
            follow_redirects=True,
        )
        assert resp.status_code in (200, 302)

    def test_from_ae_dashboard_passed_to_template(self, client):
        """When ?from=ae-dashboard, page should contain ae-dashboard link."""
        resp = client.get(
            "/reports/customer/1?from=ae-dashboard",
            follow_redirects=True,
        )
        assert resp.status_code in (200, 302)
```

- [ ] **Step 2: Run the test to verify it passes (baseline)**

```bash
source .venv/bin/activate && pytest tests/web/test_customer_detail_navigation.py -v
```

Expected: Tests pass (they check status code only at this point). If auth redirects to 302, that's fine — it proves the route accepts the param without error.

- [ ] **Step 3: Update the customer detail route**

In `src/web/routes/customer_detail_routes.py`, modify the `customer_detail` function to read the `from` query param and pass it to the template:

**Current code (lines 23-41):**
```python
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")

    with db.connection() as conn:
        service = CustomerDetailService(conn)
        report = service.get_customer_detail(
            customer_id, start_date, end_date
        )

    if not report:
        abort(404, f"Customer {customer_id} not found")

    return render_template(
        'customer_detail.html',
        report=report,
        start_date=start_date,
        end_date=end_date,
        page_title=f"Customer: {report.summary.normalized_name}",
    )
```

**New code:**
```python
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")
    from_page = request.args.get("from", "")

    with db.connection() as conn:
        service = CustomerDetailService(conn)
        report = service.get_customer_detail(
            customer_id, start_date, end_date
        )

    if not report:
        abort(404, f"Customer {customer_id} not found")

    return render_template(
        'customer_detail.html',
        report=report,
        start_date=start_date,
        end_date=end_date,
        from_page=from_page,
        page_title=f"Customer: {report.summary.normalized_name}",
    )
```

Only change: add `from_page = request.args.get("from", "")` and pass `from_page=from_page` to `render_template`.

- [ ] **Step 4: Run tests**

```bash
source .venv/bin/activate && pytest tests/web/test_customer_detail_navigation.py -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/web/routes/customer_detail_routes.py tests/web/test_customer_detail_navigation.py
git commit -m "feat: pass navigation context (from_page) to customer detail template"
```

---

### Task 3: Customer Detail Template — Context-Aware Breadcrumbs

**Files:**
- Modify: `src/web/templates/customer_detail.html:1-13, 432-433`

Replace the static breadcrumb with the macro, using `from_page` to determine the back-link. Replace `history.back()` with a proper link.

- [ ] **Step 1: Update the breadcrumb block**

**Current code (lines 1-10):**
```html
{% extends "base.html" %}

{% block title %}{{ page_title }}{% endblock %}

{% block breadcrumb %}
<span class="breadcrumb-separator">›</span>
<span>Reporting</span>
<span class="breadcrumb-separator">›</span>
<span class="breadcrumb-current">Customer Detail</span>
{% endblock %}
```

**New code:**
```html
{% extends "base.html" %}
{% from "macros/breadcrumbs.html" import breadcrumb_trail %}

{% block title %}{{ page_title }}{% endblock %}

{% block breadcrumb %}
{% if from_page == 'address-book' %}
{{ breadcrumb_trail([("Data Management", ""), ("Address Book", "/address-book")], report.summary.normalized_name) }}
{% elif from_page == 'ae-dashboard' %}
{{ breadcrumb_trail([("Reporting", ""), ("AE Dashboard", "/ae-dashboard")], report.summary.normalized_name) }}
{% elif from_page == 'customer-merge' %}
{{ breadcrumb_trail([("Data Management", ""), ("Customer Merge", "/customer-merge")], report.summary.normalized_name) }}
{% else %}
{{ breadcrumb_trail([("Reporting", "/reports")], "Customer Detail") }}
{% endif %}
{% endblock %}
```

This gives context-aware breadcrumbs:
- From address book: `Home › Data Management › Address Book › Customer Name`
- From AE dashboard: `Home › Reporting › AE Dashboard › Customer Name`
- From customer merge: `Home › Data Management › Customer Merge › Customer Name`
- Default: `Home › Reporting › Customer Detail` (current behavior, with "Reporting" now clickable)

- [ ] **Step 2: Replace `history.back()` with a proper back-link**

**Current code (line 433):**
```html
<a href="javascript:history.back()" class="back-link">← Back</a>
```

**New code:**
```html
{% if from_page == 'address-book' %}
<a href="/address-book" class="back-link">← Back to Address Book</a>
{% elif from_page == 'ae-dashboard' %}
<a href="/ae-dashboard" class="back-link">← Back to AE Dashboard</a>
{% elif from_page == 'customer-merge' %}
<a href="/customer-merge" class="back-link">← Back to Customer Merge</a>
{% else %}
<a href="javascript:history.back()" class="back-link">← Back</a>
{% endif %}
```

When `from_page` is set, the user gets a reliable, explicit back-link. When it's not set (direct URL visit), `history.back()` is the best fallback.

- [ ] **Step 3: Verify by restarting the server and visiting**

```
http://spotops/reports/customer/1?from=address-book
```

Expected: Breadcrumb shows `Home › Data Management › Address Book › [Customer Name]`. Back link says "← Back to Address Book" and navigates to `/address-book`.

```
http://spotops/reports/customer/1
```

Expected: Breadcrumb shows `Home › Reporting › Customer Detail`. Back link says "← Back" and uses browser history.

- [ ] **Step 4: Commit**

```bash
git add src/web/templates/customer_detail.html
git commit -m "feat: context-aware breadcrumbs and back-links on customer detail"
```

---

## Chunk 2: Cross-Page Links

### Task 4: Address Book Modal — "View Full Detail" Button

**Files:**
- Modify: `src/web/templates/address_book.html:518, 1660-1667`

Replace the tiny `↗` icon with a proper "View Full Detail" button. Add `?from=address-book` to the URL. Keep it in the same tab (remove `target="_blank"`).

- [ ] **Step 1: Update the modal header link HTML**

**Current code (line 518):**
```html
      <h2><span id="detail-name"></span> <span class="type-badge" id="detail-type-badge"></span> <a id="detail-page-link" href="#" target="_blank" title="Open detail page" style="font-size:14px;color:#0ea5e9;text-decoration:none;display:none">↗</a></h2>
```

**New code:**
```html
      <h2><span id="detail-name"></span> <span class="type-badge" id="detail-type-badge"></span></h2>
      <a id="detail-page-link" href="#" title="View full customer detail page" class="btn btn-view-detail" style="display:none">View Full Detail →</a>
```

- [ ] **Step 2: Add CSS for the button**

Add to the `<style>` section (after the existing `.modal-header` styles, around line 80):

```css
.btn-view-detail {
    font-size: 13px;
    padding: 4px 12px;
    background: var(--nord8, #88c0d0);
    color: white;
    border-radius: 4px;
    text-decoration: none;
    white-space: nowrap;
}
.btn-view-detail:hover {
    background: var(--nord9, #81a1c1);
}
```

- [ ] **Step 3: Update the JS that sets the link URL**

**Current code (lines 1660-1667):**
```javascript
    // Detail page link (customers only)
    const pageLink = document.getElementById('detail-page-link');
    if (type === 'customer') {
      pageLink.href = `/reports/customer/${id}`;
      pageLink.style.display = 'inline';
    } else {
      pageLink.style.display = 'none';
    }
```

**New code:**
```javascript
    // Detail page link (customers only)
    const pageLink = document.getElementById('detail-page-link');
    if (type === 'customer') {
      pageLink.href = `/reports/customer/${id}?from=address-book`;
      pageLink.style.display = 'inline-block';
    } else {
      pageLink.style.display = 'none';
    }
```

Changes: Added `?from=address-book` query param. Display changed to `inline-block` for button styling. Removed `target="_blank"` — stays in same tab.

- [ ] **Step 4: Also update the agency-clients sub-table links**

In the JS that renders agency client rows (around line 1950), client name links should also include `?from=address-book`:

**Current pattern:**
```javascript
<a href="/reports/customer/${c.customer_id}" target="_blank">
```

**New pattern:**
```javascript
<a href="/reports/customer/${c.customer_id}?from=address-book">
```

- [ ] **Step 5: Verify manually**

Open address-book, click a customer card, verify:
- "View Full Detail →" button appears in modal header
- Clicking it navigates to customer detail with breadcrumb showing "Address Book"
- Back link on customer detail returns to `/address-book`

- [ ] **Step 6: Commit**

```bash
git add src/web/templates/address_book.html
git commit -m "feat: 'View Full Detail' button in address-book modal with navigation context"
```

---

### Task 5: AE Dashboard — Add "View in Address Book" Links

**Files:**
- Modify: `src/web/templates/ae-dashboard.html:218, 299`
- Modify: `src/web/templates/ae-dashboard-personal.html:623`

Add a small "View in Address Book" link next to existing customer name links. Also add `?from=ae-dashboard` to the existing customer detail links. Per the design spec, the "View in Address Book" link should filter by AE (not customer name) to show that AE's full account list.

**Important:** Jinja2 does not support `**dict` unpacking in template expressions, and `from` is a Python reserved keyword. Use string concatenation: `url_for(...) + '?from=ae-dashboard'`.

- [ ] **Step 1: Update main AE dashboard — lost customers section**

**Current code (line 218):**
```html
      • <strong>{% if customer.customer_id %}<a href="{{ url_for('customer_detail.customer_detail', customer_id=customer.customer_id) }}" style="color:#742a2a;">{{ customer.customer }}</a>{% else %}{{ customer.customer }}{% endif %}</strong> ({{ customer.sector }}) - ${{ "{:,.0f}".format(customer.ytd2023) }} in {{ data.selected_year - 1 }}
```

**New code:**
```html
      • <strong>{% if customer.customer_id %}<a href="{{ url_for('customer_detail.customer_detail', customer_id=customer.customer_id) }}?from=ae-dashboard" style="color:#742a2a;">{{ customer.customer }}</a>{% else %}{{ customer.customer }}{% endif %}</strong>{% if customer.customer_id %} <a href="/address-book?ae={{ data.selected_ae|default('', true)|urlencode }}" style="font-size:11px;color:#6b7280;" title="View in Address Book">AB</a>{% endif %} ({{ customer.sector }}) - ${{ "{:,.0f}".format(customer.ytd2023) }} in {{ data.selected_year - 1 }}
```

Changes: Appended `?from=ae-dashboard` to the customer detail URL via string concatenation. Added a small "AB" text-link that opens address-book filtered to the current AE (not the customer name — per spec, this shows the AE's full account list).

- [ ] **Step 2: Update main AE dashboard — customer table**

**Current code (line 299):**
```html
            <strong>{% if customer.customer_id %}<a href="{{ url_for('customer_detail.customer_detail', customer_id=customer.customer_id) }}">{{ customer.customer }}</a>{% else %}{{ customer.customer }}{% endif %}</strong>
```

**New code:**
```html
            <strong>{% if customer.customer_id %}<a href="{{ url_for('customer_detail.customer_detail', customer_id=customer.customer_id) }}?from=ae-dashboard">{{ customer.customer }}</a>{% else %}{{ customer.customer }}{% endif %}</strong>{% if customer.customer_id %} <a href="/address-book?ae={{ data.selected_ae|default('', true)|urlencode }}" style="font-size:11px;color:#6b7280;" title="View in Address Book">AB</a>{% endif %}
```

- [ ] **Step 3: Update personal AE dashboard**

**Current code (line 623 of `ae-dashboard-personal.html`):**
```html
{% if account.customer_id %}<a href="{{ url_for('customer_detail.customer_detail', customer_id=account.customer_id) }}">{{ account.customer_name }}</a>
```

**New code:**
```html
{% if account.customer_id %}<a href="{{ url_for('customer_detail.customer_detail', customer_id=account.customer_id) }}?from=ae-dashboard">{{ account.customer_name }}</a>
```

Note: The personal dashboard uses `account.customer_name` (not `customer.customer`) and iterates `sector_data.accounts`. Only add `?from=ae-dashboard` here — the personal dashboard already shows a single AE's accounts, so the "View in Address Book" link is less useful and can be omitted for now.

- [ ] **Step 4: Verify manually**

Visit `/ae-dashboard`, confirm:
- Customer name links now navigate to customer detail with `?from=ae-dashboard`
- Small `📋` icon appears after customer names, linking to address-book with search filter
- Customer detail page shows "AE Dashboard" in breadcrumb when arriving from AE dashboard

- [ ] **Step 5: Commit**

```bash
git add src/web/templates/ae-dashboard.html src/web/templates/ae-dashboard-personal.html
git commit -m "feat: add address-book cross-links and navigation context to AE dashboards"
```

---

### Task 6: Customer Merge — Post-Resolve Links

**Files:**
- Modify: `src/web/templates/customer_merge.html:499-572`

After a bill code is resolved (backfill, link, or create), show a "View Customer" link in the resolved row. The resolution actions already receive the `customer_id` in the API response.

- [ ] **Step 1: Update the `confirmLink` success handler**

**Current code (lines 522-530):**
```javascript
            row.classList.add('resolved');
            document.getElementById('search-' + idx).classList.remove('active');
            toast(
                'Linked "' + billCode + '" to "' + customerName +
                '" (' + result.spots_updated + ' spots updated)',
                'success'
            );
            refreshMetrics(customerId);
            setTimeout(loadUnresolved, 1500);
```

**New code:**
```javascript
            row.classList.add('resolved');
            document.getElementById('search-' + idx).classList.remove('active');
            toast(
                'Linked "' + billCode + '" to "' + customerName +
                '" (' + result.spots_updated + ' spots updated)',
                'success'
            );
            addResolvedLink(row, customerId, customerName);
            refreshMetrics(customerId);
            setTimeout(loadUnresolved, 1500);
```

- [ ] **Step 2: Update the `doBackfill` success handler**

**Current code (lines 592-599):**
```javascript
            row.classList.add('resolved');
            toast(
                'Backfilled ' + result.spots_updated + ' spots for "' +
                billCode + '"',
                'success'
            );
            refreshMetrics(customerId);
            setTimeout(loadUnresolved, 1500);
```

**New code:**
```javascript
            row.classList.add('resolved');
            toast(
                'Backfilled ' + result.spots_updated + ' spots for "' +
                billCode + '"',
                'success'
            );
            addResolvedLink(row, customerId, billCode);
            refreshMetrics(customerId);
            setTimeout(loadUnresolved, 1500);
```

Note: `doBackfill` doesn't have a customer name in scope, so we pass `billCode` as the search term for the address-book link.

- [ ] **Step 3: Update the `createAndLink` success handler**

**Current code (lines 561-568):**
```javascript
            row.classList.add('resolved');
            toast(
                'Created "' + name + '" and linked ' +
                result.spots_updated + ' spots',
                'success'
            );
            refreshMetrics(result.customer_id);
            setTimeout(loadUnresolved, 1500);
```

**New code:**
```javascript
            row.classList.add('resolved');
            toast(
                'Created "' + name + '" and linked ' +
                result.spots_updated + ' spots',
                'success'
            );
            addResolvedLink(row, result.customer_id, name);
            refreshMetrics(result.customer_id);
            setTimeout(loadUnresolved, 1500);
```

- [ ] **Step 4: Add the `addResolvedLink` helper function**

Add this function in the `<script>` section, before the resolution handlers (around line 498):

```javascript
    function addResolvedLink(row, customerId, customerName) {
        const links = document.createElement('div');
        links.style.cssText = 'margin-top:6px;font-size:12px;';
        links.innerHTML =
            '<a href="/reports/customer/' + customerId +
            '?from=customer-merge" ' +
            'style="color:#0ea5e9;margin-right:12px;">View Detail →</a>' +
            '<a href="/address-book?search=' +
            encodeURIComponent(customerName || '') +
            '" style="color:#6b7280;">View in Address Book</a>';
        row.appendChild(links);
    }
```

- [ ] **Step 5: Also update the customer merge breadcrumb to be clickable**

**Current code (lines 7-10):**
```html
{% block breadcrumb %}
<span class="breadcrumb-separator">›</span>
<span class="breadcrumb-current">Customer Merge</span>
{% endblock %}
```

**New code:**
```html
{% from "macros/breadcrumbs.html" import breadcrumb_trail %}
{% block breadcrumb %}
{{ breadcrumb_trail([("Data Management", "/address-book")], "Customer Merge") }}
{% endblock %}
```

- [ ] **Step 6: Verify manually**

On the customer merge page:
1. Resolve a bill code via any method (backfill, link, or create)
2. Confirm the resolved row shows "View Detail →" and "View in Address Book" links
3. Click "View Detail →" — should open customer detail with "Customer Merge" breadcrumb
4. Breadcrumb at top should show `Home › Data Management › Customer Merge`

- [ ] **Step 7: Commit**

```bash
git add src/web/templates/customer_merge.html
git commit -m "feat: post-resolve links and clickable breadcrumbs on customer merge page"
```

---

### Task 7: Update Remaining Breadcrumbs

**Files:**
- Modify: `src/web/templates/address_book.html:7-12`
- Modify: `src/web/templates/ae-dashboard.html:7-12`
- Modify: `src/web/templates/ae-dashboard-personal.html:7-12`

Update the address-book, AE dashboard, and personal AE dashboard breadcrumbs to use the macro for consistency (clickable parent links).

- [ ] **Step 1: Update address-book breadcrumb**

**Current code:**
```html
{% block breadcrumb %}
<span class="breadcrumb-separator">›</span>
<span>Data Management</span>
<span class="breadcrumb-separator">›</span>
<span class="breadcrumb-current">Address Book</span>
{% endblock %}
```

**New code (add import at top of file, after extends):**
```html
{% from "macros/breadcrumbs.html" import breadcrumb_trail %}
```

Then replace the breadcrumb block:
```html
{% block breadcrumb %}
{{ breadcrumb_trail([("Data Management", "")], "Address Book") }}
{% endblock %}
```

Note: "Data Management" has empty URL (`""`) since there's no landing page for that category. The macro's `{% if url %}` conditional renders it as a plain `<span>` instead of a link. This matches the original behavior where it was a non-clickable label.

- [ ] **Step 2: Update AE dashboard breadcrumb**

**Current code:**
```html
{% block breadcrumb %}
<span class="breadcrumb-separator">›</span>
<span>Reporting</span>
<span class="breadcrumb-separator">›</span>
<span class="breadcrumb-current">AE Performance Dashboard</span>
{% endblock %}
```

**New code (add import at top of file, after extends):**
```html
{% from "macros/breadcrumbs.html" import breadcrumb_trail %}
```

Then replace the breadcrumb block:
```html
{% block breadcrumb %}
{{ breadcrumb_trail([("Reporting", "/reports")], "AE Performance Dashboard") }}
{% endblock %}
```

- [ ] **Step 3: Update personal AE dashboard breadcrumb**

`ae-dashboard-personal.html` has the same static breadcrumb pattern. Add the import and replace the breadcrumb block identically to the main AE dashboard (Step 2 above).

- [ ] **Step 4: Verify all pages have consistent breadcrumbs**

Visit each page and confirm breadcrumb format is consistent:
- `/address-book` → `Home › Data Management › Address Book`
- `/ae-dashboard` → `Home › Reporting › AE Performance Dashboard`
- `/reports/customer/1` → `Home › Reporting › Customer Detail` (default)
- `/reports/customer/1?from=address-book` → `Home › Data Management › Address Book › [Name]`
- `/customer-merge` → `Home › Data Management › Customer Merge`

- [ ] **Step 5: Commit**

```bash
git add src/web/templates/address_book.html src/web/templates/ae-dashboard.html src/web/templates/ae-dashboard-personal.html
git commit -m "refactor: use breadcrumb macro consistently across all entity pages"
```

---

## Summary

| Task | Description | Files Changed |
|------|-------------|---------------|
| 1 | Create breadcrumb Jinja macro | 1 created |
| 2 | Pass `from_page` context in customer detail route | 1 modified, 1 created |
| 3 | Context-aware breadcrumbs on customer detail | 1 modified |
| 4 | "View Full Detail" button in address-book modal | 1 modified |
| 5 | "View in Address Book" links on AE dashboards | 2 modified |
| 6 | Post-resolve links on customer merge | 1 modified |
| 7 | Consistent breadcrumb macro on remaining pages | 3 modified |

**Total:** 1 new file, 1 new test file, 8 modified files. Pure template/route changes — no service layer or database changes.
