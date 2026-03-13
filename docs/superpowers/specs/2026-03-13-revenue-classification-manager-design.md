# Revenue Classification Manager — Design Spec

## Problem

The board and sales management need to understand revenue in two buckets: **regular** (recurring, predictable — casinos, auto dealers, healthcare) and **irregular** (cyclical or one-time — political campaigns, COVID-era government spending). Today there's no way to distinguish these in the system. Every dollar looks the same in reports, which makes forecasting unreliable and board reporting imprecise.

## Solution

1. Add a `revenue_class` field to every customer (`regular` or `irregular`)
2. Build a management tool at `/reports/revenue-classification-manager` that shows revenue broken out by classification, lets managers reclassify customers, and provides the year-over-year comparison that tells the predictability story

## Data Model

### Migration: `sql/migrations/026_revenue_classification.sql`

Add column to `customers` table:

```sql
ALTER TABLE customers ADD COLUMN revenue_class TEXT DEFAULT 'regular'
    CHECK (revenue_class IN ('regular', 'irregular'));
```

Seed from sector:

```sql
UPDATE customers SET revenue_class = 'irregular'
WHERE sector_id IN (
    SELECT sector_id FROM sectors
    WHERE sector_code IN ('POLITICAL', 'POLITICAL-OUTREACH', 'POLITICALOUTREACH')
);
```

All other customers default to `'regular'` via the column default.

### No new tables

The classification lives on the customer record. Revenue data comes from the existing `spots` table aggregated at query time.

## Page: `/reports/revenue-classification-manager`

### Access Control

`@role_required(UserRole.MANAGEMENT)` — same as the Manager Dashboard.

### Layout

Four sections top to bottom:

1. **Summary bar** — 4 stat cards
2. **Controls row** — year selector, filters, search
3. **Monthly chart** — grouped bars by classification
4. **Customer table** — all customers with inline classification toggle

### Summary Bar (4 cards)

| Card | Value | Source |
|------|-------|--------|
| Regular Revenue | Dollar total for selected year | SUM of gross_rate from spots where customer.revenue_class = 'regular' |
| Irregular Revenue | Dollar total for selected year | SUM of gross_rate from spots where customer.revenue_class = 'irregular' |
| Regular % of Total | Percentage | regular / (regular + irregular) * 100 |
| Unclassified | Customer count | Customers where revenue_class IS NULL (should be 0 after migration, covers future) |

Revenue queries:
- Include all booked months for the selected year (historical AND forward-booked, `is_historical = 0` is NOT filtered)
- Exclude Trade: `WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)`
- Filter by broadcast_month year matching the selected year
- Only include spots with a non-NULL `customer_id` (unresolved spots are excluded from classification revenue)

### Controls Row

| Control | Behavior |
|---------|----------|
| **Year selector** | Dropdown of available years (derived from distinct broadcast_month year suffixes in the spots table). Defaults to current year. Changes update all sections. Populated by the summary API which returns an `available_years` list. |
| **Sector filter** | Dropdown of all sectors. Filters table and recalculates summary/chart for filtered subset. |
| **AE filter** | Dropdown of assigned AEs. Same filtering behavior. |
| **Classification filter** | All / Regular / Irregular. Filters table, summary adjusts to show filtered totals. |
| **Search** | Text input, filters customer table by name. |

### Monthly Revenue Chart

Chart.js grouped bar chart. One group per broadcast month in the selected year.

- **Blue bars**: regular revenue
- **Amber bars**: irregular revenue
- X-axis: broadcast months (Jan through Dec)
- Y-axis: dollar amount
- Includes forward-booked months (not just closed/historical)
- Tooltip shows exact dollar amounts
- Responds to all filters (sector, AE, classification)

### Customer Table

Columns:

| Column | Content | Sortable |
|--------|---------|----------|
| Customer | `normalized_name`, links to address book entity detail | Yes (alpha) |
| Sector | Primary sector name | Yes (alpha) |
| Classification | Inline toggle button: Regular / Irregular. Click to switch, saves via API immediately. | Yes |
| AE | `assigned_ae` | Yes (alpha) |
| {Selected Year} Revenue | Total booked gross_rate for selected year | Yes (numeric) |
| {Prior Year} Revenue | Total booked gross_rate for prior year | Yes (numeric) |
| YoY Change | Dollar difference and percentage. Green if positive, red if negative. | Yes (numeric) |

Table features:
- Client-side sorting on all columns
- Search filters by customer name
- Sector/AE/classification filters apply
- Shows all active customers (not just those with revenue)

### API Endpoints

All under the existing reports blueprint or a new classification blueprint:

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/revenue-classification/summary?year=YYYY` | Summary stats + monthly chart data |
| GET | `/api/revenue-classification/customers?year=YYYY` | Customer list with revenue for selected year and prior year |
| PATCH | `/api/revenue-classification/<int:customer_id>` | Update a customer's revenue_class. Body: `{"revenue_class": "regular"}` |

### Service: `RevenueClassificationService`

Methods:

**`get_summary(conn, year, filters=None)`**
Returns summary cards data, monthly breakdown for the chart, and available years list. Single query groups by broadcast_month and revenue_class, with optional filtering. Also queries distinct years from spots for the year dropdown.

**`get_customers(conn, year, filters=None)`**
Returns customer list with revenue for the selected year and prior year. Joins customers with aggregated spots data. Two subqueries: one for selected year revenue, one for prior year. YoY percentage is `None` when prior year revenue is zero (displayed as "New" in the UI).

**`update_classification(conn, customer_id, revenue_class)`**
Updates the customer's `revenue_class` field. Validates the value is 'regular' or 'irregular'.

**`filters` parameter shape** (applies to `get_summary` and `get_customers`):
```python
filters = {
    "sector_id": int | None,      # filter by sector
    "ae": str | None,             # filter by assigned_ae (exact match)
    "classification": str | None,  # 'regular' or 'irregular'
}
```
All keys are optional. `None` or missing key means no filter on that dimension.

### Broadcast Month to Year Mapping

Revenue is stored by broadcast_month (`'Jan-25'`, `'Feb-25'`, etc.). To filter by year, extract the year suffix:

```sql
CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) = :year
```

### Template and JavaScript

**Template:** `src/web/templates/revenue_classification_manager.html`
- Extends `base.html`
- Light theme (white cards, same as sector manager and AE dashboard)
- Summary bar, controls, chart canvas, table
- Guide modal (? button) explaining the regular/irregular concept and how to use the tool

**JavaScript:** `src/web/static/js/revenue_classification_manager.js`
- Loads summary + customer data on page load
- Chart.js grouped bar chart
- Client-side filtering and sorting
- Inline classification toggle with optimistic update (PATCH, revert on failure)
- Year selector triggers full data reload

### Navigation

Add to the Reporting dropdown in `base.html`, in the Data Management section near the Customer Sector Manager link. Visible to management/admin roles.

## File Structure

| Action | File |
|--------|------|
| Create | `sql/migrations/026_revenue_classification.sql` |
| Create | `src/services/revenue_classification_service.py` |
| Create | `tests/services/test_revenue_classification_service.py` |
| Create | `src/web/templates/revenue_classification_manager.html` |
| Create | `src/web/static/js/revenue_classification_manager.js` |
| Modify | `src/web/routes/reports.py` (add route + API endpoints) |
| Modify | `src/services/factory.py` (register service) |
| Modify | `src/web/templates/base.html` (nav link) |

## Out of Scope

- Bulk reclassification (can be added later if needed)
- Revenue class on agencies (agencies inherit from their customers' mix)
- Historical tracking of classification changes (simple field, not audited)
- Export/CSV (can be added later)
