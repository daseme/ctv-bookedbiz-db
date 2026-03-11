# Address Book v2 — Design Spec

**Date:** 2026-03-11
**Status:** Draft

## Problem Statement

The address-book (`/address-book`) is the primary directory for managing agencies
and advertisers. It has grown into a 2,200-line route file with all SQL inline,
a 3,150-line template with ~2,100 lines of inline JavaScript, and overlapping
functionality with three other pages (customer detail, AE dashboard, customer
merge). A planned
rollout to Account Executives as real users demands a solid foundation.

This spec covers: architectural cleanup, navigation improvements, a hierarchical
entity model, and an AE-facing experience.

## Goals

1. Extract a shared service layer so address-book and customer-detail use
   identical data paths — no more inconsistent numbers
2. Establish clear navigation between all entity-related pages
3. Present entities hierarchically: agencies and direct advertisers at top level,
   agency clients nested underneath
4. Build an AE landing page and scoped access model
5. Make activity logging low-friction enough to replace spreadsheets

## Non-Goals

- Rebuilding the UI framework or switching CSS/JS libraries
- Building a full sales pipeline / deal tracking CRM
- Consolidating all pages into one — each page keeps its purpose
- Major database schema changes (existing tables cover the data model; minor
  additions like `username` on AE config are in scope)

---

## 1. Architecture: Service Layer Extraction

### Current State

`src/web/routes/address_book.py` (~2,200 lines) contains:
- Route handlers (HTTP/JSON layer)
- All SQL queries inline (~130+ SQL statements)
- Business logic (metric computation, signal calculation, fuzzy matching)
- Formatting helpers (`_fmt_revenue`, `_client_portion`)

`src/web/routes/customer_detail_routes.py` duplicates some of the same queries
(revenue, contacts, AE data) with different SQL producing potentially different
numbers.

### Target State

**New services:**

| Service | Responsibility | Consumers |
|---------|---------------|-----------|
| `entity_service.py` | CRUD for agencies and customers: create, read, update, deactivate, reactivate. Sector and agency assignment. | address-book routes, customer-detail routes |
| `entity_metrics_service.py` | Compute and cache `entity_metrics` and `entity_signals`. Revenue queries, spot counts, market lists, signal thresholds. | address-book routes, AE dashboard, broadcast month import |
| `activity_service.py` | Activity CRUD, follow-up management, completion tracking. | address-book routes, AE landing page |

**Existing services (keep, extend where noted):**

| Service | Notes |
|---------|-------|
| `contact_service.py` | Exists as standalone module but NOT yet integrated into address-book routes (which use inline SQL for contacts). Phase 1 wires address-book to use this service. |
| `customer_resolution_service.py` | Merge, aliases. No changes needed. |
| `ae_service.py` | Loads AE config, provides filtered AE list and revenue data. Keep; evaluate overlap with `ae_dashboard_service` — may consolidate. |
| `ae_dashboard_service.py` | YoY customer performance analysis. Extend to support AE landing page data (signals, follow-ups). |
| `customer_detail_service.py` | Refactor to call `entity_service` and `entity_metrics_service` instead of its own SQL. |

**Route files after extraction:**

Each route file becomes a thin HTTP layer:
- Validate request parameters
- Call service methods
- Format response (JSON or render template)
- No SQL, no business logic

Target: each route handler under 30 lines. Route file total under 500 lines.

### Critical: Untangle Cross-Layer Import

`broadcast_month_import_service.py` currently imports `refresh_entity_metrics`
and `refresh_entity_signals` directly from the route file — a service importing
from a route. Phase 1 must move these functions to `entity_metrics_service.py`
first and update the import in `broadcast_month_import_service.py` before
touching anything else. Breaking this import breaks the spots import pipeline.

### Formatting Helpers

`_fmt_revenue()` and `_client_portion()` move to `src/utils/formatting.py` (or
the existing `src/utils/template_formatters.py` if the scope overlaps). Usable
by any route or template.

### JavaScript Extraction

All inline JS in `address_book.html` moves to
`src/web/static/js/address_book.js`. The template includes it via a script tag.
Same pattern for any other templates with substantial inline JS.

---

## 2. Navigation: Cross-Linking Pages

### Current State

Pages are islands. The address-book detail modal has no link to customer-detail.
Customer-detail has no link back to the address-book. The AE dashboard
(existing at `/ae-dashboard`, with a personal variant at `/ae-dashboard-personal`)
shows YoY customer performance but doesn't link to customer-detail or the
address-book.

### Target State

**Address-book detail modal:**
- "View Full Detail" button links to `/customer-detail/<id>` (or
  `/agency-detail/<id>` when that exists)
- Modal stays lightweight: quick edits, contact management, activity log snippet

**Customer detail page:**
- "Back to Address Book" breadcrumb/link
- If accessed from AE dashboard, breadcrumb shows
  "AE Dashboard > [Customer Name]"

**AE dashboard:**
- Each customer row links to customer-detail
- "View in Address Book" link opens address-book filtered to that AE

**Customer merge page:**
- After resolving a bill code, link to the customer's address-book detail modal
  or customer-detail page

### Implementation

Add a simple breadcrumb component that reads a `?from=` query parameter to
construct the back-link. No framework needed — a Jinja macro that renders
`<nav class="breadcrumb">`.

---

## 3. Hierarchical Entity List

### Current State

Agencies and customers appear in a flat mixed list. Filtering by type helps,
but the relationship between an agency and its clients isn't visible in the
list view.

### Target State

The address-book list shows two kinds of top-level items:
1. **Agencies** — expandable to show their linked customers
2. **Direct advertisers** — customers with no `agency_id`

**Card view:**
- Agency cards show a client count badge and are expandable
- Clicking the expand control reveals client cards nested underneath
- Direct advertiser cards appear at the same level as agency cards

**Table view:**
- Agency rows are expandable (tree-table pattern)
- Client rows are indented underneath their agency
- Direct advertiser rows appear at the top level

**Bulk view:**
- Flat list (current behavior) for bulk operations — hierarchy complicates
  multi-select

### Data Changes

No schema changes needed. The hierarchy is derived from `customers.agency_id`:
- `agency_id IS NULL` = direct advertiser (top-level)
- `agency_id IS NOT NULL` = agency client (nested under parent)

### API Changes

The main list endpoint (`GET /api/address-book`) gains an optional
`?view=hierarchical` parameter. When set:
- Returns agencies with a `clients` array nested inside
- Returns direct advertisers as top-level items with `entity_type: "customer"`
- Default (no param) returns the current flat list for backward compatibility

### Filtering Behavior

When a filter matches a client but not its parent agency:
- The agency still appears (as a container) but is visually muted
- The matching client is shown expanded underneath
- This avoids orphaned results

When a filter matches an agency:
- The agency appears with all its clients (collapsed by default)
- Client-level filters (sector, signal) still apply within the expanded view

---

## 4. AE Experience

### 4a. Auth & Scoping

**Current state:** Auth middleware exists with a role model already in place.
`src/models/users.py` defines a `UserRole` enum with `ADMIN`, `MANAGEMENT`,
`AE`, and `VIEWER` roles, and `has_permission()` implements the hierarchy.
The address-book route already has `_require_admin_for_writes()` checking
`current_user.role`. However, there is no query-level scoping — all roles
see all entities.

**Target state:** Two effective access levels:
- **Admin/Management** — sees everything, can edit everything (current behavior)
- **AE** — sees only their assigned accounts, can edit activity/follow-ups
  on those accounts, read-only on entity details they don't own

**Implementation:**
- Role infrastructure already exists — no new fields needed on the user model
- AE config (`src/web/ae_config.json`) already maps AE names — extend with
  a `username` field to map login identity to AE name for query scoping
- Add query-level scoping: API endpoints that return entity lists add a
  `WHERE assigned_ae = :ae_name` clause for AE role users
- Verify `assigned_ae` is consistently populated on active entities; backfill
  gaps as a data task if needed

**Unassigned accounts:** Only visible to admins. AEs see a clean, focused
list of their own accounts.

### 4b. AE Landing Page

**Route:** `GET /ae-dashboard` (extends existing page)

**Layout — four sections:**

**Section 1: Attention Needed**
- Accounts with active signals (churned, declining, gone_quiet), sorted by
  signal priority
- Each row: customer name, signal badge, last active date, trailing revenue
- Click to open address-book detail modal or navigate to customer-detail

**Section 2: Upcoming Follow-ups**
- Open follow-ups sorted by due date (overdue first, highlighted)
- Each row: customer name, description, due date, urgency
- Quick "mark complete" action inline
- Click to open the activity detail

**Section 3: Recent Activity**
- Last 10 activities across all their accounts
- Shows: customer name, activity type, date, description snippet
- Provides a "what happened recently" at-a-glance view

**Section 4: Quick Stats**
- Active account count
- Total trailing 12-month revenue
- Accounts with signals count
- New accounts in last 90 days

**Bottom:** "View All My Accounts" button → address-book filtered to this AE

### 4c. AE Activity Workflow

The key design constraint: **less friction than a spreadsheet.**

**Existing infrastructure:** The `entity_activity` table already exists
(migrations 003 + 009) with support for notes, calls, emails, meetings,
follow-ups, due dates, and completion tracking. The address-book route file
already has endpoints for activity CRUD, completion toggling, and follow-up
listing. The UI in the detail modal wires to these endpoints. However, adoption
is essentially zero (1 note on 1 entity total). The problem is not missing
features — it's that the workflow isn't compelling enough to use. The changes
below focus on reducing friction and surfacing follow-ups prominently.

**Quick-add activity from anywhere:**
- The address-book detail modal already has an activity section
- One-line input: type dropdown (note/call/email/meeting) + text field + submit
- No separate page, no form wizard, no required fields beyond description
- Optionally link to a contact (dropdown, not required)
- Optionally set a follow-up date (date picker, not required)

**Follow-up flow:**
- When an activity has a follow-up date, it appears in the AE dashboard's
  "Upcoming Follow-ups" section
- Overdue follow-ups get a visual indicator (red badge)
- "Mark complete" is one click — adds a completion timestamp
- Completing a follow-up can optionally prompt for a new follow-up
  ("Follow up again?")

**Activity timeline in detail modal:**
- Reverse chronological list of all activities for the entity
- Color-coded by type (note=gray, call=blue, email=green, meeting=purple)
- Shows: date, type, description, who logged it, linked contact (if any)
- Follow-ups show completion status

---

## 5. What Stays, What Changes, What's Removed

### Stays As-Is (no changes)
- Card/table/bulk view toggling (bulk stays flat)
- Saved filters and filter presets
- CSV export/import (including contact CSV import via `/api/address-book/import-contacts`)
- Contact management (wired to `contact_service.py` in Phase 1)
- Address management (main + additional addresses)
- Commission rate handling with agency-to-customer inheritance
- Health signals computation (existing `refresh_entity_signals` logic)
- Activity/follow-up schema and endpoints (already built, extracted to service)
- Customer merge page (separate page, gains cross-links)

### Changes
- Route file: 2,200 lines → ~400 lines (logic moves to services)
- Template: ~2,100 lines of inline JS extracted to separate file
- Entity list: flat → hierarchical (agencies expand to show clients)
- Detail modal: gains "View Full Detail" link to customer-detail page
- Customer-detail page: refactored to use shared services, gains breadcrumbs
- AE dashboard: extended with follow-ups, signals, activity summary
- Activity section in modal: streamlined for low-friction logging

### Removed
- Duplicate SQL across route files (replaced by shared services)
- Inline JS in template (extracted to file)
- Any dead code discovered during extraction (formatting helpers that are
  unused, duplicate query functions, etc.)

---

## 6. Migration Strategy

This is not a big-bang rewrite. The work decomposes into independent phases
that each deliver value and can be shipped separately.

### Phase 1: Service Layer Extraction
Extract services from the address-book route file. Refactor customer-detail
to use the same services. All existing functionality continues to work
identically — this is a pure refactor with no UI changes.

**Critical first step:** Move `refresh_entity_metrics` and
`refresh_entity_signals` to `entity_metrics_service.py` and update the import
in `broadcast_month_import_service.py`. This unblocks the rest of the
extraction without risking the spots import pipeline.

**Verification:** All existing tests pass. Revenue numbers match between
address-book and customer-detail. Spots import pipeline still triggers metric
and signal refresh correctly.

### Phase 2: Navigation & Cross-Linking
Add breadcrumbs and cross-links between all entity-related pages. Extract
inline JS to separate file.

**Verification:** Can navigate from any entity page to any other related page
and back.

### Phase 3: Hierarchical List
Add the hierarchical view to the address-book. Agencies expand to show clients,
direct advertisers remain top-level.

**Verification:** All entities visible. Filtering works correctly with
hierarchy. No entities lost or duplicated.

### Phase 4: AE Landing Page & Scoped Access
Build the AE dashboard extension. Add role-based filtering. Wire up activity
logging in the detail modal.

**Verification:** AE sees only their accounts. Dashboard shows correct signals,
follow-ups, activity. Activity logging works end-to-end.

---

## 7. Data Flow

```
Spots Import (broadcast_month_import_service)
    │
    ├─► entity_metrics_service.refresh_metrics()
    │       └─► entity_metrics table (markets, revenue, spots, last_active)
    │
    └─► entity_metrics_service.refresh_signals()
            └─► entity_signals table (churned, declining, gone_quiet, new, growing)

Address Book List Request
    │
    ├─► entity_service.list_entities(filters, view=hierarchical)
    │       ├─► agencies + direct advertisers (top-level)
    │       └─► agency clients (nested)
    │
    ├─► JOIN entity_metrics (pre-computed)
    ├─► JOIN entity_signals (pre-computed)
    ├─► JOIN entity_contacts (primary contact)
    └─► Response: hierarchical entity list with metrics, signals, contacts

Detail Modal
    │
    ├─► entity_service.get_entity(type, id)
    ├─► contact_service.get_contacts(type, id)
    ├─► activity_service.get_activities(type, id)
    ├─► entity_service.get_addresses(type, id)
    └─► "View Full Detail" → /customer-detail/<id>

Customer Detail Page
    │
    ├─► entity_service.get_entity(type, id)
    ├─► entity_metrics_service.get_revenue_trend(id)
    ├─► entity_metrics_service.get_breakdowns(id)  [AE, market, language]
    ├─► contact_service.get_contacts(type, id)
    └─► Breadcrumb back to address-book

AE Landing Page
    │
    ├─► entity_metrics_service.get_signals(ae_name)
    ├─► activity_service.get_follow_ups(ae_name)
    ├─► activity_service.get_recent_activity(ae_name)
    ├─► entity_metrics_service.get_ae_stats(ae_name)
    └─► "View All Accounts" → /address-book?ae=<name>
```

---

## 8. Testing Strategy

### Existing Test Preservation
All existing tests must continue passing throughout the refactor. Run the full
test suite after each phase to catch regressions. Key test files:
`tests/test_backfill_triggers.py` (13 tests for alias/spot triggers).

### Service Layer Tests
Each new service gets unit tests with the dev database:
- `entity_service`: CRUD operations, deactivation, agency assignment
- `entity_metrics_service`: metric computation correctness, signal thresholds
- `activity_service`: activity CRUD, follow-up completion, date filtering

### Integration Tests
- Address-book list endpoint returns correct hierarchical structure
- Filtering with hierarchy: agency appears when child matches
- AE scoping: AE user only sees assigned accounts
- Cross-page data consistency: same entity shows same revenue everywhere

### Manual Verification
- Navigate the full loop: address-book → modal → customer detail → back
- AE dashboard → customer detail → address-book → back to dashboard
- Activity logging round-trip: create, view in timeline, mark follow-up complete
