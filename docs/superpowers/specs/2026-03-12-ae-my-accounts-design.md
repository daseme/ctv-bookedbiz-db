# AE My Accounts — Design Spec

**Date:** 2026-03-12
**Status:** Draft
**Parent:** Address Book v2 Phase 4

## Problem Statement

AEs have no single page that shows their book of business with CRM-style
workflow. The existing AE personal dashboard (`/reports/ae-dashboard-personal`)
focuses on revenue/forecast numbers. The address book has activity logging
and follow-up features, but they're buried in a modal behind a page that
shows all entities for all AEs. Adoption of activity logging is near zero.

This spec defines a standalone AE-facing page that serves as a CRM testing
ground — scoped to the AE's assigned accounts, with activity logging and
follow-ups as first-class features.

## Goals

1. Give AEs a daily-driver page they open every morning to see their full
   book of business at a glance
2. Make activity logging low-friction enough to replace spreadsheet tracking
3. Surface signals (churning, declining, quiet accounts) alongside follow-ups
   so AEs know where to focus
4. Reuse existing APIs and data — no new tables or migrations

## Relationship to Parent Spec

The parent spec (Address Book v2, Phase 4, Section 4b) described the AE
landing page as an extension of the existing `/reports/ae-dashboard` route.
This spec departs from that plan: the AE CRM page is a standalone page at
`/ae/my-accounts` under its own blueprint. The existing AE dashboards
remain unchanged. This avoids coupling CRM workflows to the revenue-focused
dashboard and gives us a clean testing ground.

## Non-Goals

- Replacing the existing AE personal dashboard (revenue/forecast stays there)
- Building a full sales pipeline or deal tracker
- Modifying the address book itself
- New database tables (minor column additions are acceptable)

---

## 1. Page Structure

**Route:** `GET /ae/my-accounts`
**Blueprint:** New `ae_crm` blueprint, registered in `initialize_blueprints()`

### 1a. Summary Bar

Horizontal stats strip across the top of the page.

| Stat | Source |
|------|--------|
| Active account count | Count of entities with `assigned_ae` matching AE |
| Trailing 12-month revenue | Live query against spots (last 12 broadcast months) |
| Accounts needing attention | Count of entities with active signals |
| Open follow-ups | Count of incomplete follow-ups (overdue count highlighted) |

### 1b. Action Items

Compact section below the summary bar showing what needs doing today.

- Overdue follow-ups (red) and today's follow-ups, sorted by urgency
- Each row: customer name, description, due date, one-click "complete" button
- Completing a follow-up prompts "Follow up again?" with optional date picker
- Section hidden entirely when zero follow-ups are due today or overdue.
  Future follow-ups (due later) do not cause the section to appear — they
  show in the accounts table's "Next Follow-up" column instead.

### 1c. Accounts Table

Main content area. One row per assigned account (customers and agencies).

| Column | Content |
|--------|---------|
| Name | Account name (clickable, opens detail panel) |
| Type | Badge: customer or agency |
| Signal | Signal badge if active (churning, declining, gone_quiet, etc.) |
| Trailing Revenue | Last 12 months, live query against spots |
| Last Activity | Date of most recent activity logged by AE |
| Next Follow-up | Due date + description of next open follow-up |
| Actions | Quick "log activity" button |

**Default sort:** Signal priority descending (needs-attention accounts first),
then by trailing revenue descending.

**Search/filter:** Text search bar above the table filtering by account name.

### 1d. Recent Activity Feed

Below the accounts table. Last 15 activities across all the AE's accounts.

- Reverse chronological
- Each entry: date, account name, activity type badge, description snippet
- Provides "what happened recently" context at a glance

---

## 2. Detail Panel

Clicking an account name opens a **right slide-out panel**. The accounts
table remains visible on the left, keeping context while working with a
specific account.

### Panel Header

- Account name + type badge
- Signal badge (if active)
- Trailing 12-month revenue
- Link to full detail page: `/customer-detail/<id>` for customers,
  or address book modal link for agencies (no agency detail page exists)

### Panel Body — Three Tabs

**Tab 1: Activity Timeline (default)**

- Reverse chronological list of all activities for this account
- Color-coded type badges: note (gray), call (blue), email (green),
  meeting (purple)
- Each entry: date, type, description, who logged it
- Follow-ups show due date and completion status
- **Sticky quick-add form at top** (always visible):
  - Activity type dropdown (note/call/email/meeting)
  - Description text field (single line, expands on focus)
  - Optional follow-up date (date picker, hidden by default, toggle to show)
  - Submit button
  - Only description is required — minimum friction

**Tab 2: Account Info (read-only)**

- Primary contact: name, phone (click-to-call), email (click-to-email)
- Sector assignments
- Parent agency (if customer has one)
- Markets served
- Assigned-since date

**Tab 3: Revenue Snapshot**

- Trailing 12-month total
- Mini bar chart showing monthly revenue for last 12 months (Chart.js,
  consistent with existing project patterns)
- YoY comparison if prior year data exists

### Panel Interactions

- Completing a follow-up from Action Items auto-opens the detail panel
  for that account with the "Follow up again?" prompt
- Panel remembers which account was last open during the session
  (sessionStorage), re-opens on page refresh
- Escape key or click-outside closes the panel

---

## 3. Data & API

### No New Tables

All data reads from existing tables. The `entity_metrics` table has
`total_revenue` (all-time) but NOT trailing 12-month revenue. The new
service computes trailing revenue with a live query against spots —
acceptable for the small result set (one AE's accounts).

| Data | Source |
|------|--------|
| Account list | New query in `ae_crm_service` with `assigned_ae` filter |
| Signals | `entity_signals` table, joined in the account list query |
| Trailing revenue | Live query: spots for last 12 broadcast months per entity |
| Activities | `activity_service.get_activities()` per entity |
| Follow-ups | `activity_service.get_follow_ups()` (modified, see below) |
| Recent activity | New `activity_service.get_recent_activity_for_ae()` method |
| Revenue trend | Spots table, monthly gross_rate by entity |

### New Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `GET /ae/my-accounts` | GET | Render the page |
| `GET /api/ae/my-accounts` | GET | Account list JSON with metrics/signals |
| `GET /api/ae/my-accounts/stats` | GET | Summary bar aggregates |
| `GET /api/ae/my-accounts/<type>/<id>/revenue-trend` | GET | Monthly revenue for sparkline (last 12 months) |

### Reused Endpoints (no changes)

| Endpoint | Purpose |
|----------|---------|
| `GET /api/address-book/<type>/<id>/activities` | Activity timeline |
| `POST /api/address-book/<type>/<id>/activities` | Create activity |
| `POST /api/address-book/activities/<id>/complete` | Toggle completion |

### Modifications to Existing Code

**1. `activity_service.py` — add AE-scoped methods:**

- `get_follow_ups(ae_name=None)`: Add optional `ae_name` parameter.
  When provided, JOIN entity table and filter by `assigned_ae`. Current
  callers pass no argument (backward compatible). The route endpoint
  `GET /api/address-book/follow-ups` gains an optional `?ae=` query
  parameter.

- `get_recent_activity_for_ae(ae_name, limit=15)`: New method. Queries
  `entity_activity` joined to customers/agencies on `(entity_type,
  entity_id)` filtered by `assigned_ae`. Returns last N activities
  across all of the AE's accounts.

**2. `ae_crm_service.py` — new service (does NOT call `list_entities()`):**

The existing `entity_service.list_entities()` loads all entities for all
AEs with multiple batch queries — wasteful for a single AE's view.
Instead, `ae_crm_service` has its own targeted query that fetches only
entities where `assigned_ae` matches, joining `entity_signals` and
computing trailing revenue inline. This keeps the page fast.

---

## 4. Auth & Access Control

### Access Rules

| Role | Sees | Can Do |
|------|------|--------|
| AE | Only their assigned accounts | Log activities, manage follow-ups, view account info |
| Management | All accounts, AE selector dropdown | Same as AE for any selected AE |
| Admin | Same as Management | Same as Management |
| Viewer | No access (redirect) | N/A |

### AE Identity Resolution

- Match `current_user.full_name` against `assigned_ae` on entities
- This relies on the user's display name exactly matching the string
  stored in `assigned_ae`. If mismatches arise in practice, we add
  an explicit mapping (e.g., `username` field in ae_config.json) —
  but we start simple and see if it works.
- If no match: empty state with message "No accounts assigned. Contact
  your manager."
- Admin/Management get an AE selector dropdown (same pattern as
  ae-dashboard-personal)

### Permissions

- Route uses `@role_required(UserRole.AE)` decorator from
  `src/web/utils/auth.py` — this grants access to AE, Management,
  and Admin roles while blocking Viewer
- Any user who can view the page can log activities on visible accounts
- Entity edits (name, sector, billing) remain admin-only via address book
- No changes to auth middleware needed

---

## 5. Tech Stack

- **Template:** New `ae_my_accounts.html` with Nord-themed CSS (consistent
  with existing pages)
- **JavaScript:** Separate `ae_my_accounts.js` file (no inline JS)
- **Charts:** Chart.js for revenue sparkline (already a project dependency)
- **Blueprint:** New `ae_crm_bp` registered in `initialize_blueprints()`
- **Service layer:** New `ae_crm_service.py` that composes calls to
  `entity_service`, `activity_service`, and direct DB queries for
  stats/revenue-trend

---

## 6. Implementation Phases

This spec is implemented as a single phase with incremental commits:

1. **Blueprint + route + empty template** — page loads, auth works, AE
   selector works for admins
2. **Summary bar + accounts table** — data flows from entity_service,
   sortable table with signal badges
3. **Detail panel with activity timeline** — slide-out panel, activity
   CRUD using existing endpoints
4. **Action items section** — follow-ups with AE filter, completion flow
5. **Revenue snapshot tab** — sparkline chart, YoY comparison
6. **Recent activity feed** — cross-account activity stream
7. **Polish** — empty states, loading indicators, session persistence

Each commit is independently shippable — the page improves incrementally.

---

## 7. Empty & Error States

| Scenario | Behavior |
|----------|----------|
| AE has no assigned accounts | Summary bar shows zeros, accounts table shows "No accounts assigned. Contact your manager." |
| AE has accounts but no activities logged | Activity feed says "No activity yet. Log your first note or call." |
| AE has accounts but no follow-ups | Action items section hidden, "Next Follow-up" column shows "—" |
| Revenue trend for new account (no spots) | Tab 3 shows "No revenue data yet" instead of empty chart |
| API error loading detail panel | Toast notification with error message, panel stays closed |

---

## 8. Testing Strategy

### Service Tests

- `ae_crm_service`: account list scoping, stats aggregation, revenue trend
- Verify AE filter returns only assigned accounts
- Verify admin with no AE filter returns all accounts

### Integration Tests

- Page renders for AE role user
- Page renders for admin with AE selector
- Viewer role gets redirected
- Follow-ups endpoint respects `?ae=` filter
- Activity creation via existing endpoint works from new page context

### Manual Verification

- AE logs in, sees only their accounts
- Admin switches between AEs via dropdown
- Log activity from detail panel, verify it appears in timeline and
  recent activity feed
- Create follow-up, see it in action items, complete it, verify
  "follow up again" flow
- Revenue sparkline renders correctly
