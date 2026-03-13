# CRM Enhancements — Signal-Driven Workflow + Manager Dashboard

## Goal

Transform the AE My Accounts page from a passive dashboard into an action-driven CRM workflow. Surface renewal risk with dollar amounts, enforce touch cadence accountability, and give the sales manager a single page to monitor the team.

## Context

- **Users:** 2 AEs (Charmaine Lane, House), 3 managers/admins
- **Sales motion:** Hybrid with renewals — most revenue comes from existing account expansions and renewals, supplemented by new business
- **Pain points:** (1) Renewal risk invisible until too late, (2) no activity accountability, (3) AEs don't know which accounts to prioritize
- **Data foundation:** 381K active spots, 384 active entities, 126 computed signals (unused in workflow), activity infrastructure in place but nearly empty (1 record), budget tables, contacts

## Architecture

Three phases, each delivering standalone value. All phases build on the existing My Accounts page (`/ae/my-accounts`) and existing services (AeCrmService, ActivityService, EntityMetricsService).

---

## Phase 1: Signal Queue + Renewal Gaps

### 1a. Signal Actions Table

New table `signal_actions` tracks the lifecycle of each signal as a work item.

Note: `entity_signals` has a composite PK `(entity_type, entity_id, signal_type)` — no `signal_id` column. The signal_actions table references signals via the same composite key.

```sql
CREATE TABLE signal_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    assigned_ae TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'acknowledged', 'snoozed', 'dismissed')),
    reason TEXT,               -- required for snooze/dismiss
    snooze_until DATE,         -- when snoozed, wake-up date
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT
);
CREATE INDEX idx_signal_actions_ae_status ON signal_actions(assigned_ae, status);
CREATE INDEX idx_signal_actions_entity ON signal_actions(entity_type, entity_id, signal_type);
```

**Lifecycle:**

- **New:** Created when entity_signals refreshes and a signal exists without a corresponding open signal_action.
- **Acknowledged:** Set automatically when any activity is logged against the account (via ActivityService). AE doesn't click a separate button — doing their job closes the loop.
- **Snoozed:** AE explicitly defers with a reason and a date. Reverts to "new" when snooze_until passes.
- **Dismissed:** AE marks as not actionable with a reason (e.g., "seasonal pattern"). Stays dismissed until the signal itself changes.

**Signal-to-action sync:** Runs at signal refresh time (import pipeline), **after** the existing `refresh_signals()` delete-and-reinsert completes. The sync uses a diff approach:

**Important:** `refresh_signals()` does `DELETE FROM entity_signals` then re-inserts all computed signals. This means signal_actions cannot use foreign keys to entity_signals — the composite key reference is logical, not enforced. The sync logic handles consistency:

1. **Snapshot before refresh:** Before `refresh_signals()` runs, capture the set of current `(entity_type, entity_id, signal_type)` tuples from entity_signals.
2. **After refresh:** Compare new entity_signals rows against the snapshot.
3. **New signals** (in new set, not in old): Create signal_action with status = "new" if no open action already exists for that composite key.
4. **Removed signals** (in old set, not in new): Mark any open signal_actions for that key as "acknowledged" (entity recovered).
5. **Unchanged signals:** Leave existing signal_actions alone.

**Expired snooze handling:** At sync time (and at query time), run `UPDATE signal_actions SET status = 'new' WHERE status = 'snoozed' AND snooze_until < date('now')` before evaluating which actions are open. This ensures sync logic correctly sees expired snoozes as needing a new action, and query results always reflect current state.

### 1b. Renewal Gap Detection

Computed at query time from spots data. No new table needed.

**Definition:** An account has a renewal gap when:
- Trailing 3 broadcast months have revenue > $0
- Forward 3 broadcast months have revenue = $0 (or < 25% of trailing)

**New signal type:** `renewal_gap` added to entity_signals at refresh time, with:
- `signal_label`: "Renewal gap: $X trailing, $0 forward"
- `signal_priority`: 1 (highest — money is about to stop)
- `trailing_revenue`: sum of trailing 3 broadcast months (note: other signal types use 12-month trailing in this column — the column is repurposed for renewal_gap to reflect the at-risk amount over the comparison window)

**Broadcast month resolution:** The renewal gap query uses the same `broadcast_month` format ('Jan-25') and CASE-based date conversion already used in `AeCrmService.get_revenue_trend()`. "Trailing 3" and "forward 3" are relative to the current calendar month mapped to broadcast month format. No broadcast month boundary table is needed — the existing pattern works.

This integrates directly into the signal queue — a renewal gap becomes a signal action the AE must work.

**Revenue At Risk:** Sum of trailing_revenue for all renewal_gap signals. Displayed as a new card on My Accounts summary bar and on the manager dashboard (Phase 3).

### 1c. Signal Queue UI

Replaces (or augments) the current Action Items section on My Accounts. Shows all signal_actions with status = "new" for the current AE.

**Display per item:**
- Signal type badge + label (e.g., "Churned — $12K trailing" or "Renewal gap: $8K trailing, $0 forward")
- Account name (clickable, opens detail panel)
- Days since signal surfaced (from created_date)
- Aging indicator: 0-3 days normal, 4-7 yellow, 8+ red
- Action buttons: "Snooze" (opens date picker + reason field), "Dismiss" (requires reason)

**Existing Action Items (overdue follow-ups)** remain in a separate section below the signal queue. Follow-ups are task reminders; signals are account health alerts. Different workflows.

**Sort order:** By signal_priority ASC (renewal_gap first), then by days aging DESC.

### 1d. Auto-Acknowledge Hook

When `ActivityService.create_activity()` succeeds **and the activity_type is one of note, call, email, or meeting**, check if the entity has any signal_actions with status = "new". If so, update them to "acknowledged" with updated_by = the activity creator.

Excluded from auto-acknowledge: `status_change` (system-generated, not an AE action) and `follow_up` (scheduling a reminder is not the same as making contact — consistent with Phase 2c touch counting rules).

The hook only acknowledges signals on the specific entity. Logging activity on a customer does not acknowledge signals on its linked agency, or vice versa. Each entity's signals are independent.

This is the key design choice: AEs don't manage signals as a separate task list. They work their accounts normally (log calls, notes, meetings) and the signal queue clears itself.

---

## Phase 2: Health Score + Touch Cadence

### 2a. Account Health Score

Computed live at page load. No caching needed for 384 entities.

**Formula (0-100):**

| Factor | Weight | Scoring |
|---|---|---|
| Revenue trend | 30% | trailing_3mo vs prior_3mo. >= +10% = 100, flat = 60, -10% to -25% = 40, -25% to -50% = 20, < -50% = 0 |
| Signal state | 25% | none/growing = 100, new_account = 80, gone_quiet = 40, declining = 25, churned/renewal_gap = 0 |
| Last touch | 25% | days since last activity (note/call/email/meeting only). <7d = 100, 8-14d = 75, 15-30d = 50, 31-60d = 25, >60d = 0. Uses `MAX(activity_date)` from entity_activity, computed in the health score query — no new ActivityService method needed. |
| Follow-up compliance | 20% | no follow-ups or all on time = 100, has any overdue = 0. Intentionally binary — one overdue follow-up drops the score to zero for this factor, because an overdue follow-up means a broken promise to yourself. |

**Display:** New column on My Accounts table. Color-coded circle: green (70-100), yellow (40-69), red (0-39). Default sort changes to health score ascending (worst accounts first).

### 2b. Account Tiering

Auto-assigned by trailing 12-month revenue rank within each AE's book:

| Tier | Percentile | Expected touch cadence |
|---|---|---|
| A | Top 20% by revenue | Every 7 days |
| B | Middle 40% | Every 14 days |
| C | Bottom 40% | Every 30 days |

Computed at query time based on account revenue rank. No stored tier field — always current.

### 2c. Touch Cadence Tracking

**What counts as a touch:** Any completed activity (note, call, email, meeting). Follow-ups only count when completed. Status_change doesn't count.

**Display on My Accounts table:**
- "Days Since Touch" column replacing or augmenting "Last Activity"
- Color coding relative to the account's tier cadence: green (within window), yellow (>75% elapsed), red (overdue)
- Tier badge (A/B/C) shown in a small column or as part of the account name row

**Touch compliance metric per AE:** percentage of accounts where days_since_touch <= tier cadence window. Single number for manager dashboard.

---

## Phase 3: Manager Dashboard

### 3a. Page & Access

- Route: `/manager/dashboard`
- Auth: `@role_required(UserRole.MANAGEMENT)`
- New blueprint (`manager_bp`) + service, registered in `src/web/blueprints.py` via `initialize_blueprints()` (per project convention — never in app.py)

### 3b. Layout

**Top — Scoreboard (side-by-side AE comparison table):**

| Metric | AE 1 | AE 2 |
|---|---|---|
| Active accounts | count | count |
| Revenue at risk | $ sum of renewal_gap trailing_revenue | $ |
| Unworked signals (>7 days) | count | count |
| Touch compliance | % accounts within cadence | % |
| Open follow-ups / overdue | n / n | n / n |
| Avg health score | 0-100 | 0-100 |

Hardcoded for 2 AEs. If the team grows, this becomes a loop — but no need to over-engineer for 2.

**Middle — Attention Required (merged, sorted by dollar impact):**

Single list of items that need manager awareness:
- Signals unworked >7 days (account name, signal type, $ amount, days aging, AE name)
- A-tier accounts overdue for touch (account name, days since touch, tier, AE name)
- Renewal gap accounts with no activity in last 14 days

Each row links to the account on the AE's My Accounts page (via `?ae=Name` param). Manager can drill in without switching tools.

**Bottom — Weekly Activity Summary:**

Per AE for the last 7 days: counts by activity type (calls, emails, meetings, notes, follow-ups completed). Simple table, not charts. A week with zero from an AE is a conversation starter.

### 3c. Service Layer

New `ManagerDashboardService` that composes queries from existing services:
- AeCrmService.get_stats() for account counts and revenue
- Signal action queries for unworked signal counts
- Health score computation for averages
- Touch cadence computation for compliance %
- ActivityService queries for weekly activity counts

No new data tables in Phase 3 — all derived from Phase 1 and Phase 2 data.

---

## Data Model Summary

**New table:** `signal_actions` (Phase 1 migration)

**New signal type:** `renewal_gap` added to existing `entity_signals` computation

**Modified services:**
- ActivityService — auto-acknowledge hook on create_activity
- EntityMetricsService — renewal gap detection in signal refresh
- AeCrmService — health score computation, touch cadence, tier computation

**New services:**
- SignalActionService — signal queue CRUD, sync, snooze/dismiss
- ManagerDashboardService — composed metrics for manager view

**New routes:**
- Phase 1: API endpoints for signal queue (list, snooze, dismiss) on existing ae_crm blueprint
- Phase 3: `/manager/dashboard` page + APIs on new manager blueprint

---

## Out of Scope

- Email/calendar integration
- Manual pipeline stages
- Custom fields on accounts
- Contact preference tracking
- Automated email/SMS alerts
- Commission tracking
- Territory management
- Leaderboards or gamification

---

## Phasing

| Phase | Deliverable | Depends on |
|---|---|---|
| 1 | Signal Queue + Renewal Gaps + Revenue At Risk card | — |
| 2 | Health Score + Touch Cadence + Tier badges | Phase 1 (signal state feeds health score) |
| 3 | Manager Dashboard | Phases 1 + 2 (composes their data) |

Each phase ships independently. Phase 1 is the most impactful and should be built first. Phase 3 is the management sell but requires the underlying data from 1 and 2.
