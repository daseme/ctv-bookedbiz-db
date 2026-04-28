# Active Tasks

## AE My Accounts — Phase 4 (Address Book Refactor)

**Status: DEBUGGING — page loads but data doesn't populate**
**Branch:** `refactor/address-book-service-extraction`

All code is committed (9 commits `cbcb059..9e8fdb0`). Backend verified:
- [x] Service layer (AeCrmService) — 15 tests pass
- [x] Activity extensions (AE filter, recent activity) — 15 tests pass
- [x] Routes registered, correct HTTP codes (302/401 unauthenticated)
- [x] Factory registration confirmed
- [x] Blueprint registration confirmed
- [x] **BUG: Data doesn't populate in browser** — FIXED: template used `{% block extra_scripts %}` but base.html defines `{% block extra_js %}`. Scripts never loaded. (commit e903410)

**Key files:**
- Service: `src/services/ae_crm_service.py`
- Routes: `src/web/routes/ae_crm.py`
- Template: `src/web/templates/ae_my_accounts.html`
- JS: `src/web/static/js/ae_my_accounts.js`
- Tests: `tests/services/test_ae_crm_service.py`, `tests/services/test_activity_service.py`

**To debug:** Check browser console for JS errors. Likely causes:
1. API fetch URLs not matching routes (check window.location.search passthrough)
2. DOM element IDs in template don't match JS selectors
3. CRM_AE_NAME variable not set correctly for non-admin users
4. JSON response shape doesn't match what JS expects

---

---

## Tailscale MagicDNS Rename: pi-ctv -> spotops

**Status: NEARLY COMPLETE** (2026-03-04)

Remaining:
- [ ] Check pi2 for any scripts/config referencing `pi-ctv` (SSH host key needs fixing first)
- [ ] Fix `pi-weekly-update.service` (script `/usr/local/bin/pi-weekly-update.sh` missing — decide: remove service or replace with `unattended-upgrades`)

---

## Worldlink Auto-Resolution

**Status: BACKLOG** (Low priority)

Worldlink is a broker/agency — their individual advertisers aren't important for day-to-day tracking but we want to preserve the data. Currently each new `Worldlink:<Advertiser>` bill code shows up in the resolution queue and needs manual alias creation.

Options to explore:
- [ ] Auto-create customer + alias for any `Worldlink:*` / `WorldLink:*` bill code on ingest (strip prefix, create customer if needed)
- [ ] Mark Worldlink-prefixed bill codes as "auto-resolved" so they don't clutter the resolution queue
- [ ] Bulk-flag existing Worldlink customers as low-priority so they sort to the bottom

**Context**: 70 distinct Worldlink bill codes, ~860K spots. Most are already resolved. Only 3 were unresolved as of 2026-03-06 (manually fixed).

---

# Completed

- **Diff-Based Import** — Replaced delete-and-reinsert with contract-group fingerprint diff. 4x daily imports now produce zero writes when nothing changed (was ~33K writes/import). Reduces Backblaze WAL bloat and stabilizes spot_ids. PR merged 2026-03-24.
- **Fix Worldlink Dollars Missing** — Docker migration dropped the multi-sheet combiner; import only read "Commercials" sheet. Fixed to read all 4 sheets (Worldlink Lines, Pending, Add to booked business). PR #253 (2026-03-24)
- **Eliminate hardcoded DB path fallbacks** — Removed all silent fallback DB paths; production code requires explicit env var (2026-03)
- **Fix Dropbox Backup Path** — Fixed `DATABASE_PATH` in sync env, added systemd sandbox path, rewrote failover guide. PR #187 (2026-03-02)
- **Phase 2 Step 1: Shared Utilities** — Consolidated language CASE, date range utils, added 15 tests (2026-03-05)
- **Phase 2 Step 2: Query Builder** — Moved RevenueQueryBuilder to shared utils, eliminated 7 inline CASE blocks, deleted dead methods, added 10 tests (2026-03-05)
- **Phase 2 Step 3: DB Connection Standardization** — Replaced raw `sqlite3.connect()` in blueprints.py, stale_customers.py, address_book.py with DatabaseConnection from DI container. Deleted unused CustomerMatchingRepository. -353 lines. PR #207 (2026-03-07)
