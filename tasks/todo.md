# Active Tasks

## Tailscale MagicDNS Rename: pi-ctv → spotops

**Status: NEARLY COMPLETE** (2026-03-04)

Remaining:
- [ ] Check pi2 for any scripts/config referencing `pi-ctv` (SSH host key needs fixing first)
- [ ] Fix `pi-weekly-update.service` (script `/usr/local/bin/pi-weekly-update.sh` missing — decide: remove service or replace with `unattended-upgrades`)

---

## Phase 2 Step 3: Database Connection Standardization

**Status: NOT STARTED** (High risk)

- [ ] Audit all raw `sqlite3.connect()` usage across codebase
- [ ] Create migration plan for DatabaseConnection class adoption
- [ ] Replace raw connections with DatabaseConnection in services (15+ files)
- [ ] Standardize transaction handling using context managers
- [ ] Verify ALL affected routes and services work correctly
- [ ] Test critical business operations (budget, customer, canon tools)

**Estimated Impact**: 230 lines consolidated across 15+ files

---

# Completed

- **Eliminate hardcoded DB path fallbacks** — Removed all silent fallback DB paths; production code requires explicit env var (2026-03)
- **Fix Dropbox Backup Path** — Fixed `DATABASE_PATH` in sync env, added systemd sandbox path, rewrote failover guide. PR #187 (2026-03-02)
- **Phase 2 Step 1: Shared Utilities** — Consolidated language CASE, date range utils, added 15 tests (2026-03-05)
- **Phase 2 Step 2: Query Builder** — Moved RevenueQueryBuilder to shared utils, eliminated 7 inline CASE blocks, deleted dead methods, added 10 tests (2026-03-05)
