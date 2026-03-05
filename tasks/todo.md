# Eliminate hardcoded/wrong database path fallbacks — COMPLETED

## Summary
Removed all silent fallback DB paths that could cause writes to wrong database.
Production code now requires explicit `DB_PATH` or `DATABASE_PATH` env var.

### Changes
- [x] Flask routes (9 files): `config.get("DB_PATH") or "fallback"` → `config["DB_PATH"]`
- [x] CLI scripts: removed `"data/database/production.db"` default, fail with clear error if unset
- [x] Service factory: removed all hardcoded fallbacks, added `_resolve_db_path()` helper
- [x] Settings module: production env raises `RuntimeError` if no DB env var set
- [x] Shell scripts: `daily_update.sh` requires `DATABASE_PATH`; `daily-download.sh` uses `$DATABASE_PATH` var
- [x] Utility scripts (4 files): use env var defaults, fail if not set
- [x] `.env`: updated `DATABASE_PATH` to canonical `/var/lib/ctv-bookedbiz-db/production.db`
- [x] Bonus: fixed `language_blocks.py` route (not in original plan) — same fallback pattern

### Verification
- App startup with `DATABASE_PATH` set: DB_PATH correctly resolved
- `_choose_db_path("prod", ...)` without env vars: raises `RuntimeError`
- `grep "data/database/production.db" src/web/routes/ src/services/factory.py cli/ scripts/*.py`: zero matches

---

# Tailscale MagicDNS Rename: pi-ctv → spotops

## Status: IN PROGRESS

### Completed
- [x] Audit all `pi-ctv` references in repo (scripts, docs, env, systemd)
- [x] Fix wrong Tailscale IP in failback script (100.81.73.46 → 100.99.11.55)
- [x] Rename `scripts/failback-to-pi-ctv.sh` → `scripts/failback-to-spotops.sh`
- [x] Update all script internals (failback, failover, daily-download)
- [x] Update all 5 docs files (TAILSCALE_AUTH, USER_MANAGEMENT_SETUP, GUIDE_GIT_WORKFLOW, GUIDE-RaspberryWorkflow, GUIDE-failover-failback)
- [x] Fix wrong IP in GUIDE-failover-failback.md (100.81.73.46 → 100.99.11.55)
- [x] Update repo_tree.txt and _repo_tree_paths.txt
- [x] Commit repo changes (commits e428552, 94e4394)

### Infrastructure steps (run manually)
- [x] `sudo tailscale set --hostname=spotops`
- [x] `sudo hostnamectl set-hostname spotops`
- [x] `sudo reboot`
- [x] Verify after reboot: `hostname`, `tailscale ip -4` (should still be 100.99.11.55), `cat /etc/hosts`

### Post-reboot
- [x] Update `~/.ssh/config` and `~/.ssh/known_hosts` on client machines (desktop, laptop)
- [x] Update browser bookmarks (`http://pi-ctv:8000` → `http://spotops:8000`)
- [ ] Check pi2 for any scripts/config referencing `pi-ctv`

### Service cleanup (2026-03-04)
- [x] `tailscale-serve.service` — already removed (not found)
- [x] `code-tunnel.service` — updated tunnel name `raspberrypi` → `spotops`, restarted
- [x] Removed stale `pihole-FTL.service`
- [x] Unmasked and removed `flaskapp.service` symlink
- [ ] Fix `pi-weekly-update.service` (script `/usr/local/bin/pi-weekly-update.sh` missing)

---

# Fix Dropbox Backup Path + Update Failover Guide - COMPLETED

Implemented 2026-03-02. PR #187.

## Dropbox Backup Fix
- Fixed `DATABASE_PATH` in `/etc/ctv-db-sync.env` back to `/var/lib/ctv-bookedbiz-db/production.db`
- Added `ReadOnlyPaths=/var/lib/ctv-bookedbiz-db` to `ctv-db-sync.service` (systemd sandbox)
- Changed directory group: `chgrp ctvapps /var/lib/ctv-bookedbiz-db` (was `ctvbooked:ctvbooked` 0750)
- Changed file group: `chgrp ctvapps /var/lib/ctv-bookedbiz-db/production.db`
- Verified: upload succeeded, integrity check passed from correct `/var/lib/` path

### Root Cause
The service ran with `ProtectSystem=strict` which blocks `/var/lib/` access. Three layers of access were missing: systemd sandbox path, directory Unix group, and file Unix group. This caused 20 days of silent backup failures (Feb 11 – Mar 2).

## Failover Guide Rewrite
- Replaced emoji-heavy Pi2-only doc with accurate 3-layer backup stack guide
- Documented Litestream continuous WAL replication to Backblaze B2 (~1s RPO)
- Documented Dropbox nightly backup (timer, sandboxing, log location)
- Documented Pi2 cold standby failover/failback procedures
- Added RPO/RTO table, file locations, monitoring commands
- Added Feb 2026 incident log with full fix details

## Lesson Captured
- Rule 33: Canonical production DB path is the single source of truth

### Files Modified
- `docs/GUIDE-failover-failback.md` (full rewrite)
- `tasks/lessons.md` (Rule 33)
- `/etc/ctv-db-sync.env` (system file, DATABASE_PATH fix)
- `/etc/systemd/system/ctv-db-sync.service` (system file, ReadOnlyPaths)

---

# Phase 2: Strategic Refactoring - Moderate Risk

## Overview
Execute strategic refactoring to consolidate duplicate code patterns and standardize database access.
Total estimated impact: ~540 lines of code consolidation across 25+ files.

**Key Lessons Applied:**
- Rule 1: Grep for all references before modifying shared code
- Rule 2: Verify ALL affected files after each change
- Rule 7: Test application after each major change
- Rule 5: Atomic commits with impact metrics

## Phase 2 Tasks

### Step 1: LOW RISK - Consolidate Shared Utilities (COMPLETED 2026-03-05)
- [x] Add `build_language_case_sql()` to LanguageConstants (generates SQL CASE from LANGUAGE_GROUPS dict)
- [x] Remove duplicate `build_year_filter()` from DateRangeUtils (callers use BroadcastMonthQueryBuilder)
- [x] Replace hardcoded language CASE in market_analysis.py (was mapping P→Portuguese, now uses canonical P→South Asian)
- [x] Replace hardcoded language CASE in market_analysis_service.py
- [x] Update unified_analysis.py QueryBuilder to delegate to BroadcastMonthQueryBuilder
- [x] Update unified_analysis-old.py to use BroadcastMonthQueryBuilder
- [x] Add 15 tests in tests/test_utils.py (TDD: RED→GREEN→REFACTOR)

**Impact**: Eliminated 2 hardcoded SQL CASE statements, 1 duplicate `build_year_filter`, fixed P→Portuguese bug
**Files Modified**: 7 (language_constants.py, date_range_utils.py, market_analysis.py, market_analysis_service.py, unified_analysis.py, unified_analysis-old.py, tests/test_utils.py)

### Step 2: MEDIUM RISK - Enhance Query Builders (Careful Planning)
- [ ] Create CustomerNormalizationQueryBuilder class for customer JOIN patterns
- [ ] Enhance existing RevenueQueryBuilder with broadcast month filtering
- [ ] Update database configurations to use centralized connection profiles
- [ ] Grep for all usages before refactoring shared query patterns
- [ ] Update affected services and repositories (8+ files)
- [ ] Test application thoroughly after each change
- [ ] Commit with descriptive message

**Estimated Impact**: 115 lines consolidated
**Risk Level**: MEDIUM - Core business logic requiring verification
**Files Affected**: 8+ files (services/ae_dashboard_service.py, services/report_data_service.py, web/routes/customer_sector_api.py, repositories/customer_matching_repository.py)

### Step 3: HIGH RISK - Database Connection Standardization (Thorough Testing)
- [ ] Audit all raw sqlite3.connect() usage across codebase
- [ ] Create migration plan for DatabaseConnection class adoption
- [ ] Replace raw connections with DatabaseConnection in services (15+ files)
- [ ] Standardize transaction handling using context managers
- [ ] Verify ALL affected routes and services work correctly
- [ ] Test critical business operations (budget, customer, canon tools)
- [ ] Commit with descriptive message

**Estimated Impact**: 230 lines consolidated
**Risk Level**: HIGH - Core infrastructure changes affecting 15+ files
**Files Affected**: 15+ files including budget_service.py, canon_tools.py, ae_service.py, customer_service.py

## Success Criteria
- [ ] All LOW risk utilities consolidated (Step 1)
- [ ] All MEDIUM risk query patterns enhanced (Step 2)
- [ ] All HIGH risk database connections standardized (Step 3)
- [ ] 3 separate commits with impact metrics and risk levels
- [ ] Application functions correctly after each step
- [ ] All affected files verified for proper imports and functionality

## Safety Measures
**Before Each Step:**
1. Grep for ALL references to code being modified
2. Create list of affected files for verification
3. Test application functionality after changes
4. Commit atomically by risk level

**Flag for Review:**
- Any uncertainty about business logic impact
- Unexpected usage patterns during grep analysis
- Test failures or application errors
- Database connection issues during Step 3

## Rollback Plan
If issues encountered:
1. Revert specific commit that caused issue
2. Re-analyze problematic patterns with deeper grep search
3. Skip high-risk items and proceed with lower risk consolidations
4. Document issues in tasks/lessons.md for future reference
