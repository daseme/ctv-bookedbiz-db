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

### Step 1: LOW RISK - Create Shared Utilities (Immediate Action)
- [ ] Create src/utils/date_range_utils.py for year range parsing functions
- [ ] Create src/utils/language_constants.py for language group mappings
- [ ] Standardize month case statement usage with existing RevenueQueryBuilder
- [ ] Update imports in affected files (unified_analysis-old.py, market_analysis.py, etc.)
- [ ] Commit with descriptive message

**Estimated Impact**: 115 lines consolidated
**Risk Level**: LOW - Pure utility functions with no side effects
**Files Affected**: 5 files (unified_analysis-old.py, market_analysis.py, services/market_analysis_service.py, services/report_data_service.py)

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
