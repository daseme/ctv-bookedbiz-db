# Data Management Guides & Sector Manager Revenue Fix - COMPLETED ✅

Implemented 2026-02-27. PRs #181–#185.

## Customer Merge Guide (PRs #181, #182)
- ✅ Created `customer_merge_guide.html` — full guide page with sidebar TOC, scroll-spy JS, responsive mobile TOC
- ✅ 5 sections: Overview, Unresolved Bill Codes (Backfill + Link), Merge Customers, When to Use What, Tips
- ✅ Added `?` guide button to `customer_merge.html` (opens in new tab)
- ✅ Added `/customer-merge/guide` route to `customer_merge.py`
- ✅ Softened merge warning — merges are reversible via raw spot data

## Info Modals for Remaining Pages (PR #183)
- ✅ Added `?` info modal to Customer Sector Manager (sectors, assigning, bulk ops, revenue filter)
- ✅ Added `?` info modal to Customer Normalization Audit (columns, actions, when to use)
- ✅ Both follow existing info-modal pattern from Stale Customers / Entity Resolution

## Guide Coverage (all data management pages reviewed)
| Page | Help Type |
|------|-----------|
| Address Book | Dedicated guide page |
| Customer Merge | Dedicated guide page |
| Entity Resolution | Info modal |
| Entity Aliases | Info modal |
| Stale Customers | Info modal |
| Customer Sector Manager | Info modal (new) |
| Customer Normalization | Info modal (new) |

## Sector Manager Revenue Fix (PR #184)
- ✅ Revenue query was commented out (`# TEMP: Disabled for performance`), showing all $0
- ✅ Old query joined through `v_customer_normalization_audit` by `bill_code` — too slow
- ✅ Replaced with direct `GROUP BY customer_id` on spots table (under 1s for 308 customers)
- ✅ 193/199 customers now show real revenue data

## Lesson Captured (PR #185)
- ✅ Rule 32: Never leave "TEMP: Disabled" code in production — fix the root cause

### Files Created
- `src/web/templates/customer_merge_guide.html`

### Files Modified
- `src/web/templates/customer_merge.html` (guide button)
- `src/web/routes/customer_merge.py` (guide route)
- `src/web/templates/customer_sector_manager.html` (info modal)
- `src/web/templates/customer_normalization_manager.html` (info modal)
- `src/web/routes/customer_sector_api.py` (revenue query fix)
- `tasks/lessons.md` (Rule 32)

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
