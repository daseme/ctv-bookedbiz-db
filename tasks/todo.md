# Planning Page Revenue Fix + DB Path Alignment - COMPLETED ✅

Implemented 2026-02-10. PRs #117–#121.

## Fix 1: DB Path Mismatch (PR #117, #118)
- ✅ `factory.py`: Added `DATABASE_PATH` env var fallback so service layer uses correct DB
- ✅ `import_closed_data.py`: CLI default now checks `DB_PATH` / `DATABASE_PATH` before project-local fallback
- ✅ `daily_update.py`: Same fix as above
- ✅ `/etc/ctv-db-sync.env`: Fixed Dropbox backup to sync canonical production DB (`/var/lib/...`)
- ✅ Re-ran January import against correct production DB → $164,171 matches expected total

## Fix 2: Planning Page Shows Actuals for Closed Months (PR #117)
- ✅ TOTAL row: shows booked for past months, forecast for future
- ✅ Entity forecast inputs: display booked value (readonly) for past months
- ✅ Column headers: "(Actual)" label on closed month headers
- ✅ Year total: new `total_effective` sums booked (past) + forecast (future)
- ✅ Added `effective` property to `PeriodDataWrapper`, `total_effective` to `CompanySummaryWrapper`
- Files: `factory.py`, `planning_service.py`, `planning.py`, `planning_session.html`

## Fix 3: Address Book Create Modal (PR #120)
- ✅ Modal crashed silently on open — agency dropdown JS used `a.name` but API returns `entity_name`
- ✅ `undefined.localeCompare()` TypeError killed `openCreateModal()` before showing modal

## Fix 4: Agency Assignment in Detail Panel (PR #121)
- ✅ Customer detail API was missing `agency_id`/`agency_name` from query
- ✅ Added Agency dropdown to customer detail panel (between Sector and AE)
- ✅ New PUT endpoint `/api/address-book/customer/<id>/agency`
- ✅ XQ Institute → We Are Rally assignment was in DB but invisible in UI

## Lessons Captured
- ✅ Rule 21 added to `tasks/lessons.md`: Canonical production DB path pattern

---

# Reports Index Redesign - COMPLETED ✅

Implemented 2026-02-08. PRs #106–#110.

## Round 1: Bug Fixes & Quick Wins
- ✅ Fixed copy-pasted description on Monthly Revenue Summary
- ✅ Fixed animation-fill-mode flash on staggered card entrance
- ✅ Deduplicated icons (5 cards shared 📈, 2 shared 🌐 → all unique)
- ✅ Merged broken split report-grid divs + fixed indentation on pricing cards
- ✅ Added search/filter bar with live card filtering by title/description

## Round 2: Density & Usability Overhaul
- ✅ Made entire cards clickable (removed separate buttons)
- ✅ Compact layout: padding 36→14px, icons 56→36px, gaps 32→12px
- ✅ One-line descriptions with text-overflow ellipsis, expand on hover
- ✅ Removed hero header banner, promoted search bar to top
- ✅ 3-column forced grid, smaller left-aligned uppercase category headers

## Round 3: Chrome Removal & Featured Row
- ✅ Hidden ghost `.header` div (empty title/subtitle wasting ~80px)
- ✅ Hidden breadcrumb ("Home" on home page is redundant)
- ✅ Zeroed double padding (content-wrapper 32px + index-content 24px → single 20px)
- ✅ Added dark-themed Quick Access row (Customer Revenue, Address Book, Planning Hub)
- ✅ Added `/` keyboard shortcut to focus search, `Escape` to clear
- ✅ Featured card icons tinted to match category colors (blue/purple/green)

## Round 4: Section Rebalancing
- ✅ Split 3 Pricing reports into dedicated Pricing section
- ✅ Removed duplicate Monthly Revenue Summary from Coming Soon
- ✅ Final layout: Quick Access (3) → Reporting (6) → Pricing (3) → Data Management (5) → Budget (2) → Coming Soon (3)

Files: `src/web/templates/index.html`

---

# AE Account Ownership with History - COMPLETED ✅

Implemented 2026-02-08.

- ✅ Created `ae_assignments` table for CRM-style assignment history tracking
- ✅ Backfilled 107 entities (34 agencies + 73 customers) from 2025+ spot activity
- ✅ Updated `api_update_ae()` to manage history (end old assignment, create new)
- ✅ Added `GET /ae-history` endpoint returning assignment timeline
- ✅ Added AE history timeline UI in entity detail modal
- ✅ Agencies with AE badges: 3 → 35; customers: 0 → 73
- Files: `007_backfill_assigned_ae.sql`, `address_book.py`, `address_book.html`

---

# Consistent Breadcrumbs - COMPLETED ✅

Deployed to production 2026-02-07. PR #102.

- ✅ Fixed stale nav categories in 14 existing breadcrumbs (Sales/Executive/Analytics/etc. → Reporting/Data Management/Budgeting/Admin)
- ✅ Added missing breadcrumb blocks to 26 templates (all categories covered)
- ✅ Removed duplicate inline Bootstrap breadcrumbs from 8 pricing/length_analysis templates
- ✅ Fixed typo: `pricing_titletitle` → `pricing_title` in rate_trends.html
- ✅ 45 files changed, +242/-69 lines

---

# Phase 1: Quick Wins - COMPLETED ✅

Phase 1 successfully completed with 9,000+ lines of dead code removed:
- ✅ All 'old/' directories removed (~4,310 lines)
- ✅ Orphaned templates removed (~4,712 lines)
- ✅ Unused imports removed (4 import lines)
- ✅ 3 separate commits made with impact metrics
- ✅ Lessons documented in tasks/lessons.md

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
