# Multi-Sector Support for Customers - COMPLETED ✅

Implemented 2026-02-10. 3 commits on dev branch.

## Migration (010_customer_sectors.sql)
- ✅ `customer_sectors` junction table with UNIQUE(customer_id, sector_id)
- ✅ 3 indexes: (customer_id, is_primary DESC), (sector_id), partial (customer_id WHERE is_primary=1)
- ✅ Backfilled 179 rows from customers.sector_id (all is_primary=1)
- ✅ 3 triggers: INSERT primary, UPDATE primary, DELETE primary → sync customers.sector_id cache

## Backend
- ✅ New `PUT /api/address-book/customer/<id>/sectors` — replaces all sector assignments
- ✅ Updated `PUT .../sector` — backward compat, writes to junction table via upsert
- ✅ Entity detail returns `sectors` array with sector_id, sector_name, sector_code, is_primary
- ✅ List endpoint returns `sector_count` for badge display
- ✅ Sector filter queries junction table (matches ANY position, not just primary)
- ✅ Create entity inserts into junction table
- ✅ `customer_sector_api.py`: update_customer_sector and bulk_update use junction table
- ✅ `customer_sector_api.py`: delete_sector removes from junction table (triggers handle cache)
- ✅ `canon_tools.py`: create_customer inserts into junction table

## Frontend (address_book.html)
- ✅ Detail panel: multi-sector tag UI with star (primary) and remove buttons
- ✅ Add sector dropdown filters out already-assigned sectors
- ✅ Sectors save inline (no more Save button needed for sectors)
- ✅ Card display: primary sector badge + "+N" count for additional sectors
- ✅ Table display: same "+N" treatment
- ✅ Bulk Set Sector: sets as primary, preserves existing secondary sectors
- ✅ Create modal: unchanged (single sector dropdown for simplicity)

### Migration Required
Run `sql/migrations/010_customer_sectors.sql` on production before deploying.

### Files Changed
- `sql/migrations/010_customer_sectors.sql` (new)
- `src/web/routes/address_book.py` (~6 sections)
- `src/web/routes/customer_sector_api.py` (3 functions)
- `src/web/routes/canon_tools.py` (1 function)
- `src/web/templates/address_book.html` (HTML + CSS + JS)

### 39 LEFT JOIN queries across 14 files: UNCHANGED
All existing read queries continue reading `customers.sector_id` — no migration needed for reports.

---

# Address Book Enhancement — 9 Features, 3 Phases - COMPLETED ✅

Implemented 2026-02-10. 6 commits on dev branch.

## Phase 1: Foundation (commit 7e727f6)
- ✅ Feature 1: Fix DB path fallback in contacts.py (`.data/dev.db`)
- ✅ Feature 2: `apiFetch()` helper — refactored all 18+ fetch calls, consistent error handling
- ✅ Feature 3: `hasUnsavedChanges` warning on modal close (tracks 11 editable fields)
- ✅ Feature 4: Bulk Set Sector button wired for selected advertisers

## Phase 2: CRM Features (commits 56f7bdd, e88cb8d)
- ✅ Feature 5: Inline address editing (mirrors contact edit pattern, green-tinted forms)
- ✅ Feature 6: Entity deactivate button (POST endpoint + red button in save-bar + audit)

## Phase 3: Adoption Enablers (commits 6a2e4d7, a6aa380, 76d7332)
- ✅ Feature 7: Fuzzy duplicate detection on create (threshold 0.60, confirm to force)
- ✅ Feature 8: CSV import for contacts (multipart upload, entity lookup, error reporting)
- ✅ Feature 9: Follow-up task reminders (migration 009, 3 new endpoints, dashboard widget)

### Migration Required
Run `sql/migrations/009_follow_up_activities.sql` on production before deploying.

Files changed: `contacts.py`, `address_book.py` (+5 endpoints), `address_book.html`, `index.html`, `009_follow_up_activities.sql`

---

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
