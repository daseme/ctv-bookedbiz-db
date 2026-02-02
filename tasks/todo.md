# Phase 1: Quick Wins - Redundant Code Cleanup

## Overview
Execute Phase 1 of redundancy cleanup focusing on safe, zero-risk removal of confirmed dead code. 
Total estimated impact: ~11,500 lines of code removal.

## Phase 1 Tasks

### Step 1: Delete 'old/' Directories
- [ ] Remove src/importers/old/ directory (3 legacy importer files, ~2,500 lines)
- [ ] Remove src/services/old/ directory (2 legacy service files, ~1,535 lines)  
- [ ] Remove src/cli/old/ directory (2 legacy CLI files, ~363 lines)
- [ ] Commit with descriptive message

**Estimated Impact**: ~4,000 lines removed
**Risk Level**: ZERO - All files have '-OLD' suffixes and are in 'old' directories

### Step 2: Remove Orphaned Templates
- [ ] Double-check each template for references before deletion
- [ ] Remove src/web/templates/old/ directory (14 legacy templates, ~6,228 lines)
- [ ] Check and remove budget_management_old.html (288 lines) 
- [ ] Check and remove budget-management-main.html (995 lines)
- [ ] Commit with descriptive message

**Estimated Impact**: ~7,500 lines removed  
**Risk Level**: ZERO - No route references found, but will double-check

### Step 3: Remove Unused Imports  
- [ ] Remove `from decimal import Decimal` from src/models/pricing_intelligence.py
- [ ] Remove unused `timedelta` from src/repositories/spot_repository.py
- [ ] Remove unused `datetime` from src/services/pricing_analysis_service.py
- [ ] Remove unused `url_for` from src/web/routes/pricing.py
- [ ] Commit with descriptive message

**Estimated Impact**: 4 import lines removed
**Risk Level**: VERY LOW - Confirmed unused through analysis

## Success Criteria
- [ ] All 'old/' directories removed successfully
- [ ] All orphaned templates removed (after verification)
- [ ] All unused imports removed
- [ ] 3 separate commits with descriptive messages
- [ ] Codebase still builds and runs without errors

## STOP Condition
**Do NOT proceed to Phase 2** - This task is limited to Phase 1 only.

## Backup Plan
If any issues encountered:
1. Revert specific commit that caused issue
2. Re-analyze the problematic files
3. Skip problematic files and continue with remaining cleanup