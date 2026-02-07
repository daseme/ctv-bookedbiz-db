# Lessons Learned - Refactoring and Code Cleanup Sessions

## Template and File Deletion Patterns

### Rule 1: Always Verify References Before Deletion
**Context**: The nord_base.html deletion broke 7 templates that still included it  
**Pattern**: Before deleting ANY template file:
```bash
# Check for references in all source files
rg -l "filename.html" src/
# Check for both include and extend patterns
rg "{% include.*filename\.html" src/
rg "{% extends.*filename\.html" src/
```
**Action**: Never delete a template without comprehensive reference checking across the entire codebase.

### Rule 2: Verify ALL Templates After Bulk Changes
**Context**: CSS migration missed sector-analysis.html, causing 500 errors  
**Pattern**: After any bulk template modification (CSS migrations, base template changes):
```bash
# Find ALL templates that might be affected
find src/web/templates -name "*.html" -exec grep -l "old_pattern" {} \;
# Verify each one was properly updated
# Test the application after bulk changes
```
**Action**: Sample verification is insufficient - check every affected file systematically.

## CSS Architecture and Migration Patterns

### Rule 3: Document CSS Dependencies During Migration
**Context**: Complex Nord theme CSS had interdependencies not immediately obvious  
**Pattern**: Before CSS refactoring:
1. Map all CSS files and their dependencies
2. Identify templates using each CSS approach
3. Create migration plan with verification steps
4. Test each template type after changes

**Action**: Create dependency maps before major CSS restructuring.

## Code Cleanup and Redundancy Analysis

### Rule 4: Categorize Files by Reference Risk Level
**Context**: Successfully removed 9,000+ lines by categorizing files by risk  
**Pattern**: For cleanup tasks:
- **ZERO RISK**: Files in 'old/' directories with '-OLD' suffixes
- **LOW RISK**: Unused imports confirmed by static analysis
- **MEDIUM RISK**: Templates requiring reference verification
- **HIGH RISK**: Active code with potential runtime dependencies

**Action**: Always start with zero-risk items, work up the risk ladder with increasing verification.

### Rule 5: Commit Atomically by Risk Level
**Context**: Three separate commits for old directories, orphaned templates, unused imports  
**Pattern**: Each cleanup phase should be a separate commit:
1. Delete confirmed dead code (zero risk)
2. Remove verified orphaned files (low risk)  
3. Clean up unused imports (very low risk)

**Action**: Atomic commits allow easy rollback if any phase causes issues.

## Template Analysis and Reference Checking

### Rule 6: Use Multiple Grep Patterns for Template References
**Context**: Templates can be referenced via routes, includes, extends, or documentation  
**Pattern**: Check for template references using multiple patterns:
```bash
# Route references (Python files)
rg "template_name\.html" --type py
# Template includes/extends (HTML files)  
rg "template_name" --type html
# Documentation references (Markdown files)
rg "template_name" --type md
```
**Action**: Cast a wide net when checking for template references.

## Error Recovery Patterns

### Rule 7: Test Application After Each Major Change
**Context**: 500 errors were caught immediately after CSS migration  
**Pattern**: After any template or CSS changes:
1. Start the application locally if possible
2. Check key routes for 500 errors
3. Verify browser console for missing CSS/JS resources
4. Test Nord theme pages specifically

**Action**: Build testing into the refactoring workflow, not just at the end.

## Git Workflow for Refactoring

### Rule 8: Use Descriptive Commit Messages with Impact Metrics
**Context**: All commits included line counts and specific changes made  
**Pattern**: Commit message format for cleanup:
```
Phase X: Brief description

- Specific action 1 (impact metrics)
- Specific action 2 (impact metrics)
- Total impact: X lines removed

🤖 Generated with [Claude Code](https://claude.ai/code)
Co-Authored-By: Claude <noreply@anthropic.com>
```
**Action**: Include quantified impact in commit messages for better tracking.

## Planning and Execution Patterns

### Rule 9: Write Phase Plans to tasks/ Directory Before Execution
**Context**: tasks/todo.md provided clear roadmap and stopping points  
**Pattern**: For multi-phase refactoring:
1. Document all phases with success criteria
2. Include explicit STOP conditions  
3. Define risk levels and verification steps
4. Track progress with TodoWrite tool

**Action**: Always plan before executing, especially for destructive operations.

### Rule 10: Verify with User Before Proceeding to Higher Risk Phases
**Context**: User explicitly requested "Execute Phase 1 only" with stop condition  
**Pattern**: For cleanup projects:
- Start with lowest risk phases
- Get user confirmation before proceeding to higher risk changes
- Provide clear phase boundaries and impact estimates

**Action**: Respect user-defined boundaries, especially for potentially breaking changes.

---

### Rule 11: Understand Business Context for Data Constants
**Context**: P language code had inconsistent mapping (Portuguese vs Punjabi) during consolidation  
**Pattern**: Asian language television company context:
- All languages are Asian except English
- P = Punjabi (South Asian), not Portuguese
- Language mappings are business logic, not technical data

**Action**: Always verify business context when consolidating data constants.

---

## Service and Deployment Patterns

### Rule 12: Check for Service Conflicts Before Starting
**Context**: `spotops-dev` and `ctv-dev` both tried to bind to port 5100
**Pattern**: Multiple services may target the same port:
```bash
# Check what's using a port
sudo lsof -i :5100
# Check systemd services (both system AND user level)
systemctl list-units --type=service | grep ctv
systemctl --user list-units --type=service | grep ctv
```
**Action**: Before starting a dev service, verify no other service is using the same port.

### Rule 13: User Services vs System Services Can Conflict
**Context**: There was both `/etc/systemd/system/ctv-dev.service` and `~/.config/systemd/user/ctv-dev.service`
**Pattern**: Systemd services exist at two levels:
- System: `/etc/systemd/system/` (controlled via `sudo systemctl`)
- User: `~/.config/systemd/user/` (controlled via `systemctl --user`)

**Action**: When troubleshooting service conflicts, check BOTH system and user service directories.

---

## Blueprint Registration Patterns

### Rule 14: Register Blueprints in ONE Place Only
**Context**: `customer_resolution_bp` was registered in both `app.py` and `blueprints.py`, causing warnings
**Pattern**: Flask blueprints should be registered in exactly one location:
- Standard location: `src/web/blueprints.py` via `initialize_blueprints()`
- Never register the same blueprint in multiple files

**Action**: When adding new blueprints, only add to `blueprints.py`, not `app.py`.

---

## UX Patterns for Resolution Pages

### Rule 15: Provide Helpful Empty States
**Context**: Agency resolution page showed nothing when all agencies were resolved
**Pattern**: Empty states should include:
1. Clear message that everything is done (positive framing)
2. Explanation of filter context ("matching your filters")
3. Link to related management page

**Action**: Always design empty states with helpful guidance and next actions.

---

## Database Schema Patterns

### Rule 16: Verify Column Names Before Writing Queries
**Context**: Address Book API failed because customers table uses `normalized_name` not `customer_name`
**Pattern**: Customer vs Agency naming differs:
- **agencies** table: `agency_name`
- **customers** table: `normalized_name` (NOT `customer_name`)

**Action**: Always check `PRAGMA table_info(table_name)` when writing new queries against unfamiliar tables.

---

### Rule 17: Agencies vs Customers Have Different Data Models
**Context**: Building unified Address Book needed to account for schema differences
**Pattern**: Key differences:
- **Agencies**: No sector (they're ad buyers), have `agency_name`
- **Customers**: Have `sector_id` linking to sectors table, use `normalized_name` not `customer_name`
- Both have: address, city, state, zip, notes, contacts (via entity_contacts)

**Action**: When building unified views, handle entity-type-specific fields conditionally.

---

### Rule 18: Dev Database Path — ALWAYS Override DB_PATH in Tests
**Context**: Stale Customer Report endpoints returned 500 (no such table: customers) during test_client testing
**Pattern**: `create_app()` sets `DB_PATH` from settings → `data/database/production.db` (4KB empty skeleton). The `_get_db_path()` fallback `or "./.data/dev.db"` does NOT trigger because the config IS set — just to the wrong path.
```python
# WRONG — will use empty production.db
app = create_app()

# CORRECT — override to real dev data
app = create_app()
app.config['DB_PATH'] = '.data/dev.db'
```
**Action**: When testing ANY endpoint via `app.test_client()`, always set `app.config['DB_PATH'] = '.data/dev.db'` after `create_app()`. The running `spotops-dev.service` doesn't have this problem because its environment sets the correct path.

---

### Rule 19: Orphan Customer Lifecycle — How They're Created and Why They Persist
**Context**: ~140+ orphan customers with zero spots found during stale report cleanup (Feb 2026). 5 years of accumulated data.
**How orphans are created**: Customer rows are created during **manual resolution** (CustomerResolutionService.create_customer_and_alias), NOT by the daily import itself. The daily update only adds bill codes to `raw_customer_inputs` and resolves against existing customers via `BatchEntityResolver` (which never creates rows).
**Why they persist**: The daily update and monthly close both delete+reimport **spots** but never touch the **customers** table. When spots get remapped to a different entity (e.g., from "Bay Area AQMD" to "Allison + Partners:Bay Area AQMD"), the original customer row stays with zero spots. The customers table has `ON DELETE RESTRICT` — it only grows, never shrinks.
**The cycle**:
1. Daily update adds new bill code to `raw_customer_inputs` → appears in resolution queue
2. User manually resolves → new customer row created, spots linked
3. Later daily update or monthly close reimports data with different mapping (e.g., agency-prefixed name)
4. Spots move to different customer_id; original customer row left with zero spots
5. `raw_customer_inputs` also only appends, never prunes
**Indicators**: Customer has 0 spots, name is a variant of another entity (abbreviation, agency prefix, "&" vs "and")
**Current cleanup**: Stale Customer Report → deactivate orphans manually (or bulk via future tooling)
**Future prevention ideas**:
- Pre-creation fuzzy match against `normalized_name` + `entity_aliases` during resolution
- Post-import job: auto-flag customers that went from >0 to 0 spots in that batch
- Nightly reconciliation: auto-deactivate zero-spot customers older than 7 days
- Bulk deactivate on stale report for faster cleanup

---

### Rule 20: Deploy to Dev First, Then Production
**Context**: Deployed address book enhancements to production first, then couldn't verify dev was working because the dev database had different data (fewer Automotive customers). Appeared broken on dev but was actually a data difference.
**Pattern**: Deployment order must always be:
1. Dev: restart `spotops-dev.service`, verify on port 5100
2. Production: `git pull`, run migrations, restart `ctv-bookedbiz-db.service`, verify on port 8000

**Why it matters**:
- Dev is the safety net — if something breaks there, production is untouched
- Data differences between dev.db and production.db can mask or create false failures
- Always verify the feature works on dev with appropriate test data before touching production

**Action**: Never skip dev verification. If dev data doesn't cover the test case, test with a query that does hit dev data to confirm the code path works.

---

**Last Updated**: 2026-02-06
**Session Context**: Address book enhancements deployment — search fix appeared broken on dev due to data differences, not code.
