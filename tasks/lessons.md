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

### Rule 21: Canonical Production DB Path — Never Hardcode Project-Local Defaults
**Context**: Planning page showed $163,017 instead of $164,171 for January. The CLI import (`import_closed_data.py`) wrote 28,073 records to `data/database/production.db` (project-local skeleton) instead of `/var/lib/ctv-bookedbiz-db/production.db` (canonical production DB). The Dropbox backup timer was also syncing the wrong file.
**Pattern**: Four components had the same bug — hardcoded `data/database/production.db` as default:
- `factory.py` (service layer) — fixed PR #117
- `import_closed_data.py` (CLI) — fixed PR #118
- `daily_update.py` (CLI) — fixed PR #118
- `/etc/ctv-db-sync.env` (backup timer) — fixed manually

**The canonical DB path resolution chain**:
```
DB_PATH env var > DATABASE_PATH env var > data/database/production.db (fallback)
```

**Production paths**:
- App env (`/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env`): `DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db`
- Backup env (`/etc/ctv-db-sync.env`): `DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db`
- CLI scripts: Now check `DB_PATH` / `DATABASE_PATH` env vars before falling back

**When adding new CLI tools or services**: Always resolve the DB path with:
```python
db_path = os.environ.get("DB_PATH") or os.environ.get("DATABASE_PATH") or "data/database/production.db"
```

**How to verify you're hitting the right DB**:
```bash
# Check record counts differ significantly between the two
sqlite3 /var/lib/ctv-bookedbiz-db/production.db "SELECT COUNT(*) FROM spots WHERE broadcast_month='Jan-26';"
sqlite3 data/database/production.db "SELECT COUNT(*) FROM spots WHERE broadcast_month='Jan-26';"
```

**Action**: Never hardcode `data/database/production.db` without also checking env vars. When data looks wrong, first verify which DB file the component is actually reading/writing.

---

---

### Rule 22: Shell Scripts Must Have Execute Permission in Git
**Context**: `bin/daily_update.sh` lost its execute bit on Feb 13 (file was edited/rewritten). Daily import silently failed for 3 days with exit code 126 ("permission denied"). Two other scripts (`commercial_import.sh`, `rotate_commercial_logs.sh`) had the same problem.
**Pattern**: Git tracks file permissions as either `100644` (non-executable) or `100755` (executable). When a `.sh` file is rewritten (not edited in-place), the execute bit can be lost. Git won't restore it on `git pull` unless the index has `100755`.
**How to check**:
```bash
# Show git-tracked permissions for all scripts
git ls-files -s bin/*.sh
# Should show 100755 for all .sh files, NOT 100644

# Find .sh files missing execute permission
find bin/ -name "*.sh" ! -perm -u+x
```
**How to fix**:
```bash
chmod +x bin/script.sh
git update-index --chmod=+x bin/script.sh
git commit -m "Fix execute permission on bin/script.sh"
```
**Prevention**: After editing ANY shell script, verify the execute bit is still set before committing. If creating a new `.sh` file, always `chmod +x` and `git update-index --chmod=+x` before the first commit.

**Action**: When adding or modifying shell scripts, always check `git ls-files -s` to confirm `100755` mode before pushing.

---

### Rule 23: Service Names for Dev vs Production
**Context**: After deploying code changes, the planning page was updated on port 5100 (dev) but not on port 8000 (production) because the production service wasn't restarted.
**Pattern**: Two separate services run the app:
- **Dev** (port 5100): `sudo systemctl restart spotops-dev.service`
- **Production** (port 8000): `sudo systemctl restart ctv-bookedbiz-db`

Deploy directory for both: `/opt/apps/ctv-bookedbiz-db/`

**Action**: After pulling new code into the deploy directory, restart both services if you want changes reflected on both ports.

---

### Rule 24: Deploy Directory Must Match Dev Branch — No Out-of-Band Commits
**Context**: After deploying the date range picker, `/dev/reports/customer/431` returned 404. The deployed repo at `/opt/apps/ctv-bookedbiz-db/` had 2 extra commits (tailscale refactoring) that weren't on the local dev branch. Those commits removed the `/dev` prefix middleware from `app.py` and broke the `user_management` blueprint import.
**Pattern**: The deploy directory can drift from the dev branch when commits are made directly in `/opt/apps/` or pushed from another machine. This creates invisible breakage — `git pull origin dev` brings in your changes but keeps the extra commits on top.
**How to detect**:
```bash
# Compare local dev HEAD vs deployed HEAD
git log --oneline -3
git -C /opt/apps/ctv-bookedbiz-db log --oneline -3
# If deployed has commits not in local dev, the repos have diverged
```
**How to fix**:
```bash
cd /opt/apps/ctv-bookedbiz-db && git reset --hard <dev-branch-HEAD>
sudo systemctl restart spotops-dev.service
```
**Prevention**: Never commit directly in the deploy directory. All changes flow through the dev working directory → push to origin → pull in deploy directory.

**Action**: After deploying, always compare `git log` between dev and deploy to confirm they match.

---

### Rule 25: Missing Dependencies in Production Venv Break Blueprints Silently
**Context**: Every page on port 8000 returned 500. The coworker's Tailscale login code added `src/web/utils/tailscale.py` (imports `requests_unixsocket`) and modified `user_management.py` to import it. The production venv (`/opt/venvs/ctv-bookedbiz-db/`) didn't have `requests_unixsocket` installed.
**Pattern**: Blueprint registration in `app.py` is wrapped in try/except that catches `ImportError` and only logs it. The app continues without the blueprint. But `base.html` calls `url_for('user_management.login')` on every page — so the missing blueprint causes a `BuildError` on every render, returning 500.
**The cascade**:
1. Missing pip package → `ImportError` during blueprint import
2. try/except swallows the error, logs "Failed to register user management blueprint"
3. App starts without the blueprint
4. Every template render hits `url_for('user_management.login')` → `BuildError` → 500
**How to diagnose**: Check service startup logs for "Failed to register" messages:
```bash
journalctl -u ctv-bookedbiz-db --no-pager | grep -i 'failed.*blueprint'
```
**Action**: When adding new Python dependencies to the codebase, install them in ALL venvs — both dev (`.venv/`) and production (`/opt/venvs/ctv-bookedbiz-db/`). The dev service (port 5100) and production service (port 8000) use different venvs.

---

### Rule 26: Tailscale Socket Permissions for App Access
**Context**: Even after fixing the import error, the Tailscale whois lookups returned 403 Forbidden because the `ctvbooked` user couldn't access `/run/tailscale/tailscaled.sock`.
**Pattern**: The Tailscale daemon socket is owned by `root:root` with `0755` directory and `0666` or stricter socket permissions by default. The app process runs as `ctvbooked` and needs read/write access to query the local API.
**Fix**: systemd override on `tailscaled.service`:
```ini
[Service]
ExecStartPost=/bin/sh -c 'sleep 1 && chgrp ctvbooked /run/tailscale/tailscaled.sock && chmod 0660 /run/tailscale/tailscaled.sock'
```
**Note**: `tailscaled` does NOT support `--socket-group` or `--socket-perms` flags (those don't exist). The `ExecStartPost` approach is the correct way to grant group access.
**Action**: After any `tailscaled` restart or upgrade, verify socket permissions: `ls -la /run/tailscale/tailscaled.sock` should show `root:ctvbooked`.

---

### Rule 27: Full Deploy Workflow — Dev Directory to Production

**Context**: Pending insertion orders feature was built in `/home/daseme/dev/ctv-bookedbiz-db/` (dev branch), but the running services serve from `/opt/apps/ctv-bookedbiz-db/` (main branch). Manually copying files during development caused permission issues and stale state.

**The complete deploy workflow**:
1. **Develop and test** in `~/dev/ctv-bookedbiz-db/` on the `dev` branch
2. **Commit and push** `dev` to origin: `git push origin dev`
3. **Create PR** from `dev` → `main` (main branch has push protection): `gh pr create --base main --head dev`
4. **Merge PR**: `gh pr merge <number> --merge`
5. **Pull into deploy dir**: `git -C /opt/apps/ctv-bookedbiz-db pull origin main`
6. **Restart services**:
   - Dev: `sudo systemctl restart spotops-dev.service`
   - Production: `sudo systemctl restart ctv-bookedbiz-db.service`

**Common pitfalls**:
- **Manual file copies** (`cp` from dev to `/opt/apps/`) cause permission mismatches (files owned by `daseme` instead of deploy user) and drift from git state. Use `git pull` instead.
- **File permissions on generated data**: Files created by `daseme` (e.g., scanner JSON output) default to `600`. The `ctvbooked` service user can't read them. Always `chmod 644` generated data files in the deploy dir.
- **Stash before pull**: If you did manual copies during dev testing, the deploy dir has uncommitted changes. `git stash --include-untracked` before pulling, then drop the stash.
- **Main branch requires PRs**: Can't push directly to main. Must go through `gh pr create` + merge.

**For new systemd timers/services**:
```bash
sudo cp /opt/apps/ctv-bookedbiz-db/scripts/<name>.service /etc/systemd/system/
sudo cp /opt/apps/ctv-bookedbiz-db/scripts/<name>.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now <name>.timer
```

**Action**: Never manually copy files to the deploy dir as a deployment strategy. Always flow through git: commit → push dev → PR → merge → git pull in deploy dir.

---

### Rule 28: Generated Data Files — Gitignore and Permissions

**Context**: The insertion order scanner writes `data/pending_orders.json` at runtime. This file should not be committed (it's environment-specific), and must be readable by the service user.

**Pattern**:
- Add generated data files to `.gitignore` immediately
- When the scanner runs as `daseme` but the web service runs as `ctvbooked`, the file defaults to `600` (owner-only)
- The service silently returns empty results instead of erroring — easy to miss

**Action**: After any script generates files that the web service reads, verify permissions: `chmod 644` and confirm the service user can read them. Add to `.gitignore` so they don't pollute the repo.

---

---

### Rule 29: All Route Files Need Auth — Never Assume Tailscale Is Enough

**Context**: Planning, reporting, data management, and all other route files had zero auth decorators. Any Tailscale user could view all data and perform write operations (create/edit/delete entities, modify forecasts, bulk deactivate customers).
**Pattern**: Being behind Tailscale provides network-level auth, but it doesn't provide role-based access control. Every route file needs:
- **Login required** on all endpoints (not just "important" ones)
- **Admin required** on all write endpoints (POST/PUT/DELETE)
- **Never trust client-supplied identity** in write operations (e.g., `data.get("updated_by")` — use `current_user.full_name` server-side)

**Implementation pattern used**:
- Global `before_request` in `app.py` for login enforcement (with exemptions for /health GET, /users/login, /static/)
- Blueprint-level `before_request` for admin checks on write methods
- Explicit `@login_required` / `@admin_required` decorators on planning.py (the first file fixed)

**Action**: When adding new route files or endpoints, auth decorators must be present from the start. Use `before_request` on the blueprint for blanket coverage.

---

**Last Updated**: 2026-02-26
**Session Context**: Auth hardening — locked down all route files with login_required + admin_required for writes.
