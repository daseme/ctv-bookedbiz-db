# SpotOps Agent Lessons Learned

Operational scar tissue from production failures. Organized by domain.
Load the relevant section before working in that area.

---

## ⚠️ RECURRING FAILURE: Production Database Path

This mistake has been made three separate times (Rules 21, 33, 36). It keeps
recurring because every new script, service, and CLI tool defaults to a
project-relative path that resolves to a 4KB empty skeleton.

**The only production database is:**
```
/var/lib/ctv-bookedbiz-db/production.db
```

**These are NOT the production database:**
```
data/database/production.db          ← 4KB empty skeleton
data/database/production_dev.db      ← dev service copy
.data/dev.db                         ← dev working directory copy
```

**Canonical path resolution for any new component:**
```python
db_path = (
    os.environ.get("DB_PATH")
    or os.environ.get("DATABASE_PATH")
    or "data/database/production.db"  # fallback — almost always wrong in prod
)
```

**For systemd services:** Set `DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db`
in the env file. If the unit uses `ProtectSystem=strict`, add
`ReadOnlyPaths=/var/lib/ctv-bookedbiz-db`.

**Verify the right file is being used:**
```bash
sqlite3 /var/lib/ctv-bookedbiz-db/production.db "SELECT COUNT(*) FROM spots WHERE broadcast_month='Jan-26';"
sqlite3 data/database/production.db "SELECT COUNT(*) FROM spots WHERE broadcast_month='Jan-26';"
# Record counts should differ significantly — prod is ~1.4GB, skeleton is ~4KB
```

**When adding any new CLI tool, service, or migration:** Explicitly confirm it
resolves to `/var/lib/ctv-bookedbiz-db/production.db`. Never trust the default.

---

## Database & Schema

### Column Names Differ Between Entities
- `agencies` table uses `agency_name`
- `customers` table uses `normalized_name` (NOT `customer_name`)
- Both have `is_active`, `notes`, contacts via `entity_contacts`
- When building unified views, handle entity-type-specific fields conditionally

Always run `PRAGMA table_info(table_name)` when writing queries against
unfamiliar tables. Don't guess column names.

### CHECK Constraints Are DB-Level — Python Enums Don't Update Them
Adding a value to a Python enum does NOT update the database CHECK constraint.
SQLite doesn't support `ALTER TABLE ... DROP CONSTRAINT` — table must be
recreated. The failure cascade is silent: Python accepts the value, the INSERT
hits the DB constraint, rollback occurs, the calling code gets a generic error.

When adding new enum values that map to DB columns, always check `.schema table`
for CHECK constraints and write a migration if needed.

Example: Adding `VIEWER = "viewer"` to `UserRole` failed silently until migration
updated `CHECK (role IN ('admin', 'management', 'AE', 'viewer'))`.

### NULL customer_id Has Two Distinct Causes — Don't Conflate Them
A spot with `customer_id IS NULL` is in one of two states:

1. **No alias exists** — bill_code was never resolved → needs Link action
2. **Alias exists, backfill missed** — alias was created but spots weren't updated → needs Backfill action

The `entity_resolution` unresolved query correctly filters both
(`c.customer_id IS NULL AND ea.alias_id IS NULL`). Any new query against
unresolved spots must join to `entity_aliases` and handle both cases separately.

### Triggers Must Cover Both Directions of a Relationship
When two tables must stay in sync, a trigger on one table covers only half:
- Trigger on `entity_aliases` INSERT/UPDATE → backfills existing spots when alias changes
- Trigger on `spots` INSERT → looks up existing alias when new spot arrives

Both are required. Between migrations 022 (alias triggers) and 023 (spot
insert trigger), 5,864 spots arrived with NULL `customer_id` despite having
matching aliases — the gap window from only covering one direction.

When adding sync triggers, always ask: "What if a row is added to the *other* table?"

### Orphan Customer Lifecycle
Customer rows are created during **manual resolution** only
(`CustomerResolutionService.create_customer_and_alias`). The daily import
never creates customer rows — it only adds bill codes to `raw_customer_inputs`.

The `customers` table has `ON DELETE RESTRICT` and only grows. When spots are
remapped to a different entity (e.g., agency-prefixed name replaces bare name),
the original customer row persists with zero spots.

Orphan indicators: `customer_id` has 0 spots, name is a variant of another
entity (abbreviation, agency prefix, `&` vs `and`).

Current cleanup path: Stale Customer Report → manual deactivation.

### Test Client Always Needs DB Path Override
`create_app()` sets `DB_PATH` from settings → resolves to `data/database/production.db`
(the empty skeleton). The `or "./.data/dev.db"` fallback does NOT trigger because
the config IS set — just to the wrong path.

```python
# WRONG — uses empty skeleton
app = create_app()

# CORRECT
app = create_app()
app.config['DB_PATH'] = '.data/dev.db'
```

Always set `app.config['DB_PATH']` after `create_app()` in any test context.

---

## Query Patterns

### WorldLink Filter — Use `'WorldLink%'` With No Colon
WorldLink spots have `sales_person = 'House'` and are identified by
`bill_code LIKE 'WorldLink%'`. The prefix has NO colon because some bill codes
use a space separator (`WorldLink Broker Fees (DO NOT INVOICE)`).

**Wrong patterns found in the codebase:**
```sql
NOT LIKE 'WL%'           -- wrong prefix entirely
LIKE 'WL:%'              -- no bill codes use this
LIKE 'WORLDLINK:%'       -- SQLite LIKE is case-sensitive by default
LIKE 'WorldLink:%'       -- misses space-separator variant
```

**Correct patterns:**
```sql
-- Select WorldLink spots
WHERE bill_code LIKE 'WorldLink%'

-- Exclude WorldLink from House
WHERE sales_person = 'House'
  AND (bill_code NOT LIKE 'WorldLink%' OR bill_code IS NULL)
```

Audit for wrong patterns:
```bash
rg "LIKE.*WL|LIKE.*orldlink|LIKE.*orldLink" --type py src/
```
All results should use exactly `'WorldLink%'`.
Canonical reference: `planning_repository.py:get_booked_revenue()`.

### Never Disable Query Logic and Return Fake Data
The Customer Sector Manager revenue column showed $0 for an unknown period
because the revenue query was commented out (`# TEMP: Disabled for performance`)
and replaced with `revenue_rows = []`. The silent fallback (zeros instead of
errors) made it invisible.

When a query is too slow, rewrite it. A direct `GROUP BY customer_id` on spots
takes under 1s for 308 customers. Don't comment out logic and ship fake data.

### Timestamps Must Be Pacific Time in All User-Facing Output
SQLite `CURRENT_TIMESTAMP` is UTC. All user-facing timestamps must be converted
to US Pacific time (PST UTC-8 / PDT UTC-7) with DST awareness before display.
Never show raw UTC or ISO timestamps in the UI.

---

## Deployment & Services

### The Full Deploy Workflow
```
1. Develop and test in ~/dev/ctv-bookedbiz-db/ on dev branch
2. git push origin dev
3. gh pr create --base main --head dev
4. gh pr merge <number> --merge
5. git -C /opt/apps/ctv-bookedbiz-db pull origin main
6. sudo systemctl restart spotops-dev.service      # port 5100
7. sudo systemctl restart ctv-bookedbiz-db.service # port 8000
```

**Never manually copy files to the deploy dir.** Files copied as `daseme` have
wrong ownership for the `ctvbooked` service user. Always flow through git.

### Service Names and Ports
- **Dev** (port 5100): `spotops-dev.service`
- **Production** (port 8000): `ctv-bookedbiz-db.service`
- Deploy directory for both: `/opt/apps/ctv-bookedbiz-db/`
- Dev and production use **different venvs** — packages installed in one are
  not available in the other

### Deploy Dev First, Then Production
Dev is the safety net. If something breaks on dev, production is untouched.
Data differences between `dev.db` and production can mask or create false
failures — if dev data doesn't cover the test case, use a query that does hit
dev data to confirm the code path works.

### Deployed Repo Can Drift From Dev Branch
If commits are made directly in `/opt/apps/` or pushed from another machine,
the deploy dir diverges from the local dev branch. `git pull origin dev` brings
in your changes but keeps the extra commits on top, causing invisible breakage.

Detect divergence:
```bash
git log --oneline -3
git -C /opt/apps/ctv-bookedbiz-db log --oneline -3
```

Fix:
```bash
cd /opt/apps/ctv-bookedbiz-db && git reset --hard <dev-branch-HEAD>
```

### Missing pip Dependencies Break Blueprints Silently
Blueprint registration in `app.py` is wrapped in `try/except ImportError` that
logs and continues. The app starts without the blueprint. If `base.html` calls
`url_for('missing_blueprint.route')` on every render, every page returns 500.

Diagnose:
```bash
journalctl -u ctv-bookedbiz-db --no-pager | grep -i 'failed.*blueprint'
```

When adding new Python dependencies, install in ALL venvs:
- Dev: `.venv/`
- Production: `/opt/venvs/ctv-bookedbiz-db/`

### System vs User Systemd Services Can Conflict
Services exist at two levels — check both when troubleshooting port conflicts:
```bash
systemctl list-units --type=service | grep ctv          # system level
systemctl --user list-units --type=service | grep ctv   # user level
sudo lsof -i :5100                                       # what's on the port
```

### Shell Scripts Must Have Execute Permission in Git
Git tracks `100644` (non-executable) vs `100755` (executable). When a `.sh`
file is rewritten rather than edited in-place, the execute bit can be lost.
A script missing execute permission fails with exit code 126 silently in cron
or systemd — the job appears to run but does nothing.

```bash
# Audit
git ls-files -s bin/*.sh   # all should show 100755, not 100644

# Fix
chmod +x bin/script.sh
git update-index --chmod=+x bin/script.sh
git commit -m "Fix execute permission on bin/script.sh"
```

After editing any shell script, verify the execute bit before committing.

---

## Infrastructure & Permissions

### tempfile.mkstemp Creates 0600 Files — Always chmod Before Rename
`tempfile.mkstemp` always creates files with mode `0600` regardless of umask
or directory setgid bits. When one service writes a file that another reads,
the default permissions silently block access. Silent fallbacks (empty list
instead of error) make this invisible for weeks.

```python
fd, tmp_path = tempfile.mkstemp(dir=output_dir, suffix=".tmp")
try:
    with os.fdopen(fd, "w") as f:
        json.dump(result, f)
    os.chmod(tmp_path, 0o640)   # group-readable BEFORE rename
    os.replace(tmp_path, output_path)
```

`chmod` must happen BEFORE `os.replace` — otherwise there's a window where
the destination file has wrong permissions.

### Generated Data Files Need gitignore and Correct Permissions
Files written at runtime (e.g., scanner JSON output) are environment-specific
and should not be committed. Add to `.gitignore` immediately. When the writer
runs as `daseme` but the reader runs as `ctvbooked`, verify the file is group-
readable after each run.

### Tailscale Socket Permissions
The Tailscale daemon socket (`/run/tailscale/tailscaled.sock`) is `root:root`
by default. The app process runs as `ctvbooked` and needs access to query the
local API.

Fix via systemd override on `tailscaled.service`:
```ini
[Service]
ExecStartPost=/bin/sh -c 'sleep 1 && chgrp ctvbooked /run/tailscale/tailscaled.sock && chmod 0660 /run/tailscale/tailscaled.sock'
```

`tailscaled` does NOT support `--socket-group` or `--socket-perms` flags.
After any `tailscaled` restart or upgrade, verify:
```bash
ls -la /run/tailscale/tailscaled.sock   # should show root:ctvbooked
```

---

## Auth & Security

### Every Route Needs Auth — Tailscale Is Not RBAC
Being behind Tailscale provides network-level access control, not role-based
access control. Any Tailscale user can hit any route unless decorators are
applied.

- All endpoints: login required
- All write endpoints (POST/PUT/DELETE): admin required
- Never use `data.get("updated_by")` from client payload for write attribution — use `current_user.full_name` server-side

Implementation pattern:
- Global `before_request` in `app.py` for login enforcement (exempt: `/health` GET, `/users/login`, `/static/`)
- Blueprint-level `before_request` for admin checks on write methods

When adding new route files, auth must be present from the start.

---

## Template & Code Cleanup

### Verify All References Before Deleting Any File
```bash
rg -l "filename.html" src/
rg "{% include.*filename\.html" src/
rg "{% extends.*filename\.html" src/
rg "template_name" --type py --type html --type md
```
The `nord_base.html` deletion broke 7 templates. Never delete without
comprehensive reference checking.

### Categorize Files by Risk Before Bulk Cleanup
- **Zero risk**: Files in `old/` directories with `-OLD` suffixes
- **Low risk**: Unused imports confirmed by static analysis
- **Medium risk**: Templates requiring reference verification
- **High risk**: Active code with potential runtime dependencies

Start with zero-risk, work up the ladder. Commit atomically by risk level so
each phase can be rolled back independently.

### Blueprint Registration: One Place Only
Register blueprints in `src/web/blueprints.py` via `initialize_blueprints()`.
Never also register in `app.py`. Dual registration causes warnings and
unpredictable behavior.

---

## Data Operations

### Customer Merge and Agency ID Backfill (2026-03-11)

**Problem**: `agency_id` on the `customers` table was almost entirely unused (4 of 302
active customers). Agency relationships were encoded only in the `Agency:Name` prefix
of `normalized_name`. This caused:
- Duplicate customers when the same advertiser was imported under agency name variants
  (e.g., "Sagent:Cal Fire" vs "Sagent Marketing:Cal Fire")
- No structured way to query "all customers through Agency X"
- Customer detail page couldn't show agency badge (template already supported it via
  `report.summary.agency_name`, but the JOIN on `agency_id` returned NULL)

**Merges performed** (6 total — source deactivated, spots+aliases moved to target):

| Source | Target | Spots moved |
|---|---|---|
| 431: Admerasia:McDonald's | 23: McDonald's | 1,120 |
| 396: Sagent:CalTrans | 448: Sagent Marketing:CalTrans | 2,561 |
| 395: Sagent:Cal Fire | 393: Sagent Marketing:Cal Fire | 2,927 |
| 330: Imprenta Communications Group, Inc.:PG&E | 331: Imprenta:PG&E | 0 |
| 450: opAD Media Solutions LLC:NY State DoH | 555: opAD:NY State DoH | 0 |
| 451: opAD Media Solutions LLC:NY Dept HMH | 554: opAD:NY Dept HMH | 0 |
| 449: Solsken Communications:My Sister's House | 399: Solsken:My Sister's House | 767 |

**Agency ID backfill**: 146 customers now have `agency_id` set (was 4). Backfill matched
`SUBSTR(normalized_name, 1, INSTR(normalized_name, ':') - 1)` to `agencies.agency_name`.
Unmatched cases were due to:
- Agency record was inactive (6 reactivated: IDs 90, 91, 99, 100, 104, 107)
- Name variant: hyphen vs no hyphen ("Brown-Miller" → agency "Brown Miller"),
  "Inc." suffix, "LLC" suffix, case mismatch

**Remaining**: 22 Worldlink-prefixed customers have no `agency_id` (intentional — Worldlink
is handled separately). 128 customers without agency prefix have no `agency_id` (correct —
they're direct advertisers).

**How to find future duplicates**:
```sql
-- Same advertiser, same agency (name variants)
WITH parsed AS (
  SELECT customer_id, normalized_name,
    SUBSTR(normalized_name, INSTR(normalized_name, ':') + 1) as advertiser,
    SUBSTR(normalized_name, 1, INSTR(normalized_name, ':') - 1) as prefix
  FROM customers WHERE is_active = 1 AND normalized_name LIKE '%:%'
)
SELECT p1.customer_id, p1.normalized_name, p2.customer_id, p2.normalized_name
FROM parsed p1 JOIN parsed p2
  ON p1.advertiser = p2.advertiser AND p1.customer_id < p2.customer_id
  AND (p1.prefix LIKE p2.prefix || '%' OR p2.prefix LIKE p1.prefix || '%');
```

**How to find missing `agency_id`**:
```sql
SELECT customer_id, normalized_name,
  SUBSTR(normalized_name, 1, INSTR(normalized_name, ':') - 1) as prefix
FROM customers
WHERE is_active = 1 AND agency_id IS NULL AND normalized_name LIKE '%:%'
  AND normalized_name NOT LIKE 'Worldlink:%' AND normalized_name NOT LIKE 'WorldLink:%';
```

---

## Business Logic

### Language Codes Are Business Logic, Not Technical Data
All languages on this platform are Asian except English.
- `P` = Punjabi (South Asian), NOT Portuguese
- Language mappings encode business identity — verify business context before
  consolidating or remapping any language constant

**Last Updated**: 2026-03-10