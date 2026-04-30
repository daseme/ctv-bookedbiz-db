# LLM System Guide

**Audience:** Claude Code agents and other coding LLMs working on this repo
**Purpose:** Precise reference for paths, commands, env vars, schema invariants, and footguns — designed to prevent you from hallucinating any of these. Dense and lookup-shaped, not narrative.
**Last reviewed:** 2026-04-30

---

## Read these first (in this order)

1. **`.claude/CLAUDE.md`** — auto-loaded into every Claude Code session. The cheat-sheet form of these rules. **Authoritative when it conflicts with this file.**
2. **`.claude/tasks/lessons.md`** — running corrections log. Read it; you've probably made a mistake before that's documented there.
3. This file — deeper reference. Cross-links to the other consolidated docs:
   - [HUMAN_OPERATOR_GUIDE.md](HUMAN_OPERATOR_GUIDE.md) — operator surface
   - [DEV_WORKFLOW.md](DEV_WORKFLOW.md) — branching, dev loop, schema changes
   - [RUNBOOKS.md](RUNBOOKS.md) — operational commands
   - [ARCHITECTURE.md](ARCHITECTURE.md) — topology, data model, DR
   - [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) — endpoint contracts

---

## System in one paragraph

SpotOps is a Flask + uvicorn app running in a single Docker container on a Linux host (`spotops`), serving a Tailscale-only web dashboard plus authenticated API endpoints that feed `Revenue Master.xlsx`. It reads and writes one SQLite database (`/srv/spotops/db/production.db`, ~1.4 GB, WAL mode). Imports pull from a K-drive Excel file four times a day via systemd timers. Backups go to Backblaze B2 (continuous via Litestream + nightly via restic) and Dropbox (nightly snapshot).

---

## What you can trust without re-checking (as of last review)

These are the load-bearing facts. If you find code that contradicts them, the code is probably right and this file should be updated.

| Fact | Value |
|---|---|
| Repo / working dir | `/opt/spotops` |
| Container name | `spotops-spotops-1` |
| Compose service name | `spotops` |
| Image name | `spotops-spotops` |
| Port (host:container) | `127.0.0.1:8000:8000` |
| Live SQLite DB | `/srv/spotops/db/production.db` |
| Compose env file | `/opt/spotops/.env` |
| Restart for code changes | `docker compose up -d --build spotops` |
| Restart for env/config changes only | `docker compose up -d spotops` |
| Bounce a healthy container (no code change) | `docker compose restart spotops` |
| Health endpoint | `GET /health/` (note: `/api/health` returns 401) |
| Default branch | `main` (protected, PR + squash only) |
| Dev branch | `dev` |
| Trade exclusion clause | `WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)` |
| Broadcast month format | Title-cased `Mmm-YY` (`Sep-26`, `Oct-25`) |
| Customer table identity column | `normalized_name` (NOT `customer_name`) |
| Agency table identity column | `agency_name` |
| Blueprint registration | `src/web/blueprints.py` via `initialize_blueprints()` — **never** `app.py` |

---

## Filesystem reference

### Repo (`/opt/spotops/`)

| Path | What it is |
|---|---|
| `docker-compose.yml` | Compose stack definition (one service `spotops`) |
| `Dockerfile` | App image |
| `backblaze_startup.sh` | Container ENTRYPOINT (handles `RESTORE_ON_START`, sets `READ_ONLY_MODE` from `APP_MODE`, exec uvicorn) |
| `.env` | env_file loaded by compose |
| `.venv/` | Host venv (used **only** by `bin/db_sync.sh` — keep small; do NOT consolidate into the container) |
| `cli/` | Python CLI entry points run inside the container (`daily_update.py`, `import_closed_data.py`, `daily_update.py`) |
| `cli_db_sync.py` | Dropbox sync CLI (host venv); operations `upload`, `backup`, `download`, `info`, `test` |
| `bin/` | Host wrapper scripts invoked by systemd: `commercial_import.sh`, `daily_update.sh`, `db_sync.sh`, `daily-download.sh`, `rotate_commercial_logs.sh`, `run-dev.sh` |
| `scripts/` | Python scripts + systemd unit templates. Some unit files are **templates only** (not installed) — see [Service inventory](#service-inventory-live-vs-deprecated) |
| `sql/migrations/` | Numbered SQL migrations (apply manually; see [DEV_WORKFLOW.md](DEV_WORKFLOW.md#schema-changes)) |
| `src/web/app.py` | Flask app factory (`create_app()`) |
| `src/web/asgi.py` | ASGI adapter for uvicorn |
| `src/web/blueprints.py` | **Where blueprint registration happens**. `initialize_blueprints(app)` |
| `src/web/routes/` | Route blueprints |
| `src/web/templates/` | Jinja2 templates (Nord-themed; Chart.js for visualizations) |
| `src/services/container.py` | DI container |
| `src/services/factory.py` | Service registration |
| `src/repositories/` | Data access; raw SQL lives here, not in services or routes |
| `src/config/settings.py` | Configuration (settings model, env loading) |
| `tests/` | Pytest suite |
| `data/database/production.db` | **4 KB empty skeleton** — DO NOT use; see [Footgun #1](#footgun-1-the-empty-skeleton-db) |

### Live data (`/srv/spotops/`)

| Path | Purpose |
|---|---|
| `db/production.db` | Live SQLite DB |
| `db/.snapshot.db` | Transient (created/cleaned by `db_sync.sh`) |
| `db/snap-*.sqlite3` | Ad-hoc snapshots |
| `data/raw/daily/` | Where `bin/commercial_import.sh` lands `Commercial Log YYMMDD.xlsx` (mounted into container at `/app/data/raw/daily/`) |
| `data/` (host) | Mounted as `/app/data/` inside the container |
| `processed/` | Processed import artifacts |
| `uploads/` | Currently empty; included in restic backup target |

### System config (`/etc/`)

| Path | Purpose | Readable as |
|---|---|---|
| `litestream.yml` | Litestream replication config | root only |
| `litestream.env` | B2 credentials for Litestream | root only |
| `restic.env` | Restic repo URL + password | root only |

> Pi-era files like `/etc/ctv-litestream.env`, `/etc/ctv-db-sync.env`, `/etc/ctv-bookedbiz-db/` **no longer exist**. If you see references to them in old code or docs, they're stale.

### Logs (`/var/log/`)

| Path | Source |
|---|---|
| `ctv-daily-update/update.log` | `bin/daily_update.sh` |
| `ctv-commercial-import/import.log` | `bin/commercial_import.sh` |
| `ctv-db-sync/sync.log` | `bin/db_sync.sh` |
| `restic/backup.log` | `restic-backup.service` |
| (systemd journal) | `journalctl -u <unit>` for `litestream.service`, `ctv-*` services |

---

## Environment variables

Loaded into the container from `/opt/spotops/.env` via compose `env_file:`. Also sourced by `bin/db_sync.sh` for the host-side Dropbox sync.

| Variable | Purpose | Set in |
|---|---|---|
| `DATABASE_PATH` | Live DB path. Pinned to `/srv/spotops/db/production.db` | `/opt/spotops/.env` |
| `APP_MODE` | `replica_readonly` (default) or `failover_primary` | `.env` |
| `READ_ONLY_MODE` | Derived from `APP_MODE` by `backblaze_startup.sh` | (auto-set in container) |
| `RESTORE_ON_START` | If `true`, entrypoint runs Litestream restore from B2 before starting uvicorn | `.env` |
| `SHEET_EXPORT_TOKEN` | Shared secret for `/api/revenue/sheet-export` and `/api/revenue/planning-export` | `.env` |
| `DROPBOX_APP_KEY` | Dropbox OAuth | `.env` |
| `DROPBOX_APP_SECRET` | Dropbox OAuth | `.env` |
| `DROPBOX_REFRESH_TOKEN` | Dropbox OAuth | `.env` |
| `DROPBOX_DB_PATH` | Path inside Dropbox where DB snapshot lives (typically `/database.db`) | `.env` |
| `NTFY_TOPIC` | Push-notification topic for daily update success/failure | `.env` (current default fallback in `bin/daily_update.sh`: `ctv-import-2a11ef7e7a84`) |
| `B2_*` | Backblaze credentials (Litestream + restic) | `/etc/litestream.env`, `/etc/restic.env` (NOT in app `.env`) |
| `RESTIC_REPOSITORY`, `RESTIC_PASSWORD_FILE` | restic config | `/etc/restic.env` |

> **Don't hardcode any of these in code.** Read from env via `src/config/settings.py`.

---

## Service inventory (live vs deprecated)

`systemctl list-timers --all` and `systemctl list-unit-files 'ctv-*' 'litestream*' 'restic*'` give the truth. As of last review:

### Active

| Unit | Type | Schedule | What it does |
|---|---|---|---|
| `litestream.service` | service | continuous | WAL replication → Backblaze B2 |
| `ctv-commercial-import.timer` | timer | 4×/day ≈03:00, 09:00, 15:00, 21:00 PT | K-drive copy to `/srv/spotops/data/raw/daily/` |
| `ctv-daily-update.timer` | timer | 4×/day, ≈30 min after each commercial-import | `docker compose exec spotops uv run python cli/daily_update.py` |
| `ctv-db-sync.timer` | timer | nightly ≈02:04 (+5 min jitter) | DB → Dropbox snapshot via SQLite online backup |
| `restic-backup.timer` | timer | nightly 02:30 (+10 min jitter) | restic backup of `/srv/spotops/{data,processed,uploads}` to B2 |

### Decommissioned (do not enable without re-verifying)

These have unit files in `/opt/spotops/scripts/` but are **not installed** as system units:

| Unit | Why retired |
|---|---|
| `ctv-pi2-download.timer` / `.service` | Pi2 nightly mirror not currently running. Re-enabling requires verifying paths and target host |
| `ctv-io-scanner.timer` / `.service` | Insertion Order Scanner gone. `pending_orders.json` does not exist on disk; dashboard "pending orders" widget is defunct or fed elsewhere |
| `ctv-db-validation.timer` / `.service` | Never installed; intent unclear |

### Pi-era (gone, do not reference)

`ctv-bookedbiz-db.service`, `flaskapp.service`, `spotops-dev.service` — all from the pre-Docker era. The app no longer runs as systemd; it's the `spotops-spotops-1` container.

---

## Database — invariants

These are hard contracts. Violating them silently corrupts revenue numbers or breaks external clients.

### Trade exclusion

Every revenue-bearing query MUST exclude Trade revenue:

```sql
WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
```

The two API export endpoints already apply this server-side. New in-app queries must too.

### Identity columns

| Table | Identity column | NOT |
|---|---|---|
| `customers` | `normalized_name` | not `customer_name` |
| `agencies` | `agency_name` | — |

`spots.bill_code` is the raw identifier; resolution to a canonical customer goes through `v_customer_normalization_audit`.

### Broadcast month

- Stored: title-cased `Mmm-YY` (`Sep-26`, `Oct-25`). Source: Excel column 18.
- Old format (pre-2024): mixed `YYYY-MM-DD` / `YYYY-MM-DD HH:MM:SS` / `mmm-yy`. Migration normalized to title-cased `Mmm-YY`.
- API outputs: ISO `YYYY-MM-01` (first-of-month). Server converts on emission.
- LIKE patterns: `WHERE broadcast_month LIKE '%-24'` (year suffix). Old `LIKE '2024%'` patterns are dead.

### Blueprint registration

```python
# src/web/blueprints.py
def initialize_blueprints(app):
    app.register_blueprint(some_bp)
    app.register_blueprint(another_bp)
    ...
```

Called once from the app factory. **Never** add `app.register_blueprint(...)` in `app.py` directly. The older `GUIDE-CanonTools.md` told contributors to do that — it's wrong.

### DB connection access

```python
db = container.get("database_connection")
with db.connection() as conn:
    cur = conn.execute(...)
```

Don't `import sqlite3` and call `sqlite3.connect()` in routes/services/repositories. The DI-managed connection handles WAL configuration and connection lifecycle.

---

## Schema "do not break" list

Changes here have external blast radius:

| Schema element | Why it's load-bearing |
|---|---|
| `spots.bill_code`, `revenue_type`, `sales_person`, `gross_rate`, `station_net`, `broker_fees`, `broadcast_month`, `agency_flag` | Sheet-export contract reads these. Changes need a `hash_version` bump or you silently invalidate `tblKnownRows` acknowledgements in the workbook |
| `revenue_entities.entity_name`, `is_active` | Planning-export reads these as the AE list |
| `budget`, `forecast` tables | Planning-export reads these. The dashboard's `POST /planning/api/forecast` is the sole writer; **don't add a second writer** |
| `spot_language_assignments` (`assignment_method`, `language_status`, `confidence`, `requires_review`) | Reporting queries depend on these enum values |
| `spot_category` values: `language_assignment_required`, `review_category`, `default_english` | Hardcoded in queries throughout |
| `customer_canonical_map`, `agency_canonical_map` (case-insensitive duplicate detection) | Duplicates here cause silent revenue double-counting in normalization views |
| `entity_aliases.entity_type` enum (`customer`, `agency`) | Canon Tool depends on this distinction |
| `users.email` | Tailscale-resolved login email matches this exactly. Adding case normalization changes auth behavior |
| `broadcast_month_closures.status` (`OPEN`, `CLOSED`) | Determines whether `cli/import_closed_data.py` skips a month in WEEKLY_UPDATE mode |

---

## API endpoints (one-line each)

Defer to [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) for the full spec. Quick lookup:

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET /api/revenue/sheet-export` | `X-SpotOps-Token` | Workbook Data tab — all-history per-month tuple grain |
| `GET /api/revenue/planning-export[?year=YYYY]` | `X-SpotOps-Token` | Workbook Planning tab — AE × month rollup |
| `POST /api/canon/agency` | session | Add/update agency canonical alias |
| `POST /api/canon/customer` | session | Add/update customer-tail alias |
| `POST /api/canon/raw-to-customer` | session | Map raw bill_code to canonical customer |
| `GET /api/canon/suggest/normalized?q=…` | session | Autocomplete normalized customer names |
| `GET /health` | none | Liveness — **use this**, not `/api/health` |
| `GET /api/health` | (allow-list miss) | **Returns 401**; do not use for liveness |
| `POST /planning/api/forecast` | `@admin_required` | Sole writer to `forecast` table — see [Schema do-not-break](#schema-do-not-break-list) |
| `POST /planning/api/forecast/bulk` | `@admin_required` | Bulk forecast write |
| `POST /planning/api/new-business/bulk` | `@admin_required` | Bulk new-business write |
| `POST /planning/api/forecast/reset` | `@admin_required` | Forecast reset |

---

## Footguns

### Footgun 1: The empty-skeleton DB

The repo ships `data/database/production.db` as a 4 KB **empty placeholder**. The Flask `create_app()` factory's default `DB_PATH` resolves to it.

- **Tests / scripts that "work" against this file are reading nothing.** No data.
- The `_get_db_path()` fallback `or "./.data/dev.db"` does **not** fire when config is set — and config IS set (just to the wrong empty file).
- **Fix:** for any host-side work that should hit real data, set `DATABASE_PATH=/srv/spotops/db/production.db` explicitly. For Flask test client work:

  ```python
  app.config['DB_PATH'] = '/srv/spotops/db/production.db'
  ```

### Footgun 2: `restart` does NOT pick up Python changes

```bash
docker compose restart spotops    # ✗ won't load new Python
docker compose up -d --build spotops    # ✓ rebuilds + recreates
```

If you ran `restart` and your change "didn't take effect," this is why.

### Footgun 3: `--skip-closed` during monthly close

`cli/import_closed_data.py` (and `update_yearly_recap.sh`) takes `--skip-closed`. Passing it during the **monthly close** procedure imports the data but does **not** close the months — dashboard stays 🟡 yellow. The flag is intended for mid-month refreshes only.

| Scenario | Pass `--skip-closed`? |
|---|---|
| Monthly close (after accounting closes month) | NO. Use HISTORICAL mode (default). |
| Mid-month / mid-quarter refresh of current-year actuals | Yes |

See [HUMAN_OPERATOR_GUIDE.md → Monthly Close](HUMAN_OPERATOR_GUIDE.md#monthly-close) and [ARCHITECTURE.md → Import modes](ARCHITECTURE.md#import-modes).

### Footgun 4: Case-insensitive duplicates in canonical maps

`agency_canonical_map` and `customer_canonical_map` must NOT contain case-insensitive duplicates. Two rows like `('iGRAPHIX', 'iGraphix')` and `('iGraphix', 'iGraphix')` produce duplicate records in `v_normalized_candidates` → silent revenue double-counting. Detection query in [ARCHITECTURE.md → Critical: case-insensitive duplicates](ARCHITECTURE.md#critical-case-insensitive-duplicates-in-canonical-maps).

### Footgun 5: `/api/health` returns 401

The `_require_login` allow-list lists `/health` but not `/api/health`. Use `/health/` for any liveness probe, monitoring check, or `curl` smoke test. This is documented; don't "fix" it without considering whether other code expects `/api/health` to require auth.

### Footgun 6: `raw_customer_inputs` lags `spots.bill_code`

The Canon Tool reads `raw_customer_inputs`, but live data lands in `spots.bill_code`. **They don't auto-sync.** New customer names from imports won't appear in the Canon Tool until the sync query runs. See [RUNBOOKS.md → Customer normalization](RUNBOOKS.md#customer-normalization-raw-input-sync).

### Footgun 7: SHA1 hash on the sheet-export endpoint

The hash protocol is precisely specified — any drift in field order, separator (`U+001F`), null handling (`""` not `"null"`), case (`Text.Lower(x, "en-US")` invariant culture), or whitespace handling (`Text.Trim`, no interior collapse) silently breaks workbook acknowledgements. If you change the hash, **bump `hash_version`** and the workbook errors loudly on mismatch. See [API_AND_EXPORT_CONTRACTS.md → Hash contract](API_AND_EXPORT_CONTRACTS.md#4-hash-contract-critical).

### Footgun 8: Two writers to the `forecast` table

The dashboard endpoints (`POST /planning/api/forecast` and friends, all `@admin_required`) are the **sole** writers. Adding a second writer (e.g., from the workbook) creates conflicting writes. The planning-export endpoint is read-only by design.

### Footgun 9: Pi-era paths in old plans / specs

Files under `docs/plans/` and `docs/superpowers/` are dated point-in-time records. They reference Pi-era paths (`/var/lib/ctv-bookedbiz-db/`, `/opt/apps/ctv-bookedbiz-db/`, `ctvbooked` user, `ctv-bookedbiz-db.service`). **Don't follow them as runbook.** Don't rewrite them either — they're historical record. Current-state reference is in this file + `.claude/CLAUDE.md`.

### Footgun 10: Container holds an open DB handle

The app process inside the container keeps an open SQLite connection. So does Litestream. Operations that need exclusive access (full file replace, vacuum, schema rewrite) must stop both first:

```bash
sudo systemctl stop litestream.service
docker compose -f /opt/spotops/docker-compose.yml stop spotops
# ... destructive op ...
docker compose -f /opt/spotops/docker-compose.yml start spotops
sudo systemctl start litestream.service
```

The pre-Docker recipe (`pkill -f uvicorn; pkill -f sqlite3`) is obsolete — pipelines now run *inside* the container, not on the host.

### Footgun 11: Title-cased `Mmm-YY` in queries

When writing LIKE patterns or string comparisons, remember the format is title-cased:

```sql
-- Right
WHERE broadcast_month = 'Sep-26'
WHERE broadcast_month LIKE '%-26'

-- Wrong
WHERE broadcast_month = 'sep-26'   -- doesn't match
WHERE broadcast_month LIKE 'sep%'  -- doesn't match
```

If the source data ever loses title-casing, fix the source — don't lower-case at query time.

### Footgun 12: Don't mock the database in tests

A prior incident: mocked tests passed but a prod migration failed. Rule: integration tests hit a real SQLite DB (a snapshot or in-memory DB seeded from `sql/migrations/`). Unit tests can stay pure.

---

## Coding rules (LLM-facing)

The full set lives in `.claude/CLAUDE.md` (auto-loaded). The headlines:

| Rule | Where to find authoritative version |
|---|---|
| Plan-mode default for non-trivial tasks | `.claude/CLAUDE.md` § Workflow Orchestration |
| Verification before claiming done | `.claude/CLAUDE.md` § Workflow Orchestration |
| Use subagents for long research / open-ended exploration | `.claude/CLAUDE.md` § Subagent Strategy |
| Self-improvement loop (update `lessons.md` after corrections) | `.claude/CLAUDE.md` § Workflow Orchestration |
| Demand elegance (balanced) | `.claude/CLAUDE.md` § Demand Elegance |
| Autonomous bug fixing | `.claude/CLAUDE.md` § Autonomous Bug Fixing |
| Plan first, write to `.claude/tasks/todo.md` | `.claude/CLAUDE.md` § Task Management |

If in doubt, `.claude/CLAUDE.md` overrides this file when they conflict.

### Code conventions (already in DEV_WORKFLOW.md, surfaced here)

- Lint: `uvx ruff check . && uvx ruff format .` before every commit.
- No emoji in code/comments/commits unless the user asks.
- Default to **no** comments. Only when the *why* is non-obvious.
- No premature abstraction; three similar lines is better than a wrong helper.
- Trust internal callers; validate at system boundaries only.
- Routes → services → repositories. Don't reach across layers.

---

## Where to look before changing X

| Change area | Read first |
|---|---|
| Anything touching the database schema | [ARCHITECTURE.md → Database & storage](ARCHITECTURE.md#database--storage), [Schema do-not-break](#schema-do-not-break-list), `sql/migrations/` for the migration sequence |
| Routes | `src/web/blueprints.py` (registration); the relevant blueprint under `src/web/routes/` |
| New endpoint | `src/web/routes/`, then [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) for the contract style |
| Authentication / authorization | [ARCHITECTURE.md → Authentication](ARCHITECTURE.md#authentication), `src/web/auth/` (or wherever `_require_login` lives), `users` table schema |
| Sheet export / planning export contract | [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) — note hash-version invariant; `src/repositories/planning_repository.py:435-548` for booked semantics |
| Customer normalization / Canon Tool | [ARCHITECTURE.md → Customer/agency canon system](ARCHITECTURE.md#customer--agency-canon-system), `src/web/routes/canon_tools.py`, the view chain in `sql/migrations/` |
| Language assignment | [ARCHITECTURE.md → Language assignment system](ARCHITECTURE.md#language-assignment-system), `cli_01_language_assignment.py` |
| Daily/commercial import behavior | [ARCHITECTURE.md → Import pipeline](ARCHITECTURE.md#import-pipeline), `cli/daily_update.py`, `bin/daily_update.sh`, `bin/commercial_import.sh` |
| Container build / Dockerfile / entrypoint | `Dockerfile`, `backblaze_startup.sh`, `docker-compose.yml` |
| Backups | [RUNBOOKS.md → Backup posture](RUNBOOKS.md#backup-posture); `bin/db_sync.sh`, `cli_db_sync.py`, `/etc/litestream.yml` (root only) |
| Failover / DR | [ARCHITECTURE.md → DR / Backup posture](ARCHITECTURE.md#dr--backup-posture), [RUNBOOKS.md → Failover / failback](RUNBOOKS.md#failover--failback) |
| Service container / DI | `src/services/container.py`, `src/services/factory.py` |
| Settings / config loading | `src/config/settings.py` |
| Templates / styling | `src/web/templates/` (Nord theme), `src/web/static/` |

---

## Verification before claiming done (the actual commands)

Before saying "this is fixed" or "this works":

```bash
# 1. Lint
uvx ruff check .
uvx ruff format --check .

# 2. Tests (inside container, against the dev environment)
docker exec spotops-spotops-1 uv run pytest tests/ -v

# 3. Container builds and starts
cd /opt/spotops
docker compose up -d --build spotops
docker compose ps   # expect "Up"
docker compose logs --tail=50 spotops   # expect no traceback

# 4. Health check
curl -sf http://localhost:8000/health/ && echo OK

# 5. If you touched schema or queries: spot-check the DB
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  "SELECT MAX(load_date) FROM spots;"   # should be recent

# 6. If you touched export endpoints: smoke test
set -a; . /opt/spotops/.env; set +a
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:8000/api/revenue/sheet-export | jq '.metadata'
```

If any step above fails, the work is not done — diagnose the failure before reporting back.

---

## Things that are NOT load-bearing — feel free to change

- The repo's top-level `README.md` (currently a stub)
- Code formatting (let `ruff format` decide)
- Variable names inside functions
- Comment style (default: no comment unless the *why* is non-obvious)
- Test naming / organization
- Test fixtures (as long as they don't mock the DB)

---

## When this guide and the code disagree

The code wins. Update this guide.

If you find yourself doing this, also:
1. Update the relevant fact in this file
2. Bump the **Last reviewed** date at the top
3. If the disagreement reflects a class of mistake an LLM keeps making, add a rule to `.claude/tasks/lessons.md`

---

## Related docs

- [HUMAN_OPERATOR_GUIDE.md](HUMAN_OPERATOR_GUIDE.md) — operator surface
- [DEV_WORKFLOW.md](DEV_WORKFLOW.md) — branching, dev loop, schema changes
- [RUNBOOKS.md](RUNBOOKS.md) — operational commands
- [ARCHITECTURE.md](ARCHITECTURE.md) — topology, data model, DR
- [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) — endpoint contracts
- `.claude/CLAUDE.md` — auto-loaded LLM instructions (cheat sheet)
- `.claude/tasks/lessons.md` — corrections log
