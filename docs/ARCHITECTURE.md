# Architecture

**Audience:** Engineers and LLM agents who need to understand how SpotOps fits together
**Purpose:** System topology, data model, DR posture, auth — the single "how does this work" reference
**Last reviewed:** 2026-04-30

> For day-to-day developer commands, see [DEV_WORKFLOW.md](DEV_WORKFLOW.md).
> For operations, see [RUNBOOKS.md](RUNBOOKS.md).
> For the API contracts, see [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md).

---

## System overview

SpotOps is a Flask + uvicorn app running in a Docker container on a single Linux host (`spotops`), reading and writing one SQLite database. It serves a Tailscale-only web dashboard plus a small set of authenticated API endpoints that feed `Revenue Master.xlsx`. Imports flow once-daily-times-four from a K-drive Excel file. Backups go to Backblaze B2 (continuous via Litestream + nightly via restic) and to Dropbox (nightly snapshot).

```
                Tailnet clients
                      │
                      │ Tailscale
                      ▼
        ┌──────────────────────────┐
        │  Host: spotops           │
        │                          │
        │  /opt/spotops  (repo)    │ ← Docker compose stack
        │  ┌────────────────────┐  │
        │  │ container          │  │
        │  │ spotops-spotops-1  │  │
        │  │  uvicorn :8000     │  │
        │  │  Flask app         │  │
        │  └─────────┬──────────┘  │
        │            │ volume mount│
        │            ▼             │
        │  /srv/spotops/db/        │
        │   production.db          │  ← SQLite WAL
        │  /srv/spotops/data       │
        │  /srv/spotops/processed  │
        │  /srv/spotops/uploads    │
        │                          │
        │  systemd timers:         │
        │   ctv-commercial-import  │ K-drive  → /srv/spotops/data/raw/daily/
        │   ctv-daily-update       │ host calls into container to import
        │   ctv-db-sync            │ DB → Dropbox snapshot
        │   restic-backup          │ /srv/spotops/{data,processed,uploads} → B2
        │   litestream             │ DB WAL → B2 (continuous)
        └──────────────────────────┘
```

---

## Deployment topology (current — Docker)

| Item | Value |
|---|---|
| Host | One Linux box, hostname `spotops`, on the company tailnet |
| Repo / working dir | `/opt/spotops` |
| Compose file | `/opt/spotops/docker-compose.yml` |
| Compose service | `spotops` |
| Container name | `spotops-spotops-1` |
| Image | `spotops-spotops` (built from `/opt/spotops/Dockerfile`) |
| Port mapping | `127.0.0.1:8000:8000` (loopback only — Tailscale fronts external access) |
| Env file | `/opt/spotops/.env` (loaded by compose via `env_file:`) |
| Volumes | `/srv/spotops/db:/srv/spotops/db` · `/srv/spotops/processed:/srv/spotops/processed` · `/srv/spotops/data:/app/data` · `/var/run/tailscale/tailscaled.sock:/var/run/tailscale/tailscaled.sock` |
| Restart policy | `unless-stopped` |

The Tailscale socket mount is what lets the container call the host's `tailscaled` for auth (see [Authentication](#authentication)).

### App modes (`APP_MODE`)

The container entrypoint reads `APP_MODE` from the environment and configures itself accordingly:

| Mode | Behavior | When to use |
|---|---|---|
| `replica_readonly` | `READ_ONLY_MODE=true`. Web app blocks `POST/PUT/PATCH/DELETE` at the route layer. | Default. Standby instances; safe even if pointed at a stale DB. |
| `failover_primary` | `READ_ONLY_MODE=false`. Writes accepted. | Only when this host has taken over from the prior primary. |

`RESTORE_ON_START=true` makes the entrypoint run a Litestream restore from B2 to `DATABASE_PATH` before starting uvicorn. Off by default for the production live host (which already holds the canonical DB); on for fresh failover spin-ups.

---

## App structure

### Clean architecture layering

```
HTTP  →  Routes (src/web/routes/)
              │
              ▼
         Services (src/services/)
              │
              ▼
       Repositories (src/repositories/)
              │
              ▼
        SQLite (production.db)
```

- **Routes** parse HTTP, call services, render templates / serialize JSON. No business logic.
- **Services** hold business logic and aggregation. They depend on repositories via the DI container.
- **Repositories** wrap raw SQL. Routes/services don't write SQL directly; repositories own it.

### Service container / DI

A service container in `src/services/container.py` holds singletons (database connection, repositories) and factories (per-request services). Services are wired up in `src/services/factory.py` and registered via `register_default_services()` at app startup.

```python
db = container.get("database_connection")
with db.connection() as conn:
    ...
```

### Blueprint registration (hard rule)

Blueprints are registered **exactly once**, in `src/web/blueprints.py` via `initialize_blueprints()`. **Never in `app.py`.** The factory function calls `initialize_blueprints(app)` after the app is constructed.

> The older `GUIDE-CanonTools.md` doc told contributors to add blueprint imports to `app.py` directly. That instruction is wrong for the current codebase — register in `blueprints.py`.

### Entry points

- **Flask app factory:** `src/web/app.py` → `create_app()`
- **ASGI adapter for uvicorn:** `src/web/asgi.py`
- **Container ENTRYPOINT:** `backblaze_startup.sh` (handles `RESTORE_ON_START`, sets `READ_ONLY_MODE` from `APP_MODE`, then `exec uvicorn ...`)

---

## Filesystem layout

```
/opt/spotops/                     # repo + compose stack
├── docker-compose.yml
├── Dockerfile
├── backblaze_startup.sh          # container ENTRYPOINT
├── .env                          # env_file (DATABASE_PATH, DROPBOX_*, SHEET_EXPORT_TOKEN, APP_MODE, …)
├── .venv/                        # host venv (used only by bin/db_sync.sh)
├── cli/                          # CLI entry points run inside the container
│   └── daily_update.py
├── cli_db_sync.py                # Dropbox sync CLI (host venv)
├── bin/                          # systemd-invoked wrapper scripts (host)
│   ├── commercial_import.sh
│   ├── daily_update.sh
│   ├── db_sync.sh
│   ├── daily-download.sh
│   ├── rotate_commercial_logs.sh
│   └── run-dev.sh
├── scripts/                      # python scripts + systemd unit templates
│   ├── ctv-db-sync.{service,timer}        # installed
│   ├── ctv-db-validation.{service,timer}  # NOT installed (template only)
│   └── ctv-io-scanner.{service,timer}     # NOT installed (template only)
├── sql/migrations/               # numbered SQL migrations
├── src/
│   ├── web/        # Flask app (routes, blueprints, templates, static)
│   ├── services/
│   ├── repositories/
│   ├── config/settings.py
│   └── …
├── tests/
└── docs/                         # this directory

/srv/spotops/                     # host-side persistent data
├── db/
│   ├── production.db             # the live SQLite DB
│   ├── .snapshot.db              # transient (created/cleaned by db_sync.sh)
│   └── snap-*.sqlite3            # ad-hoc snapshots
├── data/                         # mounted into container at /app/data
│   └── raw/daily/                # Commercial Log YYMMDD.xlsx lands here
├── processed/                    # processed import artifacts
└── uploads/                      # currently empty; included in restic backup target

/etc/                             # system config
├── litestream.yml                # Litestream replication config (root-only)
├── litestream.env                # B2 credentials for Litestream (root-only)
└── restic.env                    # Restic repo URL + password (root-only)

/var/log/                         # logs
├── ctv-daily-update/update.log
├── ctv-commercial-import/import.log
├── ctv-db-sync/sync.log
└── restic/backup.log
```

---

## Database & storage

### The DB

- Single SQLite file at `/srv/spotops/db/production.db`, ~1.4 GB, WAL-mode journaling.
- Mounted into the container at the same path; both host and container see one file.
- Container resolves the path via `DATABASE_PATH=/srv/spotops/db/production.db` from `.env`.

### Empty-skeleton trap (codified footgun)

The repo ships a 4 KB `data/database/production.db` placeholder. The Flask `create_app()` factory's default `DB_PATH` resolves to that placeholder unless overridden. Never run scripts or tests against it expecting real data. The container's `.env` pins the live path; ad-hoc host scripts must set `DATABASE_PATH=/srv/spotops/db/production.db` explicitly.

### System-wide query invariants

- **Trade exclusion.** Every revenue query must include `WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)`.
- **Customer identity.** Customer table uses `normalized_name` (NOT `customer_name`). Agency table uses `agency_name`.
- **Broadcast month format.** Stored as title-cased `Mmm-YY` (`Sep-26`, `Oct-25`). API outputs convert to first-of-month ISO date `YYYY-MM-01`.

---

## Data dictionary — the 29-column source structure

The Excel commercial-log file has 29 columns (positions 0–28). This is the canonical source schema — the database `spots` table mirrors most fields directly.

### Quick reference grid

| Pos | Field | Display | Brief |
|-----|-------|---------|-------|
| 0 | `bill_code` | Bill Code | Agency:customer identifier with colon separators |
| 1 | `air_date` | Start Date | Calendar date the spot aired |
| 2 | `end_date` | End Date | Same as `air_date` (one record per airing) |
| 3 | `day_of_week` | Day(s) | Full day name |
| 4 | `time_in` | Time In | Programming-block start (12-hour) |
| 5 | `time_out` | Time Out | Programming-block end (12-hour) |
| 6 | `length_seconds` | Length | Spot duration in MM:SS |
| 7 | `media` | Media/Name/Program | Spot name for master control |
| 8 | `comments` | Comments | Actual air time |
| 9 | `language_code` | Language | Single-letter code (C, V, H, J, E, T, P, …) |
| 10 | `format` | Format | Program name where spot aired |
| 11 | `sequence_number` | Units-Spot count | Unknown purpose |
| 12 | `line_number` | Line | Unknown purpose |
| 13 | `spot_type` | Type | `BB`, `Com`, `BNS`, `SVC`, `PRD`, `PKG`, `CRD`, `PRG` |
| 14 | `estimate` | Agency/Episode# | Estimate or contract numbers |
| 15 | `gross_rate` | Unit rate Gross | Rate before deductions |
| 16 | `make_good` | Make Good | Notes for compensatory re-airings |
| 17 | `spot_value` | Spot Value | Equal to `gross_rate` |
| 18 | `broadcast_month` | Month | `Mmm-YY` (e.g. `Nov-24`) — week-based Sun–Sat |
| 19 | `broker_fees` | Broker Fees | Broker commission amounts |
| 20 | `priority` | Sales/rep com | Unclear; possibly priority + rev share |
| 21 | `station_net` | Station Net | Final revenue after deductions |
| 22 | `sales_person` | Sales Person | Sales rep — feeds AE attribution |
| 23 | `revenue_type` | Revenue Type | Internal Ad Sales · Branded Content · Paid Programming · Direct Response · Other · Services · Production (legacy) · **Trade (always excluded from revenue queries)** |
| 24 | `billing_type` | Billing Type | `Calendar` or `Broadcast` |
| 25 | `agency_flag` | Agency? | `Agency` or `Non-agency` (server converts `Agency` → `Y` in API) |
| 26 | `affidavit_flag` | Affidavit? | Repurposed; current use undocumented |
| 27 | `contract` | Notarize? | Now stores contract number; original use was notarization flag |
| 28 | `market_name` | Market | `SEA` · `SFO` · `LAX` · `MMT` · `DAL` · `CVC` · `NYC` · `CMP` · `HOU` |

### Notes on key columns

**`bill_code` (position 0)** — Uses colon `:` as separator between agency and customer. Forms:
- `customer` — direct customer, no agency
- `agency:customer` — single agency
- `agency1:agency2:customer` — multiple agencies; what follows the **final** colon is always the customer name

**`spot_type` (position 13)** —

| Code | Meaning |
|---|---|
| `BB` | Billboard |
| `Com` | Commercial (paid) |
| `BNS` | Bonus |
| `SVC` | Services |
| `PRD` | Production |
| `PKG` | Package |
| `CRD` | Credit |
| `PRG` | Paid programming (infomercial / long-form) |

**`broadcast_month` (position 18)** — TV-industry month, week-based (Sun–Sat boundaries) rather than calendar boundaries. Stored as `Mmm-YY` title-cased.

**`revenue_type` (position 23)** — Drives both reporting category and the language assignment system's categorization (see [Language assignment system](#language-assignment-system)).

**`market_name` (position 28)** — Full code expansions:

| Code | Market |
|---|---|
| SEA | Seattle |
| SFO | San Francisco |
| LAX | Los Angeles |
| MMT | Multimarket coverage |
| DAL | Dallas |
| CVC | Central Valley |
| NYC | New York |
| CMP | Chicago / Minneapolis / St Paul |
| HOU | Houston |

### Fields with unclear purpose

`sequence_number` (11), `line_number` (12), `priority` (20), `affidavit_flag` (26) are flagged in the source data dictionary as not fully documented. Treat their values as opaque unless you can verify intent for your specific use case.

---

## Import pipeline

The daily import is a two-stage chain run by systemd timers on the host:

```
K: drive (Traffic/Media library/Commercial Log.xlsx)
        │
        │  ctv-commercial-import.timer (4×/day ≈03:00, 09:00, 15:00, 21:00 PT)
        │  bin/commercial_import.sh
        ▼
/srv/spotops/data/raw/daily/Commercial Log YYMMDD.xlsx
        │
        │  ctv-daily-update.timer (≈30 min after each commercial import)
        │  bin/daily_update.sh → docker compose exec spotops uv run python cli/daily_update.py
        ▼
spots table (and friends) inside production.db
        │
        ▼
Litestream replicates WAL → Backblaze B2 (continuous, near-zero RPO)
```

`/srv/spotops/data/` on the host is mounted as `/app/data/` inside the container, so the file the host wrote at `/srv/spotops/data/raw/daily/Commercial Log YYMMDD.xlsx` is what the container picks up at `/app/data/raw/daily/Commercial Log YYMMDD.xlsx`.

### Import modes

`cli/daily_update.py` and `cli/import_closed_data.py` support two modes that determine whether existing months are touched:

| Mode | Trigger | Effect |
|---|---|---|
| **HISTORICAL** | Default (no `--skip-closed` flag) | Replaces existing data for imported months **and closes them** (sets `broadcast_month_closures.status = 'CLOSED'`). Used for monthly close. |
| **WEEKLY_UPDATE** | `--skip-closed` flag | Skips already-closed months, only processes open ones. Does **not** close any months. Used for daily auto-imports and mid-month refreshes. |

**Footgun:** running monthly close with `--skip-closed` imports the data but doesn't close — the dashboard stays yellow (open) instead of going green (closed). See [HUMAN_OPERATOR_GUIDE.md → Monthly Close](HUMAN_OPERATOR_GUIDE.md#monthly-close).

### Notification surface

`bin/daily_update.sh` posts to ntfy.sh on every run. `NTFY_TOPIC` lives in `/opt/spotops/.env`. Success → `Tags: white_check_mark`. Failure → `Priority: 5`, `Tags: rotating_light`. The same pattern is used by `bin/db_sync.sh` for Dropbox-backup outcomes.

---

## Language assignment system

A separate categorization + language-resolution layer sits on top of the raw `spots` table. Every spot is classified into one of three processing categories, then language-assigned according to category-specific rules. Final results land in `spot_language_assignments`.

### Categorization

| Category | Business rule | Effect |
|---|---|---|
| `language_assignment_required` | `revenue_type = 'Internal Ad Sales'` AND `spot_type IN ('COM', 'BNS')` | Real language determination required |
| `review_category` | `revenue_type = 'Internal Ad Sales'` AND `spot_type IN ('PKG', 'CRD', 'AV', 'BB')` · OR · `revenue_type = 'Other'` · OR · `revenue_type = 'Local'` (legacy, should be reclassified) | Manual business review needed |
| `default_english` | `revenue_type IN ('Direct Response Sales', 'Paid Programming', 'Branded Content')` | English by business rule, no review |

Categorization happens via `cli_01_language_assignment.py --categorize-all`; subsequent assignment runs via `--process-all-remaining`. See [RUNBOOKS.md](RUNBOOKS.md) for operational invocations.

### Schema

```sql
-- spots (key fields for assignment)
spots(
  spot_id INTEGER PRIMARY KEY,
  revenue_type TEXT,              -- drives categorization
  spot_type TEXT,                 -- drives categorization
  language_code TEXT,             -- single-letter source code
  spot_category TEXT,             -- result of categorization
  bill_code TEXT,
  ...
)

-- final language assignment, one row per spot
spot_language_assignments(
  assignment_id INTEGER PRIMARY KEY,
  spot_id INTEGER UNIQUE,
  language_code TEXT,             -- final assigned language
  language_status TEXT,           -- determined | undetermined | default | invalid
  confidence REAL,                -- 0.0 – 1.0
  assignment_method TEXT,         -- see table below
  requires_review BOOLEAN,
  notes TEXT,
  assigned_date TIMESTAMP
)

-- valid language codes
languages(
  language_id INTEGER PRIMARY KEY,
  language_code TEXT UNIQUE,
  language_name TEXT,
  language_group TEXT
)
```

### Assignment methods

| Method | Logic | Confidence | Review |
|---|---|---|---|
| `business_rule_default_english` | `spot_category = 'default_english'` | 1.0 | no |
| `direct_mapping` | Spot's `language_code` exists in `languages` table | 1.0 | no |
| `business_review_required` | `spot_category = 'review_category'` | 0.5 | **yes** |
| `undetermined_flagged` | Source `language_code = 'L'` (undetermined at trafficking) | 0.0 | **yes** |
| `default_english` | Internal Ad Sales spot with NULL/missing `language_code` | 0.5 | no |

### Best-practice query for revenue-by-language

```sql
SELECT sla.language_code,
       COUNT(*) AS spot_count,
       SUM(s.gross_rate) AS revenue
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
WHERE s.spot_category = 'language_assignment_required'  -- commercial spots only
  AND sla.requires_review = 0                            -- exclude review spots
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
GROUP BY sla.language_code
ORDER BY revenue DESC;
```

The full library of operational SQL recipes (review queues, health checks, performance metrics) lives in the source `GUIDE-ASSIGNMENT-SYSTEM.md` — preserved in [docs/ARCHIVE/](#legacy--deprecated-architecture) after consolidation.

---

## Customer / agency canon system

A normalization layer sits on top of `spots.bill_code` to collapse aliases, casing variants, and naming drift into canonical customers and agencies. It powers customer dashboards, the `tblKnownRows` workbook tracker, and revenue aggregation.

### View chain

```
raw_customer_inputs                          (sync target — populated from spots.bill_code)
        │
        ▼
v_raw_clean                                  basic text cleaning
        │
        ▼
v_normalized_candidates                      auto-normalization rules + canonical map application
        │
        ▼
v_customer_normalization_audit               final mapping; what dashboards JOIN against
```

Auto-normalization rules baked into the chain:

- Strip trailing ` PRODUCTION` / ` PROD` (case-sensitive).
- Split `Agency:Customer` on colons; the segment after the **final** colon is always the customer.
- Apply both `agency_canonical_map` and `customer_canonical_map` lookups.
- Match through `entity_aliases` (entity_type='customer') for raw → canonical-customer overrides.

### Tables

| Table | Purpose |
|---|---|
| `agency_canonical_map(alias_name, canonical_name, updated_date)` | Maps agency-name variants to canonical agency name |
| `customer_canonical_map(alias_name, canonical_name, updated_date)` | Maps customer-tail variants to canonical customer-tail |
| `entity_aliases(alias_name, entity_type, target_entity_id, confidence_score, created_by, notes, is_active, updated_date)` | User-approved aliases; supports raw-string → canonical-customer overrides |
| `customers` | Canonical customer rows; identity column is `normalized_name` |
| `canon_audit(ts, actor, action, key, value, extra)` | Append-only audit of every Canon Tool operation |
| `raw_customer_inputs` | Working set for normalization. **Lags `spots.bill_code` until synced** — see [RUNBOOKS.md](RUNBOOKS.md) |

Indexes: `idx_entity_aliases_customer (alias_name, entity_type)`, `idx_customers_normalized_name (normalized_name)`.

`canon_audit.action` values: `agency_canon`, `customer_canon`, `raw_map`.

### Sync footgun

`raw_customer_inputs` does not auto-update from `spots.bill_code`. After major imports (and routinely, monthly), run the sync query in [RUNBOOKS.md → Customer normalization](RUNBOOKS.md#customer-normalization-raw-input-sync). Without it, new customer names won't appear in the Canon Tool.

### API endpoints

`POST /api/canon/agency` · `POST /api/canon/customer` · `POST /api/canon/raw-to-customer` · `GET /api/canon/suggest/normalized?q=…`. See [API_AND_EXPORT_CONTRACTS.md → Canon endpoints](API_AND_EXPORT_CONTRACTS.md#canon-endpoints).

### Dashboard integration

Revenue dashboards JOIN against the audit view:

```sql
LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
```

This gives them `audit.normalized_name` (canonical display) automatically, with PROD-suffix removal and agency:customer splitting applied.

### Critical: case-insensitive duplicates in canonical maps

Mapping tables must NOT contain case-insensitive duplicates — they cause duplicate records in the views and silent revenue double-counting. Detection query:

```sql
SELECT LOWER(alias_name) AS lowercase_alias,
       GROUP_CONCAT(alias_name, ', ') AS variants,
       COUNT(*) AS variant_count
FROM agency_canonical_map
GROUP BY LOWER(alias_name)
HAVING COUNT(*) > 1;
```

Same query against `customer_canonical_map`. Fix duplicates by collapsing to one canonical alias.

---

## Authentication

### Concept

No passwords. Identity comes from Tailscale. The app calls Tailscale's Local API `whois` over a Unix socket to identify the user behind each request.

### Flow

1. A request arrives at the container (loopback `127.0.0.1:8000`, fronted by Tailscale).
2. The Flask `_require_login` before-request hook calls Tailscale's Local API: `GET /localapi/v0/whois?addr=<remote-addr>` over `/var/run/tailscale/tailscaled.sock`.
3. Tailscale responds with the connecting user's login email (e.g. `alice@company.com`) and node info.
4. The hook looks up that email in the `users` table.
5. **If found** → authenticated, request proceeds with that user's role attached to the session.
6. **If not found** → response: `Your Tailscale account is not authorized. Ask an admin to add you.`

### Why this works inside Docker

The compose file mounts the host's tailscaled socket into the container at the same path:

```yaml
volumes:
  - /var/run/tailscale/tailscaled.sock:/var/run/tailscale/tailscaled.sock
```

The container's app process can then call the Local API as if it were running on the host. Critical: Tailscale only allows root or the configured operator user to call `whois`; `sudo tailscale set --operator=<user>` on the host configures this. (Inside the container, the app runs as the user defined in the `Dockerfile`.)

### Roles

The `users` table:

```sql
CREATE TABLE users (
  user_id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  role TEXT NOT NULL DEFAULT 'AE' CHECK (role IN ('admin', 'management', 'AE')),
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_login TIMESTAMP,
  updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

| Role | Access |
|---|---|
| `admin` | Full access including user management |
| `management` | All reports, all AEs |
| `AE` | Own AE dashboard only |

User adds/removes happen via the `/users/` admin UI ([HUMAN_OPERATOR_GUIDE.md → User Management](HUMAN_OPERATOR_GUIDE.md#user-management)) or `scripts/create_admin_user.py` for bootstrap.

### Session

Session timeout is 24 hours. There is no logout flow that meaningfully helps — once you're on the tailnet with a valid email, you're authenticated automatically.

### Footguns

- **`/health` works; `/api/health` returns 401.** The auth allow-list lists `/health` but not `/api/health`. Use `/health` for liveness probes.
- **Email match is exact.** Tailscale's reported email and the `users.email` column must match byte-for-byte.
- **The app must be reachable only over the tailnet.** The `127.0.0.1` bind enforces this on the Docker side; whatever fronts the host externally (Tailscale serve, reverse proxy) must not expose it to the public internet.

---

## Network model

- **No public ingress by design.** The Docker port is bound to `127.0.0.1`. External access is via Tailscale only.
- **Hostnames over Tailnet.** The host advertises as `spotops` via MagicDNS; clients reach it as `http://spotops:8000/` or via Tailscale IP.
- **The `Pi-ctv` rename to `spotops` happened earlier in 2026.** Old client configs may still reference `pi-ctv`; treat any such reference as needing update.

---

## DR / Backup posture

Four independent layers, each protecting different things. Operational details (verification, restore, scheduling) are in [RUNBOOKS.md](RUNBOOKS.md); this section is the conceptual model.

| Layer | What it protects | Frequency | Target | RPO |
|---|---|---|---|---|
| **Litestream** | Live SQLite DB (WAL stream) | continuous | Backblaze B2 | seconds |
| **Dropbox snapshot** | Full DB file | nightly 02:04 PT | Dropbox `/database.db` + 7-deep `/backups/` history | ~24 h |
| **Restic** | `/srv/spotops/{data,processed,uploads}` | nightly 02:30 PT | Backblaze B2 (separate repo from Litestream) | ~24 h |
| **Pi2 cold standby** | (intentionally not running) | — | — | — |

### Why two B2 layers

Litestream covers the SQLite DB only — WAL frames replicate near-instantly so RPO is essentially zero on the DB. Restic covers everything *else* under `/srv/spotops`: the raw imported Excel files, the processed artifacts, and the (currently empty) uploads dir. Without restic, a host loss would lose the most recent unimported `.xlsx` files plus any in-flight processed data.

### Why Dropbox in addition to B2

Operator-friendly. A human can log into Dropbox and download `database.db`. B2 needs Litestream + credentials + a restore command.

### Pi2 cold standby — historical state

A second Raspberry Pi (`pi2`, Tailscale IP `100.96.96.109`) exists on the network. Earlier in 2026 it ran `ctv-pi2-download.timer` to pull a nightly DB copy. **That timer is no longer installed.** The cold-standby scripts at `/opt/spotops/scripts/failover-to-pi2.sh` and `/opt/spotops/scripts/failback-to-spotops.sh` still exist as historical artifacts; they reference the prior Pi-era paths and have not been verified against the current Docker world. Treat Pi2 as not-currently-DR until the mirror is reinstated or replaced.

### Failover sequence (current)

1. **App-only failure** → restart container (`docker compose up -d spotops`).
2. **DB corruption** → restore from Dropbox snapshot or Litestream B2; see [RUNBOOKS.md → Restore procedures](RUNBOOKS.md#restore-procedures).
3. **Host loss / extended outage** → activate Railway emergency target. Doc lives in `docs/ARCHIVE/GUIDE-Railway.md` (status: dated 2025-08-27, verify project still exists before relying on it). Railway uses a Dropbox-restore-on-boot pattern.

---

## Legacy / deprecated architecture

The system was migrated from a Pi-systemd architecture earlier in 2026. Several artifacts of that era persist in the codebase, scripts, and (formerly) the docs:

| Subsystem | Then | Now |
|---|---|---|
| App runtime | systemd `ctv-bookedbiz-db.service` (uvicorn on `:8000` as `ctvbooked`) | Docker container `spotops-spotops-1` |
| Live DB | `/var/lib/ctv-bookedbiz-db/production.db` | `/srv/spotops/db/production.db` |
| App code dir | `/opt/apps/ctv-bookedbiz-db/` | `/opt/spotops/` |
| Dev environment | systemd `spotops-dev.service` on `:5100`, `~/dev/ctv-bookedbiz-db/` | dev runs in the same compose stack; no separate dev port |
| Pi2 mirror | `ctv-pi2-download.timer` nightly download | not installed |
| Insertion Order Scanner | `ctv-io-scanner.timer` hourly scan → `pending_orders.json` | not installed; `pending_orders.json` does not exist on disk |
| Env files | `/etc/ctv-bookedbiz-db/*.env`, `/etc/ctv-db-sync.env`, `/etc/ctv-litestream.env` | `/opt/spotops/.env` (compose), `/etc/litestream.env`, `/etc/restic.env` |
| Secondary backup target | Railway (active) | Railway preserved as last-resort emergency only |

Runtime user `ctvbooked` no longer exists. References to `apps-deploy` group, `flaskapp.service`, `/opt/venvs/ctv-bookedbiz-db`, or `/etc/ctv-bookedbiz-db/` are all Pi-era — expect them only in archived docs.

The historical record lives in `docs/ARCHIVE/`:

- `ops.md` — Feb-2026 systemd-on-Pi snapshot
- `GUIDE-failover-failback.md` — pre-Docker DR runbook (still has useful general lessons; paths are stale)
- `GUIDE-Railway.md` — Railway emergency setup as of 2025-08-27
- (and more — see the archive index)

When you encounter Pi-era references in old plans or specs (under `docs/plans/` or `docs/superpowers/`), they're intentionally preserved as point-in-time records; don't rewrite them.

---

## Related docs

- [HUMAN_OPERATOR_GUIDE.md](HUMAN_OPERATOR_GUIDE.md) — operator-friendly procedures (no commands)
- [DEV_WORKFLOW.md](DEV_WORKFLOW.md) — branching, dev loop, schema changes
- [RUNBOOKS.md](RUNBOOKS.md) — operational commands, recovery, backups
- [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) — endpoint contracts
- [LLM_SYSTEM_GUIDE.md](LLM_SYSTEM_GUIDE.md) — LLM-facing reference (paths, env, invariants, footguns)
- `.claude/CLAUDE.md` — auto-loaded project instructions (cheat sheet form)
- `.claude/tasks/lessons.md` — running corrections log for LLM agents
