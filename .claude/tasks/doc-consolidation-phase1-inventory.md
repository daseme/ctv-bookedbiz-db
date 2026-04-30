# Phase 1 — Documentation Inventory & Proposed File Map

**Status:** Awaiting owner approval before Phase 2+ begins
**Last reviewed:** 2026-04-30
**Source spec:** `.claude/tasks/doc-consolidation-assignment.md`
**Owner guardrail:** Stop after this deliverable; do not write any new canonical docs until approved.

---

## 1. Inventory table

Audience values: **HumOp** (Human operator) · **Dev** · **LLM** · **DevOps** · **Mixed** · **Hist** (Historical / point-in-time)

Verdict values: **Keep** (lift mostly intact) · **Merge** (split into 1+ destinations) · **Archive** (move under `docs/ARCHIVE/`)

| File | Main topic | Audience | Verdict | Destination | Notes |
|---|---|---|---|---|---|
| `docs/GUIDE_DEV_WORKFLOW.md` | Docker dev workflow on `/opt/spotops` (just rewritten today; folded in the deleted `GUIDE-RaspberryWorkflow.md`) | Dev / DevOps | Merge | **DEV_WORKFLOW** core | This is essentially the new DEV_WORKFLOW.md spine; trim a bit for branching+commits and link out to RUNBOOKS for compose commands. |
| `docs/GUIDE_GIT_WORKFLOW.md` | Branching model (`dev` → `main`, squash merge), branch protection | Dev | Merge | **DEV_WORKFLOW** | Plain git workflow — fold into DEV_WORKFLOW. |
| `docs/GUIDE-OPERATIONS.md` | DB-lock management, pipeline progress monitoring, rollback procedures, sqlite health checks | DevOps / Operator | Merge | **RUNBOOKS** + small ARCHITECTURE snippet | The `lsof | grep production.db` lock-management content predates Docker (the container itself holds an open DB handle); behavioral guidance needs verification before lifting. |
| `docs/GUIDE-MONTHLY-CLOSING.md` | Monthly close procedure: yearly recap import + close-month flag | HumOp | Merge | **HUMAN_OPERATOR_GUIDE** (procedure) + **RUNBOOKS** (CLI invocations) | Owner-driven monthly process; clean lift. |
| `docs/GUIDE-DAILY-COMMERCIALLOG.md` | Daily commercial-log import pipeline (4×/day timers, K-drive copy, daily_update.py) | Mixed | Merge | **HUMAN_OPERATOR_GUIDE** (what runs, how to know it's healthy) + **RUNBOOKS** (manual run, debug) + **ARCHITECTURE** (pipeline overview) | Largest doc; multiple audiences. NEEDS CONFIRMATION whether timers `ctv-commercial-import.timer` / `ctv-daily-update.timer` are still active under Docker. |
| `docs/ops.md` | Feb-2026 systemd-on-Pi production architecture snapshot | Hist | Archive | **ARCHIVE/** | Banner already added today; lift sections 11–13 (timer schedule, freshness footer, commercial-log timer schedule) into ARCHITECTURE if still accurate. |
| `docs/docker-setup.md` | Docker image setup + Backblaze/Litestream + `APP_MODE=replica_readonly`/`failover_primary` | DevOps | Merge | **ARCHITECTURE** (modes/Litestream) + **RUNBOOKS** (build/run/restore commands) | Has a "current deploy uses compose, not docker run" banner added today; the Litestream restore + replicate procedures are valuable RUNBOOKS material. |
| `docs/USER_MANAGEMENT_SETUP.md` | Tailscale-based user provisioning (no passwords) | HumOp / Admin | Merge | **HUMAN_OPERATOR_GUIDE** (add user) + **ARCHITECTURE** (auth model link to TAILSCALE_AUTH) | Cross-references TAILSCALE_AUTH.md. |
| `docs/GUIDE_Customer_Name_Normalization.md` | Customer normalization view chain + alias resolution + review-queue CLI | Dev / Analyst | Merge | **ARCHITECTURE** (view chain, tables) + **RUNBOOKS** (CLI invocations) | Overlaps with `GUIDE-CanonTools.md` (same view chain `v_normalized_candidates` → `v_customer_normalization_audit`); merge carefully. |
| `docs/GUIDE-ASSIGNMENT-SYSTEM.md` | Three-stage language assignment (categorization → assignment → review) | Dev / Analyst | Merge | **ARCHITECTURE** (schema + business rules) + **DEV_WORKFLOW** (CLI snippet) | Spot counts (288K/5.9K/826K) are stale snapshots — strip them or mark as point-in-time. |
| `docs/GUIDE-CanonTools.md` | Canon Tool API endpoints + canonical-map / entity_aliases tables, sync workflow | Mixed | Merge | **ARCHITECTURE** (system + view chain) + **RUNBOOKS** (monthly raw-input sync) | **One outdated dev instruction**: tells caller to register blueprint in `app.py` — must be fixed on lift (CLAUDE.md mandates `initialize_blueprints()`). |
| `docs/GUIDE-TwentyNineColumns.md` | Position-by-position 29-column data dictionary | Mixed | Keep | **ARCHITECTURE** (data dictionary) | Cleanest lift; pure reference, no infra baggage. Pin `broadcast_month` casing. |
| `docs/GUIDE-failover-failback.md` | DR stack: Litestream→B2 + Dropbox nightly + Pi2 cold standby + Insertion Order Scanner | DevOps | Merge | **RUNBOOKS** (recovery procedures, IO scanner) + **ARCHITECTURE** (DR topology) + **ARCHIVE** (Pi-era paths/services) | Single biggest reconciliation: every command uses `/var/lib/ctv-bookedbiz-db/...` and `/opt/apps/...` paths and `ctvbooked` user. NEEDS CONFIRMATION which services survived the Docker migration. |
| `docs/GUIDE-Railway.md` | Railway emergency-failover (scale 0↔1, Dropbox-restore on boot) | DevOps | Archive | **ARCHIVE** + 5-line stub in **RUNBOOKS** | Owner said Railway is "still sort of in play". Doc dated 2025-08-27 with legacy service name `ctv-bookedbiz-db`. Recommend ARCHIVE with a tiny RUNBOOKS stub naming the project, until owner confirms current state. |
| `docs/TAILSCALE_AUTH.md` | Auth via Tailscale Local API `whois` over `tailscaled.sock` | DevOps / Dev | Merge | **HUMAN_OPERATOR_GUIDE** (concept + add-user) + **ARCHITECTURE** (auth model + Docker socket mount) | NEEDS CONFIRMATION how container accesses `tailscaled.sock` today (compose mounts `/var/run/tailscale/tailscaled.sock` — confirm semantics match what TAILSCALE_AUTH describes for systemd). Operator-user examples (`ctvbooked`/`daseme`) are Pi-era. |
| `docs/planning-export-client-contract.md` | `GET /api/revenue/planning-export` contract (AE × broadcast_month) | LLM / Dev | Keep | **API_AND_EXPORT_CONTRACTS** | Already aligned with current Docker stack (port 8000). Drop in nearly verbatim. |
| `docs/sheet-export-client-contract.md` | `GET /api/revenue/sheet-export` contract + hash + v1.1 plan | LLM / Dev | Keep | **API_AND_EXPORT_CONTRACTS** | Companion to planning-export contract; explicitly flags sheet-export-runbook.md as stale on port 5000. |
| `docs/sheet-export-runbook.md` | Operator runbook for sheet-export endpoint (env, token, smoke test) | DevOps / Operator | Merge | **RUNBOOKS** | Needs port (5000→8000) and env model (systemd→compose `.env`) refresh on lift. |
| `docs/workbook-ae-drift-tracker.md` | Proposed `tblKnownAEs` workbook design for AE rename/casing-drift detection | LLM / Dev | Keep | **API_AND_EXPORT_CONTRACTS** | Status: **Proposed** (not implemented). Belongs with the two endpoint contracts. |
| `README.md` (repo root) | Empty 1-line stub | n/a | Replace later | top-level README pointing at HUMAN_OPERATOR_GUIDE + LLM_SYSTEM_GUIDE | Not part of this consolidation per se; flag for follow-up. |
| `service-dependencies-simple.md` (repo root) | Auto-generated import-graph snapshot (Mermaid + tables of orphaned services) | Dev | Archive | **ARCHIVE** | Stale auto-generated artifact. Recommend ARCHIVE + (optional) regenerate-on-demand script later. |

**Files referenced in user's list but not present in repo:** `GUIDE-RaspberryWorkflow.md` was deleted earlier this session and folded into `GUIDE_DEV_WORKFLOW.md` — the migration plan should record this so the deletion doesn't look like silent loss.

---

## 2. Proposed final file map

### `docs/HUMAN_OPERATOR_GUIDE.md` ← built from
- `GUIDE-DAILY-COMMERCIALLOG.md` (operator-facing surface: what auto-runs, how to tell it's healthy, where to look when it's not)
- `GUIDE-MONTHLY-CLOSING.md` (full procedure)
- `USER_MANAGEMENT_SETUP.md` (add-user procedure)
- `TAILSCALE_AUTH.md` (concept-level: "how sign-in works")
- (Optional) yearly-recap subset of `GUIDE-MONTHLY-CLOSING.md` if owner wants it called out separately

### `docs/LLM_SYSTEM_GUIDE.md` ← built from
- New synthesis grounded in `.claude/CLAUDE.md` + `docker-compose.yml` + `Dockerfile`
- Pulls invariants from `GUIDE-ASSIGNMENT-SYSTEM.md`, `GUIDE-TwentyNineColumns.md`, `GUIDE-CanonTools.md`, `GUIDE_Customer_Name_Normalization.md` (system purpose, paths, env vars, footguns, broadcast_month format, Trade exclusion, `normalized_name`/`agency_name` invariants)
- Footguns from `GUIDE-failover-failback.md` (DB recreation reverts ownership; mode 0640 vs 0600; sandbox `ProtectSystem=strict` blocking reads)
- Footguns from `sheet-export-client-contract.md` (hash version mismatch must error loudly; "Agency rate is per contract number, never an average")
- Replaces / supersedes the in-conversation guidance currently sitting in CLAUDE.md verbatim — CLAUDE.md becomes the cheat sheet, this becomes the deeper reference

### `docs/RUNBOOKS.md` ← built from
- `GUIDE-OPERATIONS.md` (DB lock management, rollback, health checks — verify Docker semantics first)
- `GUIDE-DAILY-COMMERCIALLOG.md` (manual run, debug, log inspection)
- `docker-setup.md` (build/run, Litestream restore, Litestream replicate)
- `GUIDE-failover-failback.md` (Litestream restore, Dropbox restore, Pi2 failover/failback, IO scanner manual run, backup health check) — **conditional on confirming services are still active**
- `sheet-export-runbook.md` (token rotation, smoke test) — refreshed for Docker `.env`
- `GUIDE-Railway.md` 5-line "scale up / scale down" stub — conditional on Railway still existing
- New runbooks from current state: container restart-after-code-change, container restart-after-env-change, view container logs, rebuild with no cache, exec shell into container

### `docs/ARCHITECTURE.md` ← built from
- `.claude/CLAUDE.md` runtime section (Docker, container, DB, env)
- `GUIDE-TwentyNineColumns.md` (data dictionary)
- `GUIDE-ASSIGNMENT-SYSTEM.md` (categorization → assignment → review schema + rules)
- `GUIDE-CanonTools.md` (canon system, tables, view chain) + `GUIDE_Customer_Name_Normalization.md` (alias resolution flow) — merged
- `docker-setup.md` (APP_MODE modes, Litestream replication topology)
- `GUIDE-failover-failback.md` (DR topology: Litestream→B2, Dropbox nightly, Pi2 mirror)
- `TAILSCALE_AUTH.md` (auth model + Docker socket mount)
- A `## Legacy` section briefly summarizing what `ops.md` describes (Feb-2026 systemd-on-Pi era), with a link to the archived doc
- Pipeline overview lifted from `GUIDE-DAILY-COMMERCIALLOG.md` (the import flow)

### `docs/DEV_WORKFLOW.md` ← built from
- `GUIDE_DEV_WORKFLOW.md` (full Docker dev/ops loop, just rewritten today)
- `GUIDE_GIT_WORKFLOW.md` (branching, branch protection, squash merge)
- The "How to safely make schema/import changes" block from current `GUIDE_DEV_WORKFLOW.md`
- A pointer to `.claude/tasks/lessons.md` for LLM coding rules

### `docs/API_AND_EXPORT_CONTRACTS.md` ← built from
- `sheet-export-client-contract.md` (verbatim with light editing)
- `planning-export-client-contract.md` (verbatim)
- `workbook-ae-drift-tracker.md` (verbatim, marked as "Proposed v1")
- Possibly a header section listing all known endpoints (`/api/revenue/sheet-export`, `/api/revenue/planning-export`, `/api/canon/*`, `/api/health`)

### `docs/ARCHIVE/`
- `ops.md` (whole file)
- `service-dependencies-simple.md` (whole file)
- `GUIDE-Railway.md` (whole file; stub-only reference left in RUNBOOKS)
- A `legacy-pi-era.md` aggregator capturing the systemd/`ctvbooked`/`/var/lib/ctv-bookedbiz-db` paths from `GUIDE-failover-failback.md` (so they're preserved as record but not in current docs)
- A `README.md` inside `ARCHIVE/` explaining what this directory is for and pointing back at the canonical docs

---

## 3. Conflicts / items needing owner confirmation

Format per the assignment spec.

### C1 — Live DB path on the failover/backup stack — **RESOLVED 2026-04-30**
- **Source A** (`docs/GUIDE-failover-failback.md`): `/var/lib/ctv-bookedbiz-db/production.db`
- **Source B** (`.claude/CLAUDE.md`): `/srv/spotops/db/production.db`
- **Resolution (live system check):** B is correct. The active backup/import services on `/opt/spotops` are:

  | Unit | Schedule | Runs as | What it does | Path/env |
  |---|---|---|---|---|
  | `litestream.service` | continuous | root | streams WAL to Backblaze B2 | EnvironmentFile=`/etc/litestream.env`; config `/etc/litestream.yml` |
  | `ctv-commercial-import.timer` | 4×/day (≈03:00, 09:00, 15:00, 21:00) | root | `bash /opt/spotops/bin/commercial_import.sh`: copies K-drive `Commercial Log.xlsx` → `/srv/spotops/data/raw/daily/Commercial Log YYMMDD.xlsx` | env via `/opt/spotops/.env` (sourced by script) |
  | `ctv-daily-update.timer` | 4×/day (≈30 min after each commercial-import) | root | `bash /opt/spotops/bin/daily_update.sh`: runs `docker compose exec spotops uv run python cli/daily_update.py "$DATED_FILE" --auto-setup --unattended` | container env (compose `.env`); `$DATED_FILE` resolves inside the container via the `/srv/spotops/data:/app/data` volume mount |
  | `ctv-db-sync.timer` | nightly 02:04 (+ 5 min jitter) | daseme | `/opt/spotops/bin/db_sync.sh`: SQLite online-backup `$DATABASE_PATH` → `/srv/spotops/db/.snapshot.db`, then `cli_db_sync.py upload && backup` to Dropbox | host venv `/opt/spotops/.venv/bin/python` (deliberately host-side so it works when container is unhealthy); EnvironmentFile=`/opt/spotops/.env` |
  | `restic-backup.timer` | nightly 02:30 (+ 10 min jitter) | root | `restic backup /srv/spotops/{data,processed,uploads}`; retains 7 daily / 4 weekly / 12 monthly | EnvironmentFile=`/etc/restic.env`; logs `/var/log/restic/backup.log` — **not in any current doc** |

  - **NOT installed** (Pi-era, gone): `ctv-pi2-download.service`/`.timer`, `ctv-io-scanner.service`/`.timer`, `ctv-db-validation.service`/`.timer`. The unit templates are still present in `/opt/spotops/scripts/` as deployable artifacts, but no system unit is enabled.
  - All `DATABASE_PATH` references resolve to `/srv/spotops/db/production.db` via `/opt/spotops/.env`.
  - **Note**: `/etc/ctv-db-sync.env` and `/etc/ctv-litestream.env` (referenced by `GUIDE-failover-failback.md` and `docker-setup.md`) **no longer exist**. Litestream uses `/etc/litestream.env`; everything else uses `/opt/spotops/.env`.

### C2 — App process model
- **Source A** (`docs/GUIDE-failover-failback.md`, `docs/sheet-export-runbook.md`): app runs as systemd unit `ctv-bookedbiz-db.service` / `spotops-dev.service`
- **Source B** (`.claude/CLAUDE.md`, `docker-compose.yml`): app runs as Docker container `spotops-spotops-1`
- **Proposed resolution:** B is current. All references to the systemd app units in promoted docs become Docker compose commands; legacy systemd references go to ARCHIVE/legacy-pi-era.md.
- **Needs confirmation:** none — this is settled, just calling it out as the lift is performed.

### C3 — Sheet-export smoke test port
- **Source A** (`docs/sheet-export-runbook.md`): `http://localhost:5000/api/revenue/sheet-export`
- **Source B** (`docs/sheet-export-client-contract.md`, `docker-compose.yml`): port 8000 (`127.0.0.1:8000:8000`)
- **Proposed resolution:** 8000.
- **Needs confirmation:** none.

### C4 — Blueprint registration
- **Source A** (`docs/GUIDE-CanonTools.md`): "Ensure blueprints registered in `app.py`: `app.register_blueprint(canon_bp)`"
- **Source B** (`.claude/CLAUDE.md`): "Blueprints registered in `src/web/blueprints.py` via `initialize_blueprints()` — NEVER in `app.py`"
- **Proposed resolution:** B is current. CanonTools content lifted into ARCHITECTURE will say `initialize_blueprints()` and the wrong instruction is dropped.
- **Needs confirmation:** none.

### C5 — Railway DR project status
- **Owner said earlier:** "railway is still sort of in play"
- **Doc state** (`docs/GUIDE-Railway.md`): Last-updated 2025-08-27, references legacy service name `ctv-bookedbiz-db` (not `spotops`).
- **Proposed resolution:** ARCHIVE the doc; leave a 5-line "Railway emergency activation" stub in RUNBOOKS that just says how to scale 0→1 / 1→0 if the service exists.
- **Needs confirmation:** Does the Railway project still exist under the SpotOps account? What is its current service name? Does `RAILWAY_DB_PATH=/app/data/database/production.db` still hold inside Railway?

### C6 — broadcast_month casing in stored values — **RESOLVED 2026-04-30**
- **Source A** (`docs/GUIDE-TwentyNineColumns.md`): example `"Nov-24"` (capital `N`)
- **Source B** (`.claude/CLAUDE.md`): `'mmm-yy'` with example `'Jan-25'` (capital `J`)
- **Resolution (live query):** stored values are **title-cased `Mmm-YY`** — sample: `Sep-26`, `Sep-25`, `Sep-24`, `Sep-23`, `Sep-22`, `Sep-21`, `Oct-26`, `Oct-25`. Pin "Title-cased Mmm-YY" in ARCHITECTURE data dictionary; update CLAUDE.md's `'mmm-yy'` notation to `'Mmm-YY'` (or annotate that the literal pattern means title-cased per `strftime('%b-%y')`).

### C7 — Insertion Order Scanner status — **RESOLVED 2026-04-30**
- **Resolution (live system check):** The IO scanner is **not running**. `ctv-io-scanner.service`/`.timer` are not installed on the host (only the unit templates remain in `/opt/spotops/scripts/`), and `pending_orders.json` does not exist anywhere under `/opt/spotops` or `/srv/spotops`. The dashboard "pending orders" widget is either dead or fed by a different path now.
- **What this means for consolidation:**
  1. The IO scanner runbook content from `GUIDE-failover-failback.md` does **not** lift to RUNBOOKS — it goes to ARCHIVE/legacy as a "deprecated subsystem" record.
  2. The general lesson — "shared files whose mode resets to 0600 after recreation will silently break consumers" — still has value as a footgun pattern. Lift just the lesson into LLM_SYSTEM_GUIDE's footguns section.
- **Open follow-up (out of scope for this consolidation):** worth flagging to owner that the dashboard probably has a dead "pending orders" code path; not a doc problem.

### C8 — Orphaned services in `service-dependencies-simple.md`
- File flags 5 services as deletion candidates: `basic_import_service.py`, `commercial_log_daily_report.py`, `enhanced_language_block_service.py`, `old/dallas_grid_populator-OLD.py`, `old/language_block_service-OLD.py`.
- **Proposed resolution:** ignore for now — the snapshot is stale; archive the doc itself; if cleanup desired, regenerate on demand later.
- **Needs confirmation:** none for consolidation purposes.

### C9 — SHA1 test vectors in sheet-export contract
- `sheet-export-client-contract.md` §4 still says `[compute during implementation]` — the actual test vectors aren't pinned.
- **Proposed resolution:** out of scope for consolidation; flag in API_AND_EXPORT_CONTRACTS as a known gap.
- **Needs confirmation:** owner wants test vectors pinned now or later?

### C10 — DB-lock advice in `GUIDE-OPERATIONS.md` — **PARTIALLY RESOLVED 2026-04-30**
- **What changed under Docker:** the daily/import pipeline now runs *inside* the container via `docker compose exec spotops uv run python cli/daily_update.py ...` (per `/opt/spotops/bin/daily_update.sh`). The container's app process is the long-lived DB reader; the import process is a short-lived sibling inside the same container. SQLite's WAL handles concurrent reader+writer access, so the old "kill datasette/uvicorn before running pipeline" advice is largely obsolete.
- **What stays useful:** lock-detection commands (`lsof | grep production.db` from the host) still work because `/srv/spotops/db/production.db` is on the host filesystem; the container just sees it via volume mount. The diagnostic value of these commands hasn't changed.
- **What's gone:** explicit "kill competing host processes" recipes. Drop them from the RUNBOOKS lift.
- **Proposed resolution:** in RUNBOOKS, replace the kill-competing-processes section with: (1) `lsof | grep /srv/spotops/db/production.db` to enumerate readers (you'll see Litestream + the container's uvicorn — that's normal); (2) for destructive operations on the DB (e.g., file replacement), `docker compose -f /opt/spotops/docker-compose.yml stop spotops` then perform the op then `start`; (3) if Litestream needs to be paused, `sudo systemctl stop litestream.service` first.
- **Needs confirmation:** None — the path forward is clear from inspection.

---

## 4. Snapshot of what the new docs/ tree will look like (post-Phase-3)

```
docs/
├── HUMAN_OPERATOR_GUIDE.md         (~ daily/monthly/yearly procedures, add-user)
├── LLM_SYSTEM_GUIDE.md             (~ system purpose, paths, env, invariants, footguns)
├── RUNBOOKS.md                     (~ container/ops/Litestream/Dropbox/IO-scanner/Railway-stub)
├── ARCHITECTURE.md                 (~ Docker, DR topology, auth, data model, canon, pipeline)
├── DEV_WORKFLOW.md                 (~ branching, dev loop, schema changes, LLM rules)
├── API_AND_EXPORT_CONTRACTS.md     (~ sheet-export, planning-export, AE drift tracker)
├── DOCUMENTATION_MIGRATION_PLAN.md (~ Phase 4 deliverable: what moved where, what got cut)
└── ARCHIVE/
    ├── README.md
    ├── ops.md                       (Feb-2026 systemd-on-Pi snapshot)
    ├── service-dependencies-simple.md
    ├── GUIDE-Railway.md
    └── legacy-pi-era.md             (extracted Pi-era operational details from failover-failback)
```

The old top-level `GUIDE-*` files all get superseded by content above. Repo-root `README.md` and `service-dependencies-simple.md` are addressed in Phase 3 separately.

---

## 5. Decisions on remaining open items (2026-04-30)

Owner confirmed restic is live and important; on the rest, I made calls.

1. **Restic** — **document fully in RUNBOOKS as current**. Backs up `/srv/spotops/{data,processed,uploads}` to Backblaze B2 nightly at 02:30 (+10 min jitter), retains 7d/4w/12m, logs `/var/log/restic/backup.log`. Repo URL and credentials live in `/etc/restic.env` (root-only). Sits alongside Litestream→B2 (continuous DB WAL) as the second arm of the B2 backup posture: Litestream covers the SQLite DB; restic covers everything else under `/srv/spotops` (raw imports, processed artifacts, future uploads). Owner-confirmed live and important.

2. **C5 — Railway** — **ARCHIVE the doc + 5-line stub in RUNBOOKS**. Doc is dated 2025-08-27 with the legacy service name `ctv-bookedbiz-db` and references paths that don't match the current stack. The stub will say "Railway is a last-resort emergency target; full procedure in `ARCHIVE/GUIDE-Railway.md`; verify project still exists before relying on it." This preserves the option without claiming the doc is current truth.

3. **C9 — SHA1 test vectors** in `sheet-export-client-contract.md` — **leave as-is during consolidation**. The doc says `[compute during implementation]` and that's accurate (they aren't pinned). Lift verbatim into API_AND_EXPORT_CONTRACTS with a TODO marker. Pinning vectors against the live endpoint is a separate, narrowly-scoped task that doesn't block the doc rewrite.

4. **`/srv/spotops/uploads/`** — **don't document yet**. Empty directory owned by `daseme:spotops-team`; no current code path writes to it; documenting an empty placeholder is more confusing than helpful. Mention only in RUNBOOKS' restic-backup section as "currently empty; included in the backup target so a future feature using this dir is covered automatically."

---

## 6. Phase 2 / 3 plan

Skipping a standalone Phase 2 fact-sheet pass — for the docs I haven't already absorbed I have the agent inventory; for the ones I edited today I have full context. Folding fact-extraction into each Phase 3 turn.

**Phase 3 doc order** (easy → hard, one doc per turn for review):

1. `docs/API_AND_EXPORT_CONTRACTS.md` — mostly verbatim lift of `sheet-export-client-contract.md` + `planning-export-client-contract.md` + `workbook-ae-drift-tracker.md` with a header listing all known endpoints. Smallest risk.
2. `docs/DEV_WORKFLOW.md` — fold `GUIDE_DEV_WORKFLOW.md` (just rewritten) + `GUIDE_GIT_WORKFLOW.md` + the schema-change rules.
3. `docs/HUMAN_OPERATOR_GUIDE.md` — operator-friendly distillation of `GUIDE-MONTHLY-CLOSING.md`, the operator surface of `GUIDE-DAILY-COMMERCIALLOG.md`, `USER_MANAGEMENT_SETUP.md`, and the concept-level part of `TAILSCALE_AUTH.md`.
4. `docs/RUNBOOKS.md` — Litestream + restic + db_sync + container lifecycle + `GUIDE-OPERATIONS.md` updated for Docker + sheet-export token rotation + Railway stub.
5. `docs/ARCHITECTURE.md` — Docker topology, DR (Litestream + restic + Dropbox), auth model, data dictionary (29 cols), language assignment, canon system. Pulls from the most sources.
6. `docs/LLM_SYSTEM_GUIDE.md` — capstone: paths, env vars, invariants, footguns, "where to look before changing X". Uses the other docs as link targets.

**Phase 4** — `docs/DOCUMENTATION_MIGRATION_PLAN.md` + actually move legacy docs into `docs/ARCHIVE/`.

After each new doc, I'll pause for review before starting the next.

---

## 6. Estimated scope of Phase 2–4

- **Phase 2 (extract facts):** ~1 working pass per file; about 21 files; output is a structured fact sheet I keep next to this doc. Probably ~30 min of agent time.
- **Phase 3 (write the 6 canonical docs):** the heaviest phase. Each new doc is sizable. Recommend doing one new doc per turn (HUMAN_OPERATOR_GUIDE, LLM_SYSTEM_GUIDE, RUNBOOKS, ARCHITECTURE, DEV_WORKFLOW, API_AND_EXPORT_CONTRACTS) so you can review each before the next is started.
- **Phase 4 (migration plan):** after Phase 3, ~30 min to produce `docs/DOCUMENTATION_MIGRATION_PLAN.md` and to move legacy files into `docs/ARCHIVE/`.

Total: probably 3–5 substantive turns after approval.
