# Active Tasks

## Docs path-cleanup sweep: Pi/Railway → /opt/spotops Docker

**Status: PLANNING** (2026-04-30)
**Branch:** current (`feat/revenue-sheet-export-db-audit`) — small commits as we go

### Ground truth (current)
- App: Docker container `spotops-spotops-1` on `/opt/spotops`
- Live DB: `/srv/spotops/db/production.db` (mounted into container)
- Secrets/env: `/opt/spotops/.env`
- Backups: Backblaze + Dropbox; failover/failback uses Docker, Backblaze, and the two Pis
- Network: everything sits behind Tailscale
- Railway: still in play as a backup target (not retired)

### What we're NOT touching
- `old/**` — already archived
- Dated plans/specs under `docs/plans/`, `docs/superpowers/plans/`, `docs/superpowers/specs/` — historical record, leave Pi/old-path mentions intact
- Old paths inside `lessons.md` entries that are *being warned about* — those stay if the warning still applies, just re-cast around current paths

### Plan
- [x] **Step 1 — Update in-place (current docs with stale paths)** — DONE 2026-04-30
  - [x] `docs/GUIDE_DEV_WORKFLOW.md` — full rewrite around Docker; folded in content from former `GUIDE-RaspberryWorkflow.md`
  - [x] `docs/GUIDE-RaspberryWorkflow.md` — DELETED (folded into above)
  - [x] `docs/GUIDE_GIT_WORKFLOW.md` — Pi/systemd → Docker
  - [x] `docs/GUIDE-OPERATIONS.md` — banner + DB path fixes (`lsof | grep` lines preserved since they match on filename)
  - [x] `docs/GUIDE-MONTHLY-CLOSING.md` — paths
  - [x] `docs/ops.md` — historical-snapshot banner; content preserved as record of Feb-2026 systemd era
  - [x] `docs/GUIDE_Customer_Name_Normalization.md` — paths
  - [x] `docs/USER_MANAGEMENT_SETUP.md` — paths + cross-ref to GUIDE_DEV_WORKFLOW
  - [x] `docs/GUIDE-DAILY-COMMERCIALLOG.md` — `/opt/apps/ctv-bookedbiz-db` → `/opt/spotops`, DB path → `/srv/spotops/db/production.db` (added during sweep — wasn't in original list)
  - [x] `docs/docker-setup.md` — `/opt/apps/ctv-bookedbiz-db` → `/opt/spotops`; banner noting the live deploy is `docker compose`, not `docker run` (added during sweep)
  - [x] `src/web/README.md` — banner + path swap
  - [x] `src/web/dev-environment-overview-flask-wsl.md` — historical banner + path note
  - [x] `src/web/REPORTING_DOCUMENTATION_AND_TODOS.md` — historical banner + path swap
- [ ] **Step 2 — Rewrite (whole-doc-is-stale, but topic still real)**
  - [ ] `docs/GUIDE-RaspberryWorkflow.md` — gut and replace with `GUIDE-DEV-WORKFLOW-DOCKER.md` (or merge into `GUIDE_DEV_WORKFLOW.md` and delete this); covers `docker compose up -d --build spotops`, log tailing, exec, hot dev loop
  - [ ] `docs/GUIDE-failover-failback.md` — full rewrite around current architecture: Docker on `/opt/spotops`, Backblaze + Dropbox snapshots, two Pis as failover targets, Railway as last-resort. **Requires user input** on the actual current failover sequence — I'll draft a skeleton and gaps for you to fill
  - [ ] `docs/GUIDE-Railway.md` — keep, but update: confirm whether the service name is still `ctv-bookedbiz-db`, whether `/app/data/database/production.db` is still the path inside the Railway container, and update prose to say "secondary backup target behind Backblaze + Pi failover"
  - [ ] `docs/TAILSCALE_AUTH.md` — keep (Tailscale still in use); swap `ctvbooked` user references for whichever user the Docker container/host actually runs as; drop "On the Pi run…" framing where the host is now `/opt/spotops`
- [ ] **Step 3 — Refresh `lessons.md`**
  - [ ] Lines ~14–47 (DB path discovery lesson): re-cast around current `data/database/production.db` skeleton vs `/srv/spotops/db/production.db` real DB. Drop `/var/lib/ctv-bookedbiz-db/...` and `ReadOnlyPaths` references — that systemd world is gone
  - [ ] Lines ~179–189 (deploy flow lesson): rewrite around `git pull` + `docker compose up -d --build spotops` instead of `git -C /opt/apps/ctv-bookedbiz-db pull` + `systemctl restart ctv-bookedbiz-db.service`. Keep the underlying lesson ("never edit on prod, flow through git")
  - [ ] Re-read top to bottom and drop any lesson whose *world* no longer exists (vs. just stale path)
- [ ] **Step 4 — Verify**
  - [ ] `grep -rn -E "(/var/lib/ctv-bookedbiz-db|/opt/apps/ctv-bookedbiz-db|/home/daseme/dev/ctv-bookedbiz-db|raspberrypi)" --include="*.md" docs/ .claude/ src/web/ README.md` returns only `old/`, dated plans, and intentional warnings
  - [ ] Spot-check 2 docs by following their commands to make sure they work against the live container/DB

### Order of execution
Step 1 first (mechanical, low-risk, lots of small wins). Then Step 3 (lessons — small, contained). Step 2 last and one doc at a time, since failover-failback needs your input on current architecture.

---

## AE My Accounts — Phase 4 (Address Book Refactor)

**Status: DEBUGGING — page loads but data doesn't populate**
**Branch:** `refactor/address-book-service-extraction`

All code is committed (9 commits `cbcb059..9e8fdb0`). Backend verified:
- [x] Service layer (AeCrmService) — 15 tests pass
- [x] Activity extensions (AE filter, recent activity) — 15 tests pass
- [x] Routes registered, correct HTTP codes (302/401 unauthenticated)
- [x] Factory registration confirmed
- [x] Blueprint registration confirmed
- [x] **BUG: Data doesn't populate in browser** — FIXED: template used `{% block extra_scripts %}` but base.html defines `{% block extra_js %}`. Scripts never loaded. (commit e903410)

**Key files:**
- Service: `src/services/ae_crm_service.py`
- Routes: `src/web/routes/ae_crm.py`
- Template: `src/web/templates/ae_my_accounts.html`
- JS: `src/web/static/js/ae_my_accounts.js`
- Tests: `tests/services/test_ae_crm_service.py`, `tests/services/test_activity_service.py`

**To debug:** Check browser console for JS errors. Likely causes:
1. API fetch URLs not matching routes (check window.location.search passthrough)
2. DOM element IDs in template don't match JS selectors
3. CRM_AE_NAME variable not set correctly for non-admin users
4. JSON response shape doesn't match what JS expects

---

---

## Tailscale MagicDNS Rename: pi-ctv -> spotops

**Status: NEARLY COMPLETE** (2026-03-04)

Remaining:
- [ ] Check pi2 for any scripts/config referencing `pi-ctv` (SSH host key needs fixing first)
- [ ] Fix `pi-weekly-update.service` (script `/usr/local/bin/pi-weekly-update.sh` missing — decide: remove service or replace with `unattended-upgrades`)

---

## Worldlink Auto-Resolution

**Status: BACKLOG** (Low priority)

Worldlink is a broker/agency — their individual advertisers aren't important for day-to-day tracking but we want to preserve the data. Currently each new `Worldlink:<Advertiser>` bill code shows up in the resolution queue and needs manual alias creation.

Options to explore:
- [ ] Auto-create customer + alias for any `Worldlink:*` / `WorldLink:*` bill code on ingest (strip prefix, create customer if needed)
- [ ] Mark Worldlink-prefixed bill codes as "auto-resolved" so they don't clutter the resolution queue
- [ ] Bulk-flag existing Worldlink customers as low-priority so they sort to the bottom

**Context**: 70 distinct Worldlink bill codes, ~860K spots. Most are already resolved. Only 3 were unresolved as of 2026-03-06 (manually fixed).

---

## comDiff (jellee26's Raspberry Pi project)

**Status: BACKLOG — context capture only, not on this server**

Lives on a separate Raspberry Pi box (also hostnamed `spotops`) under `/home/jellee26/comDiff/`. Not present on this `/opt/spotops` server — earlier searches here returned "No such file or directory" because we were on the wrong host.

**What's there (per `ls` on the Pi, 2026-04-30):**
- `daily_diff_system.py` — main diff pipeline
- `email_sender.py`, `email_config.json`, `test_email.py`, `EMAIL_SETUP.md` — email delivery
- `comdiff.service` / `comdiff.timer` — systemd units (scope unknown — user vs system)
- `placement_confirmation_YYYYMMDD.txt` — daily output, runs Apr 16–30 2026 plus stragglers from Nov 11–12 2025
- `pyproject.toml` + `uv.lock` + `requirements.txt` — uv-managed Python project
- `data/`, `__pycache__/`, `README.md`, `WORKFLOW_SUMMARY.md`

**Open questions / possible follow-ups:**
- [ ] Read README + WORKFLOW_SUMMARY to understand what the diff actually compares
- [ ] Decide whether to port/co-locate it on this server alongside the daily CTV db sync timer
- [ ] Confirm the timer is firing reliably on the Pi (gap between Nov 2025 and Apr 2026 suggests it was paused for a stretch)

---

# Completed

- **Diff-Based Import** — Replaced delete-and-reinsert with contract-group fingerprint diff. 4x daily imports now produce zero writes when nothing changed (was ~33K writes/import). Reduces Backblaze WAL bloat and stabilizes spot_ids. PR merged 2026-03-24.
- **Fix Worldlink Dollars Missing** — Docker migration dropped the multi-sheet combiner; import only read "Commercials" sheet. Fixed to read all 4 sheets (Worldlink Lines, Pending, Add to booked business). PR #253 (2026-03-24)
- **Eliminate hardcoded DB path fallbacks** — Removed all silent fallback DB paths; production code requires explicit env var (2026-03)
- **Fix Dropbox Backup Path** — Fixed `DATABASE_PATH` in sync env, added systemd sandbox path, rewrote failover guide. PR #187 (2026-03-02)
- **Phase 2 Step 1: Shared Utilities** — Consolidated language CASE, date range utils, added 15 tests (2026-03-05)
- **Phase 2 Step 2: Query Builder** — Moved RevenueQueryBuilder to shared utils, eliminated 7 inline CASE blocks, deleted dead methods, added 10 tests (2026-03-05)
- **Phase 2 Step 3: DB Connection Standardization** — Replaced raw `sqlite3.connect()` in blueprints.py, stale_customers.py, address_book.py with DatabaseConnection from DI container. Deleted unused CustomerMatchingRepository. -353 lines. PR #207 (2026-03-07)
