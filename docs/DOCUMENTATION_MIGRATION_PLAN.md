# Documentation Migration Plan

**Date:** 2026-04-30
**Owner:** SpotOps maintainers
**Status:** Phase 4 complete — consolidated docs are live; legacy docs archived under `docs/ARCHIVE/`.

> Source assignment: `.claude/tasks/doc-consolidation-assignment.md` (verbatim).
> Phase 1 inventory + decisions: `.claude/tasks/doc-consolidation-phase1-inventory.md`.

---

## What changed

The repository's documentation has been consolidated from ~20 scattered markdown files (mixed audience, partly stale, often duplicating each other) into **6 canonical docs** with clear audience separation, plus an `ARCHIVE/` folder preserving the originals as historical record.

### Before (in `docs/`, before this consolidation)

20 source files spanning operator guides, developer guides, system architecture, API contracts, and historical snapshots — many with overlapping content, drifted between Pi-era and Docker-era references.

### After (current `docs/`)

| File | Audience | Purpose |
|---|---|---|
| `HUMAN_OPERATOR_GUIDE.md` | Non-developer operators | Plain-English procedures (daily monitoring, monthly close, user management, sign-in concept) |
| `DEV_WORKFLOW.md` | Engineers + LLM agents | Branching, dev loop, schema changes, conventions, LLM rules |
| `RUNBOOKS.md` | DevOps / ops-savvy operators | Command-oriented procedures: container lifecycle, daily-import debug, backup/restore, Litestream / restic / Dropbox, token rotation, decision tree |
| `ARCHITECTURE.md` | Engineers + LLM agents | System topology, app structure, filesystem, data dictionary (29 cols), import pipeline, language assignment, canon system, auth, DR posture, legacy notes |
| `API_AND_EXPORT_CONTRACTS.md` | LLM / engineers (workbook side) | Canonical contract reference for `/api/revenue/sheet-export`, `/api/revenue/planning-export`, `/api/canon/*`, plus the proposed workbook AE drift tracker |
| `LLM_SYSTEM_GUIDE.md` | Coding LLM agents | Dense lookup reference: paths, env vars, invariants, footguns, "where to look before changing X" |

Plus:

- `ARCHIVE/` — all 20 original docs preserved with an index in `ARCHIVE/README.md`
- `DOCUMENTATION_MIGRATION_PLAN.md` — this file

---

## New docs created

| File | Lines | Built from |
|---|---|---|
| `docs/HUMAN_OPERATOR_GUIDE.md` | 327 | `GUIDE-MONTHLY-CLOSING.md` (full operator-facing lift), `GUIDE-DAILY-COMMERCIALLOG.md` (operator surface only — what runs, when, how to know it's healthy), `USER_MANAGEMENT_SETUP.md`, `TAILSCALE_AUTH.md` (concept-level). Restructured around the per-process template (When / Inputs / Steps / Validation / Failures / Escalate). |
| `docs/DEV_WORKFLOW.md` | 333 | `GUIDE_DEV_WORKFLOW.md` (the Docker-era rewrite from earlier in this session) + `GUIDE_GIT_WORKFLOW.md`. Added: code-conventions section (Trade exclusion, identity columns, broadcast_month, blueprint rule), Testing section, LLM coding rules section pointing to `.claude/CLAUDE.md`. |
| `docs/RUNBOOKS.md` | 626 | `GUIDE-OPERATIONS.md` (DB lock material — Docker-era), `GUIDE-DAILY-COMMERCIALLOG.md` (manual run / debug), `docker-setup.md` (container lifecycle, Litestream restore), `GUIDE-failover-failback.md` (verified-live procedures only), `sheet-export-runbook.md` (port refreshed to 8000, env model refreshed to compose `.env`). **Net new content:** restic backup posture / verification / restore (whole subsystem was undocumented), container lifecycle command table, Docker-era lock investigation, "Decommissioned subsystems" table, "When something looks wrong" decision tree. |
| `docs/ARCHITECTURE.md` | 619 | `GUIDE-TwentyNineColumns.md` (full data dictionary lift), `GUIDE-ASSIGNMENT-SYSTEM.md` (categorization rules + schema + assignment-methods table — SQL recipe library not lifted), `GUIDE-CanonTools.md` + `GUIDE_Customer_Name_Normalization.md` (merged: view chain + tables + sync rule), `docker-setup.md` (APP_MODE modes, container/volume topology), `GUIDE-failover-failback.md` (DR topology), `TAILSCALE_AUTH.md` (auth flow + Docker socket mount). Includes a "Legacy / deprecated architecture" section enumerating Pi-era → current changes. |
| `docs/API_AND_EXPORT_CONTRACTS.md` | 615 | `sheet-export-client-contract.md` + `planning-export-client-contract.md` + `workbook-ae-drift-tracker.md` (largely verbatim with deduplication of common conventions). Added: top-level Endpoints table, Common conventions section (auth/port/error/Trade exclusion), Canon endpoints summary, aggregated Known gaps section. |
| `docs/LLM_SYSTEM_GUIDE.md` | 470 | New synthesis. Built primarily from `.claude/CLAUDE.md` + `docker-compose.yml` + live system inspection. Pulls invariants and footguns from `GUIDE-ASSIGNMENT-SYSTEM.md`, `GUIDE-TwentyNineColumns.md`, `GUIDE-CanonTools.md`, `GUIDE-failover-failback.md`, `sheet-export-client-contract.md`. 12 numbered footguns with their own anchors for findability. |
| `docs/ARCHIVE/README.md` | n/a | New — archive index with mapping back to current docs |
| `docs/DOCUMENTATION_MIGRATION_PLAN.md` | (this file) | New — Phase 4 deliverable |

**Total:** 6 canonical docs + 2 meta docs (archive index, migration plan) = ~3000 lines vs. the original 20 docs at ~3500 lines (rough — some sources were ~250 lines each, others under 50). Net reduction is from deduplication of common conventions across the two API contract docs and from cutting Pi-era operational recipes that no longer apply.

---

## Old docs merged (and archived)

All 20 originals were moved to `docs/ARCHIVE/` via `git mv` to preserve commit history.

### Operational guides (Pi/systemd-era)

| Original (now in `ARCHIVE/`) | Merged into |
|---|---|
| `GUIDE-DAILY-COMMERCIALLOG.md` | HUMAN_OPERATOR_GUIDE (operator surface) + RUNBOOKS (manual runs) + ARCHITECTURE (pipeline overview) |
| `GUIDE-MONTHLY-CLOSING.md` | HUMAN_OPERATOR_GUIDE (full procedure) + RUNBOOKS (CLI invocations referenced) |
| `GUIDE-OPERATIONS.md` | RUNBOOKS (DB lock investigation refreshed for Docker) |
| `GUIDE-failover-failback.md` | RUNBOOKS (verified-live procedures) + ARCHITECTURE (DR topology). **Pi2 nightly-mirror content retained in archive only**, since the timer is no longer installed |
| `GUIDE-Railway.md` | 5-line stub in RUNBOOKS pointing at archive for full procedure (pure-archive decision per C5 resolution) |
| `docker-setup.md` | ARCHITECTURE (APP_MODE, Litestream topology) + RUNBOOKS (build/run/restore commands) |
| `ops.md` | ARCHITECTURE (Legacy / deprecated section). Whole-doc archive — content was a Feb-2026 systemd snapshot, not current truth |

### Developer / system guides

| Original | Merged into |
|---|---|
| `GUIDE_DEV_WORKFLOW.md` | DEV_WORKFLOW (this was already the Docker-era rewrite from earlier today) |
| `GUIDE_GIT_WORKFLOW.md` | DEV_WORKFLOW |
| `GUIDE-ASSIGNMENT-SYSTEM.md` | ARCHITECTURE (categorization, schema, assignment methods, best-practice query). **Deep SQL recipe library not lifted** — see "Conflicts / things to track" below |
| `GUIDE-CanonTools.md` | ARCHITECTURE (canon system, view chain, tables) + RUNBOOKS (raw-input sync). **Stale instruction corrected during lift**: blueprint registration goes in `src/web/blueprints.py`, not `app.py` |
| `GUIDE-TwentyNineColumns.md` | ARCHITECTURE (Data dictionary section — verbatim lift; this is the canonical home) |
| `GUIDE_Customer_Name_Normalization.md` | ARCHITECTURE (overlapped with `GUIDE-CanonTools.md`; merged carefully — same view chain, more matching-classification detail) + RUNBOOKS (CLI invocations) |
| `TAILSCALE_AUTH.md` | ARCHITECTURE (auth flow, Docker socket mount explanation) + HUMAN_OPERATOR_GUIDE (concept-level) |
| `USER_MANAGEMENT_SETUP.md` | HUMAN_OPERATOR_GUIDE (procedures) + ARCHITECTURE (auth model, users table schema) |

### API contracts

| Original | Merged into |
|---|---|
| `sheet-export-client-contract.md` | API_AND_EXPORT_CONTRACTS (largely verbatim) |
| `planning-export-client-contract.md` | API_AND_EXPORT_CONTRACTS (largely verbatim) |
| `workbook-ae-drift-tracker.md` | API_AND_EXPORT_CONTRACTS (verbatim, marked Proposed) |
| `sheet-export-runbook.md` | RUNBOOKS (Sheet-export endpoint operations — refreshed for Docker `.env` and port 8000) |

### Other

| Original | Disposition |
|---|---|
| `service-dependencies-simple.md` (was at repo root) | Archived as stale auto-generated artifact. Regenerate on demand if useful |
| `GUIDE-RaspberryWorkflow.md` | Already deleted earlier in this session before Phase 1 — folded into `GUIDE_DEV_WORKFLOW.md` (now `DEV_WORKFLOW.md`) |

---

## Old docs archived

All 20 listed above were moved via `git mv` to `docs/ARCHIVE/`. None were deleted (deletion proposed for some auto-generated artifacts; deferred — see "Things to track" below).

`docs/ARCHIVE/README.md` is the archive index with mapping back to the current canonical doc.

---

## Conflicts / things to track

Some are flagged for owner decision; some are records of corrections made during the lift.

### Flagged for owner

1. **C5 — Railway DR project status.** Owner said "still sort of in play" earlier. Doc dates from 2025-08-27 and uses the legacy service name `ctv-bookedbiz-db`. Decision made: archived with a 5-line activate/deactivate stub in RUNBOOKS. **If the Railway project no longer exists, delete `docs/ARCHIVE/GUIDE-Railway.md` outright.** If it's actively maintained, refresh and consider promoting back to live docs.
2. **C9 — SHA1 test vectors** in `sheet-export-client-contract.md`. Spec says `[compute during implementation]`. Carried over verbatim into `API_AND_EXPORT_CONTRACTS.md`. **Pinning the actual hex digests is a separate task; out of scope for consolidation.**
3. **`GUIDE-ASSIGNMENT-SYSTEM.md` SQL recipe library** (~100+ lines of operational queries — review queues, health checks, performance metrics) wasn't lifted into `ARCHITECTURE.md` (which got the schema + categorization rules + best-practice query). If the team uses these recipes regularly, consider promoting them into a new `docs/LANGUAGE_QUERIES.md` or appending to RUNBOOKS.
4. **`GUIDE-DAILY-COMMERCIALLOG.md` troubleshooting trees** — extensive procedural detail not fully lifted. Same disposition as #3 — promote on demand if useful.

### Resolved during lift (records, not open items)

5. **C4 — Blueprint registration.** `GUIDE-CanonTools.md` told contributors to register blueprints in `app.py`; `.claude/CLAUDE.md` mandates `src/web/blueprints.py` via `initialize_blueprints()`. The corrected guidance is in `ARCHITECTURE.md` and `LLM_SYSTEM_GUIDE.md`. The original (incorrect) instruction is preserved verbatim in the archived `GUIDE-CanonTools.md` as a record of the prior pattern.
6. **C2 — App process model** (systemd → Docker). Settled. All references to `ctv-bookedbiz-db.service` / `spotops-dev.service` etc. went to archive; current docs use `docker compose` exclusively.
7. **C3 — Sheet-export smoke test port** (5000 → 8000). Settled.
8. **C1 — DB path on the failover/backup stack.** Settled by live-system inspection earlier in this session; all current docs use `/srv/spotops/db/production.db`.
9. **C6 — broadcast_month casing.** Settled by live-DB query: title-cased `Mmm-YY` (`Sep-26`, `Oct-25`).
10. **C7 — Insertion Order Scanner.** Settled: not running, `pending_orders.json` doesn't exist on disk. `ctv-io-scanner.*` units are templates only, never installed. Captured in `LLM_SYSTEM_GUIDE.md` § Service inventory and `RUNBOOKS.md` § Decommissioned subsystems.
11. **C10 — Docker-era DB lock advice.** Settled: kill-host-processes is obsolete; the Docker-era sequence (stop Litestream → stop container → op → start container → start Litestream) is documented in RUNBOOKS.

### Things still needing owner confirmation before deletion (vs archive)

These are still in `ARCHIVE/` rather than deleted, pending owner decision:

- **`service-dependencies-simple.md`** — auto-generated, stale. **Recommendation:** delete and replace with a script that emits this on demand (or just stop generating it).
- **`docs/ARCHIVE/GUIDE-Railway.md`** — full deletion is appropriate if the Railway project has been fully retired.
- **`docs/ARCHIVE/ops.md`** — keeping as historical record is fine; deletion is also fine since `ARCHITECTURE.md → Legacy` summarizes the era. **No action needed unless drive space matters.**

---

## What needs human confirmation before old docs are deleted

Per the assignment: nothing has been **deleted** by this consolidation (one exception — `GUIDE-RaspberryWorkflow.md` was deleted earlier in this session and folded into `GUIDE_DEV_WORKFLOW.md` / now `DEV_WORKFLOW.md`).

Before deleting any archived file, confirm:

1. **`GUIDE-Railway.md`** — the Railway project status. If retired, delete is fine.
2. **`service-dependencies-simple.md`** — confirm it's safe to drop the snapshot in favor of regenerate-on-demand.
3. **The other 18 archived files** — keep them. They're cheap (~3500 lines on disk) and irreplaceable as historical record. The cost of accidentally losing operational lessons buried inside them outweighs any maintenance overhead.

---

## Suggested next review cadence

- **Quick spot-check every 6 months.** Open the canonical docs, confirm paths/services/env vars match live state. Live-system commands to run: `systemctl list-timers --all`, `docker compose ps`, `cat /opt/spotops/.env | head` (redacted).
- **Full re-review whenever infrastructure changes.** New backup target, new compose-stack restructuring, new Tailscale story, new ingest pipeline — trigger a full pass. Update the **Last reviewed** date at the top of every affected doc.
- **`LLM_SYSTEM_GUIDE.md` § "What you can trust without re-checking"** is the highest-leverage thing to keep current. If anything in that table goes wrong, hallucinations follow downstream.

---

## Audit trail

| Step | Action |
|---|---|
| Earlier in this session | Path-cleanup pass (Pi-era → /opt/spotops paths in remaining live docs). Archived `GUIDE-RaspberryWorkflow.md`. |
| Phase 1 (inventory) | `.claude/tasks/doc-consolidation-phase1-inventory.md` — inventory table + proposed file map + 10 conflicts surfaced |
| Phase 1.5 (live-system checks) | Resolved C1, C6, C7, C10 by inspecting the running system (`systemctl list-timers`, `sqlite3` against live DB, `find pending_orders.json`, etc.). Findings folded back into Phase 1 doc. |
| Phase 2 | Skipped as a separate pass; fact extraction folded into Phase 3 |
| Phase 3 | 6 canonical docs written, one per turn, with review pause between each: API_AND_EXPORT_CONTRACTS → DEV_WORKFLOW → HUMAN_OPERATOR_GUIDE → RUNBOOKS → ARCHITECTURE → LLM_SYSTEM_GUIDE |
| Phase 4 (this) | 20 source docs `git mv`-ed into `docs/ARCHIVE/`. Two stale forward-references in `RUNBOOKS.md` fixed. `ARCHIVE/README.md` and this migration plan written. |

---

## Files NOT touched by this consolidation

- `README.md` (repo root) — was a 1-line stub before; flagged in Phase 1 for separate attention. **Update separately to point at `docs/HUMAN_OPERATOR_GUIDE.md` and `docs/LLM_SYSTEM_GUIDE.md` as the entry points.**
- `src/web/README.md` — banner added during the earlier path-cleanup pointing readers at `DEV_WORKFLOW.md`. Untouched in this consolidation; not in scope.
- `src/web/dev-environment-overview-flask-wsl.md` — historical-banner already in place. Untouched.
- `src/web/REPORTING_DOCUMENTATION_AND_TODOS.md` — historical-banner already in place. Untouched.
- All files under `docs/plans/` and `docs/superpowers/` — dated point-in-time records; **do not touch**, per the assignment.
- `.claude/CLAUDE.md` — auto-loaded LLM cheat sheet. Continues to be the authoritative source for in-conversation rules. `LLM_SYSTEM_GUIDE.md` references it as such.
- `.claude/tasks/lessons.md` — per the owner's earlier instruction, this stays live and gets updated as corrections happen. The path-cleanup work proposed updating its content, but that's a separate task from this consolidation.

---

## Closing summary

- **What changed:** 20 scattered, drifted markdown files consolidated into 6 audience-segregated canonical docs + an archive folder + this migration plan.
- **What conflicts remain:** 4 things flagged for owner decision (Railway project status, SHA1 test vectors, two recipe libraries that didn't lift). All low-priority; none block any user-facing work.
- **What needs confirmation before deletion:** Only `GUIDE-Railway.md` (if Railway is fully retired) and `service-dependencies-simple.md` (safe to drop in favor of regenerate-on-demand). Everything else stays archived as historical record.
