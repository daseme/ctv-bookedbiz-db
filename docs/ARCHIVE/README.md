# docs/ARCHIVE/

**Purpose:** Historical record of the SpotOps documentation as it existed before the 2026-04-30 consolidation. Don't follow these as runbooks — the canonical current docs are one level up in `docs/`.

**Why we keep them:**

1. **Preserve durable facts that didn't make it into the new docs verbatim** — extended SQL recipe libraries, troubleshooting cookbooks, historical incident notes.
2. **Preserve the prior architecture's record** — Pi-era systemd setup, `/var/lib/ctv-bookedbiz-db/` paths, the `ctvbooked` user, Railway emergency procedures dated 2025-08-27. None of that is current truth, but the record is useful when reading old commits, plans, or specs that reference those paths.

**When to consult these instead of the current docs:**

- You're reading a dated plan under `docs/plans/` or `docs/superpowers/` that uses Pi-era paths
- You need a deep-dive recipe from one of the original guides that isn't in the consolidated docs
- You're verifying a backup procedure against the prior runbook before reinstating it

**Don't follow them as live runbooks** — paths, service names, and infra references are mostly stale.

---

## Index

### Operational guides (Pi/systemd-era)

| File | Originally | Now lives (largely) in |
|---|---|---|
| `GUIDE-DAILY-COMMERCIALLOG.md` | Daily commercial-log import procedure (extensive recipe library) | [HUMAN_OPERATOR_GUIDE.md](../HUMAN_OPERATOR_GUIDE.md), [RUNBOOKS.md](../RUNBOOKS.md), [ARCHITECTURE.md](../ARCHITECTURE.md) |
| `GUIDE-MONTHLY-CLOSING.md` | Monthly close + yearly recap (`update_yearly_recap.sh`) | [HUMAN_OPERATOR_GUIDE.md](../HUMAN_OPERATOR_GUIDE.md), [RUNBOOKS.md](../RUNBOOKS.md) |
| `GUIDE-OPERATIONS.md` | DB lock management, pipeline monitoring, rollback (largely Pi-era recipes) | [RUNBOOKS.md](../RUNBOOKS.md) (Docker-era equivalents) |
| `GUIDE-failover-failback.md` | DR runbook: Litestream → B2, Dropbox nightly, Pi2 cold standby, IO scanner | [RUNBOOKS.md](../RUNBOOKS.md) (verified-live procedures), [ARCHITECTURE.md](../ARCHITECTURE.md) (DR topology) |
| `GUIDE-Railway.md` | Railway emergency-failover setup (2025-08-27) | [RUNBOOKS.md](../RUNBOOKS.md) has a 5-line activate/deactivate stub. Full setup details remain here |
| `docker-setup.md` | Docker image setup with Backblaze + Litestream + APP_MODE modes | [ARCHITECTURE.md](../ARCHITECTURE.md), [RUNBOOKS.md](../RUNBOOKS.md) |
| `ops.md` | Feb-2026 systemd-on-Pi production architecture snapshot | [ARCHITECTURE.md → Legacy / deprecated architecture](../ARCHITECTURE.md#legacy--deprecated-architecture) |

### Developer / system guides

| File | Originally | Now lives in |
|---|---|---|
| `GUIDE_DEV_WORKFLOW.md` | Pre-Docker dev workflow guide | [DEV_WORKFLOW.md](../DEV_WORKFLOW.md) |
| `GUIDE_GIT_WORKFLOW.md` | Branching + protection rules | [DEV_WORKFLOW.md](../DEV_WORKFLOW.md) |
| `GUIDE-ASSIGNMENT-SYSTEM.md` | Three-stage language assignment system, with extensive SQL query library | [ARCHITECTURE.md → Language assignment system](../ARCHITECTURE.md#language-assignment-system); deep SQL recipe library only here |
| `GUIDE-CanonTools.md` | Customer/agency canon system + view chain + UI + endpoints | [ARCHITECTURE.md → Customer / agency canon system](../ARCHITECTURE.md#customer--agency-canon-system); operational details in [RUNBOOKS.md](../RUNBOOKS.md) |
| `GUIDE-TwentyNineColumns.md` | Position-by-position 29-column data dictionary | [ARCHITECTURE.md → Data dictionary](../ARCHITECTURE.md#data-dictionary--the-29-column-source-structure) |
| `GUIDE_Customer_Name_Normalization.md` | Customer normalization view chain + alias resolution + CLI | [ARCHITECTURE.md → Customer / agency canon system](../ARCHITECTURE.md#customer--agency-canon-system) (overlaps with `GUIDE-CanonTools.md`) |
| `TAILSCALE_AUTH.md` | Sign-in via Tailscale `whois` (Pi-era setup) | [ARCHITECTURE.md → Authentication](../ARCHITECTURE.md#authentication), [HUMAN_OPERATOR_GUIDE.md](../HUMAN_OPERATOR_GUIDE.md) |
| `USER_MANAGEMENT_SETUP.md` | Add/remove users; Tailscale identity model | [HUMAN_OPERATOR_GUIDE.md → User Management](../HUMAN_OPERATOR_GUIDE.md#user-management) |

### API contracts

| File | Now lives in |
|---|---|
| `sheet-export-client-contract.md` | [API_AND_EXPORT_CONTRACTS.md → Sheet Export](../API_AND_EXPORT_CONTRACTS.md#sheet-export) |
| `planning-export-client-contract.md` | [API_AND_EXPORT_CONTRACTS.md → Planning Export](../API_AND_EXPORT_CONTRACTS.md#planning-export) |
| `sheet-export-runbook.md` | [RUNBOOKS.md → Sheet-export endpoint operations](../RUNBOOKS.md#sheet-export-endpoint-operations) (refreshed for Docker `.env` and port 8000) |
| `workbook-ae-drift-tracker.md` | [API_AND_EXPORT_CONTRACTS.md → Workbook AE Drift Tracker](../API_AND_EXPORT_CONTRACTS.md#workbook-ae-drift-tracker-proposed) |

### Other

| File | Now |
|---|---|
| `service-dependencies-simple.md` | Auto-generated import-graph snapshot. Stale; archived as historical artifact. Regenerate on demand if useful |

---

## Notes for future cleanup

- **`GUIDE-Railway.md`** dates from 2025-08-27 and uses the legacy service name `ctv-bookedbiz-db`. If the Railway project still exists and is being actively maintained, refresh it and consider promoting back to live docs. If the project has been retired, consider deleting this file.
- **`GUIDE-failover-failback.md`** has irreplaceable operational footguns (DB-recreation reverts ownership, file-mode 0640 vs 0600, ProtectSystem=strict / sandbox blocking reads). The lessons are in [RUNBOOKS.md](../RUNBOOKS.md); the full incident-driven detail stays here.
- **`GUIDE-ASSIGNMENT-SYSTEM.md`** has ~100+ lines of SQL query recipes that didn't make it into the consolidation. Lift into [ARCHITECTURE.md](../ARCHITECTURE.md) or a new `LANGUAGE_QUERIES.md` if the team uses these regularly.
- **`GUIDE-DAILY-COMMERCIALLOG.md`** has detailed troubleshooting trees that could move into [RUNBOOKS.md](../RUNBOOKS.md) if they prove repeatedly useful.
- **`GUIDE-CanonTools.md`** had one stale instruction (register blueprints in `app.py`) that contradicted CLAUDE.md. The corrected guidance is in [ARCHITECTURE.md → Blueprint registration](../ARCHITECTURE.md#blueprint-registration-hard-rule); the original here is preserved as record of the prior pattern.

---

**Canonical current docs:** `docs/HUMAN_OPERATOR_GUIDE.md`, `docs/DEV_WORKFLOW.md`, `docs/RUNBOOKS.md`, `docs/ARCHITECTURE.md`, `docs/API_AND_EXPORT_CONTRACTS.md`, `docs/LLM_SYSTEM_GUIDE.md`. See `docs/DOCUMENTATION_MIGRATION_PLAN.md` for the full migration record.
