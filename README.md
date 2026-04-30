# SpotOps

Revenue dashboard and database for Crossings TV ad-sales data. Pulls the daily commercial log from K: drive, tracks bookings month-by-month, and serves both a Tailscale-only web dashboard and authenticated API endpoints that feed `Revenue Master.xlsx`.

Runs as a Docker container on a single Linux host, behind Tailscale.

## Where to look

- **Operating the system day-to-day** (no shell access needed) → [`docs/HUMAN_OPERATOR_GUIDE.md`](docs/HUMAN_OPERATOR_GUIDE.md)
- **Working on the codebase** (devs) → [`docs/DEV_WORKFLOW.md`](docs/DEV_WORKFLOW.md)
- **Operations and recovery** (commands) → [`docs/RUNBOOKS.md`](docs/RUNBOOKS.md)
- **System architecture** (topology, data model, DR, auth) → [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- **API and export contracts** (workbook + canon endpoints) → [`docs/API_AND_EXPORT_CONTRACTS.md`](docs/API_AND_EXPORT_CONTRACTS.md)
- **Coding LLMs landing in this repo** → [`docs/LLM_SYSTEM_GUIDE.md`](docs/LLM_SYSTEM_GUIDE.md) (paths, env vars, footguns) plus auto-loaded [`.claude/CLAUDE.md`](.claude/CLAUDE.md)

Historical docs: [`docs/ARCHIVE/`](docs/ARCHIVE/). Migration record: [`docs/DOCUMENTATION_MIGRATION_PLAN.md`](docs/DOCUMENTATION_MIGRATION_PLAN.md).
