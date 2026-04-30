# Developer Workflow

**Audience:** Engineers and LLM agents working on the SpotOps codebase
**Purpose:** Branching, local dev, deployment, schema changes, code conventions, and LLM coding rules — one place
**Last reviewed:** 2026-04-30

---

## Overview

- **Repo / working dir:** `/opt/spotops`
- **Branches:** `dev` (testing) → `main` (protected, PR-only)
- **Runtime:** Docker container `spotops-spotops-1` (compose service `spotops`) bound to `127.0.0.1:8000`
- **Live DB:** `/srv/spotops/db/production.db` (mounted into the container at the same path)
- **Env file:** `/opt/spotops/.env`
- **Network:** everything sits behind Tailscale; no public ingress

For the deployment topology, DR posture, and data model, see [ARCHITECTURE.md](ARCHITECTURE.md). For runbook-style ops (logs, backups, recovery), see [RUNBOOKS.md](RUNBOOKS.md).

---

## Branching model

| Branch | Role | Protection |
|---|---|---|
| `main` | Production. What the live container builds from. | Branch-protected: PRs only, squash merges, CI checks |
| `dev` | Testing. PRs land here first; squashed up to `main` when validated | None |
| `feat/<owner>/<slug>` | Feature/fix branches | None |

**Naming:** `feat/<owner>/<slug>` in short, kebab-case (e.g., `feat/jenna/sector-filter-fix`).

**Squash-and-merge** for both `feat/* → dev` and `dev → main`. Keeps history readable.

---

## Daily loop

```bash
# 1) sync dev and branch
git switch dev
git pull --ff-only
git switch -c feat/<yourname>/<slug>

# 2) code; lint/format before each commit
uvx ruff check .
uvx ruff format .

# 3) commit & push
git add -A
git commit -m "feat: <one clear change>"
git push -u origin HEAD

# 4) open PR: feat/* → dev (small, focused)
```

**To exercise your branch on the box:**

```bash
cd /opt/spotops
git fetch origin
git switch <your-branch>
docker compose up -d --build spotops    # rebuild required for Python changes
docker compose logs -f spotops          # tail until app reports ready
curl -sf http://localhost:8000/health/ && echo OK
```

When validated, open PR **dev → main** and squash-merge.

---

## Restart / rebuild rules

The single biggest footgun on this box: **`restart` does not pick up Python changes.**

| Change | Command |
|---|---|
| Python source | `docker compose up -d --build spotops` |
| Templates / static / SQL files (baked into image) | `docker compose up -d --build spotops` |
| `.env` / compose env vars | `docker compose up -d spotops` (recreates with new env) |
| `docker-compose.yml` | `docker compose up -d` |
| Bounce a healthy container | `docker compose restart spotops` |

If you ran `restart` and behavior didn't change, that's why.

---

## Container lifecycle (cheat sheet)

All commands assume `cd /opt/spotops` first.

```bash
# Status
docker compose ps

# Logs
docker compose logs -f spotops          # live tail
docker compose logs --tail=200 spotops  # last 200 lines

# Shell into the running container
docker exec -it spotops-spotops-1 bash

# Stop / start (preserves the container)
docker compose stop spotops
docker compose start spotops

# Recreate (after compose file or env changes)
docker compose up -d spotops

# Rebuild + recreate (after code changes)
docker compose up -d --build spotops

# Tear down (preserves volumes/data)
docker compose down
```

---

## Database access

The live DB is `/srv/spotops/db/production.db`. The host and container both see the same file via volume mount — `sqlite3` from either side reads identical data. The host doesn't ship `sqlite3` in `$PATH`; if you want a CLI, exec into the container.

```bash
# From inside the container (canonical)
docker exec -it spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  "SELECT COUNT(*) FROM spots;"
```

**Safe snapshot while the app is running** (uses SQLite's online backup; safer than `cp` of a hot DB):

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  ".backup '/srv/spotops/db/snap-$(date +%F_%H%M).sqlite3'"
```

For destructive restore (from Backblaze via Litestream, or from Dropbox), see [RUNBOOKS.md](RUNBOOKS.md).

### Empty-skeleton trap

The repo ships a 4 KB `data/database/production.db` placeholder. **Don't point tests or scripts at that** — the app's default `DB_PATH` resolves to it, and there's no real data inside. Set `DATABASE_PATH=/srv/spotops/db/production.db` (the live path) explicitly for any operation that should hit real data.

In code that uses Flask test client, override before requests fire:

```python
app.config['DB_PATH'] = '/srv/spotops/db/production.db'
```

The container's `.env` already pins this; the trap is for ad-hoc host-side scripts and tests.

---

## Schema changes

The app does not auto-migrate. Procedure:

1. Add a new SQL file under `sql/migrations/`. Numeric prefix continues the sequence (e.g. `027_…sql`).
2. Apply it against the live DB:
   ```bash
   docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
     < sql/migrations/<your-file>.sql
   ```
3. Verify with a quick `PRAGMA table_info(<table>)` or `.schema <table>`.
4. If the app caches schema in memory (most services do), restart:
   ```bash
   docker compose restart spotops
   ```
5. Commit the SQL file alongside any code changes that depend on it.

**Footgun:** if your schema change is breaking (column type narrowed, NOT NULL added with no default), the live container will keep its connection open and may serve stale errors until restart. Test the migration against a snapshot first if you're not sure.

---

## Testing

- Unit / integration tests live under `tests/`.
- Run inside the container against the dev environment to match production paths:
  ```bash
  docker exec spotops-spotops-1 uv run pytest tests/ -v
  ```
- For repository-level tests that hit a real SQLite DB, take a snapshot (see [Database access](#database-access)) and point tests at the snapshot — never at the live `production.db`.
- Do not write tests that mock the database. The data shape moves; mocks rot. Use a real DB (snapshot or in-memory SQLite seeded from `sql/migrations/`).

---

## Verifying the app is healthy

```bash
docker compose -f /opt/spotops/docker-compose.yml ps
curl -sf http://localhost:8000/health/ && echo OK
docker compose -f /opt/spotops/docker-compose.yml logs --tail=100 spotops
```

The base template footer also shows the most recent successful import timestamp; if it's stale (>24h), there's a yellow warning triangle next to the time.

---

## Code conventions

- **Lint/format:** `uvx ruff check .` and `uvx ruff format .` before every commit.
- **No emoji** in code, comments, or commits unless the user explicitly asks.
- **Comments:** default to none. Only write a comment when the *why* is non-obvious (hidden constraint, subtle invariant, workaround for a specific bug). Names should carry the *what*.
- **No premature abstraction:** three similar lines is better than a wrong helper.
- **No dead/half-finished code:** if it's unused, delete it; don't leave `_var` or `// removed` stubs.
- **Trust internal callers:** validate at system boundaries (HTTP input, external APIs), not between internal layers.
- **Clean architecture:** routes → services → repositories. Don't reach across layers.
- **Blueprints register exactly once,** in `src/web/blueprints.py` via `initialize_blueprints()`. **Never** in `app.py`.

### Customer / agency identity

- Customer table uses `normalized_name` (NOT `customer_name`).
- Agency table uses `agency_name`.
- Mixing these up is a recurring footgun.

### Revenue queries

Every revenue query in this codebase **must** exclude Trade revenue:

```sql
WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
```

This is a hard system-wide invariant. The two API exports already apply it; in-app queries must too.

### Broadcast month

Stored as title-cased `Mmm-YY` (e.g., `Sep-26`, `Oct-25`). Do not parse with case-insensitive matchers; do not assume the month abbreviation is uppercase or lowercase — it's title-case.

---

## LLM coding rules

This codebase is regularly worked on by Claude Code agents. The canonical instructions for an LLM agent are:

- **`/opt/spotops/.claude/CLAUDE.md`** — auto-loaded into every Claude Code conversation. The single source of truth for workflow rules (plan mode, subagent use, verification before done, autonomous bug fixing), runtime context (Docker, container name, DB path, restart command), file locations, and core principles.
- **`/opt/spotops/.claude/tasks/lessons.md`** — running corrections log. When the LLM gets corrected on something the same way twice, the rule lands here.
- **`/opt/spotops/.claude/tasks/todo.md`** — active work tracking. Plans land here before any non-trivial implementation; results get reviewed and checked off.

Headlines worth knowing as a human dev so you understand what the LLMs are doing:

| Rule | What it means in practice |
|---|---|
| **Plan-mode default** | LLMs write plans to `.claude/tasks/todo.md` before any non-trivial multi-step change |
| **Verification before done** | LLMs run tests, hit endpoints, or otherwise demonstrate behavior before claiming a task is complete |
| **Subagent for long research** | Multi-file or open-ended exploration is delegated to a research agent; main context stays clean |
| **Self-improvement loop** | After a correction, the LLM updates `lessons.md` so the same mistake is harder to repeat |
| **Demand elegance (balanced)** | For non-trivial changes, LLMs are asked to second-pass for simplicity. Skipped on obvious one-line fixes |

If you're an LLM landing in this repo: read `.claude/CLAUDE.md` first. It overrides anything in this file when they conflict.

---

## Troubleshooting

**Container won't start**
```bash
docker compose -f /opt/spotops/docker-compose.yml ps
docker compose -f /opt/spotops/docker-compose.yml logs --tail=200 spotops
```

**App is up but serving stale code**
You ran `restart` instead of `up -d --build`. Rebuild:
```bash
cd /opt/spotops && docker compose up -d --build spotops
```

**Wrong database in use**
The container's `.env` pins `DATABASE_PATH=/srv/spotops/db/production.db`. If a host-side script seems to be hitting the empty 4 KB skeleton at `data/database/production.db`, check the script's shell env and set `DATABASE_PATH` explicitly. The default `_get_db_path()` fallback `or "./.data/dev.db"` does NOT fire when config is set — and it usually is, just to the wrong path.

**Port 8000 already in use**
```bash
ss -ltnp | grep ':8000'
docker compose -f /opt/spotops/docker-compose.yml restart spotops
```

**`docker compose` says "no configuration file"**
You're not in `/opt/spotops`. Either `cd /opt/spotops` or pass `-f /opt/spotops/docker-compose.yml`.

**`git pull --ff-only` fails**
Either rebase your local work onto the remote:
```bash
git fetch origin
git rebase origin/dev
git push --force-with-lease
```
Or, if you don't have local work to keep, hard-reset (destructive):
```bash
git fetch origin
git reset --hard origin/dev
```

**PR says "out-of-date" or has conflicts**
```bash
git fetch origin
git switch <your-branch>
git rebase origin/dev
git push --force-with-lease
```

**Lint/format issues at commit time**
```bash
uvx ruff check . --fix
uvx ruff format .
```

---

## Quick reference

| What | Where / How |
|---|---|
| Repo | `/opt/spotops` |
| Live DB | `/srv/spotops/db/production.db` |
| Container | `spotops-spotops-1` (service `spotops`) |
| Env file | `/opt/spotops/.env` |
| HTTP | `http://localhost:8000` (host-only; tailnet via Tailscale) |
| Restart after code change | `docker compose up -d --build spotops` |
| Logs | `docker compose logs -f spotops` |
| Shell into container | `docker exec -it spotops-spotops-1 bash` |
| Health check | `curl -sf http://localhost:8000/health/` |
| Migrations | `sql/migrations/` |
| Routes | `src/web/routes/` |
| Services | `src/services/` |
| Templates | `src/web/templates/` |
| Lint | `uvx ruff check . && uvx ruff format .` |
| LLM cheat sheet | `.claude/CLAUDE.md` |
| LLM corrections | `.claude/tasks/lessons.md` |

---

## Network / access

- The container port is bound to `127.0.0.1:8000` only. External access is via Tailscale (host's tailnet name/IP).
- VS Code Remote-SSH against the host works the same as any other Linux box; SSH over Tailscale and open `/opt/spotops`.
- Auth model: see [ARCHITECTURE.md](ARCHITECTURE.md) for the Tailscale `whois` flow.
