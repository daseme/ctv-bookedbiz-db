# Developer Guide — spotops (Docker on /opt/spotops)

How the app is deployed, how to make changes, and how to promote them. The runtime is a Docker Compose stack on the host `spotops` (`/opt/spotops`); the live SQLite database is a host volume at `/srv/spotops/db/production.db`.

---

## Overview

- **Repo / working dir**: `/opt/spotops`
- **Container**: `spotops-spotops-1` (compose service `spotops`)
- **Bind**: `127.0.0.1:8000` on the host (reverse-proxied / accessed over Tailscale)
- **Live DB**: `/srv/spotops/db/production.db` (~1.4 GB SQLite, mounted into the container at the same path; container reads it via `DATABASE_PATH=/srv/spotops/db/production.db`)
- **Processed data**: `/srv/spotops/processed` (host) → same path in container
- **App data dir**: `/srv/spotops/data` (host) → `/app/data` (container)
- **Env file**: `/opt/spotops/.env` (loaded by compose)
- **Network**: everything sits behind Tailscale; the public internet does not reach this host
- **Backups**: Litestream → Backblaze B2 + a nightly Dropbox snapshot (see `docs/docker-setup.md`, `docs/GUIDE-failover-failback.md`)

The container entrypoint reads `APP_MODE`:
- `replica_readonly` (default) — writes are blocked at the web/API layer
- `failover_primary` — writes allowed; used when this host has taken over from the prior primary

---

## Quick start (day-to-day loop)

```bash
# 1) sync dev and branch (run from anywhere; not necessarily on the server)
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

To exercise your branch on the live server:

```bash
cd /opt/spotops
git fetch origin
git switch <your-branch>
docker compose up -d --build spotops    # rebuild required for Python changes
docker compose logs -f spotops          # tail until the app reports ready
curl -sf http://localhost:8000/health/ && echo OK
```

When validated, open PR **dev → main** and **Squash merge**.

> `docker compose restart spotops` is only correct when you've changed config/env, not Python. For code changes you must rebuild.

---

## Restart / rebuild rules

| Change | Command |
|---|---|
| Python source | `docker compose up -d --build spotops` |
| Templates / static / SQL files (already mounted as code is) | rebuild as well — they're baked into the image at build |
| `.env` / compose env vars | `docker compose up -d spotops` (recreates container with new env) |
| `docker-compose.yml` | `docker compose up -d` |
| Just want to bounce a healthy container | `docker compose restart spotops` |

---

## Container lifecycle commands

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

# Tear down (use with care; preserves volumes/data)
docker compose down
```

---

## Database access

The live DB lives on the host at `/srv/spotops/db/production.db` and is mounted into the container at the same path. Either side can read it.

```bash
# From the host
sqlite3 /srv/spotops/db/production.db "SELECT COUNT(*) FROM spots;"

# From inside the container
docker exec -it spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  "SELECT COUNT(*) FROM spots;"
```

**Safe snapshot while the app is running** (uses SQLite's online backup; safer than `cp` on a hot DB):

```bash
sqlite3 /srv/spotops/db/production.db \
  ".backup '/srv/spotops/db/snap-$(date +%F_%H%M).sqlite3'"
```

For a destructive restore (e.g., from Backblaze via Litestream), see `docs/docker-setup.md` §11 and `docs/GUIDE-failover-failback.md`.

> **Empty skeleton trap**: the repo ships a 4 KB `data/database/production.db` placeholder. Don't point tests or scripts at that — set `DB_PATH` / `DATABASE_PATH` explicitly to `/srv/spotops/db/production.db` for any operation that should hit real data.

---

## Where things live

```
/opt/spotops/                  # repo, compose file, .env
├── docker-compose.yml
├── Dockerfile
├── .env                        # env_file: loaded by compose
├── src/                        # app code (rebuild image to pick up changes)
├── scripts/                    # ops scripts (run on host or via docker exec)
├── sql/migrations/             # schema changes
└── docs/                       # this file lives here

/srv/spotops/                  # host-side persistent data
├── db/
│   ├── production.db           # the live SQLite DB
│   └── snap-*.sqlite3          # local snapshots
├── processed/                  # processed import artifacts
└── data/                       # mounted to /app/data inside the container
```

---

## Making schema changes

1. Add a new SQL file under `sql/migrations/`.
2. Apply it against the live DB (the app does not auto-migrate):
   ```bash
   sqlite3 /srv/spotops/db/production.db < sql/migrations/<your-file>.sql
   ```
3. Verify with a quick `PRAGMA table_info(<table>)` or equivalent.
4. Restart the container if the app caches schema in memory:
   ```bash
   docker compose restart spotops
   ```

---

## Verifying the app is healthy

```bash
docker compose -f /opt/spotops/docker-compose.yml ps
curl -sf http://localhost:8000/health/ && echo OK
docker compose -f /opt/spotops/docker-compose.yml logs --tail=100 spotops
```

The base template footer also shows the most recent successful import timestamp; if it's stale (>24h), there's a yellow warning.

---

## Troubleshooting

**Container won't start**
```bash
docker compose -f /opt/spotops/docker-compose.yml ps
docker compose -f /opt/spotops/docker-compose.yml logs --tail=200 spotops
```

**App is up but serving stale code**
You probably ran `restart` instead of `up -d --build`. Rebuild:
```bash
cd /opt/spotops && docker compose up -d --build spotops
```

**Wrong database in use**
`create_app()` defaults `DB_PATH` to `data/database/production.db` (an empty 4 KB skeleton in the repo) when nothing else sets it. Inside the container, the env file pins `DATABASE_PATH=/srv/spotops/db/production.db`. If a host-side script seems to be hitting the wrong DB, check `DB_PATH` / `DATABASE_PATH` in its shell and set explicitly.

**Port 8000 already in use**
```bash
ss -ltnp | grep ':8000'
# kill the offending process or stop the conflicting container
docker compose -f /opt/spotops/docker-compose.yml restart spotops
```

**`docker compose` says "no configuration file"**
You're not in `/opt/spotops`. Either `cd /opt/spotops` or pass `-f /opt/spotops/docker-compose.yml`.

---

## Branching & merge rules

- Feature branches: `feat/<owner>/<slug>` (kebab-case)
- PRs into `dev`, then `dev` → `main` via **Squash and merge**
- Don't push directly to `main` (branch-protected)
- Rebase, don't merge, when bringing your branch up to date:
  ```bash
  git fetch origin
  git rebase origin/dev
  git push --force-with-lease
  ```

---

## Network / access

Everything sits behind Tailscale. You reach the app over the host's tailnet name. There is no public ingress by design — the Docker port is bound to `127.0.0.1`, so external clients only get in if they're on the tailnet (and a reverse proxy / Tailscale serve fronting `:8000` is configured).

VS Code Remote-SSH against the host works the same as any other Linux box; just SSH over Tailscale and open `/opt/spotops`.
