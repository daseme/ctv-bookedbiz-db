# Developer Guide — ctv-bookedbiz-db (Raspberry Pi)

This guide covers day-to-day development on the Pi, how to view the **dev server**, and how to merge **feature → dev → main**. A troubleshooting section follows.

---

## Overview

* Repo: `/opt/apps/ctv-bookedbiz-db`
* Python env: `.venv` managed by `uv`
* Dev server: Flask reloader on **:5100** via `ctv-dev.service`
* DBs:

  * **Prod**: `data/database/production.db`
  * **Dev**:  `data/database/production_dev.db` (copy of prod)
* Env files:

  * `.env.dev` → `APP_ENV=dev`, `DB_PATH=data/database/production_dev.db`
  * `.env.prod` → `APP_ENV=prod`, `DB_PATH=data/database/production.db`

---

## Prerequisites (already set up)

```bash
# On the Pi
cd /opt/apps/ctv-bookedbiz-db
uv sync                           # creates .venv and installs deps
ls .env.dev .env.prod             # both should exist
systemctl --user status ctv-dev.service
```

If `ctv-dev.service` is not running:

```bash
sudo loginctl enable-linger $USER
systemctl --user daemon-reload
systemctl --user enable --now ctv-dev.service
```

---

## How to see the dev server

From a browser on your LAN/Tailscale:

```
http://<pi-ip>:5100
```

Health check:

```bash
curl -sf http://localhost:5100/health/ && echo DEV_OK
```

If the page isn’t reachable, see **Troubleshooting**.

---

## Day-to-day workflow

### 1) Sync and branch

```bash
cd /opt/apps/ctv-bookedbiz-db
git fetch origin

# Work branches are short-lived: feat/<owner>/<slug>
git switch -c feat/jenna/<slug> origin/dev
```

> Rule: All feature branches start from `origin/dev`.

### 2) Run locally (optional ad-hoc run instead of systemd)

```bash
# Uses dev DB and enables hot-reload
uv run --env-file .env.dev \
  flask --app src.web.app:create_app run --debug -h 0.0.0.0 -p 5101
# browse http://<pi-ip>:5101
```

Systemd dev stays on :5100. Don’t double-bind the same port.

### 3) Code standards

```bash
uvx ruff check .
uvx ruff format .
```

Keep commits small and focused.

### 4) Commit and push

```bash
git add -A
git commit -m "feat: <slug> ..."
git push -u origin HEAD
```

Open a PR: **feat/* → dev**. Require green checks and review.

### 5) Verify on dev server

After merging the feature PR into `dev`, pull and restart the dev service:

```bash
git switch dev && git pull --ff-only
systemctl --user restart ctv-dev.service

# Sanity
curl -sf http://localhost:5100/health/ && echo DEV_OK
```

Validate functionality against **dev DB**.

### 6) Promote dev → main (release)

When staging on dev is validated:

* Open PR: **dev → main**
* Require green checks and review
* Use **Squash merge**
* Tag release in GitHub if needed

If prod also runs on this Pi under a prod service, refresh it after merge:

```bash
git switch main && git pull --ff-only
# (start or restart prod service if used here)
# systemctl --user enable --now ctv-prod.service
```

---

## Database practices

* Dev database is a copy of prod (`production_dev.db`).
* Selection is **env-driven**; runtime reads `DB_PATH` from `.env.dev` or `.env.prod`.
* To refresh dev DB from prod (non-destructive to prod):

```bash
# STOP dev if you need a clean copy without live writes
systemctl --user stop ctv-dev.service

sqlite3 data/database/production.db \
  ".backup 'data/database/production_dev.db'"

systemctl --user start ctv-dev.service
```

* Migrations or schema changes must be applied **first to dev**, then promoted.

---

## Branching rules

* `main`: production, always releasable, protected.
* `dev`: integration/staging, protected.
* `feat/<owner>/<slug>`: short-lived; PR into `dev`.

**Merging policy**

* **Squash merge** only.
* Rebase your feature branch on `origin/dev` before merge when needed:

  ```bash
  git fetch origin
  git rebase origin/dev
  git push --force-with-lease
  ```

---

## Configuration reference

* `src/config/settings.py` chooses DB path via environment in this order:

  1. `DB_PATH` (if set)
  2. `DEV_DB_PATH` / `PROD_DB_PATH` (by `APP_ENV`)
  3. Defaults: `production_dev.db` for `dev/test`, `production.db` for `prod`

* `.env.dev`

  ```ini
  APP_ENV=dev
  DB_PATH=data/database/production_dev.db
  DEV_DB_PATH=data/database/production_dev.db
  PROD_DB_PATH=data/database/production.db
  ```

* Dev service unit: `~/.config/systemd/user/ctv-dev.service`

  ```ini
  [Service]
  WorkingDirectory=/opt/apps/ctv-bookedbiz-db
  EnvironmentFile=/opt/apps/ctv-bookedbiz-db/.env.dev
  ExecStart=/opt/apps/ctv-bookedbiz-db/.venv/bin/flask --app src.web.app:create_app run --debug -h 0.0.0.0 -p 5100
  Restart=always
  RestartSec=2
  ```

---

## Verification checklist

```bash
# Branch state
git branch -a
git status

# Env → DB mapping
uv run --env-file .env.dev python - <<'PY'
from src.config.settings import get_settings
s=get_settings(); print(s.environment, s.database.db_path)
PY
# expect: dev data/database/production_dev.db

# Service health
systemctl --user status ctv-dev.service --no-pager
curl -sf http://localhost:5100/health/ && echo DEV_OK

# No hardcoded prod paths in runtime modules
grep -RIn "data/database/production\.db" src | sed -n '1,50p'
```

---

## Troubleshooting

### Port 5100 in use

Symptoms: unit flaps with `Address already in use`.

```bash
ss -ltnp | grep ':5100' || true
fuser -k 5100/tcp
systemctl --user restart ctv-dev.service
```

If you need a parallel manual run, use `-p 5101`.

### Dev server won’t start under systemd

1. Inspect logs:

```bash
journalctl --user -u ctv-dev --no-pager -n 200
```

2. Ensure Flask binary exists and PYTHONPATH is correct:

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/flask --version
```

If imports fail, add to the unit:

```ini
# in [Service] section
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=/opt/apps/ctv-bookedbiz-db
```

Then:

```bash
systemctl --user daemon-reload
systemctl --user restart ctv-dev.service
```

### Wrong database is being used

Check env mapping:

```bash
sed -n '1,20p' .env.dev
uv run --env-file .env.dev python - <<'PY'
from src.config.settings import get_settings
print(get_settings().environment, get_settings().database.db_path)
PY
```

Confirm runtime module code reads `DB_PATH` (no hard-coded fallbacks). Restart dev:

```bash
systemctl --user restart ctv-dev.service
```

### Dev DB out of date vs prod

Re-copy from prod:

```bash
systemctl --user stop ctv-dev.service
sqlite3 data/database/production.db ".backup 'data/database/production_dev.db'"
systemctl --user start ctv-dev.service
```

### Health endpoint redirects

Flask may redirect `/health` → `/health/`. Use:

```bash
curl -sf http://localhost:5100/health/ && echo OK
```

### Linting or formatting fails

```bash
uvx ruff check .
uvx ruff format .
```

Fix issues and re-run.

---

## Safe operations

* Do not edit `production.db` in dev flows.
* Keep feature branches small; rebase before merge if diverged.
* Use PRs for **feat/* → dev** and **dev → main** with reviews and CI.
* Back up prod DB periodically:

  ```bash
  mkdir -p data/database/backups
  sqlite3 data/database/production.db ".backup 'data/database/backups/prod-$(date +%F_%H%M).sqlite3'"
  ```

---

## Appendix: Useful commands

```bash
# Restart dev server
systemctl --user restart ctv-dev.service

# Tail logs
journalctl --user -u ctv-dev --no-pager -f

# Run quick ad-hoc server on 5101
uv run --env-file .env.dev flask --app src.web.app:create_app run --debug -h 0.0.0.0 -p 5101

# Clean branch update
git fetch origin
git switch dev && git pull --ff-only
git switch -c feat/<owner>/<slug> origin/dev

# Rebase feature on latest dev
git fetch origin
git rebase origin/dev
git push --force-with-lease
```

---

Place this file at `docs/DEV_WORKFLOW.md` and keep it updated as processes evolve.
