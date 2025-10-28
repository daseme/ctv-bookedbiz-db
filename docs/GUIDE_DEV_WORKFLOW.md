````markdown
# Developer Guide — ctv-bookedbiz-db (Raspberry Pi)

Clear workflow for developing on the Pi, viewing the **dev server**, and promoting changes **feat/* → dev → main**. Includes baby-step guides and troubleshooting.

---

## Overview

- Repo: `/opt/apps/ctv-bookedbiz-db`
- Python env: `.venv` via `uv`
- Dev server (systemd): **:5100** (`ctv-dev.service`)
- Databases:
  - **Prod**: `data/database/production.db`
  - **Dev**:  `data/database/production_dev.db` (copy of prod)
- Env files (kept **out of git**):
  - `.env.dev`  → `APP_ENV=dev`,  `DB_PATH=data/database/production_dev.db`
  - `.env.prod` → `APP_ENV=prod`, `DB_PATH=data/database/production.db`

---

## Quick start (day-to-day loop)

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
````

After your PR is merged into `dev`:

```bash
git switch dev && git pull --ff-only
systemctl --user restart ctv-dev.service
curl -sf http://localhost:5100/health/ && echo DEV_OK
```

When validated on dev, open PR **dev → main** and **Squash merge**.

---

## How to view the dev server

* Browser: `http://<pi-ip>:5100`
* Health:

  ```bash
  curl -sf http://localhost:5100/health/ && echo DEV_OK
  ```

Optional local run (don’t collide with :5100):

```bash
uv run --env-file .env.dev \
  flask --app src.web.app:create_app run --debug -h 0.0.0.0 -p 5101
# browse http://<pi-ip>:5101
```

---

## Baby-step: Start a feature branch

1. Update `dev`:

```bash
git switch dev
git pull --ff-only
```

2. Branch from `dev`:

```bash
git switch -c feat/<yourname>/<slug>
# ex: feat/jenna/sector-filter-fix
```

3. Work and test:

* Systemd dev uses the **dev DB** on **:5100**.
* Optional ad-hoc run on **:5101** (above) if you want a separate instance.

4. Lint/format and commit small units:

```bash
uvx ruff check . ; uvx ruff format .
git add -A
git commit -m "feat: <one clear change>"
git push -u origin HEAD
```

5. Open PR: **base = dev**, **compare = your feat/** branch.

---

## Baby-step: Promote `dev` → `main`

1. Ensure `dev` is current:

```bash
git switch dev
git fetch origin
git pull --ff-only
```

2. Open PR on GitHub:

* **base = main**, **compare = dev**
* Title: short and clear (e.g., `docs: add GUIDE_DEV_WORKFLOW`)
* Create PR

3. Merge with **Squash and merge** (keeps main history tidy).

4. If this box also runs prod, update `main` locally:

```bash
git switch main
git pull --ff-only
# restart prod service here if this machine serves prod
```

---

## Configuration reference

* DB path resolution in `src/config/settings.py` (priority):

  1. `DB_PATH`
  2. `DEV_DB_PATH` / `PROD_DB_PATH` (selected by `APP_ENV`)
  3. Defaults: `production_dev.db` for `dev/test`, `production.db` for `prod`

* Dev service unit: `~/.config/systemd/user/ctv-dev.service`

  ```ini
  [Service]
  WorkingDirectory=/opt/apps/ctv-bookedbiz-db
  EnvironmentFile=/opt/apps/ctv-bookedbiz-db/.env.dev
  ExecStart=/opt/apps/ctv-bookedbiz-db/.venv/bin/flask --app src.web.app:create_app run --debug -h 0.0.0.0 -p 5100
  Restart=always
  RestartSec=2
  # Recommended:
  Environment=PYTHONUNBUFFERED=1
  Environment=PYTHONPATH=/opt/apps/ctv-bookedbiz-db
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
s = get_settings(); print("ENV:", s.environment, "| DB:", s.database.db_path)
PY
# expect: ENV: dev | DB: data/database/production_dev.db

# Service health
systemctl --user status ctv-dev.service --no-pager
curl -sf http://localhost:5100/health/ && echo DEV_OK

# Ensure no hardcoded prod DB in runtime modules
grep -RIn "data/database/production\.db" src | sed -n '1,50p'
```

---

## VS Code branch sanity

* Check current branch in the status bar.
* Switch: **Ctrl+Shift+P** → “Git: Checkout to…”
* Verify from terminal:

  ```bash
  git branch --show-current
  git status
  ```

---

## Definitions (plain language)

* **`git pull --ff-only`**
  Update your current branch *only if* it can move forward cleanly to match the remote (no local divergence). Prevents accidental merge commits on the box.

  * If it fails:

    * Rebase local changes on the remote:

      ```bash
      git fetch origin
      git rebase origin/dev   # or origin/main, as appropriate
      git push --force-with-lease
      ```
    * Or, discard local changes and match remote (destructive):

      ```bash
      git fetch origin
      git reset --hard origin/dev
      ```

* **Squash merge**
  PR is merged as a single commit → keeps `main` history compact and readable.

* **Feature branch naming**
  `feat/<owner>/<slug>` in short, kebab-case (e.g., `feat/jenna/report-cleanup`).

---

## Database practices

* Dev DB is a copy of prod; selection is **env-driven** (`DB_PATH`).
* Refresh dev DB from prod (non-destructive to prod):

  ```bash
  systemctl --user stop ctv-dev.service
  sqlite3 data/database/production.db ".backup 'data/database/production_dev.db'"
  systemctl --user start ctv-dev.service
  ```
* Apply migrations/schema changes on **dev** first, then promote.

---

## Troubleshooting

**Port 5100 in use**

```bash
ss -ltnp | grep ':5100' || true
fuser -k 5100/tcp
systemctl --user restart ctv-dev.service
```

**Dev service won’t start**

```bash
journalctl --user -xeu ctv-dev.service --no-pager -n 100
/opt/apps/ctv-bookedbiz-db/.venv/bin/flask --version
```

If imports fail, add the `Environment=` lines shown in the unit, then:

```bash
systemctl --user daemon-reload
systemctl --user restart ctv-dev.service
```

**Wrong database in use**

```bash
sed -n '1,20p' .env.dev
uv run --env-file .env.dev python - <<'PY'
from src.config.settings import get_settings
print(get_settings().environment, get_settings().database.db_path)
PY
systemctl --user restart ctv-dev.service
```

**PR says “out-of-date” or conflicts**

```bash
git fetch origin
git switch dev
git rebase origin/main
git push --force-with-lease
# refresh PR, then Squash and merge
```

**Lint/format issues**

```bash
uvx ruff check . --fix
uvx ruff format .
```

---

## Safe operations

* Do **not** write to `production.db` during dev flows.
* Keep PRs small; rebase as needed.
* Protect `main` and `dev` in GitHub (require PRs + squash + CI).
* Back up prod DB periodically:

  ```bash
  mkdir -p data/database/backups
  sqlite3 data/database/production.db ".backup 'data/database/backups/prod-$(date +%F_%H%M).sqlite3'"
  ```

---

## Appendix: Useful commands

```bash
# restart dev service
systemctl --user restart ctv-dev.service

# tail logs
journalctl --user -u ctv-dev --no-pager -f

# ad-hoc dev server on 5101
uv run --env-file .env.dev flask --app src.web.app:create_app run --debug -h 0.0.0.0 -p 5101

# clean branch update
git fetch origin
git switch dev && git pull --ff-only
git switch -c feat/<owner>/<slug> origin/dev

# rebase feature on latest dev
git fetch origin
git rebase origin/dev
git push --force-with-lease
```

```
```
