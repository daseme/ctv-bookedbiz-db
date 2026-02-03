

---

# CTV BookedBiz DB — Production Architecture & Changes (Feb 2026)

## 1. High-level outcome

* The application is now served **only** by a single, explicit production service:

  * **`ctv-bookedbiz-db.service`**
* The legacy Flask dev service is **fully disabled and masked**.
* Python dependencies, permissions, and runtime paths are now aligned with production expectations.
* Port ownership is unambiguous.

---

## 2. Runtime model (current state)

### Network

* **Port 8000**: production web service

  * Served by `uvicorn`
  * Bound to `0.0.0.0:8000`
* **Port 5100**: not in use (reserved for future dev if needed)

### Process

```text
uvicorn
└─ ctv-bookedbiz-db.service
   └─ src.web.asgi:app
```

---

## 3. Systemd services

### Production service (active)

**`/etc/systemd/system/ctv-bookedbiz-db.service`**

* User: `ctvbooked`
* Group: `ctvbooked`
* Working dir: `/opt/apps/ctv-bookedbiz-db`
* Virtualenv: `/opt/venvs/ctv-bookedbiz-db`
* Entrypoint:

  ```bash
  uvicorn src.web.asgi:app --host 0.0.0.0 --port 8000
  ```
* Status:

  * Enabled
  * Running
  * Restart policy active

### Legacy service (retired)

**`flaskapp.service`**

* Old Flask dev runner
* Required `.venv` and `runserver.sh`
* Incompatible with new dependency model
* **Now masked** (cannot be started accidentally)

Masking rationale:

* Preserves history
* Prevents accidental enable/start
* Reversible if needed

---

## 4. Filesystem layout (authoritative)

### Application code

```
/opt/apps/ctv-bookedbiz-db
```

* Owned by: `ctvbooked:apps-deploy`
* Setgid enabled (`2775`)
* Group-readable by deploy users
* No in-place virtualenvs

### Virtual environment

```
/opt/venvs/ctv-bookedbiz-db
```

* Owned by: `ctvbooked`
* Managed with **uv**
* Used exclusively by systemd service

### Data

```
/var/lib/ctv-bookedbiz-db/
├── production.db
└── processed/
```

* Owned by: `ctvbooked`
* Not writable by dev users
* Configured via env file

---

## 5. Environment configuration

**`/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env`**

```env
DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db
DATA_PATH=/var/lib/ctv-bookedbiz-db/processed
```

* Loaded by systemd (`EnvironmentFile=`)
* Not implicitly available in shell sessions
* Verified readable and usable by `ctvbooked`

---

## 6. Python dependency management

### Tooling

* **uv** is the single source of truth
* `pyproject.toml` defines dependencies
* `uv.lock` is committed and authoritative

### Recent fix

* `flask-login` was imported in code but missing from `pyproject.toml`
* Added explicitly:

  ```toml
  "flask-login==0.6.3"
  ```
* `uv lock` regenerated successfully
* Verified import under production venv

---

## 7. Permissions & groups

### Groups

* `apps-deploy`: developers (daseme, jellee26)
* `ctvbooked`: runtime user

### Policy

* Devs:

  * Read code
  * Edit code (group write)
  * Cannot write production data
* Runtime:

  * Full access to venv + data
  * No reliance on dev home directories

---

## 8. Application initialization

* Entry via `src.web.asgi`
* Factory pattern (`create_app`)
* Blueprints registered once

  * Duplicate `reports` registration removed
* Service container initialized at startup
* Verified clean import under systemd user

---

## 9. Verification checklist (current)

* `ss -lptn` → only uvicorn owns port 8000
* `systemctl status ctv-bookedbiz-db` → active/running
* `curl http://127.0.0.1:8000` → 302 → `/reports/`
* No `.venv` under `/opt/apps`
* Legacy service cannot start

---

## 10. Operating rules going forward

1. **No dev work in `/opt/apps/ctv-bookedbiz-db`**

   * Use per-user dev checkouts
2. **All dependency changes go through `pyproject.toml`**

   * Followed by `uv lock`
3. **Only `ctv-bookedbiz-db.service` may bind port 8000**
4. **Production data lives only under `/var/lib/ctv-bookedbiz-db`**
5. **Never revive `flaskapp.service`**

   * If needed for reference: unmask explicitly and rename

---


