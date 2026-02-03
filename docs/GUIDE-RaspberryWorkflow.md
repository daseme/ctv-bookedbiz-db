````markdown
# Raspberry Pi Development Workflow (pi-ctv)

## Overview
This document describes how the **CTV BookedBiz DB** web app is deployed and operated on **pi-ctv**, and how developers collaborate safely (code vs data vs service ownership).

**Key idea:**  
- **Code** lives in `/opt/apps/ctv-bookedbiz-db` (shared read/write for deploy group).  
- **Runtime (production)** runs as `ctvbooked` via **systemd + uvicorn**.  
- **Production data** lives under `/var/lib/ctv-bookedbiz-db` (owned by `ctvbooked`).  
- The old `flaskapp.service` is **retired** and **masked**.

---

## System Architecture

### Infrastructure
- **Host**: Raspberry Pi 5 (ARM64) — `pi-ctv`
- **OS**: Debian GNU/Linux
- **Network**: Tailscale VPN
- **Repo**: GitHub `daseme/ctv-bookedbiz-db`
- **Database**: SQLite
- **Prod web server**: `uvicorn` (serving `src.web.asgi:app`)
- **Service manager**: systemd

### Authoritative paths (current)
- **App code (repo working tree):**
  - `/opt/apps/ctv-bookedbiz-db`
- **Production venv (systemd uses this):**
  - `/opt/venvs/ctv-bookedbiz-db`
- **Production database + processed data:**
  - `/var/lib/ctv-bookedbiz-db/production.db`
  - `/var/lib/ctv-bookedbiz-db/processed`
- **Production env file:**
  - `/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env`

### Ports
- **8000**: production web service (`ctv-bookedbiz-db.service`)
- **5100**: not in use (reserved)

---

## Team Members / Accounts
- **daseme**: primary maintainer, SSH access, can administer services
- **jellee26**: collaborator (may require group membership to edit in `/opt/apps/...`)

> Group policy is enforced via Linux groups and permissions; do not assume group membership—verify with `id <user>`.

---

## Directory & Permission Model

### Code directory (`/opt/apps/ctv-bookedbiz-db`)
- Owned by `ctvbooked:apps-deploy`
- Permissions:
  - directory: `2775` (setgid so new files inherit `apps-deploy`)
  - files: group-readable; group-write enabled where needed for collaboration

**Validate:**
```bash
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db/uv.lock
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db/pyproject.toml
````

### Data directory (`/var/lib/ctv-bookedbiz-db`)

* Owned by `ctvbooked:ctvbooked`
* Production DB is not treated as a collaborative working file.

**Validate:**

```bash
sudo ls -ld /var/lib/ctv-bookedbiz-db /var/lib/ctv-bookedbiz-db/processed
sudo ls -l /var/lib/ctv-bookedbiz-db/production.db
```

---

## Production Service (Option A)

### Service: `ctv-bookedbiz-db.service`

**File:** `/etc/systemd/system/ctv-bookedbiz-db.service`
**Runs as:** `ctvbooked`
**Exec:** `/opt/venvs/ctv-bookedbiz-db/bin/uvicorn src.web.asgi:app --host 0.0.0.0 --port 8000`

**Status / logs:**

```bash
sudo systemctl status ctv-bookedbiz-db.service --no-pager
sudo journalctl -u ctv-bookedbiz-db.service -n 100 --no-pager
```

**Restart (after code deploy or config change):**

```bash
sudo systemctl restart ctv-bookedbiz-db.service
```

**Confirm port + basic HTTP behavior:**

```bash
sudo ss -lptn | egrep ':8000' || true
curl -I http://127.0.0.1:8000/ || true
# Expected: 302 Location: /reports/
```

---

## Legacy Service (Option B) — Retired

### Service: `flaskapp.service`

This service used `runserver.sh` and a repo-local `.venv`. It failed once `flask_login` was introduced (dependency drift) and is not the production path.

**State:** masked (cannot be started)

```bash
sudo systemctl status flaskapp.service --no-pager
sudo systemctl is-enabled flaskapp.service || true
```

---

## Dependency Management (Current)

### Source of truth

* `pyproject.toml` — declared dependencies
* `uv.lock` — resolved, pinned dependency set

### Recent fix applied

* `flask-login==0.6.3` was added to `pyproject.toml`
* `uv lock` was run successfully and `uv.lock` contains flask-login entries

**Verify:**

```bash
grep -n 'flask-login' pyproject.toml
grep -nEi 'name = "flask-login"|flask-login' uv.lock | head
```

### Verify runtime venv has the dependency

Run as `ctvbooked` using the production venv:

```bash
sudo -u ctvbooked -H bash -lc '
/opt/venvs/ctv-bookedbiz-db/bin/python -c "import flask_login; print(\"flask_login ok\")"
'
```

> Installing/updating packages in `/opt/venvs/ctv-bookedbiz-db` must be done in a controlled way (preferably as `ctvbooked`) to avoid root-owned files and permission traps.

---

## Production Environment Variables

### Env file

`/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env` contains:

* `DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db`
* `DATA_PATH=/var/lib/ctv-bookedbiz-db/processed`

**Show key vars:**

```bash
sudo egrep -n '^(DATABASE_PATH|DATA_PATH|PORT)=' /etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env || true
```

**Validate sqlite open as ctvbooked (with env loaded):**

```bash
sudo -u ctvbooked -H bash -lc '
set -e
set -a
source /etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env
set +a
python3 - <<PY
import os, sqlite3
db=os.environ["DATABASE_PATH"]
print("DATABASE_PATH=", db)
con=sqlite3.connect(db)
con.execute("select 1").fetchone()
con.close()
print("sqlite open ok")
print("DATA_PATH=", os.environ.get("DATA_PATH"))
PY
'
```

---

## Daily Developer Workflow (pi-ctv)

### Connect (Tailscale SSH)

```bash
ssh daseme@<tailscale-ip-of-pi-ctv>
# or
ssh jellee26@<tailscale-ip-of-pi-ctv>
```

### Pull latest code

```bash
cd /opt/apps/ctv-bookedbiz-db
git pull
```

### Restart production service after changes that affect runtime

```bash
sudo systemctl restart ctv-bookedbiz-db.service
sudo systemctl status ctv-bookedbiz-db.service --no-pager
```

### Watch logs during a test session

```bash
sudo journalctl -u ctv-bookedbiz-db.service -f
```

---

## Moving files (Windows ↔ Pi)

### Windows → Pi (PowerShell)

```powershell
scp "C:\Path\To\File.xlsx" daseme@raspberrypi:/opt/apps/ctv-bookedbiz-db/data/raw/File.xlsx
```

### Pi → Windows (Tailscale file send)

```bash
sudo tailscale file cp "./data/raw/daily/Commercial Log 250912.xlsx" <your-windows-device-name>:
```

---

## VS Code Remote Development

### Remote-SSH (recommended)

* Use SSH into pi-ctv, open folder `/opt/apps/ctv-bookedbiz-db`
* Use Python interpreter from the production venv if you want parity with runtime:

  * `/opt/venvs/ctv-bookedbiz-db/bin/python`

---

## VS Code Remote Tunnel (pi-ctv)

> This section describes a **user-level systemd service** named `code-tunnel.service`.
> It must exist under the **developer account** (usually `daseme`) at:
> `~/.config/systemd/user/code-tunnel.service`

### Status / logs

```bash
systemctl --user status code-tunnel.service
journalctl --user -u code-tunnel.service -n 50 --no-pager
journalctl --user -u code-tunnel.service -f
```

### Restart

```bash
systemctl --user restart code-tunnel.service
```

### Desktop usage

* In VS Code, Remote Explorer → **Tunnels** → connect to `raspberrypi`
* Open folder: `/opt/apps/ctv-bookedbiz-db`

---

## Troubleshooting

### App not reachable on 8000

```bash
sudo systemctl status ctv-bookedbiz-db.service --no-pager
sudo journalctl -u ctv-bookedbiz-db.service -n 200 --no-pager
sudo ss -lptn | egrep ':8000' || true
curl -I http://127.0.0.1:8000/ || true
```

### Dependency import error in production

Validate using production venv as `ctvbooked`:

```bash
sudo -u ctvbooked -H /opt/venvs/ctv-bookedbiz-db/bin/python -c "import flask_login"
```

### Permission errors editing repo files

Check ownership + mode:

```bash
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db/pyproject.toml
id
```

---

## Quick Reference

### Production service

```bash
sudo systemctl status ctv-bookedbiz-db.service --no-pager
sudo systemctl restart ctv-bookedbiz-db.service
sudo journalctl -u ctv-bookedbiz-db.service -n 100 --no-pager
sudo journalctl -u ctv-bookedbiz-db.service -f
```

### Verify running + reachable

```bash
sudo ss -lptn | egrep ':8000' || true
curl -I http://127.0.0.1:8000/ || true
```

### Env + DB sanity check

```bash
sudo egrep -n '^(DATABASE_PATH|DATA_PATH|PORT)=' /etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env || true
sudo -u ctvbooked -H bash -lc 'set -a; source /etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env; set +a; python3 -c "import os,sqlite3; sqlite3.connect(os.environ[\"DATABASE_PATH\"]).execute(\"select 1\"); print(\"sqlite ok\")"'
```

### VS Code tunnel

```bash
systemctl --user status code-tunnel.service
systemctl --user restart code-tunnel.service
journalctl --user -u code-tunnel.service -f
```

```
```
