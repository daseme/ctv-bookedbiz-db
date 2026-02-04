# Raspberry Pi Development Workflow (pi-ctv)

## Overview

This document describes how the **CTV BookedBiz DB** web app is deployed and operated on **pi-ctv**, and how developers collaborate safely (code vs data vs service ownership).

**Key idea:**  
- **Production code** lives in `/opt/apps/ctv-bookedbiz-db` (shared read/write for deploy group).  
- **Production runtime** runs as `ctvbooked` via **systemd + uvicorn** on port 8000.  
- **Production data** lives under `/var/lib/ctv-bookedbiz-db` (owned by `ctvbooked`).  
- **Development** runs from user home directories on port 5100.  
- The old `flaskapp.service` is **retired** and **masked**.

---

## System Architecture

### Infrastructure
- **Host**: Raspberry Pi 5 (ARM64) — `pi-ctv`
- **OS**: Debian GNU/Linux
- **Network**: Tailscale VPN
- **Repo**: GitHub `daseme/ctv-bookedbiz-db`
- **Database**: SQLite
- **Web server**: `uvicorn` (serving `src.web.asgi:app`)
- **Service manager**: systemd

### Port Allocation

| Port | Environment | Service | Owner |
|------|-------------|---------|-------|
| 8000 | Production | `ctv-bookedbiz-db.service` | system (ctvbooked) |
| 5100 | Development | `spotops-dev.service` | user (daseme) |

### Authoritative Paths

#### Production
- **App code**: `/opt/apps/ctv-bookedbiz-db`
- **Venv**: `/opt/venvs/ctv-bookedbiz-db`
- **Database**: `/var/lib/ctv-bookedbiz-db/production.db`
- **Data**: `/var/lib/ctv-bookedbiz-db/processed`
- **Env file**: `/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env`

#### Development
- **App code**: `~/dev/ctv-bookedbiz-db`
- **Venv**: `~/dev/ctv-bookedbiz-db/.venv`
- **Database**: `~/dev/ctv-bookedbiz-db/.data/dev.db`
- **Data**: `~/dev/ctv-bookedbiz-db/.data/processed`
- **Logs**: `~/dev/logs/ctv-dev-uvicorn.out`

---

## Team Members / Accounts

- **daseme**: primary maintainer, SSH access, can administer services
- **jellee26**: collaborator (requires `apps-deploy` group membership)
- **ctvbooked**: production runtime user (no login)

> Group policy is enforced via Linux groups and permissions; verify with `id <user>`.

---

## Directory & Permission Model

### Code directory (`/opt/apps/ctv-bookedbiz-db`)
- Owned by `ctvbooked:apps-deploy`
- Permissions: `2775` (setgid so new files inherit `apps-deploy`)

**Validate:**
```bash
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db/pyproject.toml
```

### Data directory (`/var/lib/ctv-bookedbiz-db`)
- Owned by `ctvbooked:ctvbooked`
- Production DB is not a collaborative working file

**Validate:**
```bash
sudo ls -ld /var/lib/ctv-bookedbiz-db /var/lib/ctv-bookedbiz-db/processed
sudo ls -l /var/lib/ctv-bookedbiz-db/production.db
```

---

## Production Service

### Service: `ctv-bookedbiz-db.service`

**File:** `/etc/systemd/system/ctv-bookedbiz-db.service`  
**Runs as:** `ctvbooked`  
**Port:** 8000  
**Exec:** `/opt/venvs/ctv-bookedbiz-db/bin/uvicorn src.web.asgi:app --host 0.0.0.0 --port 8000`

### Control commands

```bash
# Status
sudo systemctl status ctv-bookedbiz-db --no-pager

# Restart (after code deploy)
sudo systemctl restart ctv-bookedbiz-db

# Logs (live)
sudo journalctl -u ctv-bookedbiz-db -f

# Logs (last 100 lines)
sudo journalctl -u ctv-bookedbiz-db -n 100 --no-pager
```

### Verify running

```bash
sudo ss -lptn | egrep ':8000'
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/
# Expected: 302
```

---

## Development Server

### Purpose
Isolated dev instance for testing changes before production deployment. Uses separate database, data paths, and port.

### Option A: Manual Start (quick testing)

```bash
cd ~/dev/ctv-bookedbiz-db
source .venv/bin/activate
export ENVIRONMENT=development
export PORT=5100
export DB_PATH=/home/daseme/dev/ctv-bookedbiz-db/.data/dev.db
export DATA_PATH=/home/daseme/dev/ctv-bookedbiz-db/.data/processed
mkdir -p "$DATA_PATH"

> ~/dev/logs/ctv-dev-uvicorn.out
nohup uvicorn src.web.asgi:app --host 0.0.0.0 --port "$PORT" \
  > ~/dev/logs/ctv-dev-uvicorn.out 2>&1 &
```

**Verify:**
```bash
ss -lptn 'sport = :5100'
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:5100/
```

**Stop:**
```bash
pkill -f "uvicorn src.web.asgi:app.*--port 5100"
```

**View logs:**
```bash
tail -f ~/dev/logs/ctv-dev-uvicorn.out
```

### Option B: systemd User Service (recommended)

Persistent dev server management without orphan processes.

#### Install service

```bash
mkdir -p ~/.config/systemd/user
mkdir -p ~/dev/logs

cat > ~/.config/systemd/user/spotops-dev.service << 'EOF'
[Unit]
Description=SpotOps Dev Server (port 5100)
After=network.target

[Service]
Type=simple
WorkingDirectory=/home/daseme/dev/ctv-bookedbiz-db
Environment=ENVIRONMENT=development
Environment=PORT=5100
Environment=DB_PATH=/home/daseme/dev/ctv-bookedbiz-db/.data/dev.db
Environment=DATA_PATH=/home/daseme/dev/ctv-bookedbiz-db/.data/processed
ExecStartPre=/bin/mkdir -p /home/daseme/dev/ctv-bookedbiz-db/.data/processed
ExecStart=/home/daseme/dev/ctv-bookedbiz-db/.venv/bin/uvicorn src.web.asgi:app --host 0.0.0.0 --port 5100
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable spotops-dev
```

#### Enable linger (keeps service running after logout)

```bash
sudo loginctl enable-linger daseme
```

Without linger, user services stop when you disconnect SSH.

#### Control commands

```bash
# Start
systemctl --user start spotops-dev

# Stop
systemctl --user stop spotops-dev

# Restart (after code changes)
systemctl --user restart spotops-dev

# Status
systemctl --user status spotops-dev

# Logs (live)
journalctl --user -u spotops-dev -f

# Logs (last 100 lines)
journalctl --user -u spotops-dev -n 100 --no-pager
```

#### Verify

```bash
systemctl --user is-active spotops-dev
ss -lptn 'sport = :5100'
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:5100/
```

#### Uninstall (if needed)

```bash
systemctl --user stop spotops-dev
systemctl --user disable spotops-dev
rm ~/.config/systemd/user/spotops-dev.service
systemctl --user daemon-reload
```

---

## Legacy Service (Retired)

### Service: `flaskapp.service`

Old Flask dev runner using `.venv` and `runserver.sh`. Incompatible with current dependency model.

**State:** masked (cannot be started)

```bash
sudo systemctl status flaskapp.service --no-pager
sudo systemctl is-enabled flaskapp.service || true
```

> Never revive this service. If needed for reference, unmask explicitly and rename.

---

## Dependency Management

### Tooling
- **uv** is the single source of truth
- `pyproject.toml` defines dependencies
- `uv.lock` is committed and authoritative

### Verify runtime venv has dependencies

```bash
# Production
sudo -u ctvbooked -H /opt/venvs/ctv-bookedbiz-db/bin/python -c "import flask_login; print('ok')"

# Development
~/dev/ctv-bookedbiz-db/.venv/bin/python -c "import flask_login; print('ok')"
```

### Adding dependencies

```bash
cd /opt/apps/ctv-bookedbiz-db
# Edit pyproject.toml
uv lock
uv sync --frozen  # in production venv context
sudo systemctl restart ctv-bookedbiz-db
```

---

## Production Environment Variables

**File:** `/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env`

```env
DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db
DATA_PATH=/var/lib/ctv-bookedbiz-db/processed
```

**Show key vars:**
```bash
sudo egrep -n '^(DATABASE_PATH|DATA_PATH|PORT)=' /etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env
```

**Validate DB access as ctvbooked:**
```bash
sudo -u ctvbooked -H bash -lc '
set -a; source /etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env; set +a
python3 -c "import os,sqlite3; sqlite3.connect(os.environ[\"DATABASE_PATH\"]).execute(\"select 1\"); print(\"sqlite ok\")"
'
```

---

## Daily Developer Workflow

### Connect (Tailscale SSH)

```bash
ssh daseme@pi-ctv
```

### Development cycle

```bash
# Pull latest to dev checkout
cd ~/dev/ctv-bookedbiz-db
git pull

# Restart dev server
systemctl --user restart spotops-dev

# Watch logs
journalctl --user -u spotops-dev -f

# Test in browser
# http://pi-ctv:5100/
```

### Deploy to production

```bash
# Pull to production code path
cd /opt/apps/ctv-bookedbiz-db
git pull

# Restart production service
sudo systemctl restart ctv-bookedbiz-db
sudo systemctl status ctv-bookedbiz-db --no-pager
```

---

## File Transfer

### Windows → Pi (PowerShell)

```powershell
scp "C:\Path\To\File.xlsx" daseme@pi-ctv:/home/daseme/dev/ctv-bookedbiz-db/.data/raw/
```

### Pi → Windows (Tailscale)

```bash
tailscale file cp "./file.xlsx" <windows-device-name>:
```

---

## VS Code Remote Development

### Remote-SSH (recommended)

- SSH into pi-ctv, open folder:
  - Production: `/opt/apps/ctv-bookedbiz-db`
  - Development: `~/dev/ctv-bookedbiz-db`
- Python interpreter:
  - Production: `/opt/venvs/ctv-bookedbiz-db/bin/python`
  - Development: `~/dev/ctv-bookedbiz-db/.venv/bin/python`

### VS Code Tunnel

User-level systemd service at `~/.config/systemd/user/code-tunnel.service`

```bash
# Status
systemctl --user status code-tunnel

# Restart
systemctl --user restart code-tunnel

# Logs
journalctl --user -u code-tunnel -f
```

In VS Code: Remote Explorer → Tunnels → connect to `raspberrypi`

---

## Troubleshooting

### Production not reachable on 8000

```bash
sudo systemctl status ctv-bookedbiz-db --no-pager
sudo journalctl -u ctv-bookedbiz-db -n 200 --no-pager
sudo ss -lptn | egrep ':8000'
curl -I http://127.0.0.1:8000/
```

### Dev not reachable on 5100

```bash
systemctl --user status spotops-dev
journalctl --user -u spotops-dev -n 100 --no-pager
ss -lptn 'sport = :5100'
curl -I http://127.0.0.1:5100/
```

### Port already in use

```bash
# Find what's holding the port
sudo lsof -i :5100
# Kill it
kill -9 <PID>
```

### Dependency import error

```bash
# Production
sudo -u ctvbooked -H /opt/venvs/ctv-bookedbiz-db/bin/python -c "import <module>"

# Development
~/dev/ctv-bookedbiz-db/.venv/bin/python -c "import <module>"
```

### Permission errors editing repo files

```bash
stat -c "%U:%G %a %n" /opt/apps/ctv-bookedbiz-db
id
# Ensure user is in apps-deploy group
```

---

## Quick Reference

### Service Control

| Environment | Start | Stop | Restart | Status |
|-------------|-------|------|---------|--------|
| Production | `sudo systemctl start ctv-bookedbiz-db` | `sudo systemctl stop ctv-bookedbiz-db` | `sudo systemctl restart ctv-bookedbiz-db` | `sudo systemctl status ctv-bookedbiz-db` |
| Development | `systemctl --user start spotops-dev` | `systemctl --user stop spotops-dev` | `systemctl --user restart spotops-dev` | `systemctl --user status spotops-dev` |

### Logs

```bash
# Production
sudo journalctl -u ctv-bookedbiz-db -f

# Development
journalctl --user -u spotops-dev -f
```

### Health Check

```bash
# Production
sudo ss -lptn | egrep ':8000' && curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/

# Development
ss -lptn 'sport = :5100' && curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:5100/
```

### URLs

| Environment | URL |
|-------------|-----|
| Production | `http://pi-ctv:8000/` |
| Development | `http://pi-ctv:5100/` |
