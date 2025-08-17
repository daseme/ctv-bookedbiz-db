# Raspberry Pi Development Workflow

## Overview
This document outlines the collaborative development workflow for the CTV BookedBiz application running on a Raspberry Pi 5, with database sync via Dropbox and code collaboration via GitHub.

## System Architecture

### Infrastructure
- **Hardware**: Raspberry Pi 5 (ARM64)
- **OS**: Debian GNU/Linux with Python 3.11
- **Network**: Tailscale VPN for secure remote access
- **Code Repository**: GitHub (`daseme/ctv-bookedbiz-db`)
- **Database**: SQLite (production.db) synced via Dropbox
- **Development Environment**: VS Code Remote-SSH
- **Service Management**: systemd service for Flask app

### Directory Structure
```
/opt/apps/ctv-bookedbiz-db/
├── .venv/                    # Python virtual environment
├── .env                      # Environment variables (DO NOT COMMIT)
├── data/
│   └── database/
│       └── production.db     # Local copy of database
├── db_sync.py               # Database sync utility
├── requirements.txt         # Python dependencies
├── runserver.sh             # Flask app startup script
└── [your Flask app files]
```

## Team Members
- **daseme**: Primary developer with Pi admin access
- **jellee26**: Collaborative developer with ctv-dev group access

## Development Workflow

### 1. Daily Development Routine

#### Starting Work Session
```bash
# 1. Connect to Pi via Tailscale
ssh daseme@pi-tailscale-ip
# or
ssh jellee26@pi-tailscale-ip

# 2. Navigate to project directory
cd /opt/apps/ctv-bookedbiz-db

# 3. Activate Python environment
source .venv/bin/activate

# 4. Get latest code changes
git pull

# 5. Get latest database
python db_sync.py download

# 6. Check if Flask app service is running
sudo systemctl status flaskapp
```

#### During Development
```bash
# Check database status
python db_sync.py info

# Create backup before major changes
python db_sync.py backup

# Install new dependencies if needed
uv pip install package-name
uv pip freeze > requirements.txt

# Restart Flask app after code changes
sudo systemctl restart flaskapp
```

#### Ending Work Session
```bash
# 1. Save database changes to Dropbox
python db_sync.py upload

# 2. Commit and push code changes
git add .
git commit -m "Description of changes"
git push

# 3. Restart Flask app service to pick up changes
sudo systemctl restart flaskapp

# 4. Exit Pi
exit
```

### 2. Flask App Service Management

The Flask application runs as a systemd service that automatically starts on boot and restarts if it crashes.

#### Service Commands
```bash
# Check service status
sudo systemctl status flaskapp

# Start the service
sudo systemctl start flaskapp

# Stop the service
sudo systemctl stop flaskapp

# Restart the service (after code changes)
sudo systemctl restart flaskapp

# View recent logs
sudo journalctl -u flaskapp -n 50

# Follow logs in real-time
sudo journalctl -u flaskapp -f

# Enable service to start on boot (already configured)
sudo systemctl enable flaskapp

# Disable service from starting on boot
sudo systemctl disable flaskapp
```

#### Service Management Workflow
1. **After code changes**: Always restart the service with `sudo systemctl restart flaskapp`
2. **Debugging issues**: Check logs with `sudo journalctl -u flaskapp -n 50`
3. **Service runs automatically**: No need to manually start after Pi reboots
4. **Service auto-restarts**: If the app crashes, systemd will restart it automatically

# 3. Database Management — Server‑First with Nightly Dropbox Backup

**Principle:** We work on the **server copy** of the database and **do not** auto‑download from Dropbox. A nightly **systemd timer** uploads the current DB to Dropbox as an off‑site backup. Manual download is **only for recovery**.

---

## Components Added

- **Primary DB (authoritative)**
  - `/opt/apps/ctv-bookedbiz-db/data/database/production.db`

- **Python entrypoint (unchanged)**
  - `cli_db_sync.py` (supports `test | upload | backup | info | list-backups | list`)

- **Virtualenv (for systemd)**
  - `/opt/apps/ctv-bookedbiz-db/.venv` with `dropbox` + `python-dotenv`

- **Wrapper executable**
  - `/opt/apps/ctv-bookedbiz-db/bin/db_sync.sh`
    - Runs the upload via the venv python
    - Uses `flock` to prevent overlap
    - Sends failure notifications if configured (`NTFY_TOPIC` / `SLACK_WEBHOOK_URL`)

- **Secrets (root-only)**
  - `/etc/ctv-db-sync.env`
    - `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`
    - Optional: `DROPBOX_DB_PATH`, `NTFY_TOPIC`, `SLACK_WEBHOOK_URL`
    - Permissions: `root:root`, `600`

- **systemd units**
  - `/etc/systemd/system/ctv-db-sync.service` (runs the wrapper once)
  - `/etc/systemd/system/ctv-db-sync.timer` (schedules daily run, e.g., `02:05` local)

- **Logs**
  - `/var/log/ctv-db-sync/sync.log` (append)
  - Optional logrotate at `/etc/logrotate.d/ctv-db-sync`

- **Permissions model**
  - App tree: `daseme:ctvapps`, mode `2775` (setgid) so `jellee26` can collaborate
  - Secrets remain root-only

---

## Daily Backup Flow (Automated)

1. **Timer fires daily** (e.g., `02:05`) → runs `db_sync.sh`.
2. Wrapper starts venv python and calls:
   ```bash
   python cli_db_sync.py upload
   ```
3. Script uploads `/opt/apps/ctv-bookedbiz-db/data/database/production.db` to Dropbox (default path `/database.db`) using **chunked upload** for large files.
4. Success/failure is logged to `/var/log/ctv-db-sync/sync.log`. Failures can notify via ntfy/Slack if configured.

**Service/timer control**
```bash
# One-off run now
sudo systemctl start ctv-db-sync.service

# Enable and show timer
sudo systemctl enable --now ctv-db-sync.timer
systemctl list-timers | grep ctv-db-sync

# Status + recent logs
sudo systemctl status ctv-db-sync.service -l --no-pager
tail -n 200 /var/log/ctv-db-sync/sync.log
```

---

## Manual Commands (When Needed)

```bash
# Test Dropbox auth/account
python cli_db_sync.py test

# Upload immediately (e.g., after major changes)
python cli_db_sync.py upload

# Create a timestamped backup in Dropbox (/backups/)
python cli_db_sync.py backup
# or with a custom name
python cli_db_sync.py backup pre_migration_backup.db

# Inspect the tracked DB in Dropbox
python cli_db_sync.py info

# List backups under /backups
python cli_db_sync.py list-backups

# Explore a Dropbox folder (default root)
python cli_db_sync.py list /backups
```

> **Note:** We **do not** auto-download daily. Use `download` **only for recovery**:
```bash
# Restore from a specific Dropbox file (manual recovery step)
python cli_db_sync.py download
# (then stop dependent services, replace local DB, and restart services)
```

---

## Operational Notes

- **Single-writer discipline:** Treat `production.db` as the single source of truth; coordinate schema changes.
- **Before schema migrations:** run `python cli_db_sync.py backup`.
- **After heavy imports/changes:** optionally force an early upload (`python cli_db_sync.py upload`).

**Health checks**
```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -c "import dropbox; print('dropbox OK')"
ls -lh /opt/apps/ctv-bookedbiz-db/data/database/production.db
```

**Troubleshooting quick hits**
- `ModuleNotFoundError: dropbox` → (re)install in venv:
  ```bash
  /opt/apps/ctv-bookedbiz-db/.venv/bin/pip install -U dropbox python-dotenv
  ```
- `invalid_access_token` → refresh token/app key/secret mismatch in `/etc/ctv-db-sync.env`. Re-issue token and update file.
- No logs → verify `ExecStart` uses shell redirection to `/var/log/ctv-db-sync/sync.log`.

---

## systemd Unit Snippets (Reference)

**Service** (key lines):
```ini
[Unit]
Description=CTV DB -> Dropbox daily upload
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=daseme
Group=ctvapps
WorkingDirectory=/opt/apps/ctv-bookedbiz-db
EnvironmentFile=/etc/ctv-db-sync.env
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/daseme/.local/bin"
ExecStart=/usr/bin/env bash -c '/opt/apps/ctv-bookedbiz-db/bin/db_sync.sh >>/var/log/ctv-db-sync/sync.log 2>&1'
TimeoutStartSec=600

ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
NoNewPrivileges=yes
ReadWritePaths=/opt/apps/ctv-bookedbiz-db /tmp /var/log/ctv-db-sync
LockPersonality=yes
RestrictSUIDSGID=yes
RestrictNamespaces=yes
CapabilityBoundingSet=
```

**Timer** (daily at 02:05):
```ini
[Unit]
Description=Run CTV DB upload daily

[Timer]
OnCalendar=*-*-* 02:05:00
RandomizedDelaySec=5m
Persistent=true

[Install]
WantedBy=timers.target
```

**Logrotate** (optional):
```conf
/var/log/ctv-db-sync/sync.log {
    weekly
    rotate 8
    compress
    missingok
    notifempty
    create 0664 daseme ctvapps
    su root root
    sharedscripts
    postrotate
        :
    endscript
}
```

---

## Policy: Server‑First Workflow

1. **Authoritative copy lives on the server** at `/opt/apps/ctv-bookedbiz-db/data/database/production.db`.
2. **Nightly timed upload** backs up to Dropbox (off‑site safety).
3. **No daily downloads** from Dropbox.
4. **Download is for recovery only**, or for cloning to a fresh environment.


### 4. VS Code Remote Development

#### Initial Setup (One-time per developer)
1. Install VS Code extensions:
   - Remote - SSH
   - Remote Development (extension pack)
   - Python
   - Python Debugger

2. Set up SSH config (`~/.ssh/config`):
   ```
   Host pi-ctv
       HostName your-pi-tailscale-ip
       User your-username
       Port 22
       IdentityFile ~/.ssh/id_ed25519
       ServerAliveInterval 60
       ServerAliveCountMax 3
   ```

#### Daily VS Code Usage
1. **Connect**: Ctrl+Shift+P → "Remote-SSH: Connect to Host" → `pi-ctv`
2. **Open Project**: File → Open Folder → `/opt/apps/ctv-bookedbiz-db`
3. **Select Python Interpreter**: Ctrl+Shift+P → "Python: Select Interpreter" → `.venv/bin/python`
4. **Develop** with full VS Code features (IntelliSense, debugging, etc.)

### 5. Collaboration Guidelines

#### Code Collaboration
- **Use descriptive commit messages**
- **Pull before push** to avoid conflicts
- **Restart Flask service** after pushing changes: `sudo systemctl restart flaskapp`
- **Communicate major changes** in advance
- **Use feature branches** for larger changes:
  ```bash
  git checkout -b feature/new-feature
  # Make changes
  git push -u origin feature/new-feature
  # Create pull request on GitHub
  ```

#### Database Collaboration
- **Communicate database changes** before making them
- **Create backups** before schema modifications
- **Use sequential workflow** - avoid simultaneous database modifications
- **Test changes locally** before uploading

#### File Permissions
Both developers are in the `ctv-dev` group with read/write access to all project files:
```bash
# Check group membership
groups

# Set correct permissions if needed
sudo chgrp -R ctv-dev /opt/apps/ctv-bookedbiz-db
sudo chmod -R g+rw /opt/apps/ctv-bookedbiz-db
```

### 6. Environment Configuration

#### Environment Variables (.env)
```env
DROPBOX_ACCESS_TOKEN=your_dropbox_token
DATABASE_PATH=/opt/apps/ctv-bookedbiz-db/data/database/production.db
DROPBOX_DB_PATH=/data/database/production.db
FLASK_ENV=development
```

**⚠️ IMPORTANT**: Never commit `.env` file to Git (it's in `.gitignore`)

#### Python Dependencies
- **Add new packages**: `uv pip install package-name`
- **Update requirements**: `uv pip freeze > requirements.txt`
- **Install from requirements**: `uv pip install -r requirements.txt`

### 7. Troubleshooting

#### Common Issues

**Flask App Service Issues**:
```bash
# Check service status
sudo systemctl status flaskapp

# View detailed logs
sudo journalctl -u flaskapp -n 100

# Restart service
sudo systemctl restart flaskapp

# Test if app is accessible
curl http://localhost:8000
```

**VS Code Remote-SSH Connection Issues**:
```bash
# Kill existing VS Code server
# In VS Code: Ctrl+Shift+P → "Remote-SSH: Kill VS Code Server on Host"

# Test SSH connection
ssh pi-ctv

# Check VS Code server logs
# View → Output → Select "Remote-SSH"
```

**Database Sync Issues**:
```bash
# Check Dropbox connection
python db_sync.py info

# Verify file paths
ls -la data/database/production.db
cat .env | grep -v DROPBOX_ACCESS_TOKEN  # Hide token
```

**Permission Issues**:
```bash
# Check group membership
groups

# Fix permissions
sudo chgrp -R ctv-dev /opt/apps/ctv-bookedbiz-db
sudo chmod -R g+rw /opt/apps/ctv-bookedbiz-db
```

**Python Environment Issues**:
```bash
# Verify uv is in PATH
uv --version

# Reactivate virtual environment
  source .venv/bin/activate

# Check Python interpreter
which python
python --version
```

### 8. Security Notes

- **Tailscale VPN**: All connections are encrypted and authenticated
- **SSH Keys**: Use SSH keys for authentication (no passwords)
- **Environment Variables**: Keep sensitive data in `.env` (never commit)
- **Regular Backups**: Database is automatically backed up with timestamps
- **Access Control**: Only team members have Pi access via SSH keys
- **Service Security**: Flask service runs as `daseme` user with minimal privileges

### 9. Datasette for Database Exploration

Datasette provides a web interface for exploring and querying your SQLite database with support for long-running queries.

#### Quick Start (Get it running now)
```bash
# Navigate to project and get latest database
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate
python db_sync.py download

# Start Datasette with long-running query support
datasette data/database/production.db \
  --host 0.0.0.0 \
  --port 8001 \
  --setting sql_time_limit_ms 300000 \
  --setting max_returned_rows 10000 \
  --reload

# Access via: http://100.81.73.46:8001 (Tailscale IP)
# Stop with Ctrl+C
```

#### Persistent Background Service
**Create systemd service:**
```bash
sudo nano /etc/systemd/system/datasette.service
```

**Service file content:**
```ini
[Unit]
Description=Datasette Service
After=network.target

[Service]
Type=simple
User=daseme
Group=ctv-dev
WorkingDirectory=/opt/apps/ctv-bookedbiz-db
Environment=PATH=/opt/apps/ctv-bookedbiz-db/.venv/bin
ExecStart=/opt/apps/ctv-bookedbiz-db/.venv/bin/datasette data/database/production.db --host 0.0.0.0 --port 8001 --setting sql_time_limit_ms 300000 --setting max_returned_rows 10000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable and start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable datasette
sudo systemctl start datasette
sudo systemctl status datasette
```

#### Daily Usage
- **After database sync**: `sudo systemctl restart datasette`
- **Check logs**: `sudo journalctl -u datasette -n 50`
- **Access**: `http://100.81.73.46:8001`



### 10. Mount network drives
- sudo mkdir -p /mnt/k-drive
- sudo mount -t cifs "//100.102.206.113/K Drive" /mnt/k-drive -o username=usrjp,password='PASSWORDNEEDED',domain=CTVETERE,vers=2.0,sec=ntlmv2

### 11. Maintenance

#### Regular Tasks
- **Weekly**: Review and clean up old database backups
- **Monthly**: Update system packages: `sudo apt update && sudo apt upgrade`
- **As Needed**: Update Python dependencies: `uv pip install --upgrade package-name`
- **Service Health**: Monitor Flask app and Datasette service logs periodically
- **Database Analysis**: Use Datasette to monitor data quality and trends

#### Monitoring
- **Database Size**: Check `python db_sync.py info` for database growth
- **Disk Space**: Monitor Pi storage with `df -h`
- **Dropbox Usage**: Monitor Dropbox storage limits
- **Service Status**: Check Flask app health with `sudo systemctl status flaskapp`
- **Datasette Health**: Check Datasette service with `sudo systemctl status datasette`

### 10. Maintenance

#### Regular Tasks
- **Weekly**: Review and clean up old database backups
- **Monthly**: Update system packages: `sudo apt update && sudo apt upgrade`
- **As Needed**: Update Python dependencies: `uv pip install --upgrade package-name`
- **Service Health**: Monitor Flask app and Datasette service logs periodically
- **Database Analysis**: Use Datasette to monitor data quality and trends

#### Monitoring
- **Database Size**: Check `python db_sync.py info` for database growth
- **Disk Space**: Monitor Pi storage with `df -h`
- **Dropbox Usage**: Monitor Dropbox storage limits
- **Service Status**: Check Flask app health with `sudo systemctl status flaskapp`
- **Datasette Health**: Check Datasette service with `sudo systemctl status datasette`

### 10. Maintenance

#### Regular Tasks
- **Weekly**: Review and clean up old database backups
- **Monthly**: Update system packages: `sudo apt update && sudo apt upgrade`
- **As Needed**: Update Python dependencies: `uv pip install --upgrade package-name`
- **Service Health**: Monitor Flask app service logs periodically

#### Monitoring
- **Database Size**: Check `python db_sync.py info` for database growth
- **Disk Space**: Monitor Pi storage with `df -h`
- **Dropbox Usage**: Monitor Dropbox storage limits
- **Service Status**: Check Flask app health with `sudo systemctl status flaskapp`

---

## Quick Reference

### Essential Commands
```bash
# Project navigation
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

# Database sync
python db_sync.py download    # Get latest
python db_sync.py upload      # Save changes
python db_sync.py backup      # Create backup

# Git workflow
git pull                      # Get latest code
git add .                     # Stage changes
git commit -m "message"       # Commit changes
git push                      # Push to GitHub

# Flask service management
sudo systemctl status flaskapp    # Check status
sudo systemctl restart flaskapp   # Restart after changes
sudo journalctl -u flaskapp -n 50 # View logs

# VS Code connection
ssh pi-ctv                    # Terminal connection
# VS Code: Ctrl+Shift+P → "Remote-SSH: Connect to Host"
```

### Important Paths
- **Project Root**: `/opt/apps/ctv-bookedbiz-db/`
- **Database**: `data/database/production.db`
- **Python Environment**: `.venv/bin/python`
- **Dropbox Sync**: `/Apps/ctv-bookedbiz-db/data/database/production.db`
- **Service Config**: `/etc/systemd/system/flaskapp.service`

### Important URLs
- **Flask App**: `http://100.81.73.46:8000` (via Tailscale)
- **Local Access**: `http://localhost:8000` (when on Pi)

This workflow ensures smooth collaboration while maintaining data integrity, code quality, and reliable service deployment. Happy coding! 🚀