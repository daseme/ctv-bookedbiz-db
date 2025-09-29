# Raspberry Pi Development Workflow

## Overview
This document outlines the collaborative development workflow for the CTV BookedBiz application running on a Raspberry Pi 5, with database sync via Dropbox and code collaboration via GitHub.

## System Architecture

### Infrastructure
- **Hardware**: Raspberry Pi 5 (ARM64) - pi-ctv (primary development)
- **Docker Host**: Raspberry Pi 2 - pi2 (containerized testing and deployment)
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
├── cli_db_sync.py           # Database sync utility
├── bin/
│   └── db_sync.sh           # Wrapper script for automated backups
├── requirements.txt         # Python dependencies
├── runserver.sh             # Flask app startup script
└── [your Flask app files]
```

## Move files from your local computer to the raspberry pi
scp "C:\Users\Kurt\Crossings TV Dropbox\kurt olmstead\Financial\Sales\WeeklyReports\ctv-bookedbiz-db\data\raw\2021.xlsx" daseme@raspberrypi:/opt/apps/ctv-bookedbiz-db/data/raw/2021.xlsx

from pi to win

sudo tailscale file cp "./data/raw/daily/Commercial Log 250912.xlsx" desktop-7402tkp:


## Team Members
- **daseme**: Primary developer with Pi admin access
- **jellee26**: Collaborative developer with ctv-dev group access

## Development Workflow

### 1. Daily Development Routine

#### Starting Work Session
```bash
# 1. Connect to pi-ctv via Tailscale
ssh daseme@100.81.73.46
# or
ssh jellee26@100.81.73.46

# 2. Navigate to project directory
cd /opt/apps/ctv-bookedbiz-db

# 3. Activate Python environment
source .venv/bin/activate

# 4. Get latest code changes
git pull

# 5. Database is already current via nightly sync
# Check database status if needed
python cli_db_sync.py info

# 6. Check if Flask app service is running
sudo systemctl status flaskapp
```

#### During Development
```bash
# Create backup before major changes
python cli_db_sync.py backup

# Install new dependencies if needed
uv pip install package-name
uv pip freeze > requirements.txt

# Restart Flask app after code changes
sudo systemctl restart flaskapp
# watch logs if needed
sudo journalctl -u flaskapp -f
```

#### Ending Work Session
```bash
# 1. Optional: Upload database changes immediately if significant
python cli_db_sync.py upload

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

### 3. Database Management — Server‑First with Nightly Dropbox Backup

**Principle:** We work on the **server copy** of the database and **do not** auto‑download from Dropbox. A nightly **systemd timer** creates timestamped backups in Dropbox as an off‑site backup store. Manual download is **only for recovery**.

#### Components

* **Primary DB (authoritative)**
   * `/opt/apps/ctv-bookedbiz-db/data/database/production.db`
* **Python entrypoint**
   * `cli_db_sync.py` (supports `upload | backup | list-backups | info`)
* **Virtualenv (for systemd)**
   * `/opt/apps/ctv-bookedbiz-db/.venv` with `dropbox`, `python-dotenv`
* **Wrapper executable**
   * `/opt/apps/ctv-bookedbiz-db/bin/db_sync.sh`
      * Runs inside venv using `cli_db_sync.py backup`
      * Uses `flock` lockfile: `/tmp/ctv-db-sync.lock`
      * Implements retention (keeps 7 backups, deletes older)
      * Performs integrity checks (latest backup vs local hash)
      * Logs to syslog with optional ntfy/Slack notifications
* **Secrets (root-only)**
   * `/etc/ctv-db-sync.env`
      * `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`
      * Permissions: `root:root`, `600`
* **systemd units**
   * `/etc/systemd/system/ctv-db-sync.service` (runs wrapper once, 3600s timeout)
   * `/etc/systemd/system/ctv-db-sync.timer` (daily at 02:05 + random 5m delay)
* **Logs**
   * `/var/log/ctv-db-sync/sync.log`
   * Logrotate: weekly, keep 8, compressed

#### Dropbox App Root Structure

* `database.db` (legacy single-file upload)
* `backups/` (timestamped nightly copies: `database_backup_YYYYMMDD_HHMMSS.db`)
* **Note:** No nested `Apps/` folder (removed)

#### Daily Backup Flow (Automated)

* **Timer fires daily** (02:05 + random delay) → runs `db_sync.sh`
* Wrapper executes:
  ```bash
  python cli_db_sync.py backup
  ```
* Process:
  1. Creates timestamped backup in `/backups/database_backup_YYYYMMDD_HHMMSS.db`
  2. Retains last 7 backups, deletes older ones
  3. Compares latest backup with local DB using Dropbox content_hash
  4. Logs integrity check results (✓/✗)
  5. Results written to `/var/log/ctv-db-sync/sync.log`

#### Service/Timer Control

```bash
# Manual run
sudo systemctl start ctv-db-sync.service

# Check next scheduled run
systemctl list-timers | grep ctv-db-sync

# Status + recent logs
sudo systemctl status ctv-db-sync.service -l --no-pager
tail -n 200 /var/log/ctv-db-sync/sync.log
```

#### Manual Commands (When Needed)

```bash
# List backups in Dropbox
sudo sh -c 'set -a; . /etc/ctv-db-sync.env; set +a; \
  /opt/apps/ctv-bookedbiz-db/.venv/bin/python \
  /opt/apps/ctv-bookedbiz-db/cli_db_sync.py list-backups'

# Create immediate backup (e.g., before major changes)
python cli_db_sync.py backup

# Upload current DB as single file (legacy method)
python cli_db_sync.py upload

# Inspect current Dropbox state
python cli_db_sync.py info
```

#### Key Changes from Previous Version

* **Backup strategy:** Nightly backups now create timestamped files in `/backups/` folder instead of overwriting a single file
* **Retention:** Automatically keeps only the last 7 backups
* **Integrity verification:** Each backup is validated against local DB hash
* **Simplified structure:** Removed nested `Apps/` directory from Dropbox path
* **Enhanced logging:** Better structured logs with integrity check results

#### Notes

* Local DB is always authoritative; Dropbox serves as off-site backup only
* Integrity failures can trigger notifications via ntfy/Slack if configured
* Recovery procedures should stop dependent services, replace local DB, then restart services

### 4. Docker Development (Pi2 - Control Station Alpha)

**IMPORTANT: All Docker operations are performed on Pi2 (100.96.96.109), NOT on pi-ctv**

Pi2 serves as the Docker development and testing environment, providing containerized deployment capabilities for both local testing and cloud deployment preparation.

#### Docker Architecture
- **Pi2 Docker Host**: Container build and test environment (100.96.96.109)
- **Multi-stage Builds**: Optimized production images with uvicorn ASGI server
- **Volume Mounts**: Database persistence via Docker volumes
- **Health Checks**: Automatic container health monitoring
- **Railway Ready**: Containers configured for cloud deployment

#### Container Management

**Build and Test Cycle:**
```bash
# Connect to pi2 (Docker host) - NOT pi-ctv
ssh daseme@100.96.96.109
cd /opt/apps/ctv-bookedbiz-db

# Pull latest code changes
git pull origin main

# Build Docker image
docker build -t ctv-flask .

# Test container locally
docker run -d --name ctv-test \
  -p 8000:8000 \
  -v ./data/database:/app/data/database \
  ctv-flask

# Health check
curl http://localhost:8000/api/system-stats
docker logs ctv-test

# Clean up test container
docker stop ctv-test && docker rm ctv-test
```

**Docker Service Integration:**
```bash
# Run with Docker Compose (full environment)
docker-compose up -d

# View running containers
docker ps

# Check container health
docker-compose ps

# View logs
docker-compose logs ctv-flask

# Stop all services
docker-compose down
```

#### Docker File Structure
```
/opt/apps/ctv-bookedbiz-db/ (on pi2)
├── Dockerfile                   # Production container image
├── .dockerignore               # Build optimization
├── docker-compose.yml          # Multi-environment orchestration
├── railway.json               # Railway deployment config (optional)
└── docker/                    # Docker variants (optional)
    ├── Dockerfile.dev         # Development with hot reload
    └── Dockerfile.railway     # Railway-optimized version
```

#### Container Development Workflow

**Daily Container Testing (on pi2):**
```bash
# After code changes on pi-ctv, test in container on pi2
ssh daseme@100.96.96.109
cd /opt/apps/ctv-bookedbiz-db
git pull origin main

# Quick container test
docker build -t ctv-flask . && \
docker run --rm -p 8000:8000 \
  -v ./data/database:/app/data/database \
  ctv-flask &

# Test API endpoint
curl http://localhost:8000/api/system-stats

# Kill test container
docker ps -q --filter ancestor=ctv-flask | xargs docker kill
```

**Production Simulation (on pi2):**
```bash
# Test with production-like environment
docker run -d --name ctv-production-test \
  -p 8000:8000 \
  -v ./data/database:/app/data/database \
  -e FLASK_ENV=production \
  --restart unless-stopped \
  ctv-flask

# Monitor for extended period
docker stats ctv-production-test

# Production health check
docker exec ctv-production-test curl -f http://localhost:8000/api/system-stats
```

#### Failover Integration

**Docker Failover Testing (pi2 as backup):**
```bash
# Test containerized failover scenario
# 1. Stop pi-ctv service (simulate failure)
ssh daseme@100.81.73.46
sudo systemctl stop flaskapp

# 2. Start container backup on pi2
ssh daseme@100.96.96.109
cd /opt/apps/ctv-bookedbiz-db
python cli_db_sync.py download  # Get latest database

# 3. Start containerized backup
docker run -d --name ctv-failover \
  -p 8000:8000 \
  -v ./data/database:/app/data/database \
  ctv-flask

# 4. Verify failover works
curl http://100.96.96.109:8000/api/system-stats

# 5. Clean up after test
docker stop ctv-failover && docker rm ctv-failover
```

#### Cloud Deployment Preparation (on pi2)

**Railway Deployment Testing:**
```bash
# Test Railway-compatible configuration on pi2
docker build -f docker/Dockerfile.railway -t ctv-railway . 

# Test with Railway-style environment variables
docker run -d --name ctv-railway-test \
  -p 8000:8000 \
  -e PORT=8000 \
  -e RAILWAY_ENVIRONMENT=production \
  -v ./data/database:/app/data/database \
  ctv-railway

# Validate Railway compatibility
curl http://localhost:8000/api/system-stats
docker logs ctv-railway-test
```

#### Docker Maintenance (on pi2)

**Container Cleanup:**
```bash
# Remove unused containers
docker container prune -f

# Remove unused images
docker image prune -f

# Remove unused volumes (be careful!)
docker volume prune -f

# Full system cleanup (use sparingly)
docker system prune -af
```

**Image Management:**
```bash
# List local images
docker images

# Remove specific image
docker rmi ctv-flask

# Tag for deployment
docker tag ctv-flask:latest daseme/ctv-flask:latest

# Push to Docker Hub (if configured)
docker push daseme/ctv-flask:latest
```

#### Integration with Existing Workflow

**Docker on pi2 fits into the existing workflow as:**
1. **Development Testing**: Validate changes in containerized environment
2. **Failover Capability**: Alternative deployment method for pi2
3. **Cloud Preparation**: Ready-to-deploy containers for Railway/cloud
4. **Production Consistency**: Same uvicorn ASGI server across all environments
5. **Monitoring Enhancement**: Container metrics integrate with Control Station Alpha

**When to Use Docker (on pi2):**
- **Testing new features** in isolated environment
- **Preparing cloud deployments** 
- **Simulating production** conditions locally
- **Emergency failover** with consistent environment
- **Debugging environment** issues

### 5. VS Code Remote Development

#### Initial Setup (One-time per developer)
1. Install VS Code extensions:
   - Remote - SSH
   - Remote Development (extension pack)
   - Python
   - Python Debugger

2. Set up SSH config (`~/.ssh/config`):
   ```
   Host pi-ctv
       HostName 100.81.73.46
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

### 6. Collaboration Guidelines

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
- **Create backups** before schema modifications: `python cli_db_sync.py backup`
- **Use sequential workflow** - avoid simultaneous database modifications
- **Test changes locally** before relying on nightly backup

#### File Permissions
Both developers are in the `ctv-dev` group with read/write access to all project files:
```bash
# Check group membership
groups

# Set correct permissions if needed
sudo chgrp -R ctv-dev /opt/apps/ctv-bookedbiz-db
sudo chmod -R g+rw /opt/apps/ctv-bookedbiz-db
```

### 7. Environment Configuration

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

### 8. Troubleshooting

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
python cli_db_sync.py info

# Verify file paths
ls -la data/database/production.db

# Check nightly backup logs
tail -n 50 /var/log/ctv-db-sync/sync.log

# Check timer status
systemctl list-timers | grep ctv-db-sync
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

### 9. Security Notes

- **Tailscale VPN**: All connections are encrypted and authenticated
- **SSH Keys**: Use SSH keys for authentication (no passwords)
- **Environment Variables**: Keep sensitive data in `.env` (never commit)
- **Regular Backups**: Database is automatically backed up nightly with timestamps
- **Access Control**: Only team members have Pi access via SSH keys
- **Service Security**: Flask service runs as `daseme` user with minimal privileges

### 10. Datasette for Database Exploration

Datasette provides a web interface for exploring and querying your SQLite database with support for long-running queries.

#### Quick Start (Get it running now)
```bash
# Navigate to project on pi-ctv
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

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
- **After significant database changes**: `sudo systemctl restart datasette`
- **Check logs**: `sudo journalctl -u datasette -n 50`
- **Access**: `http://100.81.73.46:8001`

### 11. Mount network drives
- sudo mkdir -p /mnt/k-drive
- sudo mount -t cifs "//100.102.206.113/K Drive" /mnt/k-drive -o username=usrjp,password='PASSWORDNEEDED',domain=CTVETERE,vers=2.0,sec=ntlmv2

### 12. Maintenance

#### Regular Tasks
- **Weekly**: Review database backup logs: `tail -n 100 /var/log/ctv-db-sync/sync.log`
- **Monthly**: Update system packages: `sudo apt update && sudo apt upgrade`
- **As Needed**: Update Python dependencies: `uv pip install --upgrade package-name`
- **Service Health**: Monitor Flask app and Datasette service logs periodically
- **Database Analysis**: Use Datasette to monitor data quality and trends

#### Monitoring
- **Database Size**: Check `python cli_db_sync.py info` for database growth
- **Disk Space**: Monitor Pi storage with `df -h`
- **Dropbox Usage**: Monitor Dropbox storage limits and backup retention
- **Service Status**: Check Flask app health with `sudo systemctl status flaskapp`
- **Datasette Health**: Check Datasette service with `sudo systemctl status datasette`
- **Backup Integrity**: Review backup integrity checks in sync logs

---

## Quick Reference

### Essential Commands
```bash
# Project navigation (pi-ctv)
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

# Database operations
python cli_db_sync.py info          # Check database status
python cli_db_sync.py backup        # Create immediate backup
python cli_db_sync.py upload        # Legacy upload method
python cli_db_sync.py list-backups  # List all backups

# Git workflow
git pull                      # Get latest code
git add .                     # Stage changes
git commit -m "message"       # Commit changes
git push                      # Push to GitHub

# Flask service management (pi-ctv)
sudo systemctl status flaskapp    # Check status
sudo systemctl restart flaskapp   # Restart after changes
sudo journalctl -u flaskapp -n 50 # View logs

# Docker operations (pi2 only)
ssh daseme@100.96.96.109      # Connect to pi2
docker build -t ctv-flask .   # Build container
docker ps                     # List containers

# VS Code connection
ssh pi-ctv                    # Terminal connection
# VS Code: Ctrl+Shift+P → "Remote-SSH: Connect to Host"
```

### Important Paths
- **Project Root**: `/opt/apps/ctv-bookedbiz-db/`
- **Database**: `data/database/production.db`
- **Python Environment**: `.venv/bin/python`
- **Backup Logs**: `/var/log/ctv-db-sync/sync.log`
- **Dropbox Structure**: 
  - Legacy: `/database.db`
  - Backups: `/backups/database_backup_YYYYMMDD_HHMMSS.db`

### Important URLs
- **Flask App**: `http://100.81.73.46:8000` (pi-ctv via Tailscale)
- **Datasette**: `http://100.81.73.46:8001` (pi-ctv via Tailscale)
- **Docker Testing**: `http://100.96.96.109:8000` (pi2 via Tailscale)

### Key System Differences
- **pi-ctv (100.81.73.46)**: Primary development, Flask app, database authority
- **pi2 (100.96.96.109)**: Docker host, containerized testing, failover capability