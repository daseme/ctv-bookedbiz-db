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

### 3. Database Management

#### Database Sync Commands
```bash
# Download latest database from Dropbox
python db_sync.py download

# Upload local changes to Dropbox
python db_sync.py upload

# Create timestamped backup
python db_sync.py backup

# Get database information
python db_sync.py info

# List available backups
python db_sync.py list-backups

# Explore Dropbox folder structure
python db_sync.py list [folder-path]
```

#### Database Workflow
1. **Always download** before starting work: `python db_sync.py download`
2. **Create backups** before major schema changes: `python db_sync.py backup`
3. **Upload regularly** during development: `python db_sync.py upload`
4. **Coordinate with team** - communicate when making major database changes

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

Datasette provides a powerful web interface for exploring and analyzing your SQLite database, with support for complex queries and data visualization.

#### Installation and Setup

**Install Datasette**:
```bash
# Activate virtual environment
source .venv/bin/activate

# Install Datasette and useful plugins
uv pip install datasette
uv pip install datasette-vega  # For charts and visualizations
uv pip install datasette-cluster-map  # For geographic data
uv pip install datasette-export  # For data export options

# Update requirements
uv pip freeze > requirements.txt
```

#### Configuration for Long-Running Queries

**Option 1: Command Line Settings (Recommended)**
```bash
# Start Datasette with long-running query support using command line settings
datasette data/database/production.db \
  --host 0.0.0.0 \
  --port 8001 \
  --setting sql_time_limit_ms 300000 \
  --setting facet_time_limit_ms 300000 \
  --setting max_returned_rows 10000 \
  --setting default_page_size 100 \
  --reload

# To see all available settings:
datasette --help-settings
```

**Option 2: Metadata File Configuration**
Create a `metadata.yaml` file for database-specific settings and plugin configuration:

```yaml
# /opt/apps/ctv-bookedbiz-db/metadata.yaml
databases:
  production:
    title: "CTV BookedBiz Production Database"
    description: "Production database for CTV BookedBiz application"
    queries:
      recent_transactions:
        sql: "SELECT * FROM transactions WHERE created_date >= DATE('now', '-30 days') ORDER BY created_date DESC LIMIT 100"
        title: "Recent Transactions (Last 30 Days)"
      monthly_summary:
        sql: "SELECT DATE(created_date, 'start of month') as month, COUNT(*) as count, SUM(amount) as total FROM transactions GROUP BY month ORDER BY month DESC"
        title: "Monthly Transaction Summary"
        
# Plugin configuration (installed plugins will be auto-detected)
plugins:
  datasette-vega: {}
  datasette-export: {}
  datasette-cluster-map: {}
```

Then run with:
```bash
datasette data/database/production.db \
  --host 0.0.0.0 \
  --port 8001 \
  --metadata metadata.yaml \
  --setting sql_time_limit_ms 300000 \
  --setting facet_time_limit_ms 300000 \
  --setting max_returned_rows 10000 \
  --setting default_page_size 100 \
  --reload
```

**Note**: To see all available settings, run `datasette --help-settings`

**Option 3: Environment Variables**
```bash
# Set environment variables for common settings
export DATASETTE_SQL_TIME_LIMIT_MS=300000
export DATASETTE_FACET_TIME_LIMIT_MS=300000
export DATASETTE_MAX_RETURNED_ROWS=10000
export DATASETTE_DEFAULT_PAGE_SIZE=100

# Start Datasette
datasette data/database/production.db \
  --host 0.0.0.0 \
  --port 8001 \
  --reload
```

#### Running Datasette

**Option 1: Interactive Development (Recommended)**:
```bash
# Navigate to project directory
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

# Get latest database
python db_sync.py download

# Start Datasette with long-running query support
datasette data/database/production.db \
  --host 0.0.0.0 \
  --port 8001 \
  --setting sql_time_limit_ms 300000 \
  --setting facet_time_limit_ms 300000 \
  --setting max_returned_rows 10000 \
  --setting default_page_size 100 \
  --reload

# Access via: http://100.81.73.46:8001 (Tailscale IP)
```

**Option 2: Background Service**:
```bash
# Create a simple background script
nohup datasette data/database/production.db \
  --host 0.0.0.0 \
  --port 8001 \
  --setting sql_time_limit_ms 300000 \
  --setting facet_time_limit_ms 300000 \
  --setting max_returned_rows 10000 \
  --setting default_page_size 100 \
  > datasette.log 2>&1 &

# Check if running
ps aux | grep datasette

# Stop background process
pkill -f datasette
```

**Option 3: Systemd Service (Recommended)**:

Create a systemd service file:
```bash
sudo nano /etc/systemd/system/datasette.service
```

**Service Configuration**:
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
ExecStart=/opt/apps/ctv-bookedbiz-db/.venv/bin/datasette data/database/production.db --host 0.0.0.0 --port 8001 --setting sql_time_limit_ms 300000 --setting facet_time_limit_ms 300000 --setting max_returned_rows 10000 --setting default_page_size 100
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Enable and manage the service**:
```bash
# Reload systemd configuration
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable datasette

# Start the service
sudo systemctl start datasette

# Check status
sudo systemctl status datasette

# View logs
sudo journalctl -u datasette -n 50
sudo journalctl -u datasette -f  # Follow logs

# Restart service (after database updates)
sudo systemctl restart datasette
```

#### Datasette Workflow Integration

**Daily Development with Datasette**:
```bash
# 1. Get latest database
python db_sync.py download

# 2. Restart Datasette to pick up database changes
sudo systemctl restart datasette

# 3. Access Datasette interface
# Open browser: http://100.81.73.46:8001
```

**Long-Running Query Examples**:
```sql
-- Complex aggregation query
SELECT 
  category,
  COUNT(*) as total_records,
  AVG(amount) as avg_amount,
  SUM(amount) as total_amount,
  MIN(created_date) as earliest_date,
  MAX(created_date) as latest_date
FROM transactions 
WHERE created_date >= '2024-01-01'
GROUP BY category
ORDER BY total_amount DESC;

-- Time-series analysis
SELECT 
  DATE(created_date) as date,
  COUNT(*) as daily_count,
  SUM(amount) as daily_total,
  AVG(amount) as daily_average
FROM transactions
WHERE created_date >= DATE('now', '-90 days')
GROUP BY DATE(created_date)
ORDER BY date;
```

#### Advanced Datasette Features

**Custom SQL Queries**:
- Navigate to `/production` in Datasette
- Click "View and edit SQL" 
- Write complex queries with confidence they won't timeout
- Export results as CSV, JSON, or use API endpoints

**Faceted Browsing**:
- Use suggested facets for quick data exploration
- Filter data interactively without writing SQL
- Perfect for business users who need data insights

**Data Visualization**:
- Use datasette-vega plugin for charts
- Create bar charts, line graphs, and scatter plots
- Export visualizations for reports

**API Access**:
```bash
# Get data via API
curl "http://100.81.73.46:8001/production/transactions.json?_size=1000"

# Get specific query results
curl "http://100.81.73.46:8001/production.json?sql=SELECT+*+FROM+transactions+LIMIT+10"
```

#### Security and Access Control

**Network Access**:
- Datasette runs on port 8001 (separate from Flask app on 8000)
- Only accessible via Tailscale VPN network
- No authentication required within trusted network

**Read-Only Access**:
- Datasette provides read-only access to your database
- Safe for data exploration without risking data corruption
- Original database remains protected

#### Troubleshooting Datasette

**Common Issues**:
```bash
# Check if Datasette is running
sudo systemctl status datasette
curl http://localhost:8001

# View detailed logs
sudo journalctl -u datasette -n 100

# Restart after database changes
sudo systemctl restart datasette

# Check database file permissions
ls -la data/database/production.db

# Test configuration and see available settings
datasette --help-settings
datasette data/database/production.db --setting sql_time_limit_ms 300000 --help
```

**Performance Tips**:
- Create indexes for frequently queried columns
- Use LIMIT clauses for large result sets
- Monitor query execution time in Datasette interface
- Use facets instead of complex WHERE clauses when possible

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