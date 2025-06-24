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
```

#### Ending Work Session
```bash
# 1. Save database changes to Dropbox
python db_sync.py upload

# 2. Commit and push code changes
git add .
git commit -m "Description of changes"
git push

# 3. Exit Pi
exit
```

### 2. Database Management

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

### 3. VS Code Remote Development

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

### 4. Collaboration Guidelines

#### Code Collaboration
- **Use descriptive commit messages**
- **Pull before push** to avoid conflicts
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

### 5. Environment Configuration

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

### 6. Troubleshooting

#### Common Issues

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

### 7. Security Notes

- **Tailscale VPN**: All connections are encrypted and authenticated
- **SSH Keys**: Use SSH keys for authentication (no passwords)
- **Environment Variables**: Keep sensitive data in `.env` (never commit)
- **Regular Backups**: Database is automatically backed up with timestamps
- **Access Control**: Only team members have Pi access via SSH keys

### 8. Maintenance

#### Regular Tasks
- **Weekly**: Review and clean up old database backups
- **Monthly**: Update system packages: `sudo apt update && sudo apt upgrade`
- **As Needed**: Update Python dependencies: `uv pip install --upgrade package-name`

#### Monitoring
- **Database Size**: Check `python db_sync.py info` for database growth
- **Disk Space**: Monitor Pi storage with `df -h`
- **Dropbox Usage**: Monitor Dropbox storage limits

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

# VS Code connection
ssh pi-ctv                    # Terminal connection
# VS Code: Ctrl+Shift+P → "Remote-SSH: Connect to Host"
```

### Important Paths
- **Project Root**: `/opt/apps/ctv-bookedbiz-db/`
- **Database**: `data/database/production.db`
- **Python Environment**: `.venv/bin/python`
- **Dropbox Sync**: `/Apps/ctv-bookedbiz-db/data/database/production.db`

This workflow ensures smooth collaboration while maintaining data integrity and code quality. Happy coding! 🚀