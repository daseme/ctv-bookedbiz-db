# Pi2 Mirror Backup System for Pi-CTV

**Project Lepidoptera - Control Station Alpha Infrastructure**

---

## 🎯 System Overview

This document describes the **Pi2 Mirror Backup System** - a comprehensive failover solution that provides high-availability backup for the critical pi-ctv Flask application and SQLite database. The system integrates seamlessly with **Control Station Alpha** monitoring infrastructure to provide enterprise-grade business continuity.

### Architecture Summary
```
Pi-CTV (Primary) - 100.81.73.46:8000
├── Flask Application (Active)
├── SQLite Database (1.4GB+)
├── Nightly Backup: 2:05 AM → Dropbox
└── Monitored by: Control Station Alpha

Pi2 (Mirror/Standby) - 100.96.96.109:8000  
├── Flask Application (Standby)
├── Daily Database Sync: 2:30 AM ← Dropbox
├── Kuma Professional Monitoring: Port 3001
├── Control Station Interface: Port 5001
└── Emergency Failover: 30-second activation
```

---

## 🏗️ System Components

### 1. **Automated Daily Synchronization**
- **Timer**: `ctv-pi2-download.timer` runs daily at 2:30 AM
- **Service**: `ctv-pi2-download.service` downloads fresh database 
- **Log Location**: `/var/log/ctv-pi2-download/download.log`
- **Safety Window**: 25-minute gap after pi-ctv upload (2:05 AM) prevents conflicts

### 2. **Emergency Failover System**
- **Script**: `/opt/apps/ctv-bookedbiz-db/scripts/failover-to-pi2.sh`
- **Activation Time**: ~30 seconds (database already synced)
- **Health Checks**: Code update, database validation, service startup, HTTP endpoint test
- **Monitoring Integration**: Control Station Alpha alerts when pi-ctv fails

### 3. **Failback System**
- **Script**: `/opt/apps/ctv-bookedbiz-db/scripts/failback-to-pi2.sh`  
- **Data Safety**: Read-only mirror with optional manual backup of failover changes
- **Double Confirmation**: Required for database uploads to prevent conflicts
- **Timestamped Backups**: `failover_backup_YYYYMMDD_HHMMSS.db` format

### 4. **Flask Service Management**
- **Service**: `flaskapp.service` (disabled by default on pi2)
- **Port**: 8000 (same as pi-ctv for seamless transition)
- **Dependencies**: Python virtual environment, uv package manager
- **Startup**: On-demand activation during failover events

---

## ⚡ Daily Operations

### Automated Processes

**2:05 AM - Pi-CTV Upload**
```bash
# Pi-ctv uploads database to Dropbox
systemctl status ctv-db-sync.timer
```

**2:30 AM - Pi2 Download**  
```bash
# Pi2 downloads fresh database from Dropbox
systemctl status ctv-pi2-download.timer
systemctl list-timers | grep ctv-pi2-download
```

**Monitoring**
- **Control Station Alpha**: Monitors both pi-ctv health and backup system status
- **Kuma Dashboard**: Professional monitoring with Teams integration
- **Automatic Alerts**: Immediate notification if pi-ctv becomes unavailable

### Health Monitoring Commands

```bash
# Check pi2 backup system status
sudo systemctl status ctv-pi2-download.timer

# View recent backup logs  
tail -20 /var/log/ctv-pi2-download/download.log

# Check database freshness
ls -lh /opt/apps/ctv-bookedbiz-db/data/database/production.db*
date -r /opt/apps/ctv-bookedbiz-db/data/database/production.db

# Test Flask service (without starting)
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate
sudo systemctl status flaskapp
```

---

## 🚨 Emergency Procedures

### Failover Activation (Pi-CTV Down)

**When Control Station Alpha alerts of pi-ctv failure:**

```bash
# SSH to pi2
ssh daseme@100.96.96.109

# Navigate to project
cd /opt/apps/ctv-bookedbiz-db

# Execute failover (30 seconds)
./scripts/failover-to-pi2.sh
```

**Failover Process:**
1. ✅ **Code Update**: Latest from GitHub (5 seconds)
2. ✅ **Database Check**: Validates local database freshness (instant)
3. ✅ **Service Start**: Flask application startup (10 seconds)  
4. ✅ **Health Validation**: HTTP endpoint and systemd status (10 seconds)
5. ✅ **Confirmation**: Ready to serve traffic

**Post-Failover URLs:**
- **Flask Application**: http://100.96.96.109:8000
- **Control Station Alpha**: http://100.96.96.109:5001  
- **Kuma Monitoring**: http://100.96.96.109:3001

### Failback Activation (Pi-CTV Restored)

**When pi-ctv is confirmed healthy:**

```bash
# Execute failback from pi2
./scripts/failback-to-pi-ctv.sh
```

**Failback Process:**
1. ✅ **Pi-CTV Health Check**: Confirms primary system is responding
2. ⚠️ **Data Safety Prompt**: Option to backup any changes made during failover
3. ✅ **Service Stop**: Cleanly stops Flask on pi2
4. ✅ **Final Verification**: Confirms pi-ctv is serving traffic

**Data Handling During Failback:**
- **Default**: No upload (failover data lost) - **safe operation**
- **Optional**: Manual backup with double confirmation
- **Backup Format**: `failover_backup_20250824_163045.db` (timestamped)
- **Manual Merge**: Admin decides how to handle failover period data

---

## 🛠️ System Management

### Database Synchronization

```bash
# Manual database operations (when needed)
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

# Download latest database
python cli_db_sync.py download

# Check database info
python cli_db_sync.py info

# Create manual backup
python cli_db_sync.py backup custom_backup_name.db

# Test Dropbox connection
python cli_db_sync.py test
```

### Service Management

```bash
# Daily download service
sudo systemctl status ctv-pi2-download.service
sudo systemctl start ctv-pi2-download.service  # Manual run
tail -f /var/log/ctv-pi2-download/download.log  # Follow logs

# Flask application service
sudo systemctl status flaskapp
sudo systemctl stop flaskapp     # Safe shutdown during normal operations
sudo journalctl -u flaskapp -f   # Follow Flask logs

# Timer management
systemctl list-timers | grep ctv-pi2-download
sudo systemctl restart ctv-pi2-download.timer
```

### Code Updates

```bash
# Update mirror system code
cd /opt/apps/ctv-bookedbiz-db
git pull origin main

# Update Python dependencies if needed
source .venv/bin/activate
uv pip install -r requirements.txt

# Test scripts after updates
./scripts/failover-to-pi2.sh --dry-run  # Test mode (if implemented)
```

---

## 🔧 Troubleshooting

### Common Issues

**Daily Download Failing**
```bash
# Check timer status
sudo systemctl status ctv-pi2-download.timer

# View logs for errors
tail -50 /var/log/ctv-pi2-download/download.log

# Test Dropbox connection
python cli_db_sync.py test

# Manual download to diagnose
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate
python cli_db_sync.py download
```

**Failover Script Errors**
```bash
# Check database age if failover seems slow
stat /opt/apps/ctv-bookedbiz-db/data/database/production.db

# Database older than 36 hours - download fresh copy
python cli_db_sync.py download

# Flask service won't start
sudo systemctl status flaskapp
sudo journalctl -u flaskapp -n 50

# Test HTTP endpoint manually
curl http://localhost:8000/api/system-stats
```

**Network/SSH Issues**
```bash
# Test pi2 connectivity
ssh daseme@100.96.96.109

# Test pi-ctv health from pi2
curl -sf http://100.81.73.46:8000/api/system-stats

# Test GitHub access
ssh -T git@github.com
```

### System Recovery

**Complete Pi2 System Recovery**
```bash
# If pi2 needs rebuilding
ssh daseme@100.96.96.109
cd /opt/apps
sudo rm -rf ctv-bookedbiz-db  # Nuclear option

# Follow setup procedure from main documentation
git clone git@github.com:daseme/ctv-bookedbiz-db.git
cd ctv-bookedbiz-db
python3 -m venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Restore secrets and services
sudo cp /etc/ctv-db-sync.env /etc/ctv-db-sync.env.backup
# Re-copy from pi-ctv or restore from backup
```

---

## 📊 Monitoring Integration

### Control Station Alpha Dashboard

**Monitor These Metrics:**
- **Pi-CTV Health**: Primary Flask application status
- **Pi2 Database Age**: Freshness of backup database  
- **Daily Sync Status**: Success/failure of 2:30 AM downloads
- **Failover Readiness**: Pi2 system health and connectivity

### Kuma Professional Monitoring

**Configured Monitors:**
- **Pi-CTV API Endpoint**: Business Intelligence tier (immediate alerts)
- **Pi-CTV System Health**: Infrastructure tier (immediate alerts)  
- **Pi2 Control Station**: Self-monitoring capability
- **Pi2 Database Sync**: Daily backup system health

### Teams Alerting

**Alert Scenarios:**
1. **Pi-CTV Down**: Immediate Teams notification → Manual failover decision
2. **Daily Sync Failed**: Standard alert → Check logs and retry
3. **Pi2 System Issues**: Warning alert → Affects backup capability
4. **Database Staleness**: Daily warning if >36 hours old

---

## 🔮 Operational Procedures

### Weekly Maintenance

```bash
# Check system health (Mondays)
systemctl list-timers | grep ctv
sudo systemctl status ctv-pi2-download.timer flaskapp

# Review logs for issues
tail -100 /var/log/ctv-pi2-download/download.log
sudo journalctl -u ctv-pi2-download.service --since "1 week ago"

# Test failover capability (monthly)
./scripts/failover-to-pi2.sh    # Activate  
./scripts/failback-to-pi2.sh    # Deactivate
```

### Disaster Recovery Planning

**Recovery Time Objectives (RTO):**
- **Failover Activation**: 30 seconds
- **System Rebuild**: 2-4 hours (complete pi2 recreation)
- **Manual Data Recovery**: 30 minutes (from Dropbox backups)

**Recovery Point Objectives (RPO):**
- **Normal Operations**: 24 hours max (daily sync)
- **Emergency Failover**: Current time (live database)
- **Disaster Scenarios**: 24-48 hours (depends on backup frequency)

---

## 📋 Quick Reference

### Essential Commands

```bash
# Daily monitoring (run from pi2)
systemctl list-timers | grep ctv-pi2-download
tail -10 /var/log/ctv-pi2-download/download.log
ls -lah /opt/apps/ctv-bookedbiz-db/data/database/production.db*

# Emergency failover (pi-ctv down)
ssh daseme@100.96.96.109
cd /opt/apps/ctv-bookedbiz-db
./scripts/failover-to-pi2.sh

# Emergency failback (pi-ctv restored)  
./scripts/failback-to-pi-ctv.sh

# System health check
sudo systemctl status flaskapp ctv-pi2-download.timer
curl http://100.81.73.46:8000/api/system-stats    # Pi-ctv health
curl http://100.96.96.109:8000/api/system-stats   # Pi2 health (if active)
```

### Important File Locations

```bash
# Scripts
/opt/apps/ctv-bookedbiz-db/scripts/failover-to-pi2.sh
/opt/apps/ctv-bookedbiz-db/scripts/failback-to-pi2.sh  
/opt/apps/ctv-bookedbiz-db/bin/daily-download.sh

# Configuration  
/etc/systemd/system/ctv-pi2-download.service
/etc/systemd/system/ctv-pi2-download.timer
/etc/systemd/system/flaskapp.service
/etc/ctv-db-sync.env

# Data & Logs
/opt/apps/ctv-bookedbiz-db/data/database/production.db
/var/log/ctv-pi2-download/download.log

# Application
/opt/apps/ctv-bookedbiz-db/.venv/    # Python environment  
/opt/apps/ctv-bookedbiz-db/.env      # Project secrets
```

### Network & Access

```bash
# System Access
ssh daseme@100.96.96.109              # Pi2 SSH (Tailscale)
ssh daseme@100.81.73.46               # Pi-ctv SSH (Tailscale)

# Web Interfaces  
http://100.96.96.109:5001             # Control Station Alpha
http://100.96.96.109:3001             # Kuma Professional Monitoring
http://100.81.73.46:8000              # Pi-ctv Flask (primary)
http://100.96.96.109:8000             # Pi2 Flask (failover only)

# GitHub Repository
https://github.com/daseme/ctv-bookedbiz-db
git@github.com:daseme/ctv-bookedbiz-db.git
```

---

## 🦋 Architecture Philosophy

This mirror backup system exemplifies **Project Lepidoptera** principles:

**🔄 Transformation**: Evolved from basic backups to comprehensive business continuity  
**✨ Beauty**: Integrates seamlessly with Control Station Alpha's aesthetic  
**💪 Resilience**: Professional-grade failover with enterprise monitoring  
**📈 Growth**: Designed to scale with business infrastructure needs

**Clean Architecture Benefits:**
- **Single Responsibility**: Each component has one clear purpose
- **Dependency Injection**: Scripts use configurable paths and services
- **Separation of Concerns**: Data sync, monitoring, and failover are independent
- **Fail Fast**: Clear error handling with immediate feedback
- **Testability**: Each component can be tested and validated independently

---

**Status**: 🚀 **FULLY OPERATIONAL**  
**Last Updated**: Auto-maintained via backup system  
**Repository**: https://github.com/daseme/ctv-bookedbiz-db  
**Maintained By**: Control Station Alpha Infrastructure Team

*"Professional business continuity with the soul of mission control."*
