# 🚨 Railway Emergency Failover — CTV BookedBiz

**Last updated:** 2025-08-27

---

## 🆘 EMERGENCY ACTIVATION (Start Here)

When your main server is down, follow these steps to activate the Railway backup:

### 1. **Start the Service**
```bash
# Login to Railway CLI
railway login

# Link to the project (if not already linked)
railway link

# Scale up the service (if it's stopped/scaled to zero)
railway service scale --replicas 1
```

**OR use Railway Dashboard:**
- Go to [Railway Dashboard](https://railway.app/dashboard)
- Find `ctv-bookedbiz-db` service
- Settings → Change **Replicas** from `0` to `1`
- Wait for deployment to complete (~2-3 minutes)

### 2. **Verify It's Running**
```bash
# Check service status
railway status

# Get the public URL
railway domain
```

Your backup site should now be live at the provided Railway URL.

### 3. **Verify Database Sync**
The service automatically restores from Dropbox on startup. Check logs to confirm:
```bash
railway logs
```

Look for: `"No download needed (hash match)"` or `"Restored to /app/data/database/production.db"`

---

## 💰 SHUTDOWN AFTER EMERGENCY

**Important:** To avoid ongoing costs, shut down when main server is restored:

```bash
# Scale back to zero
railway service scale --replicas 0
```

**OR in Dashboard:** Settings → Change **Replicas** back to `0`

---

## ⚙️ Initial Setup (One-time)

*Only needed if setting up Railway for the first time*

### Required Files
Place these at your project root:

**`railway_startup.sh`**
```bash
#!/bin/bash
set -e

echo "=== Railway Startup: Hash-aware Dropbox restore ==="

# Ensure volume directory exists
mkdir -p /app/data/database

# Run the restore script
python railway_db_sync.py

# Start the application
echo "=== Starting application ==="
exec "$@"
```

**`railway_db_sync.py`**
```python
import os
import hashlib
import shutil
import tempfile
import dropbox
from dropbox.exceptions import AuthError, ApiError

def get_file_hash(filepath):
    """Get SHA256 hash of local file"""
    if not os.path.exists(filepath):
        return None
    
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def restore_from_dropbox():
    """Download DB from Dropbox only if content differs"""
    
    # Configuration
    local_db_path = os.getenv("RAILWAY_DB_PATH", "/app/data/database/production.db")
    dropbox_path = os.getenv("DROPBOX_DB_PATH", "/database.db")
    
    # Dropbox authentication
    access_token = os.getenv("DROPBOX_ACCESS_TOKEN")
    if not access_token:
        # Try refresh token flow
        app_key = os.getenv("DROPBOX_APP_KEY")
        app_secret = os.getenv("DROPBOX_APP_SECRET") 
        refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
        
        if not all([app_key, app_secret, refresh_token]):
            print("❌ Missing Dropbox credentials")
            return
            
        try:
            dbx = dropbox.Dropbox(
                app_key=app_key,
                app_secret=app_secret, 
                oauth2_refresh_token=refresh_token
            )
        except Exception as e:
            print(f"❌ Dropbox auth failed: {e}")
            return
    else:
        try:
            dbx = dropbox.Dropbox(access_token)
        except Exception as e:
            print(f"❌ Dropbox auth failed: {e}")
            return

    try:
        # Get remote file metadata
        metadata = dbx.files_get_metadata(dropbox_path)
        remote_hash = metadata.content_hash
        
        # Get local file hash
        local_hash = get_file_hash(local_db_path)
        
        if local_hash == remote_hash:
            print("✅ No download needed (hash match)")
            return
            
        print(f"🔄 Hash mismatch - downloading from Dropbox")
        print(f"   Local:  {local_hash or 'missing'}")
        print(f"   Remote: {remote_hash}")
        
        # Download to temporary file first (atomic operation)
        os.makedirs(os.path.dirname(local_db_path), exist_ok=True)
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            try:
                dbx.files_download_to_file(tmp_file.name, dropbox_path)
                # Atomic move
                shutil.move(tmp_file.name, local_db_path)
                print(f"✅ Restored to {local_db_path}")
            except Exception as e:
                os.unlink(tmp_file.name)  # Clean up temp file
                raise e
                
    except ApiError as e:
        print(f"❌ Dropbox API error: {e}")
    except Exception as e:
        print(f"❌ Restore failed: {e}")

if __name__ == "__main__":
    restore_from_dropbox()
```

### Railway Configuration

**Start Command:**
```bash
bash railway_startup.sh uvicorn src.web.asgi:app --host 0.0.0.0 --port $PORT
```

**Environment Variables:**
```
PYTHONPATH=/app
DROPBOX_APP_KEY=your_app_key
DROPBOX_APP_SECRET=your_app_secret  
DROPBOX_REFRESH_TOKEN=your_refresh_token
RAILWAY_DB_PATH=/app/data/database/production.db
DROPBOX_DB_PATH=/database.db
```

**Service Settings:**
- **Replicas:** `0` (keeps costs at zero until emergency)
- **Enable Serverless:** `OFF` (for persistent storage)
- **Auto Deploy:** `DISABLED` (prevents accidental activation)

### Persistent Storage
- Add volume in Railway Dashboard: Settings → Volumes
- Mount path: `/app/data`
- This preserves your SQLite database across deployments

---

## 🔍 Troubleshooting

**Service won't start:**
- Check Railway logs: `railway logs`
- Verify environment variables are set
- Ensure `PYTHONPATH=/app` is configured

**Database not syncing:**
- Look for Dropbox auth errors in logs
- Verify Dropbox tokens in Variables tab
- Check that `DROPBOX_DB_PATH` points to correct backup file

**Import errors:**
- Confirm start command: `bash railway_startup.sh uvicorn src.web.asgi:app --host 0.0.0.0 --port $PORT`
- Ensure `PYTHONPATH=/app` environment variable

**Costs running high:**
- Verify replicas are set to `0` when not in emergency mode
- Disable auto-deploy to prevent accidental activation

---

## 📋 Quick Reference

| Action | Command |
|--------|---------|
| **Emergency Start** | `railway service scale --replicas 1` |
| **Emergency Stop** | `railway service scale --replicas 0` |
| **Check Status** | `railway status` |
| **View Logs** | `railway logs` |
| **Get URL** | `railway domain` |
| **SSH Access** | `railway ssh` |

---

**⚠️ Security Note:** This Railway instance is publicly accessible. Your main production should remain on your private Tailscale network. Use Railway only as an emergency backup.