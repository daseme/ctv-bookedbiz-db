# CTV BookedBiz Git Workflow

**Simple rule:** Work on `dev`, promote to `main` when ready for production.

---

## The Two Branches

**`dev`** = Testing branch (runs on Pi at `:5100` with test database)  
**`main`** = Production branch (protected, requires PR to update)

---

## Daily Workflow (The Basics)

### 1. Start your work day
```bash
git switch dev
git pull --ff-only
```

### 2. Make your changes
- Edit files as needed
- Test locally

### 3. Commit your changes
```bash
git add -A
git commit -m "Brief description of what you changed"
git push origin dev
```

### 4. Test on Pi
```bash
# Restart dev service to pick up changes
systemctl --user restart ctv-dev.service

# Restart production service
sudo systemctl restart flaskapp

# watch logs if needed
sudo journalctl -u flaskapp -f

# Check if it's working
curl -sf http://pi-ctv:5100/health/ && echo "DEV_OK"
```

### 5. When ready for production
1. Go to GitHub
2. Create Pull Request: `dev` → `main`
3. **Important:** Use "Squash and merge" (not regular merge)
4. Delete the PR branch after merge

### 6. Deploy to production
```bash
# On production server (not Pi)
git switch main
git pull --ff-only
# Restart production service
```

---

## Branch Protection (What We Just Set Up)

✅ **`main` branch is now protected**  
- No direct pushes allowed  
- Must use Pull Requests  
- Forces our workflow: `dev` → `main`

---

## Quick Reference

| What | Where | Database | Port |
|------|-------|----------|------|
| Development/Testing | Pi | `production_dev.db` | :5100 |
| Production | Prod Server | `production.db` | Standard |

---

## When Things Go Wrong

**Dev service won't start?**
```bash
systemctl --user status ctv-dev.service
journalctl --user -u ctv-dev.service -f
```

**Can't push to main?**  
Good! That's the protection working. Use a PR instead.

**Need to sync dev database from production?**
```bash
systemctl --user stop ctv-dev.service
sqlite3 data/database/production.db ".backup 'data/database/production_dev.db'"
systemctl --user start ctv-dev.service
```

---

## That's It!

**Normal day:** Work on `dev` → test on `:5100` → commit → push  
**Ready for production:** PR `dev` → `main` → squash merge → deploy

The protection rules prevent accidents and keep production stable.