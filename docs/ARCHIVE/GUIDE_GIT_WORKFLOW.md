# CTV BookedBiz Git Workflow

**Simple rule:** Work on `dev`, promote to `main` when ready for production.

---

## The Two Branches

**`dev`** = Testing branch
**`main`** = Production branch (protected, requires PR to update)

Production is the Docker container `spotops-spotops-1` on `/opt/spotops`, reading the live DB at `/srv/spotops/db/production.db`.

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

### 4. Test on the box
```bash
cd /opt/spotops

# Rebuild + restart container to pick up Python changes
docker compose up -d --build spotops

# Watch logs
docker compose logs -f spotops

# Check it's serving
curl -sf http://localhost/health/ && echo "OK"
```

> Use `docker compose restart spotops` only when you've changed config/env, not code — `restart` won't pick up new Python.

### 5. When ready for production
1. Go to GitHub
2. Create Pull Request: `dev` → `main`
3. **Important:** Use "Squash and merge" (not regular merge)
4. Delete the PR branch after merge

### 6. Deploy to production
```bash
cd /opt/spotops
git switch main
git pull --ff-only
docker compose up -d --build spotops
```

---

## Branch Protection (What We Just Set Up)

✅ **`main` branch is now protected**  
- No direct pushes allowed  
- Must use Pull Requests  
- Forces our workflow: `dev` → `main`

---

## Quick Reference

| What | Where | Database |
|------|-------|----------|
| Production | `/opt/spotops`, container `spotops-spotops-1` | `/srv/spotops/db/production.db` |

---

## When Things Go Wrong

**Container won't start?**
```bash
docker compose -f /opt/spotops/docker-compose.yml ps
docker compose -f /opt/spotops/docker-compose.yml logs --tail=200 spotops
```

**Can't push to main?**
Good! That's the protection working. Use a PR instead.

**Need a fresh DB snapshot?**
The live DB is `/srv/spotops/db/production.db`. For a safe copy while the container is up, use SQLite's online backup:
```bash
sqlite3 /srv/spotops/db/production.db ".backup '/srv/spotops/db/snap-$(date +%F_%H%M).sqlite3'"
```

---

## That's It!

**Normal day:** Work on `dev` → commit → push
**Ready for production:** PR `dev` → `main` → squash merge → deploy

The protection rules prevent accidents and keep production stable.