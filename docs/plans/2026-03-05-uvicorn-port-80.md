# Uvicorn Port 80 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move Uvicorn from port 8000 to port 80 so users access SpotOps at `http://spotops` without a port number.

**Architecture:** Two-line change to the existing systemd unit file. Add `CAP_NET_BIND_SERVICE` capability so the non-root `ctvbooked` user can bind port 80. Change `--port 8000` to `--port 80`. No new packages, no new processes, no new failure modes.

**Tech Stack:** systemd (existing), Uvicorn (existing)

---

## Prerequisites

- `sudo` access on `spotops` (Raspberry Pi, aarch64)
- Current setup: Uvicorn on `0.0.0.0:8000`, systemd service `ctv-bookedbiz-db.service`
- Port 80 is free (verified: `ss -tlnp | grep :80` returns nothing)

## Key file paths

| What | Path |
|------|------|
| Production systemd service | `/etc/systemd/system/ctv-bookedbiz-db.service` |
| Dev systemd service | `/etc/systemd/system/spotops-dev.service` (untouched) |

---

## Task 1: Edit the systemd service

**Files:**
- Modify: `/etc/systemd/system/ctv-bookedbiz-db.service`

The current service file:

```ini
[Unit]
Description=CTV BookedBiz DB Web
After=network.target

[Service]
Type=simple
User=ctvbooked
Group=ctvbooked
WorkingDirectory=/opt/apps/ctv-bookedbiz-db
EnvironmentFile=/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env
Environment=ENVIRONMENT=Production
ExecStart=/opt/venvs/ctv-bookedbiz-db/bin/uvicorn src.web.asgi:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

**Step 1: Edit the unit file**

```bash
sudo systemctl edit --full ctv-bookedbiz-db.service
```

Make two changes in the `[Service]` section:

1. Add this line (anywhere in `[Service]`):
```
AmbientCapabilities=CAP_NET_BIND_SERVICE
```

2. Change the port in `ExecStart`:
```
ExecStart=/opt/venvs/ctv-bookedbiz-db/bin/uvicorn src.web.asgi:app --host 0.0.0.0 --port 80
```

The full updated file should be:

```ini
[Unit]
Description=CTV BookedBiz DB Web
After=network.target

[Service]
Type=simple
User=ctvbooked
Group=ctvbooked
WorkingDirectory=/opt/apps/ctv-bookedbiz-db
EnvironmentFile=/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env
Environment=ENVIRONMENT=Production
AmbientCapabilities=CAP_NET_BIND_SERVICE
ExecStart=/opt/venvs/ctv-bookedbiz-db/bin/uvicorn src.web.asgi:app --host 0.0.0.0 --port 80
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

**Step 2: Reload systemd and restart**

```bash
sudo systemctl daemon-reload
sudo systemctl restart ctv-bookedbiz-db.service
```

**Step 3: Verify the service started**

```bash
systemctl status ctv-bookedbiz-db.service
```

Expected: `active (running)`. If it failed, check: `journalctl -u ctv-bookedbiz-db.service -n 20`

---

## Task 2: Verify end-to-end

**Step 1: Check port binding**

```bash
ss -tlnp | grep :80
```

Expected: `0.0.0.0:80` with uvicorn. Port 8000 should NOT appear.

**Step 2: Test locally**

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost/health
```

Expected: `200`.

**Step 3: Test via Tailscale hostname**

```bash
curl -s -o /dev/null -w "%{http_code}" http://spotops/health
```

Expected: `200`.

**Step 4: Test the app UI**

Open `http://spotops` in a browser. Verify:
- Page loads
- CSS/JS loads (no broken styles)
- Login works
- A report page renders

**Step 5: Verify old port is gone**

```bash
curl -s --connect-timeout 3 http://spotops:8000/health
```

Expected: Connection refused.

**Step 6: Verify dev service is unaffected**

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:5100/health
```

Expected: `200`.

---

## Task 3: Update references to port 8000

**Files:**
- Modify: `scripts/failover-to-pi2.sh:95`
- Modify: `scripts/failback-to-spotops.sh:66`

**Step 1: Update failover script**

In `scripts/failover-to-pi2.sh` line 95, change:
```
echo "• Pi2 Flask service: http://100.96.96.109:8000"
```
to:
```
echo "• Pi2 Flask service: http://100.96.96.109:8000"
```

Note: Pi2 is a separate machine — it may still run on port 8000. Only change this if Pi2 has also been moved to port 80. **Leave this alone for now.**

**Step 2: Update failback script**

In `scripts/failback-to-spotops.sh` line 66, change:
```
echo "• Spotops Flask service: http://100.99.11.55:8000"
```
to:
```
echo "• Spotops Flask service: http://100.99.11.55"
```

Run:
```bash
sed -i 's|http://100.99.11.55:8000|http://100.99.11.55|' scripts/failback-to-spotops.sh
```

**Step 3: Verify no other port 8000 references**

```bash
rg ":8000" docs/ scripts/ tasks/ CLAUDE.md --glob '!*nginx*'
```

Expected: Only Pi2 references (which stay on 8000) or plan files.

**Step 4: Update browser bookmarks**

On all client machines, change `http://spotops:8000` to `http://spotops`.

**Step 5: Commit the script change**

```bash
git add scripts/failback-to-spotops.sh
git commit -m "Update failback script URL to port 80"
```

---

## Rollback Plan

30-second rollback:

```bash
sudo systemctl edit --full ctv-bookedbiz-db.service
```

Revert the two changes:
1. Remove `AmbientCapabilities=CAP_NET_BIND_SERVICE`
2. Change `--port 80` back to `--port 8000`

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl restart ctv-bookedbiz-db.service
curl http://spotops:8000/health
```

---

## Verification Checklist

- [ ] `systemctl status ctv-bookedbiz-db` — active (running)
- [ ] `ss -tlnp | grep :80` — shows uvicorn on `0.0.0.0:80`
- [ ] `http://spotops` loads the app in browser (no port)
- [ ] `http://spotops/health` returns 200
- [ ] `http://spotops:8000` — connection refused
- [ ] `http://spotops:5100` (dev) — still works
- [ ] CSS/JS load correctly (no broken styles)
- [ ] Login flow works
- [ ] `scripts/failback-to-spotops.sh` updated
