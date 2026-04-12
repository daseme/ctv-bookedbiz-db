## Docker Setup (Backblaze + Two Modes)

This guide runs the app with a Backblaze/Litestream-backed startup flow and two runtime modes:

- `replica_readonly` (recommended default; writes blocked in the web app)
- `failover_primary` (writes allowed for emergency takeover)

Secrets (B2 keys, Flask `SECRET_KEY`) should **not** be passed on the command line. Use **`/etc/ctv-litestream.env`** (same file as Litestream on the host) and `docker run --env-file /etc/ctv-litestream.env`.

---

## 1. Prerequisites

- Docker installed and running.
- Project root: `/opt/apps/ctv-bookedbiz-db`.
- Host directories for SQLite and processed data (defaults in this setup).
- `/etc/ctv-litestream.env` on the Docker host (B2 credentials and app env); optional host copy of `litestream.yml` is not required for the image (config is baked in at `/etc/litestream.yml` inside the container).

```bash
cd /opt/apps/ctv-bookedbiz-db
sudo mkdir -p /srv/spotops/db /srv/spotops/processed
```

## 2. Secure environment file (`/etc/ctv-litestream.env`)

Use the **same** host file for Litestream (B2 credentials) and for the Docker container: **`/etc/ctv-litestream.env`**. That way B2 keys live in one place and `docker run --env-file` loads the same variables Litestream expects.

Lock down permissions (if not already):

```bash
sudo chmod 600 /etc/ctv-litestream.env
sudo chown root:root /etc/ctv-litestream.env
```

**Notes**

- One file for host Litestream usage and for `--env-file`; same `KEY=value` lines (no duplicates across two paths).
- Never add `*.env` under `/etc/` to git.

---

## 3. What startup does

Container startup uses `backblaze_startup.sh` (Docker `ENTRYPOINT`).

1. Reads `APP_MODE` and optional restore.
2. If `RESTORE_ON_START=true`, runs Litestream restore to `DB_PATH` (Backblaze via `litestream.yml`).
3. Sets `READ_ONLY_MODE=true` for `replica_readonly`, or `false` for `failover_primary`.
4. Starts uvicorn.

---

## 4. Build the image

```bash
cd /opt/apps/ctv-bookedbiz-db
sudo docker build --no-cache -t bookedbiz-db .
```

---

## 5. Run in replica read-only mode (recommended)

```bash
sudo docker rm -f bookedbiz-db || true

sudo docker run -d \
  --name bookedbiz-db \
  --network host \
  --env-file /etc/ctv-litestream.env \
  -v /var/run/tailscale:/var/run/tailscale:ro \
  -v /srv/spotops/db:/srv/spotops/db \
  -v /srv/spotops/processed:/srv/spotops/processed \
  bookedbiz-db
```

Ensure `APP_MODE=replica_readonly` in the env file (or override with `-e APP_MODE=replica_readonly` once).

---

## 6. Run in failover primary mode

Use only when this instance must accept writes. Set in the env file:

```bash
APP_MODE=failover_primary
```

Then run the same `docker run` command as in section 5 (same `--env-file` and volumes).

---

## 7. Verify startup and restore

```bash
sudo docker ps | grep bookedbiz-db
sudo docker logs -f bookedbiz-db
```

Expected log lines:

- `Backblaze startup script`
- `Restoring DB from Backblaze via Litestream` (if restore runs)
- `Starting in replica_readonly mode` or `Starting in failover_primary mode`

Health check:

```bash
curl http://localhost:8000/health
```

---

## 8. Read-only behavior

When `APP_MODE=replica_readonly` (and `READ_ONLY_MODE=true`):

- `GET` routes should work.
- `POST` / `PUT` / `PATCH` / `DELETE` are blocked for the web/API read-only guard.

When `APP_MODE=failover_primary`, writes are allowed.

---

## 9. Tailscale login

- Use `--network host` and `-v /var/run/tailscale:/var/run/tailscale:ro`.
- Access from a Tailscale client, e.g. `http://spotops-pi:8000/` .

---

## 10. Common pitfalls

- **B2 errors in logs** (e.g. `InvalidAccessKeyId`): wrong or swapped `B2_ACCESS_KEY_ID` / `B2_SECRET_ACCESS_KEY`; ensure they match the Backblaze application key pair.
- **Restore skipped or failed**: check Litestream logs and that the bucket/path in `litestream.yml` matches your backup.
- **Wrong DB**: confirm host mounts match `DB_PATH` / `DATABASE_PATH` in the env file.
- **`whois` and `127.0.0.1`**: Tailscale identity may not resolve for localhost-only tests; use a real tailnet client IP.

---

## 11. Litestream: refresh the database from Backblaze (pull into the container)

The running app uses the **local** file on the mounted volume (e.g. host `/srv/spotops/db/production.db`). That file is updated from B2 when **`litestream restore`** runs. The default image only does that **once at container start** (if `RESTORE_ON_START=true`).

To **pull the latest replica from B2 again** without changing the image:

1. **Stop the app** so nothing holds SQLite open while the file is replaced:

   ```bash
   sudo docker stop bookedbiz-db
   ```

2. Run a **one-off restore** using the same image and env file, overriding the entrypoint so only Litestream runs (paths match `litestream.yml` inside the image):

   ```bash
   sudo docker run --rm \
     --entrypoint litestream \
     --env-file /etc/ctv-litestream.env \
     -v /srv/spotops/db:/srv/spotops/db \
     bookedbiz-db restore -if-replica-exists \
       -config /etc/litestream.yml \
       -o /srv/spotops/db/production.db \
       /srv/spotops/db/production.db
   ```

3. Start the app again:

   ```bash
   sudo docker start bookedbiz-db
   ```

**Important**

- This **overwrites** the local `production.db` with data rebuilt from B2. Any **local changes that never reached B2** (for example, writes on a failover host that never ran `replicate`) are **lost** when you restore from B2.
- Alternatively you can **`docker stop`** then **`docker start`** to rerun the full entrypoint; that also runs restore on boot if `RESTORE_ON_START=true`.

---

## 12. Failback: push `failover_primary` changes to Litestream (B2) and restore production

The container image **does not** start **`litestream replicate`** automatically. In **`failover_primary`** mode, writes land only in the **mounted** SQLite file until something uploads WAL/snapshots to B2.

### 12.1 Before you replicate (avoid two writers)

- **Only one “primary”** should replicate to the same B2 bucket/path (`litestream.yml`: `ctv-bookedbiz-wal` / `production`). If the main production machine still has **`litestream.service`** (or another replicate) running against that path, **stop the app and Litestream there first**, or you risk conflicting replicas.

### 12.2 Upload failover changes to B2 from the container

While the failover container is up and `APP_MODE=failover_primary`, start **continuous replication** inside it (second process; uvicorn keeps serving):

```bash
sudo docker exec -d bookedbiz-db \
  litestream replicate -config /etc/litestream.yml
```

Watch replication (Litestream logs to stderr; use Docker logs or `docker exec` to inspect):

```bash
sudo docker logs -f bookedbiz-db
```

Let it run until you are satisfied B2 has caught up (your `litestream.yml` also defines snapshot intervals; see Litestream docs for retention). When the original production host is ready, you can stop this replicate process by restarting the container (or `docker exec` kill if you know the PID—restart is simpler).

### 12.3 Restore **production** from B2

On the **main production host** (paths may differ from Docker; this project often uses `/var/lib/ctv-bookedbiz-db/production.db` and `/etc/litestream.yml` per `docs/GUIDE-failover-failback.md`):

1. Stop the app and Litestream there.
2. Run **`litestream restore`** with the same config/replica as Docker (B2 bucket/path must match `litestream.yml`).
3. Verify SQLite (`PRAGMA integrity_check;`), fix ownership if needed, then start **Litestream replicate** and the app again.

Exact commands for the bare-metal layout are in **`docs/GUIDE-failover-failback.md`** (“Recovery from Litestream”). Swap in your real `DATABASE_PATH` and systemd units.

### 12.4 After failback (Docker standby)

- Set **`APP_MODE=replica_readonly`** in `/etc/ctv-litestream.env` if this machine returns to standby.
- Ensure you are not still running a stray **`litestream replicate`** inside the container (restart the container clears ad hoc `docker exec` processes).

### 12.5 Optional: full-file copy instead of B2 round-trip

You can **`scp`/copy** the failover `production.db` to production and replace the file there, then restart services. That bypasses Litestream’s WAL history; use only if you understand the trade-offs (e.g. reinitializing or realigning Litestream on production). Prefer **replicate → restore from B2** when both hosts use Litestream for the same bucket path.

---

## 13. Stopping and removing the container

```bash
sudo docker stop bookedbiz-db
sudo docker rm bookedbiz-db
```

Host data under `/srv/spotops/db` and `/srv/spotops/processed` remains; `/etc/ctv-litestream.env` is unchanged.
