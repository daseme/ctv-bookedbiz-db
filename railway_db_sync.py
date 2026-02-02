#!/usr/bin/env python3
# railway_db_sync.py — Restore DB in Railway from Dropbox (newest backup preferred)

import os
import sys
import hashlib
import tempfile
from typing import Optional, List

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata


# ==== Pure helpers ============================================================


def human_size(n: int) -> str:
    s = float(n)
    for u in ("B", "KB", "MB", "GB"):
        if s < 1024:
            return f"{s:.1f} {u}"
        s /= 1024
    return f"{s:.1f} TB"


def compute_dropbox_content_hash(path: str) -> str:
    """
    Dropbox content hash:
      - SHA256 over each 4MB block;
      - concat block digests;
      - SHA256 of the concat => hex digest
    """
    blocks: List[bytes] = []
    with open(path, "rb") as f:
        while True:
            chunk = f.read(4 * 1024 * 1024)
            if not chunk:
                break
            h = hashlib.sha256()
            h.update(chunk)
            blocks.append(h.digest())
    hfin = hashlib.sha256()
    for d in blocks:
        hfin.update(d)
    return hfin.hexdigest()


def atomic_replace(src_tmp: str, dst_final: str) -> None:
    os.makedirs(os.path.dirname(dst_final), exist_ok=True)
    fd = os.open(src_tmp, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(src_tmp, dst_final)


# ==== Dropbox access ==========================================================


def make_dbx() -> dropbox.Dropbox:
    app_key = os.getenv("DROPBOX_APP_KEY")
    app_secret = os.getenv("DROPBOX_APP_SECRET")
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    access_token = os.getenv("DROPBOX_ACCESS_TOKEN")  # optional fallback

    if refresh_token and app_key and app_secret:
        return dropbox.Dropbox(
            app_key=app_key,
            app_secret=app_secret,
            oauth2_refresh_token=refresh_token,
        )
    if access_token:
        return dropbox.Dropbox(access_token)
    raise RuntimeError(
        "Missing Dropbox creds: need refresh token+app keys or access token."
    )


def get_latest_backup_meta(dbx: dropbox.Dropbox) -> Optional[FileMetadata]:
    try:
        resp = dbx.files_list_folder("/backups")
    except ApiError as e:
        if e.error.is_path_not_found():
            return None
        raise
    files = [
        e
        for e in resp.entries
        if isinstance(e, FileMetadata) and e.name.endswith(".db")
    ]
    if not files:
        return None
    files.sort(
        key=lambda e: e.name, reverse=True
    )  # names are timestamped; lexicographic works
    return files[0]


def get_main_db_meta(
    dbx: dropbox.Dropbox, path="/database.db"
) -> Optional[FileMetadata]:
    try:
        md = dbx.files_get_metadata(path)
        return md if isinstance(md, FileMetadata) else None
    except ApiError as e:
        if hasattr(e.error, "is_path_not_found") and e.error.is_path_not_found():
            return None
        raise


# ==== Restore logic ===========================================================


def restore_database() -> bool:
    """
    Railway restore policy:
      1) If /backups/ contains timestamped DBs, download the newest.
      2) Else, fall back to /database.db.
      3) Write atomically to /app/data/database/production.db.
      4) Skip if identical (Dropbox content_hash matches local).
    """
    print("🔄 Starting Railway database restore...")

    # Paths in Railway container
    local_path = os.getenv("RAILWAY_DB_PATH", "/app/data/database/production.db")
    main_remote_path = os.getenv("DROPBOX_DB_PATH", "/database.db")  # matches Pi setup

    try:
        print("🔐 Authenticating with Dropbox...")
        dbx = make_dbx()
        acct = dbx.users_get_current_account()
        print(f"✅ Connected as: {acct.email}")
    except Exception as e:
        print(f"❌ Dropbox auth failed: {e}")
        return False

    # Pick source (newest backup preferred)
    src_label = None
    remote_hash = None
    size = None
    src_path = None

    latest = get_latest_backup_meta(dbx)
    if latest:
        src_path = latest.path_lower
        src_label = f"/backups/{latest.name}"
        remote_hash = latest.content_hash
        size = latest.size
    else:
        md = get_main_db_meta(dbx, main_remote_path)
        if not md:
            print(f"❌ No source DB found: neither backups nor {main_remote_path}")
            return False
        src_path = md.path_lower
        src_label = md.path_display
        remote_hash = getattr(md, "content_hash", None)
        size = md.size

    print(f"📥 Source: {src_label} ({human_size(size)})")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # No-op if identical
    if os.path.exists(local_path) and remote_hash:
        try:
            local_hash = compute_dropbox_content_hash(local_path)
            if local_hash == remote_hash:
                print(
                    "✓ Local database already matches source (content hash). No download needed."
                )
                return True
        except Exception:
            pass  # continue to download

    # Download to temp, then atomic replace
    fd, tmp_path = tempfile.mkstemp(
        prefix=".railway_restore_", dir=os.path.dirname(local_path)
    )
    os.close(fd)
    try:
        print("⬇️  Downloading to temp file...")
        dbx.files_download_to_file(tmp_path, src_path)
        atomic_replace(tmp_path, local_path)
        final_size = os.path.getsize(local_path)
        print(f"✅ Restored to {local_path} ({human_size(final_size)})")
        return True
    except Exception as e:
        print(f"❌ Restore failed: {e}")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return False


def create_minimal_database() -> bool:
    """Create a minimal DB as a fallback if restore fails."""
    print("🗄️ Creating minimal database for Railway...")
    try:
        import sqlite3

        db_path = os.getenv("RAILWAY_DB_PATH", "/app/data/database/production.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS health_check (
          id INTEGER PRIMARY KEY,
          status TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute("INSERT INTO health_check (status) VALUES ('healthy')")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS spots (
          id INTEGER PRIMARY KEY,
          customer_name TEXT,
          revenue REAL DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()
        conn.close()
        print("✅ Minimal database created")
        return True
    except Exception as e:
        print(f"❌ Failed to create minimal database: {e}")
        return False


# ==== CLI =====================================================================


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        ok = restore_database()
        if not ok:
            print("🚨 Restore failed, creating minimal database...")
            create_minimal_database()
        sys.exit(0 if ok else 1)
    else:
        print("Usage: python railway_db_sync.py download")
        sys.exit(2)


if __name__ == "__main__":
    main()
