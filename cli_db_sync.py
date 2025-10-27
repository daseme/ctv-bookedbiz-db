#!/usr/bin/env python3
"""
Dropbox Database Synchronization Script (Pi-ready)

USAGE:
  python cli_db_sync.py [COMMAND] [OPTIONS]

COMMANDS:
  test               Test connection to Dropbox and show account info
  info               Show database info for DROPBOX_DB_PATH
  list               List contents of app-root (or a path). Use no arg for root.
                     Example: python cli_db_sync.py list            # root
                              python cli_db_sync.py list backups    # /backups
  list-backups       List /backups/*.db with sizes
  upload             Upload DATABASE_PATH -> DROPBOX_DB_PATH (chunked if large)
                     Skips if content is identical (content_hash).
  download           Download DROPBOX_DB_PATH -> DATABASE_PATH
                     Skips if content is identical (content_hash).
  backup [name]      Create timestamped backup in /backups/ (or custom name)
  restore-latest     Restore newest /backups/*.db (or fallback /database.db)
                     Atomically replaces DATABASE_PATH and skips if identical.

ENVIRONMENT (.env, /etc/ctv-db-sync.env, etc.):
  # Preferred (long-lived)
  DROPBOX_APP_KEY=...
  DROPBOX_APP_SECRET=...
  DROPBOX_REFRESH_TOKEN=...

  # Optional (short-lived)
  DROPBOX_ACCESS_TOKEN=...

  # Paths
  DATABASE_PATH=/opt/apps/ctv-bookedbiz-db/data/database/production.db
  DROPBOX_DB_PATH=/database.db
"""

from __future__ import annotations

import os
import sys
import hashlib
import tempfile
from datetime import datetime
from typing import Optional, List

import dropbox
from dropbox.files import (
    WriteMode,
    UploadSessionCursor,
    CommitInfo,
    FileMetadata,
    FolderMetadata,
)
from dotenv import load_dotenv

load_dotenv()


# ───────────────────────────────────────────────────────────────────────────────
# Pure helpers (no side effects)
# ───────────────────────────────────────────────────────────────────────────────


def human_size(size_bytes: int) -> str:
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def compute_dropbox_content_hash(path: str) -> str:
    """
    Dropbox content hash spec:
      - SHA256 over each 4MB chunk
      - Concatenate those digests
      - SHA256 of the concatenation → hex digest
    """
    block_digests: List[bytes] = []
    with open(path, "rb") as f:
        while True:
            chunk = f.read(4 * 1024 * 1024)
            if not chunk:
                break
            h = hashlib.sha256()
            h.update(chunk)
            block_digests.append(h.digest())

    h_final = hashlib.sha256()
    for d in block_digests:
        h_final.update(d)
    return h_final.hexdigest()


def atomic_replace(src_tmp: str, dst_final: str) -> None:
    """Atomic replace (same filesystem)."""
    os.makedirs(os.path.dirname(dst_final), exist_ok=True)
    fd = os.open(src_tmp, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(src_tmp, dst_final)


def normalize_list_path(arg: Optional[str]) -> str:
    """
    API wants "" for root, not "/".
    We accept: None/""/"." → root.
    A bare name like "backups" → "/backups".
    A leading "/" → strip it (we operate within app root).
    """
    if not arg or arg in ("/", ".", "./"):
        return ""
    p = arg.strip()
    if p.startswith("/"):
        p = p[1:]
    return p


# ───────────────────────────────────────────────────────────────────────────────
# Dropbox client wrapper
# ───────────────────────────────────────────────────────────────────────────────


class DropboxDBSync:
    CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB
    LARGE_FILE_THRESHOLD = 100 * 1024 * 1024

    def __init__(self) -> None:
        # Auth
        self.app_key = os.getenv("DROPBOX_APP_KEY")
        self.app_secret = os.getenv("DROPBOX_APP_SECRET")
        self.refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
        self.access_token = os.getenv("DROPBOX_ACCESS_TOKEN")

        # Paths
        self.local_db_path = os.getenv("DATABASE_PATH")
        self.dropbox_db_path = os.getenv("DROPBOX_DB_PATH", "/database.db")

        if not self.access_token and not (
            self.refresh_token and self.app_key and self.app_secret
        ):
            raise ValueError(
                "No valid authentication found. Set either:\n"
                "1) DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET\n"
                "2) DROPBOX_ACCESS_TOKEN (temporary)"
            )

        self.dbx = self._create_client()

        if self.local_db_path:
            os.makedirs(os.path.dirname(self.local_db_path), exist_ok=True)

    # ── client ────────────────────────────────────────────────────────────────

    def _create_client(self) -> dropbox.Dropbox:
        if self.refresh_token and self.app_key and self.app_secret:
            print("🔄 Using refresh token for authentication")
            return dropbox.Dropbox(
                oauth2_refresh_token=self.refresh_token,
                app_key=self.app_key,
                app_secret=self.app_secret,
            )
        print("⚠️  Using access token (expires in 4 hours)")
        return dropbox.Dropbox(self.access_token)

    # ── metadata helpers ──────────────────────────────────────────────────────

    def _get_meta(self, path: str) -> Optional[FileMetadata]:
        try:
            md = self.dbx.files_get_metadata(path)
            if isinstance(md, FileMetadata):
                return md
            return None
        except dropbox.exceptions.ApiError as e:
            if hasattr(e.error, "is_path_not_found") and e.error.is_path_not_found():
                return None
            return None

    def _get_latest_backup_meta(self) -> Optional[FileMetadata]:
        try:
            resp = self.dbx.files_list_folder("/backups")
        except dropbox.exceptions.ApiError as e:
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
        # Lexicographic sort works due to YYYYMMDD_HHMMSS in names
        files.sort(key=lambda e: e.name, reverse=True)
        return files[0]

    # ── commands ──────────────────────────────────────────────────────────────

    def test_connection(self) -> bool:
        try:
            acct = self.dbx.users_get_current_account()
            print(f"✓ Connected to Dropbox as: {acct.name.display_name}")
            print(f"  Email: {acct.email}")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False

    def get_db_info(self) -> Optional[dict]:
        md = self._get_meta(self.dropbox_db_path)
        if not md:
            return None
        return {
            "name": md.name,
            "size": md.size,
            "size_formatted": human_size(md.size),
            "modified": md.server_modified.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def list_folder(self, path_arg: Optional[str] = None) -> List[str]:
        path = normalize_list_path(path_arg)
        try:
            resp = self.dbx.files_list_folder(path)
        except dropbox.exceptions.BadInputError as e:
            print(f"Error listing folder '{path_arg or '/'}': {e}")
            return []
        print(f"Contents of '{path_arg or '/'}' in Dropbox:")
        names: List[str] = []
        for entry in resp.entries:
            if isinstance(entry, FileMetadata):
                print(f"  📄 {entry.name} ({human_size(entry.size)})")
                names.append(entry.name)
            elif isinstance(entry, FolderMetadata):
                print(f"  📁 {entry.name}/")
                names.append(entry.name + "/")
        return names

    def _upload_large_file(self, file_path: str, dropbox_path: str) -> bool:
        size = os.path.getsize(file_path)
        print(f"📤 Large file detected ({human_size(size)})")
        print(f"   Using chunked upload with {human_size(self.CHUNK_SIZE)} chunks")

        try:
            with open(file_path, "rb") as f:
                start = self.dbx.files_upload_session_start(f.read(self.CHUNK_SIZE))
                cursor = UploadSessionCursor(
                    session_id=start.session_id, offset=f.tell()
                )

                chunk_num = 1
                while f.tell() < size:
                    remaining = size - f.tell()
                    to_read = min(self.CHUNK_SIZE, remaining)
                    chunk_num += 1
                    print(
                        f"   Uploading chunk {chunk_num} ({human_size(f.tell())}/{human_size(size)})"
                    )

                    if remaining <= self.CHUNK_SIZE:
                        self.dbx.files_upload_session_finish(
                            f.read(to_read),
                            cursor,
                            CommitInfo(path=dropbox_path, mode=WriteMode.overwrite),
                        )
                        break
                    else:
                        self.dbx.files_upload_session_append_v2(f.read(to_read), cursor)
                        cursor.offset = f.tell()
            print("✓ Large file upload completed")
            return True
        except Exception as e:
            print(f"✗ Error uploading large file: {e}")
            return False

    def upload_db(self) -> bool:
        """Upload local -> remote, skipping if identical by content hash."""
        try:
            if not os.path.exists(self.local_db_path):
                print(f"✗ Local database not found at {self.local_db_path}")
                return False

            remote = self._get_meta(self.dropbox_db_path)
            if remote:
                try:
                    local_hash = compute_dropbox_content_hash(self.local_db_path)
                    if local_hash == getattr(remote, "content_hash", None):
                        print(
                            "✓ Remote already up to date (content hash). Skipping upload."
                        )
                        return True
                except Exception:
                    pass

            size = os.path.getsize(self.local_db_path)
            print(f"Uploading database to Dropbox: {self.dropbox_db_path}")
            print(f"📊 File size: {human_size(size)}")

            if size > self.LARGE_FILE_THRESHOLD:
                return self._upload_large_file(self.local_db_path, self.dropbox_db_path)

            print("📤 Using simple upload")
            with open(self.local_db_path, "rb") as f:
                self.dbx.files_upload(
                    f.read(), self.dropbox_db_path, mode=WriteMode.overwrite
                )
            print("✓ Database uploaded to Dropbox")
            return True

        except Exception as e:
            print(f"✗ Error uploading: {e}")
            return False

    def download_db(self) -> bool:
        """Download remote -> local, skipping if identical by content hash."""
        try:
            print(f"Downloading database from Dropbox: {self.dropbox_db_path}")
            remote = self._get_meta(self.dropbox_db_path)
            if not remote:
                print(f"⚠ Database not found in Dropbox at {self.dropbox_db_path}")
                return False

            if os.path.exists(self.local_db_path):
                try:
                    local_hash = compute_dropbox_content_hash(self.local_db_path)
                    if local_hash == getattr(remote, "content_hash", None):
                        print(
                            "✓ Local DB already matches Dropbox (content hash). Skipping download."
                        )
                        return True
                except Exception:
                    pass

            print(f"📥 Downloading {human_size(remote.size)}")
            # download to temp then atomic replace
            tmp_dir = os.path.dirname(self.local_db_path) or "."
            fd, tmp_path = tempfile.mkstemp(prefix=".dl_tmp_", dir=tmp_dir)
            os.close(fd)
            try:
                self.dbx.files_download_to_file(tmp_path, self.dropbox_db_path)
                atomic_replace(tmp_path, self.local_db_path)
                print(f"✓ Database downloaded to {self.local_db_path}")
                return True
            except Exception as e:
                print(f"✗ Error downloading: {e}")
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                return False
        except Exception as e:
            print(f"✗ Unexpected error downloading: {e}")
            return False

    def backup_db(self, backup_name: Optional[str] = None) -> bool:
        """Create timestamped backup under /backups/ (or custom name)."""
        if not backup_name:
            backup_name = (
                f"database_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            )
        backup_path = f"/backups/{backup_name}"
        try:
            if not os.path.exists(self.local_db_path):
                print(f"✗ Local database not found at {self.local_db_path}")
                return False

            size = os.path.getsize(self.local_db_path)
            print(f"Creating backup: {backup_path}")
            print(f"📊 File size: {human_size(size)}")

            if size > self.LARGE_FILE_THRESHOLD:
                return self._upload_large_file(self.local_db_path, backup_path)

            with open(self.local_db_path, "rb") as f:
                self.dbx.files_upload(f.read(), backup_path)
            print(f"✓ Backup created: {backup_path}")
            return True
        except Exception as e:
            print(f"✗ Error creating backup: {e}")
            return False

    def list_backups(self) -> List[dict]:
        try:
            resp = self.dbx.files_list_folder("/backups")
        except dropbox.exceptions.ApiError as e:
            if e.error.is_path_not_found():
                return []
            raise
        rows = []
        for e in resp.entries:
            if isinstance(e, FileMetadata) and e.name.endswith(".db"):
                rows.append(
                    {
                        "name": e.name,
                        "size": e.size,
                        "size_formatted": human_size(e.size),
                    }
                )
        rows.sort(key=lambda x: x["name"], reverse=True)
        return rows

    def restore_latest(self) -> bool:
        """
        Restore newest /backups/*.db (preferred) or fallback to DROPBOX_DB_PATH.
        Atomically replaces DATABASE_PATH; skips if identical by content hash.
        """
        target = self.local_db_path
        if not target:
            print("✗ DATABASE_PATH not set")
            return False

        latest = self._get_latest_backup_meta()
        if latest:
            src_path = latest.path_lower
            src_label = f"/backups/{latest.name}"
            remote_hash = latest.content_hash
            size = latest.size
        else:
            md = self._get_meta(self.dropbox_db_path)
            if not md:
                print(
                    f"✗ No source found: neither /backups/ nor {self.dropbox_db_path}"
                )
                return False
            src_path = md.path_lower
            src_label = md.path_display
            remote_hash = getattr(md, "content_hash", None)
            size = md.size

        print(f"Restoring from: {src_label} ({human_size(size)})")

        if os.path.exists(target) and remote_hash:
            try:
                local_hash = compute_dropbox_content_hash(target)
                if local_hash == remote_hash:
                    print(
                        "✓ Local database already matches source (content hash). Skipping download."
                    )
                    return True
            except Exception:
                pass

        tmp_dir = os.path.dirname(target) or "."
        fd, tmp_path = tempfile.mkstemp(prefix=".restore_tmp_", dir=tmp_dir)
        os.close(fd)
        try:
            self.dbx.files_download_to_file(tmp_path, src_path)
            atomic_replace(tmp_path, target)
            print(f"✓ Restored to {target}")
            return True
        except Exception as e:
            print(f"✗ Restore failed: {e}")
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            return False


# ───────────────────────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────────────────────


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python cli_db_sync.py [test|info|list|list-backups|upload|download|backup|restore-latest]"
        )
        sys.exit(1)

    action = sys.argv[1].lower()

    try:
        sync = DropboxDBSync()
    except ValueError as e:
        print(f"✗ Authentication error: {e}")
        sys.exit(1)

    if action == "test":
        ok = sync.test_connection()
        sys.exit(0 if ok else 1)

    elif action == "info":
        info = sync.get_db_info()
        if info:
            print("Database info:")
            print(f"  Name: {info['name']}")
            print(f"  Size: {info['size_formatted']}")
            print(f"  Modified: {info['modified']}")
            sys.exit(0)
        print("No database found in Dropbox")
        sys.exit(1)

    elif action == "list":
        folder_arg = sys.argv[2] if len(sys.argv) > 2 else None
        sync.list_folder(folder_arg)
        sys.exit(0)

    elif action == "list-backups":
        backups = sync.list_backups()
        if backups:
            print("Available backups:")
            for b in backups:
                print(f"  {b['name']} ({b['size_formatted']})")
            sys.exit(0)
        print("No backups found")
        sys.exit(1)

    elif action == "upload":
        ok = sync.upload_db()
        sys.exit(0 if ok else 1)

    elif action == "download":
        ok = sync.download_db()
        sys.exit(0 if ok else 1)

    elif action == "backup":
        backup_name = sys.argv[2] if len(sys.argv) > 2 else None
        ok = sync.backup_db(backup_name)
        sys.exit(0 if ok else 1)

    elif action == "restore-latest":
        ok = sync.restore_latest()
        sys.exit(0 if ok else 1)

    else:
        print(
            "Invalid action. Use: test, info, list, list-backups, upload, download, backup, restore-latest"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
