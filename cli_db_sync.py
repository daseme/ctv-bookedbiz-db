import dropbox
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

"""
Dropbox Database Synchronization Script

USAGE: python cli_db_sync.py [COMMAND] [OPTIONS]

AVAILABLE COMMANDS:
    test                    - Test connection to Dropbox and show account info
    download               - Download database from Dropbox to local storage
    upload                 - Upload local database to Dropbox (supports large files)
    backup [name]          - Create timestamped backup in Dropbox /backups/ folder
                            Optional: specify custom backup filename
    info                   - Show database information (name, size, last modified)
    list-backups          - List all available backups with file sizes
    list [folder]         - List contents of Dropbox folder (default: root)

EXAMPLES:
    python cli_db_sync.py.py test
    python cli_db_sync.py.py upload
    python cli_db_sync.py.py backup
    python cli_db_sync.py.py backup "pre_migration_backup.db"
    python cli_db_sync.py.py download
    python cli_db_sync.py.py info
    python cli_db_sync.py.py list-backups
    python cli_db_sync.py.py list /backups

ENVIRONMENT VARIABLES (set in .env file):
    # Option 1: Long-lived authentication (recommended)
    DROPBOX_APP_KEY=your_app_key
    DROPBOX_APP_SECRET=your_app_secret  
    DROPBOX_REFRESH_TOKEN=your_refresh_token

    # Option 2: Temporary authentication (4 hours)
    DROPBOX_ACCESS_TOKEN=sl.your_access_token

    # Database paths
    DATABASE_PATH=./local/database.db
    DROPBOX_DB_PATH=/database.db

FEATURES:
    - Automatic large file handling (chunked upload for files > 100MB)
    - Progress reporting for large uploads
    - Human-readable file sizes
    - Automatic token refresh (when using refresh token)
    - Comprehensive error handling
"""

class DropboxDBSync:
    def __init__(self):
        # Try refresh token first (recommended), fallback to access token
        self.app_key = os.getenv('DROPBOX_APP_KEY')
        self.app_secret = os.getenv('DROPBOX_APP_SECRET')
        self.refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN')
        self.access_token = os.getenv('DROPBOX_ACCESS_TOKEN')
        
        self.local_db_path = os.getenv('DATABASE_PATH')
        self.dropbox_db_path = os.getenv('DROPBOX_DB_PATH', '/database.db')
        
        # File size threshold for chunked upload (100MB)
        self.CHUNK_SIZE = 100 * 1024 * 1024  # 100MB chunks
        self.LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB threshold
        
        if not self.access_token and not (self.refresh_token and self.app_key and self.app_secret):
            raise ValueError(
                "No valid authentication found. Set either:\n"
                "1. DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET (recommended)\n"
                "2. DROPBOX_ACCESS_TOKEN (temporary)"
            )
        
        # Initialize Dropbox client
        self.dbx = self._create_dropbox_client()
        
        # Ensure local directory exists
        if self.local_db_path:
            os.makedirs(os.path.dirname(self.local_db_path), exist_ok=True)
    
    def _create_dropbox_client(self):
        """Create Dropbox client with refresh token or access token"""
        if self.refresh_token and self.app_key and self.app_secret:
            print("🔄 Using refresh token for authentication")
            return dropbox.Dropbox(
                oauth2_refresh_token=self.refresh_token,
                app_key=self.app_key,
                app_secret=self.app_secret
            )
        elif self.access_token:
            print("⚠️  Using access token (expires in 4 hours)")
            return dropbox.Dropbox(self.access_token)
    
    def _format_size(self, size_bytes):
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    def _upload_large_file(self, file_path, dropbox_path):
        """Upload large file using upload session"""
        file_size = os.path.getsize(file_path)
        print(f"📤 Large file detected ({self._format_size(file_size)})")
        print(f"   Using chunked upload with {self._format_size(self.CHUNK_SIZE)} chunks")
        
        try:
            with open(file_path, 'rb') as f:
                # Start upload session
                session_start_result = self.dbx.files_upload_session_start(
                    f.read(self.CHUNK_SIZE)
                )
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=session_start_result.session_id,
                    offset=f.tell()
                )
                
                # Upload remaining chunks
                chunk_num = 1
                while f.tell() < file_size:
                    remaining = file_size - f.tell()
                    chunk_size = min(self.CHUNK_SIZE, remaining)
                    chunk_num += 1
                    
                    print(f"   Uploading chunk {chunk_num} ({self._format_size(f.tell())}/{self._format_size(file_size)})")
                    
                    if remaining <= self.CHUNK_SIZE:
                        # Final chunk
                        commit_info = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode.overwrite)
                        self.dbx.files_upload_session_finish(
                            f.read(chunk_size), cursor, commit_info
                        )
                        break
                    else:
                        # Intermediate chunk
                        self.dbx.files_upload_session_append_v2(
                            f.read(chunk_size), cursor
                        )
                        cursor.offset = f.tell()
                
                print("✓ Large file upload completed")
                return True
                
        except Exception as e:
            print(f"✗ Error uploading large file: {e}")
            return False
    
    def test_connection(self):
        """Test Dropbox connection"""
        try:
            account = self.dbx.users_get_current_account()
            print(f"✓ Connected to Dropbox as: {account.name.display_name}")
            print(f"  Email: {account.email}")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    def download_db(self):
        """Download latest DB from Dropbox"""
        try:
            print(f"Downloading database from Dropbox: {self.dropbox_db_path}")
            
            # Get file info first
            try:
                metadata = self.dbx.files_get_metadata(self.dropbox_db_path)
                file_size = metadata.size
                print(f"📥 Downloading {self._format_size(file_size)}")
            except:
                pass  # Continue without size info
            
            self.dbx.files_download_to_file(self.local_db_path, self.dropbox_db_path)
            print(f"✓ Database downloaded to {self.local_db_path}")
            return True
        except dropbox.exceptions.ApiError as e:
            if e.error.is_path_not_found():
                print(f"⚠ Database not found in Dropbox at {self.dropbox_db_path}")
                print("This might be the first time running - you'll need to upload a database first")
                return False
            else:
                print(f"✗ Error downloading: {e}")
                return False
        except Exception as e:
            print(f"✗ Unexpected error downloading: {e}")
            return False
    
    def upload_db(self):
        """Upload local DB to Dropbox"""
        try:
            if not os.path.exists(self.local_db_path):
                print(f"✗ Local database not found at {self.local_db_path}")
                return False
            
            file_size = os.path.getsize(self.local_db_path)
            print(f"Uploading database to Dropbox: {self.dropbox_db_path}")
            print(f"📊 File size: {self._format_size(file_size)}")
            
            # Use chunked upload for large files
            if file_size > self.LARGE_FILE_THRESHOLD:
                return self._upload_large_file(self.local_db_path, self.dropbox_db_path)
            else:
                # Use simple upload for small files
                print("📤 Using simple upload")
                with open(self.local_db_path, 'rb') as f:
                    self.dbx.files_upload(
                        f.read(), 
                        self.dropbox_db_path, 
                        mode=dropbox.files.WriteMode.overwrite
                    )
                print("✓ Database uploaded to Dropbox")
                return True
                
        except Exception as e:
            print(f"✗ Error uploading: {e}")
            return False
    
    def backup_db(self, backup_name=None):
        """Create timestamped backup in Dropbox"""
        if not backup_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"database_backup_{timestamp}.db"
        
        backup_path = f"/backups/{backup_name}"
        
        try:
            if not os.path.exists(self.local_db_path):
                print(f"✗ Local database not found at {self.local_db_path}")
                return False
            
            file_size = os.path.getsize(self.local_db_path)
            print(f"Creating backup: {backup_path}")
            print(f"📊 File size: {self._format_size(file_size)}")
            
            # Use chunked upload for large backups
            if file_size > self.LARGE_FILE_THRESHOLD:
                return self._upload_large_file(self.local_db_path, backup_path)
            else:
                with open(self.local_db_path, 'rb') as f:
                    self.dbx.files_upload(f.read(), backup_path)
                print(f"✓ Backup created: {backup_path}")
                return True
                
        except Exception as e:
            print(f"✗ Error creating backup: {e}")
            return False
    
    def list_backups(self):
        """List all backups in Dropbox"""
        try:
            result = self.dbx.files_list_folder("/backups")
            backups = []
            for entry in result.entries:
                if entry.name.endswith('.db'):
                    size = getattr(entry, 'size', 0)
                    backups.append({
                        'name': entry.name,
                        'size': size,
                        'size_formatted': self._format_size(size)
                    })
            return sorted(backups, key=lambda x: x['name'], reverse=True)
        except dropbox.exceptions.ApiError as e:
            if e.error.is_path_not_found():
                return []
            else:
                raise e
    
    def get_db_info(self):
        """Get information about the database in Dropbox"""
        try:
            metadata = self.dbx.files_get_metadata(self.dropbox_db_path)
            return {
                'name': metadata.name,
                'size': metadata.size,
                'size_formatted': self._format_size(metadata.size),
                'modified': metadata.server_modified.strftime("%Y-%m-%d %H:%M:%S")
            }
        except dropbox.exceptions.ApiError as e:
            if hasattr(e.error, 'is_path_not_found') and e.error.is_path_not_found():
                return None
            elif str(e.error).startswith("GetMetadataError('path', LookupError('not_found'"):
                return None
            else:
                raise e
    
    def list_folder(self, path=""):
        """List contents of a folder in Dropbox"""
        try:
            result = self.dbx.files_list_folder(path)
            print(f"Contents of '{path or '/'}' in Dropbox:")
            for entry in result.entries:
                if hasattr(entry, 'size'):  # It's a file
                    size_str = self._format_size(entry.size)
                    print(f"  📄 {entry.name} ({size_str})")
                else:  # It's a folder
                    print(f"  📁 {entry.name}/")
            return [entry.name for entry in result.entries]
        except dropbox.exceptions.ApiError as e:
            print(f"Error listing folder '{path}': {e}")
            return []

def main():
    """Command line interface for database sync"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python db_sync.py [test|download|upload|backup|info|list-backups|list]")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    
    try:
        sync = DropboxDBSync()
    except ValueError as e:
        print(f"✗ Authentication error: {e}")
        sys.exit(1)
    
    if action == "test":
        sync.test_connection()
    elif action == "download":
        sync.download_db()
    elif action == "upload":
        sync.upload_db()
    elif action == "backup":
        backup_name = sys.argv[2] if len(sys.argv) > 2 else None
        sync.backup_db(backup_name)
    elif action == "info":
        info = sync.get_db_info()
        if info:
            print("Database info:")
            print(f"  Name: {info['name']}")
            print(f"  Size: {info['size_formatted']}")
            print(f"  Modified: {info['modified']}")
        else:
            print("No database found in Dropbox")
    elif action == "list-backups":
        backups = sync.list_backups()
        if backups:
            print("Available backups:")
            for backup in backups:
                print(f"  {backup['name']} ({backup['size_formatted']})")
        else:
            print("No backups found")
    elif action == "list":
        folder_path = sys.argv[2] if len(sys.argv) > 2 else ""
        sync.list_folder(folder_path)
    else:
        print("Invalid action. Use: test, download, upload, backup, info, list-backups, or list")

if __name__ == "__main__":
    main()