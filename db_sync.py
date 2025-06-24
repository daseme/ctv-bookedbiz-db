import dropbox
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class DropboxDBSync:
    def __init__(self):
        self.access_token = os.getenv('DROPBOX_ACCESS_TOKEN')
        self.local_db_path = os.getenv('DATABASE_PATH')
        self.dropbox_db_path = os.getenv('DROPBOX_DB_PATH', '/database.db')
        
        if not self.access_token:
            raise ValueError("DROPBOX_ACCESS_TOKEN not found in environment variables")
        
        self.dbx = dropbox.Dropbox(self.access_token)
        
        # Ensure local directory exists
        os.makedirs(os.path.dirname(self.local_db_path), exist_ok=True)
    
    def download_db(self):
        """Download latest DB from Dropbox"""
        try:
            print(f"Downloading database from Dropbox: {self.dropbox_db_path}")
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
                
            print(f"Uploading database to Dropbox: {self.dropbox_db_path}")
            with open(self.local_db_path, 'rb') as f:
                self.dbx.files_upload(
                    f.read(), 
                    self.dropbox_db_path, 
                    mode=dropbox.files.WriteMode.overwrite
                )
            print(f"✓ Database uploaded to Dropbox")
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
                
            print(f"Creating backup: {backup_path}")
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
            backups = [entry.name for entry in result.entries if entry.name.endswith('.db')]
            return sorted(backups, reverse=True)  # Most recent first
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
                'modified': metadata.server_modified.strftime("%Y-%m-%d %H:%M:%S")
            }
        except dropbox.exceptions.ApiError as e:
            # Handle "not found" error properly
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
                    print(f"  📄 {entry.name} ({entry.size} bytes)")
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
        print("Usage: python db_sync.py [download|upload|backup|info|list-backups|list]")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    sync = DropboxDBSync()
    
    if action == "download":
        sync.download_db()
    elif action == "upload":
        sync.upload_db()
    elif action == "backup":
        backup_name = sys.argv[2] if len(sys.argv) > 2 else None
        sync.backup_db(backup_name)
    elif action == "info":
        info = sync.get_db_info()
        if info:
            print(f"Database info:")
            print(f"  Name: {info['name']}")
            print(f"  Size: {info['size']} bytes")
            print(f"  Modified: {info['modified']}")
        else:
            print("No database found in Dropbox")
    elif action == "list-backups":
        backups = sync.list_backups()
        if backups:
            print("Available backups:")
            for backup in backups:
                print(f"  {backup}")
        else:
            print("No backups found")
    elif action == "list":
        folder_path = sys.argv[2] if len(sys.argv) > 2 else ""
        sync.list_folder(folder_path)
    else:
        print("Invalid action. Use: download, upload, backup, info, list-backups, or list")

if __name__ == "__main__":
    main()