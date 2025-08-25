# railway_db_sync.py - Simplified database sync for Railway

import os
import sys
import dropbox
from dropbox.exceptions import AuthError, ApiError

def download_database():
    """Download database from Dropbox for Railway deployment"""
    
    print("🔄 Starting Railway database download...")
    
    # Get Dropbox credentials from environment
    app_key = os.getenv('DROPBOX_APP_KEY')
    app_secret = os.getenv('DROPBOX_APP_SECRET')
    refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN')
    
    if not all([app_key, app_secret, refresh_token]):
        print("❌ Missing Dropbox credentials in environment variables")
        print("Required: DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN")
        return False
    
    try:
        # Create Dropbox client
        print("🔐 Authenticating with Dropbox...")
        dbx = dropbox.Dropbox(
            app_key=app_key,
            app_secret=app_secret,
            oauth2_refresh_token=refresh_token
        )
        
        # Test connection
        account = dbx.users_get_current_account()
        print(f"✅ Connected as: {account.email}")
        
        # Download database
        dropbox_path = "/database.db"  # Adjust this path as needed
        local_path = "/app/data/database/production.db"
        
        print(f"📥 Downloading {dropbox_path} to {local_path}...")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download file
        dbx.files_download_to_file(local_path, dropbox_path)
        
        # Check if file was downloaded
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            print(f"✅ Database downloaded successfully!")
            print(f"📊 File size: {file_size:,} bytes")
            return True
        else:
            print("❌ Download failed - file not found after download")
            return False
            
    except AuthError as e:
        print(f"❌ Dropbox authentication failed: {e}")
        return False
        
    except ApiError as e:
        if e.error.is_path_not_found():
            print(f"❌ Database file not found in Dropbox: {dropbox_path}")
            print("💡 Make sure the database has been uploaded from pi-ctv")
        else:
            print(f"❌ Dropbox API error: {e}")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_minimal_database():
    """Create a minimal database if download fails"""
    
    print("🗄️ Creating minimal database for Railway...")
    
    try:
        import sqlite3
        
        db_path = "/app/data/database/production.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        
        # Create basic tables for health check
        conn.execute('''
            CREATE TABLE IF NOT EXISTS health_check (
                id INTEGER PRIMARY KEY,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.execute("INSERT INTO health_check (status) VALUES ('healthy')")
        
        # Add any other critical tables your app needs
        conn.execute('''
            CREATE TABLE IF NOT EXISTS spots (
                id INTEGER PRIMARY KEY,
                customer_name TEXT,
                revenue REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ Minimal database created")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create minimal database: {e}")
        return False

if __name__ == "__main__":
    """Command line interface"""
    
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        success = download_database()
        if not success:
            print("🚨 Download failed, creating minimal database...")
            create_minimal_database()
    else:
        print("Usage: python railway_db_sync.py download")