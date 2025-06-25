import dropbox
import os
from dotenv import load_dotenv

load_dotenv()

dbx = dropbox.Dropbox(os.getenv('DROPBOX_ACCESS_TOKEN'))

# Files to download
files_to_download = [
    'real_budget_data.json',
    'budget_data.json', 
    'pipeline_data.json',
    'review_sessions.json',
    'pipeline_decay.json'
]

# Create local directory
os.makedirs('data/processed', exist_ok=True)

# Download each file
for filename in files_to_download:
    dropbox_path = f'/data/processed/{filename}'
    local_path = f'data/processed/{filename}'
    
    try:
        print(f"Downloading {filename}...")
        dbx.files_download_to_file(local_path, dropbox_path)
        print(f"✓ Downloaded {filename}")
    except Exception as e:
        print(f"✗ Error downloading {filename}: {e}")
