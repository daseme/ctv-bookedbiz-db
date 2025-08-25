#!/bin/bash
# railway_startup.sh - Downloads database and starts the app

set -e  # Exit on any error

echo "🚂 Railway Startup Script Starting..."

# Create data directory if it doesn't exist
mkdir -p /app/data/database
mkdir -p /app/data/processed

echo "📁 Data directories created"

# Download database from Dropbox if it doesn't exist
if [ ! -f "/app/data/database/production.db" ]; then
    echo "🗄️ Database not found, downloading from Dropbox..."
    
    # Use your existing db sync script
    cd /app
    python cli_db_sync.py download
    
    if [ -f "/app/data/database/production.db" ]; then
        echo "✅ Database downloaded successfully"
        echo "📊 Database size: $(du -h /app/data/database/production.db)"
    else
        echo "❌ Database download failed"
        echo "🚨 Creating empty database..."
        # Create minimal database structure if download fails
        python -c "
import sqlite3
import os
os.makedirs('/app/data/database', exist_ok=True)
conn = sqlite3.connect('/app/data/database/production.db')
conn.execute('CREATE TABLE IF NOT EXISTS health_check (id INTEGER PRIMARY KEY, status TEXT)')
conn.execute('INSERT INTO health_check (status) VALUES (\"healthy\")')
conn.commit()
conn.close()
print('✅ Minimal database created')
"
    fi
else
    echo "✅ Database already exists"
fi

echo "🚀 Starting Flask application..."

# Start the application
exec "$@"