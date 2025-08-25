# Multi-stage build for CTV BookedBiz Flask App
# Railway-ready with database sync capability
# Based on clean architecture principles

# Build stage - install dependencies
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies (including uvicorn for ASGI server and dropbox for db sync)
RUN pip install --no-cache-dir --user -r requirements.txt uvicorn[standard] dropbox python-dotenv

# Production stage - minimal runtime
FROM python:3.11-slim

# Create non-root user for security
RUN groupadd -g 1001 ctvapp && \
    useradd -r -u 1001 -g ctvapp ctvapp

# Set working directory
WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies from builder stage
COPY --from=builder /root/.local /home/ctvapp/.local

# Set PATH to include user-installed packages
ENV PATH="/home/ctvapp/.local/bin:$PATH"

# Copy application code
COPY --chown=ctvapp:ctvapp . .

# Create Railway startup script
RUN cat > /app/railway_startup.sh << 'EOF'
#!/bin/bash
# Railway startup script - Downloads database and starts the app
set -e  # Exit on any error

echo "🚂 Railway Startup Script Starting..."

# Create data directories if they don't exist
mkdir -p /app/data/database /app/data/processed /app/logs

echo "📁 Data directories created"

# Download database from Dropbox if it doesn't exist
if [ ! -f "/app/data/database/production.db" ]; then
    echo "🗄️ Database not found, downloading from Dropbox..."
    
    # Use Python to download database
    python3 << 'PYTHON_EOF'
import os
import sys
import dropbox
from dropbox.exceptions import AuthError, ApiError

def download_database():
    print("🔄 Starting Railway database download...")
    
    # Get Dropbox credentials from environment
    app_key = os.getenv('DROPBOX_APP_KEY')
    app_secret = os.getenv('DROPBOX_APP_SECRET')
    refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN')
    
    if not all([app_key, app_secret, refresh_token]):
        print("❌ Missing Dropbox credentials in environment variables")
        return False
    
    try:
        print("🔐 Authenticating with Dropbox...")
        dbx = dropbox.Dropbox(
            app_key=app_key,
            app_secret=app_secret,
            oauth2_refresh_token=refresh_token
        )
        
        account = dbx.users_get_current_account()
        print(f"✅ Connected as: {account.email}")
        
        # Download database
        dropbox_path = "/database.db"
        local_path = "/app/data/database/production.db"
        
        print(f"📥 Downloading {dropbox_path} to {local_path}...")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        dbx.files_download_to_file(local_path, dropbox_path)
        
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            print(f"✅ Database downloaded successfully!")
            print(f"📊 File size: {file_size:,} bytes")
            return True
        else:
            print("❌ Download failed - file not found after download")
            return False
            
    except Exception as e:
        print(f"❌ Database download error: {e}")
        return False

def create_minimal_database():
    print("🗄️ Creating minimal database for Railway...")
    try:
        import sqlite3
        db_path = "/app/data/database/production.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        conn.execute('''CREATE TABLE IF NOT EXISTS health_check (
            id INTEGER PRIMARY KEY,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.execute("INSERT INTO health_check (status) VALUES ('healthy')")
        
        # Basic spots table for compatibility
        conn.execute('''CREATE TABLE IF NOT EXISTS spots (
            id INTEGER PRIMARY KEY,
            customer_name TEXT,
            revenue REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        conn.commit()
        conn.close()
        print("✅ Minimal database created")
        return True
    except Exception as e:
        print(f"❌ Failed to create minimal database: {e}")
        return False

# Main execution
if not download_database():
    print("🚨 Download failed, creating minimal database...")
    create_minimal_database()
PYTHON_EOF

    if [ -f "/app/data/database/production.db" ]; then
        echo "✅ Database setup completed"
        echo "📊 Database size: $(du -h /app/data/database/production.db | cut -f1)"
    else
        echo "❌ Database setup failed"
        exit 1
    fi
else
    echo "✅ Database already exists"
fi

echo "🚀 Starting Flask application..."
exec "$@"
EOF

# Make startup script executable and set proper ownership
RUN chmod +x /app/railway_startup.sh && chown ctvapp:ctvapp /app/railway_startup.sh

# Create necessary directories with proper permissions
RUN mkdir -p data/database data/processed logs && \
    chown -R ctvapp:ctvapp data/ logs/ && \
    chmod 755 data/ logs/

# Switch to non-root user
USER ctvapp

# Set environment variables for production
ENV FLASK_ENV=production \
    PROJECT_ROOT=/app \
    DATABASE_PATH=/app/data/database/production.db \
    DATA_PATH=/app/data/processed \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/api/system-stats || exit 1

# Expose port
EXPOSE 8000

# Use startup script as entrypoint
ENTRYPOINT ["/app/railway_startup.sh"]

# Use Uvicorn ASGI server (matches pi-ctv and pi2 production setup)
CMD ["uvicorn", "src.web.asgi:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]