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

# Create Railway database sync Python script
RUN echo 'import os\n\
import sys\n\
import dropbox\n\
from dropbox.exceptions import AuthError, ApiError\n\
import sqlite3\n\
\n\
def download_database():\n\
    print("🔄 Starting Railway database download...")\n\
    app_key = os.getenv("DROPBOX_APP_KEY")\n\
    app_secret = os.getenv("DROPBOX_APP_SECRET")\n\
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")\n\
    \n\
    if not all([app_key, app_secret, refresh_token]):\n\
        print("❌ Missing Dropbox credentials")\n\
        return False\n\
    \n\
    try:\n\
        print("🔐 Authenticating with Dropbox...")\n\
        dbx = dropbox.Dropbox(app_key=app_key, app_secret=app_secret, oauth2_refresh_token=refresh_token)\n\
        account = dbx.users_get_current_account()\n\
        print(f"✅ Connected as: {account.email}")\n\
        \n\
        dropbox_path = "/database.db"\n\
        local_path = "/app/data/database/production.db"\n\
        print(f"📥 Downloading {dropbox_path}...")\n\
        os.makedirs(os.path.dirname(local_path), exist_ok=True)\n\
        dbx.files_download_to_file(local_path, dropbox_path)\n\
        \n\
        if os.path.exists(local_path):\n\
            file_size = os.path.getsize(local_path)\n\
            print(f"✅ Database downloaded! Size: {file_size:,} bytes")\n\
            return True\n\
        return False\n\
    except Exception as e:\n\
        print(f"❌ Download error: {e}")\n\
        return False\n\
\n\
def create_minimal_database():\n\
    print("🗄️ Creating minimal database...")\n\
    try:\n\
        db_path = "/app/data/database/production.db"\n\
        os.makedirs(os.path.dirname(db_path), exist_ok=True)\n\
        conn = sqlite3.connect(db_path)\n\
        conn.execute("CREATE TABLE IF NOT EXISTS health_check (id INTEGER PRIMARY KEY, status TEXT)")\n\
        conn.execute("INSERT INTO health_check (status) VALUES (\"healthy\")")\n\
        conn.execute("CREATE TABLE IF NOT EXISTS spots (id INTEGER PRIMARY KEY, customer_name TEXT, revenue REAL DEFAULT 0)")\n\
        conn.commit()\n\
        conn.close()\n\
        print("✅ Minimal database created")\n\
        return True\n\
    except Exception as e:\n\
        print(f"❌ Database creation failed: {e}")\n\
        return False\n\
\n\
if __name__ == "__main__":\n\
    print("🚂 Railway Database Setup Starting...")\n\
    os.makedirs("/app/data/database", exist_ok=True)\n\
    os.makedirs("/app/data/processed", exist_ok=True)\n\
    \n\
    if not os.path.exists("/app/data/database/production.db"):\n\
        print("🗄️ Database not found, attempting download...")\n\
        if not download_database():\n\
            print("🚨 Download failed, creating minimal database...")\n\
            create_minimal_database()\n\
    else:\n\
        print("✅ Database already exists")\n\
    \n\
    print("🚀 Database setup completed")\n\
' > /app/railway_db_setup.py

# Create simple startup script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "🚂 Railway Startup Script Starting..."\n\
python3 /app/railway_db_setup.py\n\
echo "🚀 Starting Flask application..."\n\
exec "$@"\n\
' > /app/railway_startup.sh

# Make scripts executable and set proper ownership
RUN chmod +x /app/railway_startup.sh /app/railway_db_setup.py && \
    chown ctvapp:ctvapp /app/railway_startup.sh /app/railway_db_setup.py

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