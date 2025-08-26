# ---------- build ----------
FROM python:3.11-slim AS builder
WORKDIR /app

# build tooling
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# use a venv so runtime doesn't depend on user home
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt uvicorn[standard] dropbox python-dotenv

# ---------- runtime ----------
FROM python:3.11-slim
WORKDIR /app

# minimal runtime deps
RUN apt-get update && apt-get install -y --no-install-recommends curl sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# bring the venv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# copy app
COPY . /app

# ensure the volume mount point exists and is writable (volume will mount over it)
RUN mkdir -p /app/data && chmod 0777 /app/data

# --- DB bootstrap script (kept simple; you already have railway_db_sync.py) ---
# If you prefer your railway_db_sync.py, keep it and skip this block.
# This wrapper just calls your sync then starts the app.
RUN printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'echo "🚂 Railway Startup Script Starting..."' \
  'mkdir -p /app/data/database /app/data/processed' \
  'export DB_PATH="${DB_PATH:-/app/data/database/production.db}"' \
  'export DATABASE_PATH="$DB_PATH"' \
  'python /app/railway_db_sync.py download || true' \
  'echo "🚀 Starting ASGI..."' \
  'exec "$@"' \
  > /app/railway_startup.sh \
  && chmod +x /app/railway_startup.sh

# runtime env
ENV FLASK_ENV=production \
    PROJECT_ROOT=/app \
    DB_PATH=/app/data/database/production.db \
    DATABASE_PATH=/app/data/database/production.db \
    DATA_PATH=/app/data/processed \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    WEB_CONCURRENCY=1

# healthcheck: use your health blueprint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -fsS http://localhost:8000/health/ || exit 1

EXPOSE 8000

# run as root so the mounted /app/data is writable
USER root
ENTRYPOINT ["/app/railway_startup.sh"]

# IMPORTANT: single worker for SQLite; ASGI target matches your earlier logs
CMD ["uvicorn", "src.web.asgi:flask_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
