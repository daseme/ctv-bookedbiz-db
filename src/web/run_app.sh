#!/bin/bash
# run_app.sh - Script to start the CTV reporting app with proper environment

# Set environment variables
export PROJECT_ROOT="/home/kolmstead/wsldev/ctv-bookedbiz-db"
export DB_PATH="/home/kolmstead/wsldev/ctv-bookedbiz-db/ctv-bookedbiz-db/data/database/production.db"
export DATA_PATH="/home/kolmstead/wsldev/ctv-bookedbiz-db/ctv-bookedbiz-db/data/processed"
export FLASK_ENV="development"
export DEBUG="true"

# Add src to Python path
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH}"

echo "🚀 Starting CTV Reporting App"
echo "Project Root: $PROJECT_ROOT"
echo "Database: $DB_PATH"
echo "Data Path: $DATA_PATH"
echo "Python Path: $PYTHONPATH"

# Start the application
cd "$PROJECT_ROOT"
.venv/bin/python -m uvicorn src.web.asgi:asgi_app --host 0.0.0.0 --port 8000 --reload