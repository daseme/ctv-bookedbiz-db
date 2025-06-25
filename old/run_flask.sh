#!/bin/bash
# Direct Flask runner

export PROJECT_ROOT="/home/kolmstead/wsldev/ctv-bookedbiz-db"
# NEW (clean paths)
export DB_PATH="/mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/data/database/production.db"
export DATA_PATH="/mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/data/processed"
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH}"

echo "🚀 Starting CTV Reporting App (Flask Direct)"
cd "$PROJECT_ROOT"

# Run Flask directly
.venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
from web.app import create_development_app
app = create_development_app()
app.run(host='0.0.0.0', port=8000, debug=True)
"
