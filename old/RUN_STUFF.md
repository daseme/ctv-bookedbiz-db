the local server:  PYTHONPATH=src uv run uvicorn web.asgi:app --reload --host 0.0.0.0 --port 8000

datasette:  uv run datasette "/mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/data/database/production.db" --host 0.0.0.0 --setting sql_time_limit_ms 30000

weekly update: ./src/importers/safe_weekly_import.sh "/mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/data/raw/weekly/weekly-250620.xlsx"