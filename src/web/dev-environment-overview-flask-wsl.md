# 📊 Flask Reporting App — Environment Summary

This file summarizes the current development environment. Use this when asking for help building reporting views, templates, or APIs.

---

> I’m working on a Flask reporting app running in WSL2. The data directory is bind-mounted from Dropbox. Services like `PipelineService` manage JSON and SQLite files. I run Uvicorn directly via `.venv/bin/python` to avoid environment issues. Help me build out reporting views, templates, and API routes using this structure.

---

## 🖥️ Development Environment

- **Platform**: WSL2 (Ubuntu) on Windows
- **Python Env**: `.venv` created via `uv` (invoked via `.venv/bin/python`)
- **Python Version**: 3.10+
- **Key Stack**:
  - Flask (`flask==3.1.1`)
  - Uvicorn (via `asgiref`)
  - Jinja2 (HTML templates)
- **Startup Command**:
  ```bash
  .venv/bin/python -m uvicorn src.web.asgi:asgi_app --host 0.0.0.0 --port 8000 --reload


📂 Project Layout & Data

    Project Root:
    ~/wsldev/ctv-bookedbiz-db/

    Data Directory (bind-mounted from Dropbox):

        WSL path:
        ~/wsldev/ctv-bookedbiz-db/data/

        Backed by:
        /mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/data/

    Key Subdirectories:

        data/database/production.db — SQLite DB used by services

        data/processed/ — JSON files used by PipelineService

        src/web/app.py — Flask WSGI app entrypoint

        src/web/asgi.py — ASGI adapter for Uvicorn

        src/services/ — modular logic components:

            PipelineService

            BudgetService

            AEService

            CustomerService

🌐 Network & Access

    Access via Tailscale:

        Tailscale IP:
        tailscale ip -4

        Accessible from any tailnet device via:
        http://<tailscale-ip>:8000

    Runs fully from WSL (not the Windows host)