# src/web/asgi.py
"""
ASGI entrypoint: wrap Flask (WSGI) for Uvicorn.
"""

import os
from uvicorn.middleware.wsgi import WSGIMiddleware
from src.web.app import create_app  # PYTHONPATH should include /app

# Respect service env; don't override if already set by Railway
ENV = os.getenv("ENVIRONMENT") or os.getenv("FLASK_ENV") or "production"

# Create the Flask app (WSGI)
flask_app = create_app(ENV)

# Wrap into ASGI for Uvicorn
app = WSGIMiddleware(flask_app)
