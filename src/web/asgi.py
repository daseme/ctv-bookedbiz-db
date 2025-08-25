# src/web/asgi.py
"""
ASGI application for Uvicorn with proper WSGI adapter.
"""
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))

# Set environment variables
os.environ.setdefault('PROJECT_ROOT', str(project_root))
os.environ.setdefault('DB_PATH', str(project_root / 'data' / 'database' / 'production.db'))
os.environ.setdefault('DATA_PATH', str(project_root / 'data' / 'processed'))

# Import the Flask app and create ASGI adapter
from web.app import create_development_app
from asgiref.wsgi import WsgiToAsgi

# Create Flask app
environment = os.getenv('FLASK_ENV', 'production') 
flask_app = create_app(environment)

# Convert to ASGI
asgi_app = WsgiToAsgi(flask_app)

# Export the ASGI application
app = asgi_app