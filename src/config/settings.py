# src/config/settings.py
"""
Application configuration management.
Loads settings from environment variables with sensible defaults.
"""
import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database configuration."""
    db_path: str


@dataclass
class WebConfig:
    """Web server configuration."""
    secret_key: str
    debug: bool
    host: str
    port: int
    max_content_length: int


@dataclass
class ServicesConfig:
    """Services configuration."""
    data_path: str
    cache_enabled: bool
    cache_ttl: int


@dataclass
class Settings:
    """Application settings."""
    environment: str
    project_root: Path
    database: DatabaseConfig
    web: WebConfig
    services: ServicesConfig


def get_settings(environment: Optional[str] = None) -> Settings:
    """
    Get application settings based on environment.
    
    Args:
        environment: Optional environment name
        
    Returns:
        Settings configuration object
    """
    if environment is None:
        environment = os.getenv('FLASK_ENV', 'development')
    
    # Determine project root
    project_root = Path(os.getenv('PROJECT_ROOT', Path(__file__).parent.parent.parent))
    
    # Database configuration
    default_db_path = project_root / 'ctv-bookedbiz-db' / 'data' / 'database' / 'production.db'
    database = DatabaseConfig(
        db_path=os.getenv('DB_PATH', str(default_db_path))
    )
    
    # Web configuration
    web = WebConfig(
        secret_key=os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production'),
        debug=os.getenv('DEBUG', 'true' if environment == 'development' else 'false').lower() == 'true',
        host=os.getenv('HOST', '0.0.0.0'),
        port=int(os.getenv('PORT', '8000')),
        max_content_length=int(os.getenv('MAX_CONTENT_LENGTH', '16777216'))  # 16MB
    )
    
    # Services configuration
    default_data_path = project_root / 'ctv-bookedbiz-db' / 'data' / 'processed'
    services = ServicesConfig(
        data_path=os.getenv('DATA_PATH', str(default_data_path)),
        cache_enabled=os.getenv('CACHE_ENABLED', 'true').lower() == 'true',
        cache_ttl=int(os.getenv('CACHE_TTL', '300'))
    )
    
    return Settings(
        environment=environment,
        project_root=project_root,
        database=database,
        web=web,
        services=services
    )