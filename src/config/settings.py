# src/config/settings.py
"""
Application configuration management.
Loads settings from environment variables with sensible defaults.

Backward-compatible priorities for DB selection:
1) DB_PATH (legacy) or DATABASE_PATH (legacy absolute path)
2) APP_ENV-aware: DEV_DB_PATH / PROD_DB_PATH
3) Fallback to repo default: data/database/production.db (prod) or production_dev.db (dev/test)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


# -------------------------- helpers (pure) --------------------------


def _norm_env_name(raw: Optional[str]) -> str:
    """
    Normalize environment name to one of: dev | prod | test
    Accepts FLASK_ENV compatibility.
    """
    if not raw:
        raw = os.getenv("APP_ENV") or os.getenv("FLASK_ENV") or "prod"
    raw = raw.lower().strip()
    if raw in {"development", "debug"}:
        return "dev"
    if raw in {"production", "release"}:
        return "prod"
    if raw not in {"dev", "prod", "test"}:
        return "prod"
    return raw


def _project_root() -> Path:
    return Path(
        os.getenv("PROJECT_ROOT", Path(__file__).parent.parent.parent)
    ).resolve()


def _default_db_path(env: str, root: Path) -> Path:
    dbdir = root / "data" / "database"
    return (
        (dbdir / "production_dev.db")
        if env in {"dev", "test"}
        else (dbdir / "production.db")
    )


def _choose_db_path(env: str, root: Path) -> str:
    """
    Selection order (first non-empty wins):
      1) DB_PATH
      2) DATABASE_PATH
      3) DEV_DB_PATH / PROD_DB_PATH depending on env
      4) repo defaults (production_dev.db for dev/test, production.db for prod)
    """
    db_path = os.getenv("DB_PATH")
    if db_path:
        return str(Path(db_path).expanduser())

    db_path_legacy = os.getenv("DATABASE_PATH")
    if db_path_legacy:
        return str(Path(db_path_legacy).expanduser())

    if env in {"dev", "test"}:
        p = os.getenv("DEV_DB_PATH")
        if p:
            return str(Path(p).expanduser())
    else:
        p = os.getenv("PROD_DB_PATH")
        if p:
            return str(Path(p).expanduser())

    return str(_default_db_path(env, root))


def _bool(var: str, default: bool = False) -> bool:
    val = os.getenv(var)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _int(var: str, default: int) -> int:
    try:
        return int(os.getenv(var, "").strip() or default)
    except Exception:
        return default


# -------------------------- dataclasses --------------------------


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


# -------------------------- public API --------------------------


def get_settings(environment: Optional[str] = None) -> Settings:
    """
    Get application settings based on environment.
    """
    env = _norm_env_name(environment)
    project_root = _project_root()

    # Database configuration (env-driven, backward compatible)
    database = DatabaseConfig(db_path=_choose_db_path(env, project_root))

    # Web configuration (keep your current defaults; port 8000 as you use)
    web = WebConfig(
        secret_key=os.getenv("SECRET_KEY", "dev-secret-key-change-in-production"),
        debug=_bool("DEBUG", env == "dev"),
        host=os.getenv("HOST", "0.0.0.0"),
        port=_int("PORT", 8000),
        max_content_length=_int("MAX_CONTENT_LENGTH", 16 * 1024 * 1024),  # 16MB
    )

    # Services configuration
    default_data_path = project_root / "data" / "processed"
    services = ServicesConfig(
        data_path=os.getenv("DATA_PATH", str(default_data_path)),
        cache_enabled=_bool("CACHE_ENABLED", True),
        cache_ttl=_int("CACHE_TTL", 300),
    )

    return Settings(
        environment=env,
        project_root=project_root,
        database=database,
        web=web,
        services=services,
    )


# -------------------------- domain-specific defaults --------------------------
# Sector assignment defaults (unchanged)
DEFAULT_CONFIDENCE_THRESHOLD: float = float(
    os.getenv("DEFAULT_CONFIDENCE_THRESHOLD", "0.90")
)
DEFAULT_BATCH_SIZE: int = _int("DEFAULT_BATCH_SIZE", 20)
