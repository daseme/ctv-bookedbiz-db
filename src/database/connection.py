"""Database connection and transaction management."""

import sqlite3
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manages database connections with proper transaction handling."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = None
    
    def connect(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = sqlite3.connect(self.db_path)
            self._connection.row_factory = sqlite3.Row  # Enable dict-like access
            # Enable foreign key constraints
            self._connection.execute("PRAGMA foreign_keys = ON")
        return self._connection
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions with rollback on error."""
        conn = self.connect()
        try:
            conn.execute("BEGIN")
            yield conn
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None