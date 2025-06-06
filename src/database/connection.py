# CRITICAL FIX: Update your database/connection.py file

"""Database connection and transaction management - FIXED."""

import sqlite3
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """FIXED: Manages database connections with proper transaction handling."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = None
    
    def connect(self) -> sqlite3.Connection:
        """Get or create database connection - FIXED to not reuse closed connections."""
        # CRITICAL FIX: Always create a new connection instead of reusing
        # The shared connection pattern was causing premature closure
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
    @contextmanager
    def transaction(self):
        """FIXED: Context manager for database transactions with proper connection management."""
        conn = self.connect()  # Always get a fresh connection
        try:
            conn.execute("BEGIN")
            yield conn
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
        finally:
            # CRITICAL FIX: Always close the connection we created
            conn.close()
    
    def close(self):
        """Close database connection - FIXED to handle already closed connections."""
        if self._connection:
            try:
                self._connection.close()
            except sqlite3.ProgrammingError:
                # Connection already closed, ignore
                pass
            self._connection = None