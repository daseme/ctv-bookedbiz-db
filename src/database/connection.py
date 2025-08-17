"""Database connection and transaction management - ENHANCED."""
import sqlite3
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """ENHANCED: Manages database connections with proper transaction handling and SQLite optimization."""
   
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = None
        self._is_configured = False
    
    def connect(self) -> sqlite3.Connection:
        """Get database connection with optimal SQLite settings applied."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        
        # CRITICAL: Apply optimal settings to EVERY connection
        self._apply_sqlite_settings(conn)
        
        return conn
    
    def _apply_sqlite_settings(self, conn: sqlite3.Connection):
        """Apply optimal SQLite settings to connection."""
        try:
            # Enable WAL mode for better concurrency (only needs to be set once globally)
            if not self._is_configured:
                conn.execute("PRAGMA journal_mode=WAL")
                self._is_configured = True
            
            # Apply these settings to every connection
            conn.execute("PRAGMA busy_timeout=30000")      # 30 second timeout
            conn.execute("PRAGMA foreign_keys=ON")         # Enable FK constraints
            conn.execute("PRAGMA synchronous=NORMAL")      # Faster than FULL, safer than OFF
            conn.execute("PRAGMA cache_size=10000")        # 10MB cache
            conn.execute("PRAGMA temp_store=MEMORY")       # Use memory for temp tables
            
        except sqlite3.Error as e:
            logger.warning(f"Failed to apply SQLite settings: {e}")
   
    @contextmanager
    def transaction(self):
        """Context manager for database transactions with proper connection management."""
        conn = self.connect()  # Always get a fresh connection with optimal settings
        try:
            conn.execute("BEGIN IMMEDIATE")  # IMMEDIATE prevents deadlocks better than default
            yield conn
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
        finally:
            conn.close()
    
    @contextmanager 
    def connection(self):
        """Context manager for simple connection (no transaction)."""
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()
   
    def close(self):
        """Close database connection - handles already closed connections."""
        if self._connection:
            try:
                self._connection.close()
            except sqlite3.ProgrammingError:
                pass  # Connection already closed, ignore
            self._connection = None
    
    def test_configuration(self):
        """Test and display current SQLite configuration."""
        with self.connection() as conn:
            settings = {}
            
            pragmas = [
                'journal_mode', 'busy_timeout', 'foreign_keys', 
                'synchronous', 'cache_size', 'temp_store'
            ]
            
            for pragma in pragmas:
                try:
                    cursor = conn.execute(f"PRAGMA {pragma}")
                    settings[pragma] = cursor.fetchone()[0]
                except:
                    settings[pragma] = "ERROR"
            
            print("📊 SQLite Configuration:")
            for setting, value in settings.items():
                print(f"   {setting}: {value}")
            
            return settings