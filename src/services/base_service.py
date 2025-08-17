#!/usr/bin/env python3
"""
Base service class with consistent transaction management patterns.
Solves nested transaction issues and provides safe transaction handling.
"""

import sys
import sqlite3
import logging
import functools
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Any, Callable, TypeVar, Union
from abc import ABC, abstractmethod

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

# Type variables for decorators
F = TypeVar('F', bound=Callable[..., Any])


class TransactionError(Exception):
    """Raised when there's an error with transaction management."""
    pass


class BaseService(ABC):
    """
    Base service class providing consistent transaction management patterns.
    
    Features:
    - Automatic transaction state detection
    - Safe transaction handling (no nested transactions)
    - Consistent error handling and logging
    - Transaction-aware decorators
    - Compatible with existing DatabaseConnection class
    
    Usage Patterns:
    
    1. Basic transaction usage:
        ```python
        class MyService(BaseService):
            def do_work(self):
                with self.safe_transaction() as conn:
                    conn.execute("INSERT INTO table VALUES (?)", (value,))
        ```
    
    2. Transaction-aware methods:
        ```python
        class MyService(BaseService):
            @transaction_required
            def method_needs_transaction(self, conn):
                # This method requires an active transaction
                conn.execute("UPDATE table SET ...")
            
            @auto_transaction
            def method_with_auto_transaction(self):
                # This method automatically handles transactions
                conn = self.get_connection()
                conn.execute("INSERT INTO table ...")
        ```
    
    3. Checking transaction state:
        ```python
        if self.in_transaction:
            # We're already in a transaction, don't create nested one
            conn = self.get_connection()
        else:
            # Safe to create new transaction
            with self.safe_transaction() as conn:
                pass
        ```
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize base service with database connection.
        
        Args:
            db_connection: DatabaseConnection instance for database operations
        """
        self.db = db_connection
        self._current_connection: Optional[sqlite3.Connection] = None
        self._in_transaction: bool = False
        self._transaction_depth: int = 0
    
    @property
    def in_transaction(self) -> bool:
        """
        Check if we're currently in an active transaction.
        
        Returns:
            True if in active transaction, False otherwise
        """
        # Simple flag-based detection - more reliable than SQLite introspection
        return self._in_transaction and self._current_connection is not None
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get database connection, reusing existing connection if in transaction.
        
        Returns:
            SQLite connection instance
            
        Note:
            If already in a transaction, returns the existing connection.
            Otherwise, creates a new connection.
        """
        if self._current_connection is not None:
            return self._current_connection
        
        return self.db.connect()
    
    @contextmanager
    def safe_transaction(self):
        """
        Context manager for safe transaction handling.
        
        Features:
        - Detects existing transactions to prevent nesting
        - Handles transaction lifecycle properly
        - Provides consistent error handling
        - Logs transaction operations
        
        Yields:
            sqlite3.Connection: Database connection within transaction
            
        Examples:
            ```python
            # Basic usage
            with self.safe_transaction() as conn:
                conn.execute("INSERT INTO table VALUES (?)", (value,))
            
            # Error handling
            try:
                with self.safe_transaction() as conn:
                    conn.execute("UPDATE table SET ...")
                    if error_condition:
                        raise ValueError("Business rule violation")
            except ValueError:
                # Transaction automatically rolled back
                logger.error("Business rule error occurred")
            ```
        """
        # Check if we're already in a transaction
        if self._current_connection is not None and self._in_transaction:
            logger.debug("Already in transaction, reusing existing connection")
            yield self._current_connection
            return
        
        # Create new transaction
        conn = self.db.connect()
        self._current_connection = conn
        self._transaction_depth += 1
        
        transaction_id = f"txn_{id(conn)}"
        logger.debug(f"Starting transaction {transaction_id} (depth: {self._transaction_depth})")
        
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._in_transaction = True
            yield conn
            conn.commit()
            logger.debug(f"Transaction {transaction_id} committed successfully")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction {transaction_id} rolled back due to error: {e}")
            raise
            
        finally:
            self._transaction_depth -= 1
            if self._transaction_depth == 0:
                self._current_connection = None
                self._in_transaction = False
            conn.close()
            logger.debug(f"Transaction {transaction_id} closed (remaining depth: {self._transaction_depth})")
    
    @contextmanager
    def safe_connection(self):
        """
        Context manager for safe connection handling without transaction.
        
        Use this when you need a connection but don't want transaction overhead.
        
        Yields:
            sqlite3.Connection: Database connection
            
        Example:
            ```python
            with self.safe_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM table")
                count = cursor.fetchone()[0]
            ```
        """
        # If we're in a transaction, reuse the connection
        if self._current_connection is not None:
            yield self._current_connection
            return
        
        # Create new connection for read-only operations
        conn = self.db.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_in_transaction(self, operation: Callable[[sqlite3.Connection], Any]) -> Any:
        """
        Execute an operation within a safe transaction.
        
        Args:
            operation: Function that takes a connection and performs database operations
            
        Returns:
            Result of the operation
            
        Example:
            ```python
            def insert_data(conn):
                cursor = conn.execute("INSERT INTO table VALUES (?)", (value,))
                return cursor.lastrowid
            
            new_id = self.execute_in_transaction(insert_data)
            ```
        """
        with self.safe_transaction() as conn:
            return operation(conn)
    
    def is_connection_valid(self, conn: sqlite3.Connection) -> bool:
        """
        Check if a database connection is valid and usable.
        
        Args:
            conn: Connection to validate
            
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            conn.execute("SELECT 1").fetchone()
            return True
        except (sqlite3.Error, sqlite3.ProgrammingError):
            return False
    
    def get_transaction_info(self) -> dict:
        """
        Get information about current transaction state.
        
        Returns:
            Dictionary with transaction state information
        """
        return {
            'in_transaction': self.in_transaction,
            'transaction_depth': self._transaction_depth,
            'has_connection': self._current_connection is not None,
            'connection_valid': (
                self.is_connection_valid(self._current_connection) 
                if self._current_connection else False
            )
        }


# Decorators for transaction management
def transaction_required(func: F) -> F:
    """
    Decorator that ensures a method is called within an active transaction.
    
    The decorated method should accept a 'conn' parameter as its first argument
    after 'self'.
    
    Args:
        func: Method to decorate
        
    Returns:
        Decorated method
        
    Example:
        ```python
        class MyService(BaseService):
            @transaction_required
            def update_data(self, conn, data_id, new_value):
                conn.execute("UPDATE table SET value = ? WHERE id = ?", 
                           (new_value, data_id))
        
        # Usage - automatically provides connection within transaction
        service.update_data(123, "new_value")
        ```
    """
    @functools.wraps(func)
    def wrapper(self: BaseService, *args, **kwargs):
        if not isinstance(self, BaseService):
            raise TransactionError(
                f"@transaction_required can only be used on BaseService methods"
            )
        
        with self.safe_transaction() as conn:
            return func(self, conn, *args, **kwargs)
    
    return wrapper


def auto_transaction(func: F) -> F:
    """
    Decorator that automatically handles transactions for a method.
    
    The method can optionally use self.get_connection() to get the connection.
    
    Args:
        func: Method to decorate
        
    Returns:
        Decorated method
        
    Example:
        ```python
        class MyService(BaseService):
            @auto_transaction
            def process_data(self, data_list):
                conn = self.get_connection()
                for item in data_list:
                    conn.execute("INSERT INTO table VALUES (?)", (item,))
        
        # Usage - transaction handled automatically
        service.process_data(["a", "b", "c"])
        ```
    """
    @functools.wraps(func)
    def wrapper(self: BaseService, *args, **kwargs):
        if not isinstance(self, BaseService):
            raise TransactionError(
                f"@auto_transaction can only be used on BaseService methods"
            )
        
        if self.in_transaction:
            # Already in transaction, just call the method
            return func(self, *args, **kwargs)
        else:
            # Create transaction for this method
            with self.safe_transaction():
                return func(self, *args, **kwargs)
    
    return wrapper


def read_only(func: F) -> F:
    """
    Decorator for methods that only perform read operations.
    
    Uses safe_connection instead of transactions for better performance.
    
    Args:
        func: Method to decorate
        
    Returns:
        Decorated method
        
    Example:
        ```python
        class MyService(BaseService):
            @read_only
            def get_data(self, data_id):
                conn = self.get_connection()
                cursor = conn.execute("SELECT * FROM table WHERE id = ?", (data_id,))
                return cursor.fetchone()
        ```
    """
    @functools.wraps(func)
    def wrapper(self: BaseService, *args, **kwargs):
        if not isinstance(self, BaseService):
            raise TransactionError(
                f"@read_only can only be used on BaseService methods"
            )
        
        # If already in transaction, use existing connection
        if self.in_transaction:
            return func(self, *args, **kwargs)
        
        # Use safe connection for read-only operations
        with self.safe_connection():
            return func(self, *args, **kwargs)
    
    return wrapper


# Example service implementation showing usage patterns
class ExampleService(BaseService):
    """
    Example service demonstrating BaseService usage patterns.
    
    This shows all the different ways to use BaseService for consistent
    transaction management.
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.stats = {'operations': 0, 'errors': 0}
    
    @read_only
    def get_record_count(self, table_name: str) -> int:
        """Example read-only method using @read_only decorator."""
        conn = self.get_connection()
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    
    @transaction_required
    def insert_record(self, conn: sqlite3.Connection, table_name: str, data: dict) -> int:
        """Example method requiring transaction using @transaction_required."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        values = list(data.values())
        
        cursor = conn.execute(
            f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
            values
        )
        self.stats['operations'] += 1
        return cursor.lastrowid
    
    @auto_transaction
    def batch_process(self, records: list) -> dict:
        """Example method with automatic transaction using @auto_transaction."""
        conn = self.get_connection()
        results = {'inserted': 0, 'errors': 0}
        
        for record in records:
            try:
                conn.execute("INSERT INTO example_table (data) VALUES (?)", (record,))
                results['inserted'] += 1
            except sqlite3.Error as e:
                logger.error(f"Failed to insert record {record}: {e}")
                results['errors'] += 1
        
        return results
    
    def manual_transaction_example(self, data_list: list) -> bool:
        """Example using manual transaction management."""
        try:
            with self.safe_transaction() as conn:
                # Multiple operations in single transaction
                for data in data_list:
                    conn.execute("INSERT INTO table1 VALUES (?)", (data,))
                    conn.execute("UPDATE table2 SET counter = counter + 1")
                
                # Business logic validation
                cursor = conn.execute("SELECT COUNT(*) FROM table1")
                count = cursor.fetchone()[0]
                
                if count > 1000:
                    raise ValueError("Too many records, rolling back")
                
                logger.info(f"Successfully processed {len(data_list)} records")
                return True
                
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            self.stats['errors'] += 1
            return False
    
    def conditional_transaction_example(self, data: dict, use_transaction: bool) -> bool:
        """Example showing conditional transaction usage."""
        if use_transaction or not self.in_transaction:
            with self.safe_transaction() as conn:
                return self._do_database_work(conn, data)
        else:
            # We're already in a transaction, just use existing connection
            conn = self.get_connection()
            return self._do_database_work(conn, data)
    
    def _do_database_work(self, conn: sqlite3.Connection, data: dict) -> bool:
        """Helper method that works within any transaction context."""
        try:
            conn.execute("INSERT INTO work_table (data) VALUES (?)", (str(data),))
            return True
        except sqlite3.Error as e:
            logger.error(f"Database work failed: {e}")
            return False


# Utility functions for testing and validation
def test_transaction_detection():
    """Test function to validate transaction state detection."""
    from database.connection import DatabaseConnection
    
    # This would normally use your actual test database
    test_db_path = ":memory:"  # In-memory database for testing
    db_conn = DatabaseConnection(test_db_path)
    
    # Create test table
    with db_conn.transaction() as conn:
        conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)")
    
    service = ExampleService(db_conn)
    
    print("Testing transaction state detection:")
    print("=" * 50)
    
    # Test 1: Outside transaction
    info = service.get_transaction_info()
    print(f"Outside transaction: {info}")
    assert not info['in_transaction'], "Should not be in transaction"
    
    # Test 2: Inside transaction
    with service.safe_transaction() as conn:
        info = service.get_transaction_info()
        print(f"Inside transaction: {info}")
        assert info['in_transaction'], "Should be in transaction"
        
        # Test 3: Nested safe_transaction (should reuse connection)
        with service.safe_transaction() as conn2:
            assert conn is conn2, "Should reuse same connection"
            info = service.get_transaction_info()
            print(f"Nested transaction: {info}")
    
    # Test 4: Back outside transaction
    info = service.get_transaction_info()
    print(f"After transaction: {info}")
    assert not info['in_transaction'], "Should not be in transaction anymore"
    
    print("✅ All transaction detection tests passed!")


def test_decorators():
    """Test function to validate transaction decorators."""
    from database.connection import DatabaseConnection
    
    test_db_path = ":memory:"
    db_conn = DatabaseConnection(test_db_path)
    
    # Create test table
    with db_conn.transaction() as conn:
        conn.execute("CREATE TABLE decorator_test (id INTEGER PRIMARY KEY, data TEXT)")
    
    service = ExampleService(db_conn)
    
    print("\nTesting transaction decorators:")
    print("=" * 50)
    
    # Test read-only decorator
    try:
        # This should work without issues
        count = service.get_record_count("decorator_test")
        print(f"✅ @read_only decorator: Got count {count}")
    except Exception as e:
        print(f"❌ @read_only decorator failed: {e}")
    
    # Test transaction_required decorator
    try:
        record_id = service.insert_record("decorator_test", {"data": "test_value"})
        print(f"✅ @transaction_required decorator: Inserted record {record_id}")
    except Exception as e:
        print(f"❌ @transaction_required decorator failed: {e}")
    
    # Test auto_transaction decorator
    try:
        results = service.batch_process(["item1", "item2", "item3"])
        print(f"✅ @auto_transaction decorator: Results {results}")
    except Exception as e:
        print(f"❌ @auto_transaction decorator failed: {e}")
    
    print("✅ All decorator tests completed!")


if __name__ == "__main__":
    # Run tests if executed directly
    import logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("Testing BaseService Transaction Management")
    print("=" * 60)
    
    try:
        test_transaction_detection()
        test_decorators()
        print("\n🎉 All tests passed! BaseService is ready for use.")
        
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()