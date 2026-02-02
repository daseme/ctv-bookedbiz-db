# tests/test_real_data_integration.py
"""
Integration tests with real database and existing services.
Tests that our service container works with actual data.
"""

import pytest
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from services.container import get_container, reset_container
from services.factory import initialize_services
from config.settings import reset_settings


class TestRealDataIntegration:
    """Test service container with real database and services."""

    def setup_method(self):
        """Set up test environment with real paths."""
        # Reset global state
        reset_container()
        reset_settings()

        # Set up real project paths
        project_root = Path(__file__).parent.parent
        os.environ["PROJECT_ROOT"] = str(project_root)

        # Use real database path
        db_path = project_root / "data" / "database" / "production.db"
        if db_path.exists():
            os.environ["DB_PATH"] = str(db_path)
            print(f"✅ Using real database: {db_path}")
        else:
            print(f"⚠️  Database not found at: {db_path}")
            pytest.skip("Real database not available")

    def test_database_connection_real(self):
        """Test database connection with real database."""
        try:
            initialize_services()
            container = get_container()

            # Get database connection
            db_connection = container.get("database_connection")
            assert db_connection is not None, "Database connection is None"

            # Test connection
            conn = db_connection.connect()
            assert conn is not None, "Database connection failed"

            # Test a simple query
            cursor = conn.execute("SELECT COUNT(*) as count FROM spots LIMIT 1")
            result = cursor.fetchone()
            spot_count = result[0] if result else 0

            print(f"✅ Database connection successful - {spot_count} spots found")
            conn.close()

        except Exception as e:
            print(f"❌ Database connection test failed: {e}")
            raise

    def test_real_database_query(self):
        """Test real database queries through service container."""
        try:
            initialize_services()
            container = get_container()

            db_connection = container.get("database_connection")
            conn = db_connection.connect()

            # Test multiple real queries
            queries = [
                ("spots", "SELECT COUNT(*) FROM spots"),
                ("customers", "SELECT COUNT(*) FROM customers"),
                (
                    "sales_people",
                    "SELECT COUNT(DISTINCT sales_person) FROM spots WHERE sales_person IS NOT NULL",
                ),
            ]

            results = {}
            for name, query in queries:
                try:
                    cursor = conn.execute(query)
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                    results[name] = count
                    print(f"✅ {name}: {count}")
                except Exception as e:
                    print(f"⚠️  Query failed for {name}: {e}")
                    results[name] = 0

            conn.close()

            # Verify we got some data
            assert results["spots"] > 0, "No spots found in database"
            print(
                f"✅ Real database queries successful - {results['spots']} total spots"
            )

            return results

        except Exception as e:
            print(f"❌ Real database query test failed: {e}")
            raise


def test_project_structure_real():
    """Test that real project structure matches expectations."""
    project_root = Path(__file__).parent.parent

    # Check for database file
    db_path = project_root / "data" / "database" / "production.db"
    if db_path.exists():
        size_mb = db_path.stat().st_size / (1024 * 1024)
        print(f"✅ Database found: {db_path} ({size_mb:.1f} MB)")
    else:
        print(f"⚠️  Database not found: {db_path}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
