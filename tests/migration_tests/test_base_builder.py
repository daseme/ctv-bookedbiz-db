# test_base_builder.py
import sqlite3
from query_builders import BaseQueryBuilder

# Connect to your database
conn = sqlite3.connect('data/database/production.db')

# Test basic functionality
builder = BaseQueryBuilder("2024")
builder.apply_standard_filters().exclude_worldlink()

# Execute and verify it works
result = builder.execute_revenue_query(conn)
print(f"Total revenue: ${result.revenue:,.2f}")
print(f"Total spots: {result.spot_count:,}")
print(f"Execution time: {result.execution_time:.3f}s")