#!/usr/bin/env python3
"""
Script to create an initial admin user.

Usage:
    python scripts/create_admin_user.py

This script will prompt you for:
- First name
- Last name
- Email
- Password

The user will be created with the 'admin' role.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from werkzeug.security import generate_password_hash
from src.database.connection import DatabaseConnection
from src.config.settings import get_settings


def create_admin_user():
    """Create an admin user interactively."""
    print("=" * 60)
    print("CTV Booked Biz - Admin User Creation")
    print("=" * 60)
    print()
    
    # Get database path
    settings = get_settings()
    db_path = settings.database.db_path
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"❌ Error: Database not found at {db_path}")
        print("   Please run the migration script first:")
        print("   sqlite3 data/database/production.db < sql/user_management_tables.sql")
        return False
    
    # Get user input
    print("Please provide the following information:")
    print()
    
    first_name = input("First Name: ").strip()
    if not first_name:
        print("❌ Error: First name is required")
        return False
    
    last_name = input("Last Name: ").strip()
    if not last_name:
        print("❌ Error: Last name is required")
        return False
    
    email = input("Email: ").strip().lower()
    if not email or "@" not in email:
        print("❌ Error: Valid email is required")
        return False
    
    password = input("Password (min 8 characters): ").strip()
    if not password or len(password) < 8:
        print("❌ Error: Password must be at least 8 characters")
        return False
    
    confirm_password = input("Confirm Password: ").strip()
    if password != confirm_password:
        print("❌ Error: Passwords do not match")
        return False
    
    # Create database connection
    try:
        db_connection = DatabaseConnection(db_path)
        
        # Check if users table exists
        with db_connection.connect() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
            )
            if not cursor.fetchone():
                print("❌ Error: Users table not found")
                print("   Please run the migration script first:")
                print("   sqlite3 data/database/production.db < sql/user_management_tables.sql")
                return False
            
            # Check if email already exists
            cursor = conn.execute(
                "SELECT user_id FROM users WHERE email = ?",
                (email,)
            )
            if cursor.fetchone():
                print(f"❌ Error: Email {email} is already registered")
                return False
        
        # Create user
        password_hash = generate_password_hash(password)
        
        with db_connection.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO users (first_name, last_name, email, password_hash, role)
                VALUES (?, ?, ?, ?, 'admin')
                """,
                (first_name, last_name, email, password_hash)
            )
            user_id = cursor.lastrowid
        
        print()
        print("=" * 60)
        print("✅ Admin user created successfully!")
        print("=" * 60)
        print(f"   User ID: {user_id}")
        print(f"   Name: {first_name} {last_name}")
        print(f"   Email: {email}")
        print(f"   Role: admin")
        print()
        print("You can now log in at: http://localhost:5000/users/login")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating user: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = create_admin_user()
    sys.exit(0 if success else 1)
