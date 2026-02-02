"""
User Repository - Data access layer for user management.

Handles all database operations for:
- User authentication (login, password verification)
- User CRUD operations
- User queries and filtering
"""

import sqlite3
import logging
from typing import List, Optional
from datetime import datetime

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService
from src.models.users import User, UserRole

logger = logging.getLogger(__name__)


class UserRepository(BaseService):
    """Repository for user-related data access."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)

    # =========================================================================
    # User Retrieval
    # =========================================================================

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT user_id, first_name, last_name, email, password_hash, 
                       role, created_date, last_login, updated_date
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            return self._row_to_user(row) if row else None

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email address."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT user_id, first_name, last_name, email, password_hash, 
                       role, created_date, last_login, updated_date
                FROM users
                WHERE email = ?
                """,
                (email.lower(),),
            )
            row = cursor.fetchone()
            return self._row_to_user(row) if row else None

    def get_all_users(self, include_inactive: bool = False) -> List[User]:
        """Get all users."""
        with self.safe_connection() as conn:
            query = """
                SELECT user_id, first_name, last_name, email, password_hash, 
                       role, created_date, last_login, updated_date
                FROM users
                ORDER BY last_name, first_name
            """
            cursor = conn.execute(query)
            return [self._row_to_user(row) for row in cursor.fetchall()]

    def get_users_by_role(
        self, role: UserRole, include_inactive: bool = False
    ) -> List[User]:
        """Get all users with a specific role."""
        with self.safe_connection() as conn:
            query = """
                SELECT user_id, first_name, last_name, email, password_hash, 
                       role, created_date, last_login, updated_date
                FROM users
                WHERE role = ?
                ORDER BY last_name, first_name
            """
            params = [role.value]

            cursor = conn.execute(query, params)
            return [self._row_to_user(row) for row in cursor.fetchall()]

    # =========================================================================
    # User Creation
    # =========================================================================

    def create_user(
        self,
        first_name: str,
        last_name: str,
        email: str,
        password_hash: str,
        role: UserRole,
    ) -> User:
        """Create a new user."""
        with self.safe_transaction() as conn:
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO users (first_name, last_name, email, password_hash, role)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (first_name, last_name, email.lower(), password_hash, role.value),
                )
                user_id = cursor.lastrowid
                conn.commit()

                # Fetch the created user
                return self.get_user_by_id(user_id)
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed: users.email" in str(e):
                    raise ValueError(f"Email {email} is already registered")
                raise

    # =========================================================================
    # User Updates
    # =========================================================================

    def update_user(
        self,
        user_id: int,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        email: Optional[str] = None,
        password_hash: Optional[str] = None,
        role: Optional[UserRole] = None,
    ) -> Optional[User]:
        """Update user information."""
        with self.safe_transaction() as conn:
            updates = []
            params = []

            if first_name is not None:
                updates.append("first_name = ?")
                params.append(first_name)

            if last_name is not None:
                updates.append("last_name = ?")
                params.append(last_name)

            if email is not None:
                updates.append("email = ?")
                params.append(email.lower())

            if password_hash is not None:
                updates.append("password_hash = ?")
                params.append(password_hash)

            if role is not None:
                updates.append("role = ?")
                params.append(role.value)

            if not updates:
                # No updates to make
                return self.get_user_by_id(user_id)

            # Always update the updated_date
            updates.append("updated_date = ?")
            params.append(datetime.now().isoformat())

            params.append(user_id)

            try:
                conn.execute(
                    f"""
                    UPDATE users
                    SET {", ".join(updates)}
                    WHERE user_id = ?
                    """,
                    params,
                )
                conn.commit()
                return self.get_user_by_id(user_id)
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed: users.email" in str(e):
                    raise ValueError(f"Email {email} is already registered")
                raise

    def update_last_login(self, user_id: int) -> None:
        """Update the last login timestamp for a user."""
        with self.safe_transaction() as conn:
            conn.execute(
                """
                UPDATE users
                SET last_login = ?
                WHERE user_id = ?
                """,
                (datetime.now().isoformat(), user_id),
            )
            conn.commit()

    # =========================================================================
    # User Deletion
    # =========================================================================

    def delete_user(self, user_id: int) -> bool:
        """Delete a user (hard delete)."""
        with self.safe_transaction() as conn:
            cursor = conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0

    def deactivate_user(self, user_id: int) -> Optional[User]:
        """Deactivate a user (now performs hard delete since is_active column removed)."""
        user = self.get_user_by_id(user_id)
        if user and self.delete_user(user_id):
            return user
        return None

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _row_to_user(self, row: sqlite3.Row) -> User:
        """Convert database row to User model."""
        if row is None:
            return None

        # Parse datetime strings
        created_date = None
        if row["created_date"]:
            try:
                created_date = datetime.fromisoformat(row["created_date"])
            except (ValueError, TypeError):
                pass

        last_login = None
        if row["last_login"]:
            try:
                last_login = datetime.fromisoformat(row["last_login"])
            except (ValueError, TypeError):
                pass

        updated_date = None
        if row["updated_date"]:
            try:
                updated_date = datetime.fromisoformat(row["updated_date"])
            except (ValueError, TypeError):
                pass

        return User(
            user_id=row["user_id"],
            first_name=row["first_name"],
            last_name=row["last_name"],
            email=row["email"],
            password_hash=row["password_hash"],
            role=UserRole(row["role"]),
            created_date=created_date,
            last_login=last_login,
            updated_date=updated_date,
        )
