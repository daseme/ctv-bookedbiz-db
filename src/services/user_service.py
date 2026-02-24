"""
User Service - Business logic for user management.

Orchestrates user operations:
- User authentication via Tailscale identity
- User CRUD operations with validation
"""

import logging
from typing import List, Optional

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService
from src.repositories.user_repository import UserRepository
from src.models.users import User, UserRole, CreateUserRequest, UpdateUserRequest

logger = logging.getLogger(__name__)


class UserService(BaseService):
    """Service for user management operations."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.repository = UserRepository(db_connection)

    # =========================================================================
    # Authentication (Tailscale identity)
    # =========================================================================

    def authenticate_by_tailscale(
        self, login: str, display_name: Optional[str] = None
    ) -> Optional[User]:
        """
        Authenticate by Tailscale identity (Tailscale-User-Login = email, Tailscale-User-Name = display name).
        Look up user by email; optionally update first/last name from display_name.
        """
        email = (login or "").strip().lower()
        if not email or "@" not in email:
            return None
        user = self.repository.get_user_by_email(email)
        if not user:
            logger.warning(f"Tailscale auth: no user for email {email}")
            return None
        self.repository.update_last_login(user.user_id)
        if display_name and (display_name != user.full_name):
            first, _, last = (display_name.strip() + " ").partition(" ")
            if first or last:
                self.repository.update_user(
                    user.user_id,
                    first_name=first or user.first_name,
                    last_name=last.strip() if last else user.last_name,
                )
                user = self.repository.get_user_by_id(user.user_id)
        logger.info(f"User {email} authenticated via Tailscale")
        return user

    # =========================================================================
    # User Creation
    # =========================================================================

    def create_user(self, request: CreateUserRequest) -> User:
        """
        Create a new user (Tailscale identity; no password).
        """
        if not request.email or "@" not in request.email:
            raise ValueError("Invalid email address")
        existing_user = self.repository.get_user_by_email(request.email)
        if existing_user:
            raise ValueError(f"Email {request.email} is already registered")
        try:
            user = self.repository.create_user(
                first_name=request.first_name,
                last_name=request.last_name,
                email=request.email,
                role=request.role,
            )
            logger.info(f"User created: {user.email} (ID: {user.user_id})")
            return user
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            raise ValueError(f"Failed to create user: {str(e)}")

    # =========================================================================
    # User Retrieval
    # =========================================================================

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        return self.repository.get_user_by_id(user_id)

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        return self.repository.get_user_by_email(email)

    def get_all_users(self, include_inactive: bool = False) -> List[User]:
        """Get all users."""
        return self.repository.get_all_users(include_inactive=include_inactive)

    def get_users_by_role(
        self, role: UserRole, include_inactive: bool = False
    ) -> List[User]:
        """Get all users with a specific role."""
        return self.repository.get_users_by_role(
            role, include_inactive=include_inactive
        )

    # =========================================================================
    # User Updates
    # =========================================================================

    def update_user(self, user_id: int, request: UpdateUserRequest) -> Optional[User]:
        """
        Update user information with validation.

        Args:
            user_id: ID of user to update
            request: UpdateUserRequest with fields to update

        Returns:
            Updated User object

        Raises:
            ValueError: If validation fails
        """
        # Check if user exists
        existing_user = self.repository.get_user_by_id(user_id)
        if not existing_user:
            raise ValueError(f"User with ID {user_id} not found")

        # Validate email if being updated
        if request.email and request.email != existing_user.email:
            if not request.email or "@" not in request.email:
                raise ValueError("Invalid email address")
            email_user = self.repository.get_user_by_email(request.email)
            if email_user and email_user.user_id != user_id:
                raise ValueError(f"Email {request.email} is already registered")

        try:
            updated_user = self.repository.update_user(
                user_id=user_id,
                first_name=request.first_name,
                last_name=request.last_name,
                email=request.email,
                role=request.role,
            )
            logger.info(f"User updated: {updated_user.email} (ID: {user_id})")
            return updated_user
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to update user {user_id}: {e}")
            raise ValueError(f"Failed to update user: {str(e)}")

    # =========================================================================
    # User Deletion
    # =========================================================================

    def delete_user(self, user_id: int) -> bool:
        """
        Delete a user (hard delete).

        Args:
            user_id: ID of user to delete

        Returns:
            True if user was deleted
        """
        user = self.repository.get_user_by_id(user_id)
        if not user:
            return False

        deleted = self.repository.delete_user(user_id)
        if deleted:
            logger.info(f"User deleted: {user.email} (ID: {user_id})")
        return deleted

    def deactivate_user(self, user_id: int) -> Optional[User]:
        """
        Deactivate a user (soft delete).

        Args:
            user_id: ID of user to deactivate

        Returns:
            Deactivated User object
        """
        user = self.repository.deactivate_user(user_id)
        if user:
            logger.info(f"User deactivated: {user.email} (ID: {user_id})")
        return user
