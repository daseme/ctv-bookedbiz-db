"""
User Service - Business logic for user management.

Orchestrates user operations:
- User authentication and authorization
- User CRUD operations with validation
- Password hashing and verification
"""

import logging
from typing import List, Optional
from werkzeug.security import generate_password_hash, check_password_hash

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
    # Authentication
    # =========================================================================

    def authenticate_user(self, email: str, password: str) -> Optional[User]:
        """
        Authenticate a user by email and password.
        
        Args:
            email: User's email address
            password: Plain text password
            
        Returns:
            User object if authentication succeeds, None otherwise
        """
        user = self.repository.get_user_by_email(email)
        
        if not user:
            logger.warning(f"Authentication failed: user not found for email {email}")
            return None
        
        if not check_password_hash(user.password_hash, password):
            logger.warning(f"Authentication failed: incorrect password for {email}")
            return None
        
        # Update last login timestamp
        self.repository.update_last_login(user.user_id)
        
        logger.info(f"User {email} authenticated successfully")
        return user

    # =========================================================================
    # User Creation
    # =========================================================================

    def create_user(self, request: CreateUserRequest) -> User:
        """
        Create a new user with validation.
        
        Args:
            request: CreateUserRequest with user details
            
        Returns:
            Created User object
            
        Raises:
            ValueError: If validation fails or email already exists
        """
        # Validate email format (basic check)
        if not request.email or "@" not in request.email:
            raise ValueError("Invalid email address")
        
        # Validate password strength (basic check)
        if not request.password or len(request.password) < 8:
            raise ValueError("Password must be at least 8 characters long")
        
        # Check if email already exists
        existing_user = self.repository.get_user_by_email(request.email)
        if existing_user:
            raise ValueError(f"Email {request.email} is already registered")
        
        # Hash password
        password_hash = generate_password_hash(request.password)
        
        # Create user
        try:
            user = self.repository.create_user(
                first_name=request.first_name,
                last_name=request.last_name,
                email=request.email,
                password_hash=password_hash,
                role=request.role,
            )
            logger.info(f"User created: {user.email} (ID: {user.user_id})")
            return user
        except ValueError as e:
            # Re-raise validation errors
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

    def get_users_by_role(self, role: UserRole, include_inactive: bool = False) -> List[User]:
        """Get all users with a specific role."""
        return self.repository.get_users_by_role(role, include_inactive=include_inactive)

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
            
            # Check if new email already exists
            email_user = self.repository.get_user_by_email(request.email)
            if email_user and email_user.user_id != user_id:
                raise ValueError(f"Email {request.email} is already registered")
        
        # Hash password if being updated
        password_hash = None
        if request.password:
            if len(request.password) < 8:
                raise ValueError("Password must be at least 8 characters long")
            password_hash = generate_password_hash(request.password)
        
        # Update user
        try:
            updated_user = self.repository.update_user(
                user_id=user_id,
                first_name=request.first_name,
                last_name=request.last_name,
                email=request.email,
                password_hash=password_hash,
                role=request.role,
            )
            logger.info(f"User updated: {updated_user.email} (ID: {user_id})")
            return updated_user
        except ValueError as e:
            raise
        except Exception as e:
            logger.error(f"Failed to update user {user_id}: {e}")
            raise ValueError(f"Failed to update user: {str(e)}")

    def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        """
        Change a user's password.
        
        Args:
            user_id: ID of user
            old_password: Current password
            new_password: New password
            
        Returns:
            True if password changed successfully
            
        Raises:
            ValueError: If old password is incorrect or new password is invalid
        """
        user = self.repository.get_user_by_id(user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        # Verify old password
        if not check_password_hash(user.password_hash, old_password):
            raise ValueError("Current password is incorrect")
        
        # Validate new password
        if not new_password or len(new_password) < 8:
            raise ValueError("New password must be at least 8 characters long")
        
        # Update password
        password_hash = generate_password_hash(new_password)
        self.repository.update_user(user_id, password_hash=password_hash)
        
        logger.info(f"Password changed for user {user_id}")
        return True

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
