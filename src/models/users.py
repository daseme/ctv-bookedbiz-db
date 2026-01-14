"""
User Management Domain Models

Pure business entities for user authentication and authorization.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


# ============================================================================
# Enums
# ============================================================================

class UserRole(Enum):
    """User permission roles"""
    ADMIN = "admin"
    MANAGEMENT = "management"
    AE = "AE"
    
    @property
    def display_name(self) -> str:
        """Human-readable role name"""
        return self.value.title()
    
    def has_permission(self, required_role: "UserRole") -> bool:
        """Check if this role has permission for required role level."""
        # Admin has all permissions
        if self == UserRole.ADMIN:
            return True
        # Management can access management and AE level
        if self == UserRole.MANAGEMENT:
            return required_role in (UserRole.MANAGEMENT, UserRole.AE)
        # AE can only access AE level
        if self == UserRole.AE:
            return required_role == UserRole.AE
        return False


# ============================================================================
# Domain Models
# ============================================================================

@dataclass
class User:
    """User account model with Flask-Login compatibility"""
    user_id: int
    first_name: str
    last_name: str
    email: str
    password_hash: str
    role: UserRole
    created_date: Optional[datetime] = None
    last_login: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    
    # Flask-Login required properties and methods
    @property
    def is_authenticated(self) -> bool:
        """Flask-Login: User is authenticated"""
        return True
    
    @property
    def is_active(self) -> bool:
        """Flask-Login: User is active (always True since we removed is_active column)"""
        return True
    
    @property
    def is_anonymous(self) -> bool:
        """Flask-Login: User is not anonymous"""
        return False
    
    def get_id(self) -> str:
        """Flask-Login: Get user ID as string"""
        return str(self.user_id)
    
    @property
    def full_name(self) -> str:
        """Get user's full name"""
        return f"{self.first_name} {self.last_name}"
    
    def has_permission(self, required_role: UserRole) -> bool:
        """Check if user has permission for required role level"""
        return self.role.has_permission(required_role)
    
    def is_admin(self) -> bool:
        """Check if user is an admin"""
        return self.role == UserRole.ADMIN
    
    def is_management_or_admin(self) -> bool:
        """Check if user is management or admin"""
        return self.role in (UserRole.ADMIN, UserRole.MANAGEMENT)


@dataclass
class CreateUserRequest:
    """Request model for creating a new user"""
    first_name: str
    last_name: str
    email: str
    password: str
    role: UserRole


@dataclass
class UpdateUserRequest:
    """Request model for updating a user"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[UserRole] = None
