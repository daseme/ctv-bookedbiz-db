"""
Authentication and authorization utilities for Flask-Login.

Provides:
- User loader for Flask-Login
- Role-based access control decorators
- Helper functions for checking permissions
"""

from functools import wraps
from flask import redirect, url_for, flash, request, abort
from flask_login import LoginManager, current_user
from typing import Callable, Optional

from src.models.users import User, UserRole
from src.services.container import get_container


# Initialize login manager (will be configured in app.py)
login_manager = LoginManager()


@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    """
    Load user from database for Flask-Login.

    Args:
        user_id: User ID as string

    Returns:
        User object or None if not found
    """
    try:
        container = get_container()
        user_service = container.get("user_service")
        user = user_service.get_user_by_id(int(user_id))
        return user
    except Exception:
        return None


@login_manager.unauthorized_handler
def unauthorized():
    """Handle unauthorized access attempts."""
    if request.path.startswith("/api/"):
        # API requests return JSON
        from flask import jsonify

        return jsonify({"error": "Authentication required"}), 401
    else:
        # Web requests redirect to login
        flash("Please log in to access this page.", "info")
        return redirect(url_for("user_management.login"))


# ============================================================================
# Role-Based Access Control Decorators
# ============================================================================


def login_required(f: Callable) -> Callable:
    """
    Decorator to require user to be logged in.

    Usage:
        @login_required
        @user_management_bp.route('/protected')
        def protected_route():
            return "This requires login"
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return login_manager.unauthorized()
        return f(*args, **kwargs)

    return decorated_function


def admin_required(f: Callable) -> Callable:
    """
    Decorator to require admin role.

    Usage:
        @admin_required
        @user_management_bp.route('/admin-only')
        def admin_route():
            return "Admin only"
    """

    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return login_manager.unauthorized()

        if not current_user.is_admin():
            if request.path.startswith("/api/"):
                from flask import jsonify

                return jsonify({"error": "Admin access required"}), 403
            else:
                flash("Admin access required.", "error")
                abort(403)

        return f(*args, **kwargs)

    return decorated_function


def management_or_admin_required(f: Callable) -> Callable:
    """
    Decorator to require management or admin role.

    Usage:
        @management_or_admin_required
        @user_management_bp.route('/management')
        def management_route():
            return "Management or admin only"
    """

    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return login_manager.unauthorized()

        if not current_user.is_management_or_admin():
            if request.path.startswith("/api/"):
                from flask import jsonify

                return jsonify({"error": "Management or admin access required"}), 403
            else:
                flash("Management or admin access required.", "error")
                abort(403)

        return f(*args, **kwargs)

    return decorated_function


def role_required(required_role: UserRole) -> Callable:
    """
    Decorator factory to require a specific role level.

    Usage:
        @role_required(UserRole.AE)
        @user_management_bp.route('/ae-only')
        def ae_route():
            return "AE or higher only"
    """

    def decorator(f: Callable) -> Callable:
        @wraps(f)
        @login_required
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return login_manager.unauthorized()

            if not current_user.has_permission(required_role):
                if request.path.startswith("/api/"):
                    from flask import jsonify

                    return jsonify(
                        {"error": f"{required_role.display_name} access required"}
                    ), 403
                else:
                    flash(f"{required_role.display_name} access required.", "error")
                    abort(403)

            return f(*args, **kwargs)

        return decorated_function

    return decorator
