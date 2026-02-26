"""
User Management Blueprint

Handles user authentication (Tailscale identity) and user management:
- Login via Tailscale identity / logout
- User CRUD (admin only)
"""

import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user

from src.services.container import get_container
from src.models.users import UserRole, CreateUserRequest, UpdateUserRequest
from src.web.utils.auth import admin_required
from src.web.utils.tailscale import get_tailscale_identity

logger = logging.getLogger(__name__)

# Create blueprint
user_management_bp = Blueprint("user_management", __name__, url_prefix="/users")


# ============================================================================
# Authentication Routes (Tailscale identity)
# ============================================================================


@user_management_bp.route("/login", methods=["GET", "POST"])
def login():
    """Login via Tailscale identity (no password)."""
    if current_user.is_authenticated:
        return redirect("/reports/")

    identity = get_tailscale_identity(request.remote_addr)
    if identity:
        login_email, display_name = identity
        try:
            container = get_container()
            user_service = container.get("user_service")
            user = user_service.authenticate_by_tailscale(login_email, display_name)
            if user:
                login_user(user, remember=False)
                flash(f"Welcome back, {user.first_name}!", "success")
                next_page = request.args.get("next") or "/reports/"
                return redirect(next_page)
            flash("Your Tailscale account is not authorized. Ask an admin to add you.", "error")
        except Exception as e:
            logger.error(f"Login error: {e}")
            flash("An error occurred during login. Please try again.", "error")
    # Fallback: allow localhost login by auto-signing in the first active user.
    elif request.remote_addr in ("127.0.0.1", "::1"):
        try:
            container = get_container()
            user_service = container.get("user_service")
            users = user_service.get_all_users(include_inactive=False)
            if users:
                user = users[0]
                login_user(user, remember=False)
                flash(
                    f"Signed in as {user.first_name} {user.last_name} "
                    f"({user.role.display_name})",
                    "info",
                )
                next_page = request.args.get("next") or "/reports/"
                return redirect(next_page)
            else:
                flash("No user found for localhost login.", "error")
        except Exception as e:
            logger.error(f"Localhost login error: {e}")
            flash("An error occurred during login. Please try again.", "error")
    elif request.method == "POST":
        flash("Login requires Tailscale. Access the app via your Tailscale network.", "error")

    return render_template("login.html")


@user_management_bp.route("/logout")
@login_required
def logout():
    """User logout."""
    user_name = current_user.first_name if current_user.is_authenticated else "User"
    logout_user()
    flash(f"Goodbye, {user_name}! You have been logged out.", "info")
    return redirect(url_for("user_management.login"))


# ============================================================================
# User Management Routes (Admin Only)
# ============================================================================


@user_management_bp.route("/")
@admin_required
def list_users():
    """List all users (admin only)."""
    try:
        container = get_container()
        user_service = container.get("user_service")
        users = user_service.get_all_users(include_inactive=True)

        return render_template("users/list.html", users=users)
    except Exception as e:
        logger.error(f"Error listing users: {e}")
        flash("An error occurred while loading users.", "error")
        return redirect("/reports/")


@user_management_bp.route("/create", methods=["GET", "POST"])
@admin_required
def create_user():
    """Create a new user (admin only)."""
    if request.method == "POST":
        try:
            first_name = request.form.get("first_name", "").strip()
            last_name = request.form.get("last_name", "").strip()
            email = request.form.get("email", "").strip()
            role_str = request.form.get("role", "AE").strip()

            if not all([first_name, last_name, email]):
                flash("First name, last name, and email are required.", "error")
                return render_template("users/create.html", roles=UserRole)

            try:
                role = UserRole(role_str)
            except ValueError:
                flash("Invalid role selected.", "error")
                return render_template("users/create.html", roles=UserRole)

            container = get_container()
            user_service = container.get("user_service")
            create_request = CreateUserRequest(
                first_name=first_name,
                last_name=last_name,
                email=email,
                role=role,
            )

            user = user_service.create_user(create_request)
            flash(f"User {user.full_name} created successfully!", "success")
            return redirect(url_for("user_management.list_users"))

        except ValueError as e:
            flash(str(e), "error")
            return render_template("users/create.html", roles=UserRole)
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            flash("An error occurred while creating the user.", "error")
            return render_template("users/create.html", roles=UserRole)

    return render_template("users/create.html", roles=UserRole)


@user_management_bp.route("/<int:user_id>")
@admin_required
def view_user(user_id: int):
    """View user details (admin only)."""
    try:
        container = get_container()
        user_service = container.get("user_service")
        user = user_service.get_user_by_id(user_id)

        if not user:
            flash("User not found.", "error")
            return redirect(url_for("user_management.list_users"))

        return render_template("users/view.html", user=user)
    except Exception as e:
        logger.error(f"Error viewing user: {e}")
        flash("An error occurred while loading the user.", "error")
        return redirect(url_for("user_management.list_users"))


@user_management_bp.route("/<int:user_id>/edit", methods=["GET", "POST"])
@admin_required
def edit_user(user_id: int):
    """Edit user (admin only)."""
    try:
        container = get_container()
        user_service = container.get("user_service")
        user = user_service.get_user_by_id(user_id)

        if not user:
            flash("User not found.", "error")
            return redirect(url_for("user_management.list_users"))

        if request.method == "POST":
            try:
                first_name = request.form.get("first_name", "").strip()
                last_name = request.form.get("last_name", "").strip()
                email = request.form.get("email", "").strip()
                role_str = request.form.get("role", "").strip()

                if not all([first_name, last_name, email]):
                    flash("First name, last name, and email are required.", "error")
                    return render_template("users/edit.html", user=user, roles=UserRole)

                role = None
                if role_str:
                    try:
                        role = UserRole(role_str)
                    except ValueError:
                        flash("Invalid role selected.", "error")
                        return render_template(
                            "users/edit.html", user=user, roles=UserRole
                        )

                update_request = UpdateUserRequest(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    role=role,
                )

                updated_user = user_service.update_user(user_id, update_request)
                flash(f"User {updated_user.full_name} updated successfully!", "success")
                return redirect(url_for("user_management.view_user", user_id=user_id))

            except ValueError as e:
                flash(str(e), "error")
                return render_template("users/edit.html", user=user, roles=UserRole)
            except Exception as e:
                logger.error(f"Error updating user: {e}")
                flash("An error occurred while updating the user.", "error")
                return render_template("users/edit.html", user=user, roles=UserRole)

        return render_template("users/edit.html", user=user, roles=UserRole)

    except Exception as e:
        logger.error(f"Error editing user: {e}")
        flash("An error occurred while loading the user.", "error")
        return redirect(url_for("user_management.list_users"))


@user_management_bp.route("/<int:user_id>/delete", methods=["POST"])
@admin_required
def delete_user(user_id: int):
    """Delete user (admin only)."""
    try:
        # Prevent self-deletion
        if current_user.user_id == user_id:
            flash("You cannot delete your own account.", "error")
            return redirect(url_for("user_management.list_users"))

        container = get_container()
        user_service = container.get("user_service")
        user = user_service.get_user_by_id(user_id)

        if not user:
            flash("User not found.", "error")
            return redirect(url_for("user_management.list_users"))

        user_service.delete_user(user_id)
        flash(f"User {user.full_name} deleted successfully.", "success")
        return redirect(url_for("user_management.list_users"))

    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        flash("An error occurred while deleting the user.", "error")
        return redirect(url_for("user_management.list_users"))


@user_management_bp.route("/<int:user_id>/deactivate", methods=["POST"])
@admin_required
def deactivate_user(user_id: int):
    """Deactivate user (admin only)."""
    try:
        # Prevent self-deactivation
        if current_user.user_id == user_id:
            flash("You cannot deactivate your own account.", "error")
            return redirect(url_for("user_management.list_users"))

        container = get_container()
        user_service = container.get("user_service")
        user = user_service.get_user_by_id(user_id)

        if not user:
            flash("User not found.", "error")
            return redirect(url_for("user_management.list_users"))

        user_service.deactivate_user(user_id)
        flash(f"User {user.full_name} deactivated successfully.", "success")
        return redirect(url_for("user_management.list_users"))

    except Exception as e:
        logger.error(f"Error deactivating user: {e}")
        flash("An error occurred while deactivating the user.", "error")
        return redirect(url_for("user_management.list_users"))


# ============================================================================
# Profile Management Routes
# ============================================================================


@user_management_bp.route("/profile")
@login_required
def profile():
    """View current user's profile."""
    return render_template("users/profile.html", user=current_user)


@user_management_bp.route("/profile/change-password", methods=["GET"])
@login_required
def change_password():
    """No passwords; auth is via Tailscale. Redirect to profile."""
    flash("Sign-in is via Tailscale; there is no password to change.", "info")
    return redirect(url_for("user_management.profile"))
