# User Management Setup Instructions

This document provides instructions for setting up user management in the CTV Booked Biz application. **Sign-in is via Tailscale** (no passwords). See [TAILSCALE_AUTH.md](TAILSCALE_AUTH.md) for how Tailscale identity is resolved via the Local API.

## Prerequisites

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Database Migration**
   ```bash
   sqlite3 data/database/production.db < sql/user_management_tables.sql
   ```
   Or if you already have a `users` table with `password_hash` (legacy):
   ```bash
   sqlite3 /path/to/your/database.db < sql/migrations/020_tailscale_auth_remove_passwords.sql
   ```

## Creating the Initial Admin User

### Method 1: Using the Script (Recommended)

```bash
python scripts/create_admin_user.py
```

The script will prompt for First name, Last name, and Email. The user is created with the `admin` role. Sign-in is via Tailscale (no password).

### Method 2: Using SQLite Directly

```bash
sqlite3 data/database/production.db
```

```sql
INSERT INTO users (first_name, last_name, email, role)
VALUES ('YourFirstName', 'YourLastName', 'your.email@example.com', 'admin');
```

(Use the **same email** as their Tailscale account so they can sign in.)

## User Roles

- **admin**: Full access, including user management
- **management**: Management-level reports, all AEs
- **AE**: AE-level reports only (own dashboard)

## Accessing the Application

1. Start the web application (typically via systemd / uvicorn on port 8000). See `docs/GUIDE-RaspberryWorkflow.md` for Pi service details.
2. Connect to the app **over Tailscale** (e.g. `http://pi-ctv:8000/` or the Pi’s Tailscale IP). You will be signed in automatically if your Tailscale email exists in the `users` table.

## User Management Features (Admin)

- **View all users**: `/users/`
- **Create new users**: `/users/create`
- **Edit users**: `/users/<user_id>/edit`
- **Delete users**: `/users/<user_id>/delete`
- **View your profile**: `/users/profile`

## Session Configuration

- Session timeout: 1 day (24 hours)

## Security Notes

- Authentication is via the Tailscale Local API (`whois`) only. The app must be reachable only over networks you control (typically your private Pi + Tailscale), not the public internet.
- Email in `users` must match the Tailscale login (email) for that person to sign in.
- Admins cannot delete their own accounts.

## Troubleshooting

### "Your Tailscale account is not authorized"
- No user row exists for your Tailscale email. An admin must add you via User Management with that exact email.

### "Login requires Tailscale"
- You are not reaching the app over Tailscale. Make sure your client is connected to the same tailnet and use the Pi’s Tailscale address (see [TAILSCALE_AUTH.md](TAILSCALE_AUTH.md)).

### "Users table not found"
- Run: `sqlite3 data/database/production.db < sql/user_management_tables.sql`

### "Email already registered"
- That email already has a user. Use a different email or check: `SELECT * FROM users;`

## Database Schema (after migration 020)

```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'AE' CHECK (role IN ('admin', 'management', 'AE')),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
