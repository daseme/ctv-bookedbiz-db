# User Management Setup Instructions

This document provides instructions for setting up user management in the CTV Booked Biz application.

## Prerequisites

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   This will install `flask-login==0.6.3` and other required packages.

2. **Run Database Migration**
   ```bash
   sqlite3 data/database/production.db < sql/user_management_tables.sql
   ```
   
   Or if using a different database path:
   ```bash
   sqlite3 /path/to/your/database.db < sql/user_management_tables.sql
   ```

## Creating the Initial Admin User

### Method 1: Using the Script (Recommended)

Run the provided script:

```bash
python scripts/create_admin_user.py
```

The script will prompt you for:
- First Name
- Last Name
- Email (must be unique)
- Password (minimum 8 characters)
- Password confirmation

The user will be created with the `admin` role.

### Method 2: Using SQLite Directly

If you prefer to create the user manually:

```bash
sqlite3 data/database/production.db
```

Then run:

```sql
-- Replace these values with your actual user details
INSERT INTO users (first_name, last_name, email, password_hash, role)
VALUES (
    'YourFirstName',
    'YourLastName',
    'your.email@example.com',
    '--GENERATED_HASH--',  -- See below for generating hash
    'admin'
);
```

**To generate a password hash**, use Python:

```python
from werkzeug.security import generate_password_hash
print(generate_password_hash('your_password_here'))
```

Copy the output and use it as the `password_hash` value in the SQL INSERT statement.

### Method 3: Using Python Interactively

```python
from werkzeug.security import generate_password_hash
from src.database.connection import DatabaseConnection
from src.config.settings import get_settings

settings = get_settings()
db = DatabaseConnection(settings.database.db_path)

password_hash = generate_password_hash('your_password_here')

with db.transaction() as conn:
    conn.execute(
        """
        INSERT INTO users (first_name, last_name, email, password_hash, role)
        VALUES (?, ?, ?, ?, 'admin')
        """,
        ('YourFirstName', 'YourLastName', 'your.email@example.com', password_hash)
    )
```

## User Roles

The system supports three roles:

- **admin**: Full access to all features, including user management
- **management**: Access to management-level reports and features
- **AE**: Access to AE-level reports and features

## Accessing the Application

1. Start the Flask application:
   ```bash
   python -m src.web.app
   ```
   Or using your deployment method.

2. Navigate to the login page:
   ```
   http://localhost:5000/users/login
   ```

3. Log in with the admin credentials you created.

## User Management Features

Once logged in as an admin, you can:

- **View all users**: `/users/`
- **Create new users**: `/users/create`
- **Edit users**: `/users/<user_id>/edit`
- **Deactivate users**: `/users/<user_id>/deactivate`
- **Delete users**: `/users/<user_id>/delete`
- **View your profile**: `/users/profile`
- **Change your password**: `/users/profile/change-password`

## Session Configuration

- **Session timeout**: 1 day (24 hours)
- **Remember me**: Not implemented (sessions expire on browser close)

## Security Notes

- Passwords are hashed using Werkzeug's password hashing (PBKDF2)
- Email addresses must be unique
- Passwords must be at least 8 characters long
- Admins cannot delete or deactivate their own accounts
- All user management routes require admin authentication

## Troubleshooting

### "Users table not found"
- Make sure you've run the migration script: `sqlite3 data/database/production.db < sql/user_management_tables.sql`

### "Email already registered"
- The email address you're trying to use is already in the database
- Use a different email or check existing users: `SELECT * FROM users;`

### "Authentication failed"
- Verify the email and password are correct
- Check that the user account is active: `SELECT is_active FROM users WHERE email = 'your@email.com';`

### "Admin access required"
- You need to be logged in as an admin to access user management features
- Only users with the `admin` role can create, edit, or delete users

## Database Schema

The users table structure:

```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'AE' CHECK (role IN ('admin', 'management', 'AE')),
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
