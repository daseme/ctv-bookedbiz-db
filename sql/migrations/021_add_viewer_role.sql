-- Migration 021: Add viewer role to CHECK constraint
-- Recreate users table with 'viewer' added to the role CHECK.
-- Role hierarchy: admin > management > AE > viewer
-- Run: sqlite3 <your.db> < sql/migrations/021_add_viewer_role.sql

CREATE TABLE IF NOT EXISTS users_new (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'AE'
        CHECK (role IN ('admin', 'management', 'AE', 'viewer')),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users_new (
    user_id, first_name, last_name, email,
    role, created_date, last_login, updated_date
)
SELECT
    user_id, first_name, last_name, email,
    role, created_date, last_login, updated_date
FROM users;

DROP TABLE users;
ALTER TABLE users_new RENAME TO users;

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

INSERT OR REPLACE INTO sqlite_sequence (name, seq)
SELECT 'users', COALESCE(MAX(user_id), 0) FROM users;
