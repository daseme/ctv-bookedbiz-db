-- Migration 020: Tailscale auth - remove password_hash column entirely
-- Purpose: Credentialing via Tailscale identity (Tailscale-User-Login, Tailscale-User-Name).
--          App must be reached via Tailscale Serve (localhost-only) so identity headers are trusted.
-- Run: sqlite3 <your.db> < sql/migrations/020_tailscale_auth_remove_passwords.sql

-- SQLite: recreate users table without password_hash.
CREATE TABLE IF NOT EXISTS users_new (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'AE' CHECK (role IN ('admin', 'management', 'AE')),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users_new (user_id, first_name, last_name, email, role, created_date, last_login, updated_date)
SELECT user_id, first_name, last_name, email, role, created_date, last_login, updated_date
FROM users;

DROP TABLE users;
ALTER TABLE users_new RENAME TO users;

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

-- Restore AUTOINCREMENT sequence so new users get correct user_id
INSERT OR REPLACE INTO sqlite_sequence (name, seq) SELECT 'users', COALESCE(MAX(user_id), 0) FROM users;
