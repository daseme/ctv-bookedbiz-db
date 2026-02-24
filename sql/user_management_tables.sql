-- ============================================================================
-- User Management Schema Migration
-- Run against production.db
-- ============================================================================

-- ============================================================================
-- 1. Users Table
-- Stores user accounts with authentication and authorization information
-- ============================================================================

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'AE' CHECK (role IN ('admin', 'management', 'AE')),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_email 
ON users(email);

CREATE INDEX IF NOT EXISTS idx_users_role 
ON users(role);
