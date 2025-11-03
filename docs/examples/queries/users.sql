-- User Management SQL Queries
-- This file contains all user-related queries using aiosql-style named queries

-- name: get_user_by_id
-- Get a single user by their ID
SELECT

    id,
    username,
    email,
    created_at,
    updated_at
FROM users
WHERE id = :user_id;

-- name: get_user_by_email
-- Find a user by their email address
SELECT
    id,
    username,
    email,
    created_at
FROM users
WHERE LOWER(email) = LOWER(:email);

-- name: list_active_users
-- List all active users with pagination
SELECT
    id,
    username,
    email,
    last_login_at
FROM users
WHERE is_active = true
ORDER BY username
LIMIT :limit OFFSET :offset;

-- name: create_user
-- Create a new user and return the created record
INSERT INTO users (
    username,
    email,
    password_hash,
    is_active
) VALUES (
    :username,
    :email,
    :password_hash,
    :is_active
)
RETURNING id, username, email, created_at;

-- name: update_user_last_login
-- Update the last login timestamp for a user
UPDATE users
SET
    last_login_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :user_id;

-- name: deactivate_user
-- Soft delete a user by setting is_active to false
UPDATE users
SET
    is_active = false,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :user_id;

-- name: count_users_by_status
-- Count users grouped by their active status
SELECT
    is_active,
    COUNT(*) as count
FROM users
GROUP BY is_active;
