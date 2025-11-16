#!/bin/bash
# SQLSpec Migration Workflow Example
#
# Demonstrates hybrid versioning workflow:
# - Timestamp migrations in development (no conflicts)
# - Sequential numbering before merge (deterministic ordering)

set -e

echo "=== SQLSpec Migration Workflow ==="
echo ""

# 1. Initialize migration directory (first time only)
echo "1. Initialize migrations..."
sqlspec --config myapp.config init
# Creates: migrations/ directory with tracking table

echo ""

# 2. Create first migration (timestamp in dev)
echo "2. Create migration: Add users table..."
sqlspec --config myapp.config create-migration -m "Add users table"
# Creates: migrations/20251115120000_add_users_table.sql

echo ""

# 3. Create second migration (another developer, different timestamp)
echo "3. Create migration: Add posts table..."
sqlspec --config myapp.config create-migration -m "Add posts table"
# Creates: migrations/20251115123000_add_posts_table.sql

echo ""

# 4. Create Python migration for data seeding
echo "4. Create Python migration: Seed admin user..."
sqlspec --config myapp.config create-migration -m "Seed admin user" --format py
# Creates: migrations/20251115124500_seed_admin_user.py

echo ""

# 5. Apply migrations in development
echo "5. Apply all pending migrations..."
sqlspec --config myapp.config upgrade
# Applies all pending migrations in order

echo ""

# 6. Check current state
echo "6. Show current revision..."
sqlspec --config myapp.config show-current-revision

echo ""

# 7. View migration history
echo "7. Show migration history..."
sqlspec --config myapp.config history

echo ""

# 8. Before merging to main: Convert to sequential numbering
echo "8. Fix migrations (convert timestamp → sequential)..."
sqlspec --config myapp.config fix --yes
# Converts:
#   20251115120000_add_users_table.sql  → 0001_add_users_table.sql
#   20251115123000_add_posts_table.sql  → 0002_add_posts_table.sql
#   20251115124500_seed_admin_user.py   → 0003_seed_admin_user.py
# Updates sqlspec_version table automatically

echo ""

# 9. Validate migrations
echo "9. Validate migration consistency..."
sqlspec --config myapp.config check

echo ""

# 10. Rollback if needed
echo "10. Example rollback (downgrade one migration)..."
sqlspec --config myapp.config downgrade -1

echo ""

# 11. Re-apply
echo "11. Re-apply migrations..."
sqlspec --config myapp.config upgrade

echo ""

# 12. Commit changes
echo "12. Commit migrations to version control..."
git add migrations/
git commit -m "feat: add users and posts tables with admin seed"

echo ""
echo "=== Migration workflow complete! ==="
echo ""
echo "Best practices:"
echo "  - Create timestamp migrations during development (no conflicts)"
echo "  - Run 'sqlspec fix --yes' before merging to main"
echo "  - Never modify applied migrations - create new ones to fix"
echo "  - Always provide downgrade paths"
echo "  - Test migrations in staging before production"
