# Migration Patterns

Guide to managing database migrations with SQLSpec.

## CLI Commands

```bash
# Initialize migration directory
sqlspec --config myapp.config init

# Create SQL migration (default)
sqlspec --config myapp.config create-migration -m "Add users table"

# Create Python migration
sqlspec --config myapp.config create-migration -m "Seed data" --format py

# Apply all pending migrations
sqlspec --config myapp.config upgrade

# Apply to specific revision
sqlspec --config myapp.config upgrade 0003

# Rollback one migration
sqlspec --config myapp.config downgrade -1

# Rollback all
sqlspec --config myapp.config downgrade base

# Show current revision
sqlspec --config myapp.config show-current-revision

# Show migration history
sqlspec --config myapp.config history

# Validate migrations
sqlspec --config myapp.config check
```

## Hybrid Versioning Workflow

**Development: Timestamp migrations (no merge conflicts)**
```bash
$ sqlspec create-migration -m "add users table"
Created: migrations/20251115120000_add_users_table.sql

$ sqlspec create-migration -m "add posts table"
Created: migrations/20251115123000_add_posts_table.sql
```

**Before merging: Convert to sequential**
```bash
$ sqlspec fix --yes
✓ Converted migrations/20251115120000_add_users_table.sql → 0001_add_users_table.sql
✓ Converted migrations/20251115123000_add_posts_table.sql → 0002_add_posts_table.sql
✓ Updated sqlspec_version table
```

## Programmatic Control

```python
# Async
await config.migrate_up("head")  # Apply all
await config.migrate_up("0003")  # Apply to specific
await config.migrate_down("-1")  # Rollback one
await config.migrate_down("base")  # Rollback all

# Sync
config.migrate_up("head")
config.migrate_down("-1")
```

## Migration File Structure

**SQL Migration:**
```sql
-- revision: 0001
-- down_revision: None
-- description: Add users table

-- upgrade --
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);

-- downgrade --
DROP INDEX idx_users_email;
DROP TABLE users;
```

**Python Migration:**
```python
"""Add users table

Revision ID: 0001
Down Revision: None
"""

def upgrade(driver):
    driver.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    """)

def downgrade(driver):
    driver.execute("DROP TABLE users")
```

## Configuration

```python
config = AsyncpgConfig(
    pool_config={...},
    migration_config={
        "script_location": "migrations",
        "version_table": "sqlspec_version",
        "include_extensions": ["litestar"],
        "template_directory": "templates/migrations",
    }
)
```

## Best Practices

1. **Use hybrid versioning**: Timestamps in dev, sequential before merge
2. **Always provide down migrations**: Enable rollback capability
3. **Test migrations**: Run upgrade/downgrade in test environment
4. **Use transactions**: Wrap migrations in transactions when possible
5. **Keep migrations small**: One logical change per migration
6. **Never modify applied migrations**: Create new migration to fix
