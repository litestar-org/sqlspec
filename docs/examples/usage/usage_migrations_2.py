# start-example
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_sync_methods",)


config = SqliteConfig(
    pool_config={"database": "myapp.db"}, migration_config={"enabled": True, "script_location": "migrations"}
)

# Apply migrations (no await needed)
config.migrate_up("head")
# Or use the alias
config.upgrade("head")

# Rollback one revision
config.migrate_down("-1")
# Or use the alias
config.downgrade("-1")

# Check current version
current = config.get_current_migration(verbose=True)
print(current)

# Create new migration
config.create_migration("add users table", file_type="sql")

# Initialize migrations directory
config.init_migrations()

# Stamp database to specific revision
config.stamp_migration("0003")

# Convert timestamp to sequential migrations
config.fix_migrations(dry_run=False, update_database=True, yes=True)
# end-example


def test_sync_methods() -> None:
    # Smoke tests for method presence, not actual DB calls
    assert hasattr(config, "migrate_up")
    assert hasattr(config, "upgrade")
    assert hasattr(config, "migrate_down")
    assert hasattr(config, "downgrade")
    assert hasattr(config, "get_current_migration")
    assert hasattr(config, "create_migration")
    assert hasattr(config, "init_migrations")
    assert hasattr(config, "stamp_migration")
    assert hasattr(config, "fix_migrations")
