"""Sync migration commands via config methods."""

import tempfile
from pathlib import Path

__all__ = ("test_sync_methods",)


def test_sync_methods() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir()
        temp_db = Path(temp_dir) / "test.db"

        # start-example
        from sqlspec.adapters.sqlite import SqliteConfig

        config = SqliteConfig(
            connection_config={"database": str(temp_db)},
            migration_config={"enabled": True, "script_location": str(migration_dir)},
        )

        # Initialize migrations directory (creates __init__.py if package=True)
        config.init_migrations()

        # Create new migration file
        config.create_migration("add users table", file_type="sql")

        # Apply migrations to head (no await needed for sync)
        config.migrate_up("head")

        # Rollback one revision
        config.migrate_down("-1")

        # Check current version
        current = config.get_current_migration(verbose=True)
        print(current)

        # Stamp database to specific revision
        config.stamp_migration("0001")

        # Convert timestamp to sequential migrations
        config.fix_migrations(dry_run=True, update_database=False, yes=True)
        # end-example
