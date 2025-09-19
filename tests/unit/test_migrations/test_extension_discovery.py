"""Test extension migration discovery functionality."""

import tempfile
from pathlib import Path

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands


def test_extension_migration_discovery() -> None:
    """Test that extension migrations are discovered when configured."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create config with extension migrations enabled
        config = SqliteConfig(
            pool_config={"database": ":memory:"},
            migration_config={
                "script_location": str(temp_dir),
                "version_table_name": "test_migrations",
                "include_extensions": ["litestar"],
            },
        )

        # Create migration commands
        commands = SyncMigrationCommands(config)

        # Check that extension migrations were discovered
        assert hasattr(commands, "runner")
        assert hasattr(commands.runner, "extension_migrations")

        # Should have discovered Litestar migrations
        if "litestar" in commands.runner.extension_migrations:
            litestar_path = commands.runner.extension_migrations["litestar"]
            assert litestar_path.exists()
            assert litestar_path.name == "migrations"

            # Check for the session table migration
            migration_file = litestar_path / "0001_create_session_table.py"
            assert migration_file.exists()

def test_extension_migration_context() -> None:
    """Test that migration context is created with dialect information."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create config with known dialect
        config = SqliteConfig(
            pool_config={"database": ":memory:"},
            migration_config={"script_location": str(temp_dir), "include_extensions": ["litestar"]},
        )

        # Create migration commands - this should create context
        commands = SyncMigrationCommands(config)

        # The runner should have a context with dialect
        assert hasattr(commands.runner, "context")
        assert commands.runner.context is not None
        assert commands.runner.context.dialect == "sqlite"


def test_no_extensions_by_default() -> None:
    """Test that no extension migrations are included by default."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create config without extension migrations
        config = SqliteConfig(
            pool_config={"database": ":memory:"},
            migration_config={
                "script_location": str(temp_dir)
                # No include_extensions key
            },
        )

        # Create migration commands
        commands = SyncMigrationCommands(config)

        # Should have no extension migrations
        assert commands.runner.extension_migrations == {}


def test_migration_file_discovery_with_extensions() -> None:
    """Test that migration files are discovered from both primary and extension paths."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)

        # Create a primary migration
        primary_migration = migrations_dir / "0002_user_table.sql"
        primary_migration.write_text("""
-- name: migrate-0002-up
CREATE TABLE users (id INTEGER);

-- name: migrate-0002-down
DROP TABLE users;
""")

        # Create config with extension migrations
        config = SqliteConfig(
            pool_config={"database": ":memory:"},
            migration_config={"script_location": str(migrations_dir), "include_extensions": ["litestar"]},
        )

        # Create migration commands
        commands = SyncMigrationCommands(config)

        # Get all migration files
        migration_files = commands.runner.get_migration_files()

        # Should have both primary and extension migrations
        versions = [version for version, _ in migration_files]

        # Primary migration
        assert "0002" in versions

        # Extension migrations should be prefixed
        extension_versions = [v for v in versions if v.startswith("ext_")]
        assert len(extension_versions) > 0

        # Check that Litestar migration is included
        litestar_versions = [v for v in versions if "ext_litestar" in v]
        assert len(litestar_versions) > 0
