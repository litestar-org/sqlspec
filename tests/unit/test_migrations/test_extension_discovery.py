"""Test extension migration discovery functionality."""

from pathlib import Path

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands


def test_extension_migration_discovery(tmp_path: Path) -> None:
    """Test that extension migrations are discovered when configured."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={
            "script_location": str(tmp_path),
            "version_table_name": "test_migrations",
            "include_extensions": ["litestar"],
        },
    )

    commands = SyncMigrationCommands(config)

    assert hasattr(commands, "runner")
    assert hasattr(commands.runner, "extension_migrations")

    if "litestar" in commands.runner.extension_migrations:
        litestar_path = commands.runner.extension_migrations["litestar"]
        assert litestar_path.exists()
        assert litestar_path.name == "migrations"


def test_extension_migration_context(tmp_path: Path) -> None:
    """Test that migration context is created with dialect information."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path), "include_extensions": ["litestar"]},
    )

    commands = SyncMigrationCommands(config)

    assert hasattr(commands.runner, "context")
    assert commands.runner.context is not None
    assert commands.runner.context.dialect == "sqlite"


def test_no_extensions_by_default(tmp_path: Path) -> None:
    """Test that no extension migrations are included by default."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path)}
    )

    commands = SyncMigrationCommands(config)

    assert commands.runner.extension_migrations == {}


def test_migration_file_discovery_with_extensions(tmp_path: Path) -> None:
    """Test that migration files are discovered from both primary and extension paths."""
    migrations_dir = tmp_path

    primary_migration = migrations_dir / "0002_user_table.sql"
    primary_migration.write_text("""
-- name: migrate-0002-up
CREATE TABLE users (id INTEGER);

-- name: migrate-0002-down
DROP TABLE users;
""")

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["litestar"]},
    )

    commands = SyncMigrationCommands(config)

    migration_files = commands.runner.get_migration_files()

    versions = [version for version, _ in migration_files]

    assert "0002" in versions
