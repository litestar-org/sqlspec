"""Test extension migration discovery functionality."""

import tempfile

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands


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
