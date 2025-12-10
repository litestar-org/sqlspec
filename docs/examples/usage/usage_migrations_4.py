"""Using AsyncMigrationCommands directly."""

import os
import tempfile
from pathlib import Path

__all__ = ("test_async_command_class_methods",)


async def test_async_command_class_methods() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir()

        # start-example
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.migrations.commands import AsyncMigrationCommands

        dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
        config = AsyncpgConfig(connection_config={"dsn": dsn}, migration_config={"script_location": str(migration_dir)})

        # Create commands instance
        commands = AsyncMigrationCommands(config)

        # Use commands directly
        await commands.init(str(migration_dir))
        await commands.upgrade("head")
        # end-example

        # Smoke test for AsyncMigrationCommands method presence
        assert hasattr(commands, "upgrade")
        assert hasattr(commands, "downgrade")
        assert hasattr(commands, "current")
        assert hasattr(commands, "revision")
        assert hasattr(commands, "stamp")
        assert hasattr(commands, "fix")
        assert hasattr(commands, "init")
