"""Shared fixtures for events integration tests."""

import pytest

from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


@pytest.fixture
def sqlite_events_config(tmp_path):
    """Create SQLite config with events extension configured."""
    from sqlspec.adapters.sqlite import SqliteConfig

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = SyncMigrationCommands(config)
    commands.upgrade()

    yield config


@pytest.fixture
async def aiosqlite_events_config(tmp_path):
    """Create AioSQLite config with events extension configured."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "async_events.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    yield config

    await config.close_pool()
