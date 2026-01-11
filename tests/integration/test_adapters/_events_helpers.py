"""Shared helpers for adapter event channel integration tests."""

from pathlib import Path
from typing import Any

from sqlspec import SQLSpec
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


def prepare_events_migrations(tmp_path: "Path") -> "Path":
    """Create and return a migrations directory inside tmp_path."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir


async def setup_async_event_channel(config: Any) -> 'tuple["SQLSpec", Any]':
    """Run async migrations and return SQLSpec + event channel."""
    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    return spec, spec.event_channel(config)


def setup_sync_event_channel(config: Any) -> 'tuple["SQLSpec", Any]':
    """Run sync migrations and return SQLSpec + event channel."""
    commands = SyncMigrationCommands(config)
    commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    return spec, spec.event_channel(config)
