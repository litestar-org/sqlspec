"""SQLSpec Migration Tool.

A native migration system for SQLSpec that leverages the SQLFileLoader
and driver architecture for database versioning.
"""

from sqlspec.migrations.commands import AsyncMigrationCommands, MigrationCommands, SyncMigrationCommands
from sqlspec.migrations.loaders import (
    BaseMigrationLoader,
    MigrationLoadError,
    PythonFileLoader,
    SQLFileLoader,
    get_migration_loader,
)
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.migrations.utils import create_migration_file, drop_all, get_author

__all__ = (
    "AsyncMigrationCommands",
    "AsyncMigrationRunner",
    "AsyncMigrationTracker",
    "BaseMigrationLoader",
    "MigrationCommands",
    "MigrationLoadError",
    "PythonFileLoader",
    "SQLFileLoader",
    "SyncMigrationCommands",
    "SyncMigrationRunner",
    "SyncMigrationTracker",
    "create_migration_file",
    "drop_all",
    "get_author",
    "get_migration_loader",
)
