"""SQLSpec Migration Tool.

A native migration system for SQLSpec that leverages the SQLFileLoader
and driver system for database versioning.
"""

from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands
from sqlspec.migrations.loaders import (
    BaseMigrationLoader,
    MigrationLoadError,
    PythonFileLoader,
    SQLFileLoader,
    get_migration_loader,
)
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner, create_migration_runner
from sqlspec.migrations.schema import SchemaEnsureResult, SchemaTarget, ensure_schema_async, ensure_schema_sync
from sqlspec.migrations.squash import MigrationSquasher, SquashPlan
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.migrations.utils import create_migration_file, get_author

__all__ = (
    "AsyncMigrationCommands",
    "AsyncMigrationRunner",
    "AsyncMigrationTracker",
    "BaseMigrationLoader",
    "MigrationLoadError",
    "MigrationSquasher",
    "PythonFileLoader",
    "SQLFileLoader",
    "SchemaEnsureResult",
    "SchemaTarget",
    "SquashPlan",
    "SyncMigrationCommands",
    "SyncMigrationRunner",
    "SyncMigrationTracker",
    "create_migration_commands",
    "create_migration_file",
    "create_migration_runner",
    "ensure_schema_async",
    "ensure_schema_sync",
    "get_author",
    "get_migration_loader",
)
