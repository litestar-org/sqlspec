"""SQLSpec Migration Tool.

A native migration system for SQLSpec that leverages the SQLFileLoader
and driver system for database versioning.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
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

_EXPORTS: dict[str, tuple[str, str | None]] = {
    "AsyncMigrationCommands": ("sqlspec.migrations.commands", "AsyncMigrationCommands"),
    "AsyncMigrationRunner": ("sqlspec.migrations.runner", "AsyncMigrationRunner"),
    "AsyncMigrationTracker": ("sqlspec.migrations.tracker", "AsyncMigrationTracker"),
    "BaseMigrationLoader": ("sqlspec.migrations.loaders", "BaseMigrationLoader"),
    "MigrationLoadError": ("sqlspec.migrations.loaders", "MigrationLoadError"),
    "MigrationSquasher": ("sqlspec.migrations.squash", "MigrationSquasher"),
    "PythonFileLoader": ("sqlspec.migrations.loaders", "PythonFileLoader"),
    "SQLFileLoader": ("sqlspec.migrations.loaders", "SQLFileLoader"),
    "SchemaEnsureResult": ("sqlspec.migrations.schema", "SchemaEnsureResult"),
    "SchemaTarget": ("sqlspec.migrations.schema", "SchemaTarget"),
    "SquashPlan": ("sqlspec.migrations.squash", "SquashPlan"),
    "SyncMigrationCommands": ("sqlspec.migrations.commands", "SyncMigrationCommands"),
    "SyncMigrationRunner": ("sqlspec.migrations.runner", "SyncMigrationRunner"),
    "SyncMigrationTracker": ("sqlspec.migrations.tracker", "SyncMigrationTracker"),
    "commands": ("sqlspec.migrations.commands", None),
    "create_migration_commands": ("sqlspec.migrations.commands", "create_migration_commands"),
    "create_migration_file": ("sqlspec.migrations.utils", "create_migration_file"),
    "create_migration_runner": ("sqlspec.migrations.runner", "create_migration_runner"),
    "ensure_schema_async": ("sqlspec.migrations.schema", "ensure_schema_async"),
    "ensure_schema_sync": ("sqlspec.migrations.schema", "ensure_schema_sync"),
    "get_author": ("sqlspec.migrations.utils", "get_author"),
    "get_migration_loader": ("sqlspec.migrations.loaders", "get_migration_loader"),
    "loaders": ("sqlspec.migrations.loaders", None),
    "runner": ("sqlspec.migrations.runner", None),
    "schema": ("sqlspec.migrations.schema", None),
    "squash": ("sqlspec.migrations.squash", None),
    "tracker": ("sqlspec.migrations.tracker", None),
    "utils": ("sqlspec.migrations.utils", None),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    module = import_module(module_name)
    value = module if attribute is None else getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
