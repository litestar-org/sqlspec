"""Create ADK session, events, and memory tables migration using store DDL definitions."""

import inspect
import logging
from typing import TYPE_CHECKING, NoReturn, cast

from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk._config_utils import (
    _get_adk_adapter_store_class,
    _get_adk_memory_migration_store_class,
    _is_adk_memory_migration_enabled,
)
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
    from sqlspec.extensions.adk.store import BaseAsyncADKStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")

logger = get_logger("sqlspec.migrations.adk.tables")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Create the ADK session, events, and memory tables using store DDL definitions.

    This migration delegates to the appropriate store class to generate
    dialect-specific DDL. The store classes contain the single source of
    truth for table schemas.

    Args:
        context: Migration context containing config.

    Returns:
        List of SQL statements to execute for upgrade.

    Notes:
        Configuration is read from context.config.extension_config["adk"].
        Supports custom table names and optional owner_id_column for linking
        sessions to owner tables (users, tenants, teams, etc.).
        Memory table is included if enable_memory or include_memory_migration is True.
    """
    if context is None or context.config is None:
        _raise_missing_config()

    store_class = _get_store_class(context)
    store_instance = store_class(config=context.config)

    statements = [
        await store_instance._get_create_sessions_table_sql(),  # pyright: ignore[reportPrivateUsage]
        await store_instance._get_create_events_table_sql(),  # pyright: ignore[reportPrivateUsage]
    ]

    if _is_memory_enabled(context):
        memory_store_class = _get_memory_store_class(context)
        if memory_store_class is not None:
            memory_store = memory_store_class(config=context.config)
            memory_sql = memory_store._get_create_memory_table_sql()  # pyright: ignore[reportPrivateUsage]
            if inspect.isawaitable(memory_sql):
                memory_sql = await memory_sql
            if isinstance(memory_sql, list):
                statements.extend(memory_sql)
            else:
                statements.append(memory_sql)
            log_with_context(
                logger, logging.DEBUG, "adk.migration.memory.include", table_name=memory_store.memory_table
            )

    return statements


def _get_store_class(context: "MigrationContext | None") -> "type[BaseAsyncADKStore]":
    """Get the appropriate store class based on the config's module path.

    Args:
        context: Migration context containing config.

    Returns:
        Store class matching the config's adapter.

    Notes:
        Dynamically imports the store class from the config's module path.
        For example, AsyncpgConfig at 'sqlspec.adapters.asyncpg.config'
        maps to AsyncpgADKStore at 'sqlspec.adapters.asyncpg.adk.AsyncpgADKStore'.
    """
    if not context or not context.config:
        _raise_missing_config()

    return cast("type[BaseAsyncADKStore]", _get_adk_adapter_store_class(context.config, "ADKStore"))


def _get_memory_store_class(
    context: "MigrationContext | None",
) -> "type[BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore] | None":
    """Get the appropriate memory store class based on the config's module path.

    Args:
        context: Migration context containing config.

    Returns:
        Memory store class matching the config's adapter, or None if not available.

    Notes:
        Dynamically imports the memory store class from the config's module path.
        For example, AsyncpgConfig at 'sqlspec.adapters.asyncpg.config'
        maps to AsyncpgADKMemoryStore at 'sqlspec.adapters.asyncpg.adk.AsyncpgADKMemoryStore'.
    """
    if not context or not context.config:
        return None

    store_class = _get_adk_memory_migration_store_class(context.config)
    if store_class is None:
        log_with_context(logger, logging.DEBUG, "adk.migration.memory_store.missing")
        return None
    return cast("type[BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore]", store_class)


def _is_memory_enabled(context: "MigrationContext | None") -> bool:
    """Check if memory migration is enabled in the config.

    Args:
        context: Migration context containing config.

    Returns:
        True if memory migration should be included, False otherwise.

    Notes:
        Checks config.extension_config["adk"]["include_memory_migration"].
        Defaults to True if not specified and enable_memory is True.
    """
    if not context or not context.config:
        return False

    return _is_adk_memory_migration_enabled(context.config)


def _raise_missing_config() -> NoReturn:
    """Raise error when migration context has no config.

    Raises:
        SQLSpecError: Always raised.
    """
    msg = "Migration context must have a config to determine store class"
    raise SQLSpecError(msg)


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Drop the ADK session, events, and memory tables using store DDL definitions.

    This migration delegates to the appropriate store class to generate
    dialect-specific DROP statements. The store classes contain the single
    source of truth for table schemas.

    Args:
        context: Migration context containing config.

    Returns:
        List of SQL statements to execute for downgrade.

    Notes:
        Configuration is read from context.config.extension_config["adk"].
        Memory table is included if enable_memory or include_memory_migration is True.
    """
    if context is None or context.config is None:
        _raise_missing_config()

    statements: list[str] = []

    if _is_memory_enabled(context):
        memory_store_class = _get_memory_store_class(context)
        if memory_store_class is not None:
            memory_store = memory_store_class(config=context.config)
            memory_drop_stmts = memory_store._get_drop_memory_table_sql()  # pyright: ignore[reportPrivateUsage]
            statements.extend(memory_drop_stmts)
            log_with_context(
                logger, logging.DEBUG, "adk.migration.memory.drop.include", table_name=memory_store.memory_table
            )

    store_class = _get_store_class(context)
    store_instance = store_class(config=context.config)
    statements.extend(store_instance._get_drop_tables_sql())  # pyright: ignore[reportPrivateUsage]

    return statements
