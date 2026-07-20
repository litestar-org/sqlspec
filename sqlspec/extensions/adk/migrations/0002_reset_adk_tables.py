"""Reset ADK schema to the 2.0 clean-break shape.

Unconditionally drops any legacy ADK tables (sessions, events, app_states,
user_states, metadata, memory) then creates the new schema and seeds the
internal metadata row. The memory table is dropped unconditionally so users
moving from ``enable_memory=True`` to ``enable_memory=False`` get cleanup; it
is recreated only when memory is enabled for the current config.
"""

import inspect
import logging
from typing import TYPE_CHECKING, NoReturn, cast

from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk._config_utils import (
    _adk_adapter_store_class,
    _adk_memory_migration_enabled,
    _adk_memory_migration_store_class,
)
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
    from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")

logger = get_logger("sqlspec.migrations.adk.reset")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    if context is None or context.config is None:
        _raise_missing_config()

    store_class = _get_store_class(context)
    store_instance = store_class(config=context.config)
    await _prepare_schema(store_instance, context)

    statements: list[str] = []

    memory_store_class = _get_memory_store_class(context)
    if memory_store_class is not None:
        memory_store = memory_store_class(config=context.config)
        await _prepare_schema(memory_store, context)
        statements.extend(memory_store._reset_drop_memory_table_sql())  # pyright: ignore[reportPrivateUsage]
        log_with_context(logger, logging.DEBUG, "adk.migration.reset.memory.drop", table_name=memory_store.memory_table)

    statements.extend(store_instance._reset_drop_tables_sql())  # pyright: ignore[reportPrivateUsage]

    statements.extend([
        await _resolve_sql(store_instance._sessions_table_ddl()),  # pyright: ignore[reportPrivateUsage]
        await _resolve_sql(store_instance._events_table_ddl()),  # pyright: ignore[reportPrivateUsage]
        await _resolve_sql(store_instance._app_states_table_ddl()),  # pyright: ignore[reportPrivateUsage]
        await _resolve_sql(store_instance._user_states_table_ddl()),  # pyright: ignore[reportPrivateUsage]
        await _resolve_sql(store_instance._metadata_table_ddl()),  # pyright: ignore[reportPrivateUsage]
    ])

    if _is_memory_enabled(context) and memory_store_class is not None:
        memory_store = memory_store_class(config=context.config)
        memory_sql = memory_store._memory_table_ddl()  # pyright: ignore[reportPrivateUsage]
        if inspect.isawaitable(memory_sql):
            memory_sql = await memory_sql
        if isinstance(memory_sql, list):
            statements.extend(memory_sql)
        else:
            statements.append(memory_sql)
        log_with_context(
            logger, logging.DEBUG, "adk.migration.reset.memory.create", table_name=memory_store.memory_table
        )

    return statements


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    if context is None or context.config is None:
        _raise_missing_config()

    statements: list[str] = []
    store_class = _get_store_class(context)
    store_instance = store_class(config=context.config)

    if _is_memory_enabled(context):
        memory_store_class = _get_memory_store_class(context)
        if memory_store_class is not None:
            memory_store = memory_store_class(config=context.config)
            statements.extend(memory_store._reset_drop_memory_table_sql())  # pyright: ignore[reportPrivateUsage]

    statements.extend(store_instance._reset_drop_tables_sql())  # pyright: ignore[reportPrivateUsage]

    return statements


def _raise_missing_config() -> NoReturn:
    msg = "Migration context must have a config to determine store class"
    raise SQLSpecError(msg)


def _get_store_class(context: "MigrationContext | None") -> "type[BaseAsyncADKStore | BaseSyncADKStore]":
    if not context or not context.config:
        _raise_missing_config()
    return cast("type[BaseAsyncADKStore | BaseSyncADKStore]", _adk_adapter_store_class(context.config, "ADKStore"))


async def _resolve_sql(value: "str | Awaitable[str]") -> str:
    if inspect.isawaitable(value):
        return await value
    return value


async def _prepare_schema(
    store: "BaseAsyncADKStore | BaseSyncADKStore | BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore",
    context: "MigrationContext",
) -> None:
    driver = getattr(context, "driver", None)
    if driver is None:
        return
    if getattr(context, "is_async_driver", False):
        await cast("BaseAsyncADKStore | BaseAsyncADKMemoryStore", store).prepare_schema_async(driver)
        return
    cast("BaseSyncADKStore | BaseSyncADKMemoryStore", store).prepare_schema_sync(driver)


def _get_memory_store_class(
    context: "MigrationContext | None",
) -> "type[BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore] | None":
    if not context or not context.config:
        return None
    store_class = _adk_memory_migration_store_class(context.config)
    if store_class is None:
        log_with_context(logger, logging.DEBUG, "adk.migration.reset.memory_store.missing")
        return None
    return cast("type[BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore]", store_class)


def _is_memory_enabled(context: "MigrationContext | None") -> bool:
    if not context or not context.config:
        return False
    return _adk_memory_migration_enabled(context.config)
