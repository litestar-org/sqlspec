"""Create ADK tables migration.

Creates the canonical ADK tables:
- adk_session
- adk_event
- adk_app_state
- adk_user_state
- adk_internal_metadata
- adk_memory (when memory is enabled)

On PostgreSQL the migration installs required extensions before the first
memory statement that uses them.
"""

import inspect
import logging
import re
from typing import TYPE_CHECKING, NoReturn, cast

from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk._config_utils import (
    _adk_adapter_store_class,
    _adk_memory_migration_enabled,
    _adk_memory_migration_store_class,
    _adk_sessions_migration_enabled,
)
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")

logger = get_logger("sqlspec.migrations.adk.create")

CREATE_VECTOR_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"
CREATE_PG_TEXTSEARCH_EXTENSION = "CREATE EXTENSION IF NOT EXISTS pg_textsearch"

_POSTGRES_DIALECTS = frozenset({"postgres", "postgresql", "pgvector", "paradedb"})
_VECTOR_COLUMN_PATTERN = re.compile(r"\bVECTOR\s*\(", re.IGNORECASE)
_BM25_INDEX_PATTERN = re.compile(r"\bUSING\s+bm25\s*\(", re.IGNORECASE)


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    if context is None or context.config is None:
        _raise_missing_config()

    statements: list[str] = []

    if _is_sessions_enabled(context):
        store_class = _get_store_class(context)
        store_instance = store_class(config=context.config)
        await _prepare_schema(store_instance, context)
        statements.extend([
            await _resolve_sql(store_instance._sessions_table_ddl()),  # pyright: ignore[reportPrivateUsage]
            await _resolve_sql(store_instance._events_table_ddl()),  # pyright: ignore[reportPrivateUsage]
            await _resolve_sql(store_instance._app_states_table_ddl()),  # pyright: ignore[reportPrivateUsage]
            await _resolve_sql(store_instance._user_states_table_ddl()),  # pyright: ignore[reportPrivateUsage]
            await _resolve_sql(store_instance._metadata_table_ddl()),  # pyright: ignore[reportPrivateUsage]
        ])

    if _is_memory_enabled(context):
        memory_store_class = _get_memory_store_class(context)
        if memory_store_class is not None:
            memory_store = memory_store_class(config=context.config)
            await _prepare_schema(memory_store, context)
            memory_sql = memory_store._memory_table_ddl()  # pyright: ignore[reportPrivateUsage]
            if inspect.isawaitable(memory_sql):
                memory_sql = await memory_sql
            memory_statements = list(memory_sql) if isinstance(memory_sql, list) else [memory_sql]
            statements.extend(_with_postgres_extensions(memory_statements, context))
            log_with_context(
                logger, logging.DEBUG, "adk.migration.create.memory.create", table_name=memory_store.memory_table
            )

    return statements


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    if context is None or context.config is None:
        _raise_missing_config()

    statements: list[str] = []

    if _is_memory_enabled(context):
        memory_store_class = _get_memory_store_class(context)
        if memory_store_class is not None:
            memory_store = memory_store_class(config=context.config)
            statements.extend(memory_store._reset_drop_memory_table_sql())  # pyright: ignore[reportPrivateUsage]

    if _is_sessions_enabled(context):
        store_class = _get_store_class(context)
        store_instance = store_class(config=context.config)
        statements.extend(store_instance._reset_drop_tables_sql())  # pyright: ignore[reportPrivateUsage]

    return statements


def _with_postgres_extensions(statements: "list[str]", context: "MigrationContext") -> "list[str]":
    """Prepend required PostgreSQL extensions before their first use."""
    dialect = (context.dialect or "").lower()
    if dialect not in _POSTGRES_DIALECTS:
        return statements

    result: list[str] = []
    vector_enabled = False
    pg_textsearch_enabled = False
    for statement in statements:
        if not vector_enabled and _VECTOR_COLUMN_PATTERN.search(statement):
            result.append(CREATE_VECTOR_EXTENSION)
            vector_enabled = True
        if not pg_textsearch_enabled and _BM25_INDEX_PATTERN.search(statement):
            result.append(CREATE_PG_TEXTSEARCH_EXTENSION)
            pg_textsearch_enabled = True
        result.append(statement)
    return result


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
    if isinstance(store, BaseAsyncADKStore):
        await store.prepare_schema_async(driver)
        return
    if isinstance(store, BaseSyncADKStore):
        store.prepare_schema_sync(driver)


def _get_memory_store_class(
    context: "MigrationContext | None",
) -> "type[BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore] | None":
    if not context or not context.config:
        return None
    store_class = _adk_memory_migration_store_class(context.config)
    if store_class is None:
        log_with_context(logger, logging.DEBUG, "adk.migration.create.memory_store.missing")
        return None
    return cast("type[BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore]", store_class)


def _is_memory_enabled(context: "MigrationContext | None") -> bool:
    if not context or not context.config:
        return False
    return _adk_memory_migration_enabled(context.config)


def _is_sessions_enabled(context: "MigrationContext | None") -> bool:
    if not context or not context.config:
        return False
    return _adk_sessions_migration_enabled(context.config)
