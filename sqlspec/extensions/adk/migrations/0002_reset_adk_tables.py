"""Reset ADK schema to the 2.0 clean-break shape.

Unconditionally drops any legacy ADK tables (sessions, events, app_states,
user_states, metadata, memory) then creates the new schema and seeds the
internal metadata row. The memory table is dropped unconditionally so users
moving from ``enable_memory=True`` to ``enable_memory=False`` get cleanup; it
is recreated only when memory is enabled for the current config.
"""

import logging
from typing import TYPE_CHECKING, Final, NoReturn, cast

from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk._config_utils import (
    _get_adk_adapter_store_class,
    _get_adk_memory_migration_store_class,
    _is_adk_memory_migration_enabled,
)
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.store import BaseAsyncADKStore
    from sqlspec.migrations.context import MigrationContext

logger = get_logger("sqlspec.migrations.adk.reset")

__all__ = ("down", "up")

MIN_DROP_TABLE_TOKENS: Final = 3
MIN_DROP_TABLE_IF_EXISTS_TOKENS: Final = 5


def _raise_missing_config() -> NoReturn:
    msg = "Migration context must have a config to determine store class"
    raise SQLSpecError(msg)


def _get_store_class(context: "MigrationContext | None") -> "type[BaseAsyncADKStore]":
    if not context or not context.config:
        _raise_missing_config()
    return cast("type[BaseAsyncADKStore]", _get_adk_adapter_store_class(context.config, "ADKStore"))


def _get_memory_store_class(context: "MigrationContext | None") -> "type[BaseAsyncADKMemoryStore] | None":
    if not context or not context.config:
        return None
    store_class = _get_adk_memory_migration_store_class(context.config)
    if store_class is None:
        log_with_context(logger, logging.DEBUG, "adk.migration.reset.memory_store.missing")
        return None
    return cast("type[BaseAsyncADKMemoryStore]", store_class)


def _is_memory_enabled(context: "MigrationContext | None") -> bool:
    if not context or not context.config:
        return False
    return _is_adk_memory_migration_enabled(context.config)


def _is_spanner_store(store_instance: "BaseAsyncADKStore") -> bool:
    return getattr(store_instance, "connector_name", None) == "spanner"


def _spanner_existing_tables(context: "MigrationContext") -> "set[str]":
    if context.config is None:
        _raise_missing_config()
    database = context.config.get_database()
    return {table.table_id for table in database.list_tables()}


def _drop_table_name(statement: str) -> str | None:
    tokens = statement.strip().split()
    if len(tokens) >= MIN_DROP_TABLE_TOKENS and tokens[0].upper() == "DROP" and tokens[1].upper() == "TABLE":
        if (
            len(tokens) >= MIN_DROP_TABLE_IF_EXISTS_TOKENS
            and tokens[2].upper() == "IF"
            and tokens[3].upper() == "EXISTS"
        ):
            return tokens[4]
        return tokens[2]
    return None


def _filter_existing_table_drops(statements: "list[str]", existing_tables: "set[str] | None") -> "list[str]":
    if existing_tables is None:
        return statements
    return [statement for statement in statements if (_drop_table_name(statement) in existing_tables)]


def _filter_memory_drops(statements: "list[str]", memory_table: str, existing_tables: "set[str] | None") -> "list[str]":
    if existing_tables is None or memory_table in existing_tables:
        return statements
    return []


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    if context is None or context.config is None:
        _raise_missing_config()

    store_class = _get_store_class(context)
    store_instance = store_class(config=context.config)
    existing_tables = _spanner_existing_tables(context) if _is_spanner_store(store_instance) else None

    statements: list[str] = []

    memory_store_class = _get_memory_store_class(context)
    if memory_store_class is not None:
        memory_store = memory_store_class(config=context.config)
        memory_drops = memory_store._get_drop_memory_table_sql()  # pyright: ignore[reportPrivateUsage]
        statements.extend(_filter_memory_drops(memory_drops, memory_store.memory_table, existing_tables))
        log_with_context(logger, logging.DEBUG, "adk.migration.reset.memory.drop", table_name=memory_store.memory_table)

    drop_statements = store_instance._get_drop_tables_sql()  # pyright: ignore[reportPrivateUsage]
    statements.extend(_filter_existing_table_drops(drop_statements, existing_tables))

    statements.extend([
        await store_instance._get_create_sessions_table_sql(),  # pyright: ignore[reportPrivateUsage]
        await store_instance._get_create_events_table_sql(),  # pyright: ignore[reportPrivateUsage]
        await store_instance._get_create_app_states_table_sql(),  # pyright: ignore[reportPrivateUsage]
        await store_instance._get_create_user_states_table_sql(),  # pyright: ignore[reportPrivateUsage]
        await store_instance._get_create_metadata_table_sql(),  # pyright: ignore[reportPrivateUsage]
        await store_instance._get_seed_metadata_sql(),  # pyright: ignore[reportPrivateUsage]
    ])

    if _is_memory_enabled(context) and memory_store_class is not None:
        memory_store = memory_store_class(config=context.config)
        memory_sql = await memory_store._get_create_memory_table_sql()  # pyright: ignore[reportPrivateUsage]
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
    existing_tables = _spanner_existing_tables(context) if _is_spanner_store(store_instance) else None

    if _is_memory_enabled(context):
        memory_store_class = _get_memory_store_class(context)
        if memory_store_class is not None:
            memory_store = memory_store_class(config=context.config)
            memory_drops = memory_store._get_drop_memory_table_sql()  # pyright: ignore[reportPrivateUsage]
            statements.extend(_filter_memory_drops(memory_drops, memory_store.memory_table, existing_tables))

    drop_statements = store_instance._get_drop_tables_sql()  # pyright: ignore[reportPrivateUsage]
    statements.extend(_filter_existing_table_drops(drop_statements, existing_tables))

    return statements
