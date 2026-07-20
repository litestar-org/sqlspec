"""Create the SQLSpec events queue tables."""

import inspect
import logging
from typing import TYPE_CHECKING, Any, Final

from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import import_string

if TYPE_CHECKING:
    from sqlspec.extensions.events._store import BaseEventQueueStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")

logger = get_logger("sqlspec.events.migrations.queue")

_STATEMENTS_WITH_INDEX: Final = 2


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that provision the queue table and indexes."""

    store = _load_store(context)
    if context is not None and context.driver is not None:
        if context.is_async_driver:
            await store.prepare_schema_async(context.driver)
        else:
            store.prepare_schema_sync(context.driver)
    statements = store.create_statements()
    statements = await _drop_present_index(store, context, statements)
    if context is not None and context.driver is not None and store.settings.get("manage_schema", True):
        if context.is_async_driver:
            result = await store.reconcile_schema_async(context.driver)
        else:
            result = store.reconcile_schema_sync(context.driver)
        if store.table_name in result.deferred_tables:
            return []
        statements = statements[1:]
    log_with_context(logger, logging.DEBUG, "events.migration.create.prepared", table_name=store.table_name)
    return statements


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that drop the queue table."""

    store = _load_store(context)
    statements = store.drop_statements()
    log_with_context(logger, logging.DEBUG, "events.migration.drop.prepared", table_name=store.table_name)
    return statements


async def _drop_present_index(
    store: "BaseEventQueueStore[Any]", context: "MigrationContext | None", statements: "list[str]"
) -> "list[str]":
    """Drop the trailing index statement when the queue index already exists.

    Adapters whose index DDL is not self-idempotent (MySQL) opt in through
    ``_index_existence_target``. The check routes through the driver's data
    dictionary rather than an embedded existence probe.
    """

    target = store._index_existence_target()
    driver = context.driver if context is not None else None
    if target is None or driver is None or len(statements) < _STATEMENTS_WITH_INDEX:
        return statements

    schema, table = target
    result = driver.data_dictionary.get_indexes(driver, table=table, schema=schema)
    if inspect.isawaitable(result):
        result = await result
    existing = {str(row.get("index_name", "")).casefold() for row in result}
    if store._index_name().casefold() in existing:
        return statements[:-1]
    return statements


def _load_store(context: "MigrationContext | None") -> "BaseEventQueueStore[Any]":
    if context is None or context.config is None:
        msg = "Migration context with adapter configuration is required"
        raise SQLSpecError(msg)
    config = context.config
    config_class = type(config)
    module_path = config_class.__module__
    if not module_path.startswith("sqlspec.adapters."):
        msg = f"Unsupported configuration for events extension: {module_path}.{config_class.__name__}"
        raise SQLSpecError(msg)
    adapter_name = module_path.split(".")[2]
    store_class_name = config_class.__name__.replace("Config", "EventQueueStore")
    store_path = f"sqlspec.adapters.{adapter_name}.events.store.{store_class_name}"
    try:
        store_class = import_string(store_path)
    except ImportError as error:  # pragma: no cover
        msg = f"Adapter {adapter_name} missing events store {store_class_name}"
        raise SQLSpecError(msg) from error
    try:
        store: BaseEventQueueStore[Any] = store_class(config)
    except ValueError as error:  # pragma: no cover
        raise SQLSpecError(str(error)) from error
    return store
