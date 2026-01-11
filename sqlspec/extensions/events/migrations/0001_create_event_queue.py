"""Create the SQLSpec events queue tables."""

import logging
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import import_string

if TYPE_CHECKING:
    from sqlspec.extensions.events._store import BaseEventQueueStore
    from sqlspec.migrations.context import MigrationContext

logger = get_logger("sqlspec.events.migrations.queue")

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that provision the queue table and indexes."""

    store = _load_store(context)
    statements = store.create_statements()
    log_with_context(logger, logging.DEBUG, "events.migration.create.prepared", table_name=store.table_name)
    return statements


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that drop the queue table."""

    store = _load_store(context)
    statements = store.drop_statements()
    log_with_context(logger, logging.DEBUG, "events.migration.drop.prepared", table_name=store.table_name)
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
    except ImportError as error:  # pragma: no cover - missing adapter wiring
        msg = f"Adapter {adapter_name} missing events store {store_class_name}"
        raise SQLSpecError(msg) from error
    try:
        store: BaseEventQueueStore[Any] = store_class(config)
    except ValueError as error:  # pragma: no cover - invalid identifier path
        raise SQLSpecError(str(error)) from error
    return store
