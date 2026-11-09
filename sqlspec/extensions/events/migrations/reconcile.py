"""Reconcile the SQLSpec durable events queue schema."""

from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import ImproperConfigurationError, MigrationError
from sqlspec.extensions.events._channel import _resolve_backend_name
from sqlspec.migrations.schema import ensure_schema_async, ensure_schema_sync
from sqlspec.migrations.utils import validate_builtin_reconcile_adapter
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.extensions.events._store import BaseEventQueueStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("reconcile",)


def _requires_queue_table(config: Any, settings: dict[str, Any]) -> bool:
    """Return whether the configured events backend uses the SQL queue table."""
    try:
        backend_name = _resolve_backend_name(config, settings, resolve_adapter_name(config))
    except ImproperConfigurationError as exc:
        raise MigrationError(str(exc)) from exc
    return backend_name != "notify"


def _resolve_store(context: "MigrationContext") -> "tuple[BaseEventQueueStore[Any], bool, bool]":
    """Validate adapter support and instantiate the configured events queue store."""
    if context.driver is None:
        msg = "Events schema reconciliation requires an active database driver on MigrationContext."
        raise MigrationError(msg)
    is_async = context.is_async_driver
    adapter_name = validate_builtin_reconcile_adapter(context.config)
    try:
        store_cls = resolve_extension_store_class(
            context.config, extension="events", store_attr="events_store_class", store_suffix="EventQueueStore"
        )
    except ImportError as exc:
        msg = f"Failed to import events store for adapter '{adapter_name}': {exc}"
        raise MigrationError(msg) from exc
    if store_cls is None:
        msg = f"Adapter '{adapter_name}' does not define an EventQueueStore implementation."
        raise MigrationError(msg)
    try:
        store: BaseEventQueueStore[Any] = store_cls(context.config)
    except (ImproperConfigurationError, ValueError) as exc:
        raise MigrationError(str(exc)) from exc
    needs_table = _requires_queue_table(context.config, store.settings)
    return store, is_async, needs_table


def _reconcile_sync(driver: Any, store: "BaseEventQueueStore[Any]", needs_table: bool) -> None:
    """Reconcile the events queue table and companion index on a synchronous driver."""
    if not needs_table:
        return
    manage_schema, create_schema = store._schema_management_flags()
    if not manage_schema:
        return
    store.prepare_schema_sync(driver)
    ensure_schema_sync(
        driver, store.get_schema_targets(), manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )
    store.reconcile_companion_objects_sync(driver)


async def _reconcile_async(driver: Any, store: "BaseEventQueueStore[Any]", needs_table: bool) -> None:
    """Reconcile the events queue table and companion index on an asynchronous driver."""
    if not needs_table:
        return
    manage_schema, create_schema = store._schema_management_flags()
    if not manage_schema:
        return
    await store.prepare_schema_async(driver)
    await ensure_schema_async(
        driver, store.get_schema_targets(), manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )
    await store.reconcile_companion_objects_async(driver)


def reconcile(context: "MigrationContext") -> "Awaitable[None] | None":
    """Reconcile durable event queue schema on the migration runner's active driver.

    Args:
        context: Active migration execution context supplying config and driver.

    Returns:
        ``None`` for synchronous drivers or an awaitable resolving to ``None`` for async drivers.
    """
    store, is_async, needs_table = _resolve_store(context)
    if is_async:
        return _reconcile_async(context.driver, store, needs_table)
    _reconcile_sync(context.driver, store, needs_table)
    return None
