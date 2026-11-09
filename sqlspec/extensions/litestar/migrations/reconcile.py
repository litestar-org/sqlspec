"""Built-in extension schema reconciler for Litestar session stores."""

from typing import TYPE_CHECKING, Any, NoReturn

from sqlspec.exceptions import ImproperConfigurationError, MigrationError
from sqlspec.migrations.schema import ensure_schema_async, ensure_schema_sync
from sqlspec.migrations.utils import validate_builtin_reconcile_adapter
from sqlspec.utils.adapters import resolve_extension_store_class

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.extensions.litestar.store import BaseSQLSpecStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("reconcile",)


def _raise_missing_store(adapter_name: str, cause: Exception | None = None) -> NoReturn:
    """Raise MigrationError when no Litestar session store can be resolved."""
    msg = (
        f"Litestar session reconciliation could not resolve a store class for adapter {adapter_name!r}; "
        'set migration_mode="versioned" or register a custom reconcile.py via add_extension_migrations.'
    )
    if cause is not None:
        raise MigrationError(msg) from cause
    raise MigrationError(msg)


def _raise_invalid_store_config(cause: Exception) -> NoReturn:
    """Raise MigrationError when Litestar store instantiation fails validation."""
    msg = f"Invalid Litestar extension configuration for reconciliation: {cause}"
    raise MigrationError(msg) from cause


def _resolve_store(context: "MigrationContext") -> "tuple[BaseSQLSpecStore[Any], bool]":
    """Validate adapter support and instantiate the active Litestar session store."""
    is_async = context.is_async_driver
    adapter_name = validate_builtin_reconcile_adapter(context.config)
    try:
        store_cls = resolve_extension_store_class(
            context.config, extension="litestar", store_attr="litestar_store_class", store_suffix="Store"
        )
    except ImportError as exc:
        _raise_missing_store(adapter_name, exc)
    if store_cls is None:
        _raise_missing_store(adapter_name)
    try:
        store: BaseSQLSpecStore[Any] = store_cls(context.config)
    except (ImproperConfigurationError, ValueError) as exc:
        _raise_invalid_store_config(exc)
    return store, is_async


def _reconcile_sync(driver: Any, store: "BaseSQLSpecStore[Any]") -> None:
    """Reconcile Litestar session schema on a synchronous migration driver."""
    manage_schema, create_schema = store._schema_management_flags()
    if not manage_schema:
        return
    store.prepare_schema_sync(driver)
    ensure_schema_sync(
        driver, store.get_schema_targets(), manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )
    store.reconcile_companion_objects_sync(driver)


async def _reconcile_async(driver: Any, store: "BaseSQLSpecStore[Any]") -> None:
    """Reconcile Litestar session schema on an asynchronous migration driver."""
    manage_schema, create_schema = store._schema_management_flags()
    if not manage_schema:
        return
    await store.prepare_schema_async(driver)
    await ensure_schema_async(
        driver, store.get_schema_targets(), manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )
    await store.reconcile_companion_objects_async(driver)


def reconcile(context: "MigrationContext") -> "Awaitable[None] | None":
    """Converge Litestar session store tables on the active migration driver.

    Args:
        context: Active migration execution context containing config and driver.

    Returns:
        Awaitable resolving to ``None`` for async drivers, or ``None`` for sync drivers.
    """
    store, is_async = _resolve_store(context)
    if is_async:
        return _reconcile_async(context.driver, store)
    _reconcile_sync(context.driver, store)
    return None
