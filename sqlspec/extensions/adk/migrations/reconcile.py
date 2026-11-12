"""Built-in extension schema reconciler for Google ADK session, memory, and artifact stores."""

import importlib
from typing import TYPE_CHECKING, Any, NoReturn, cast

from sqlspec.exceptions import MigrationError, SQLSpecError
from sqlspec.extensions.adk._config_utils import (
    _adk_adapter_store_class,
    _adk_config_from_extension,
    _adk_memory_migration_enabled,
    _adk_memory_migration_store_class,
    _adk_sessions_migration_enabled,
)
from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.migrations.schema import (
    DropStatement,
    ensure_schema_async,
    ensure_schema_sync,
    run_drop_statements_async,
    run_drop_statements_sync,
)
from sqlspec.migrations.utils import validate_builtin_reconcile_adapter
from sqlspec.utils.adapters import resolve_adapter_name

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.migrations.context import MigrationContext
    from sqlspec.migrations.schema import SchemaTarget

__all__ = ("down", "downgrade", "reconcile")


async def _noop_async() -> None:
    """Return an empty coroutine when async ADK reconciliation is disabled."""


def _raise_missing_memory_store() -> NoReturn:
    """Raise MigrationError when memory reconciliation is enabled without a store class."""
    msg = "ADK memory reconciliation is enabled, but no ADKMemoryStore class could be resolved."
    raise MigrationError(msg)


def _adk_artifact_migration_store_class(config: Any) -> "type[Any] | None":
    """Resolve the ADK artifact store class when the adapter provides one."""
    target_cls = type(config)
    explicit_store = getattr(config, "adk_artifact_store_class", None)
    if not isinstance(explicit_store, type):
        explicit_store = getattr(target_cls, "adk_artifact_store_class", None)
    if isinstance(explicit_store, type):
        return explicit_store

    adapter_name = resolve_adapter_name(config)
    if adapter_name is None:
        return None

    module_name = f"sqlspec.adapters.{adapter_name}.adk.artifact_store"
    adapter_prefix = f"sqlspec.adapters.{adapter_name}"
    try:
        artifact_module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing_name = exc.name or ""
        if missing_name == "sqlspec.adapters" or missing_name.startswith(adapter_prefix):
            return None
        raise

    store_suffix = "ADKArtifactStore"
    exported = tuple(
        name
        for name in getattr(artifact_module, "__all__", ())
        if isinstance(name, str)
        and name.endswith(store_suffix)
        and isinstance(getattr(artifact_module, name, None), type)
    )
    for cls in target_cls.__mro__:
        cls_name = cls.__name__
        if cls_name.endswith("Config"):
            expected_prefix = cls_name.removesuffix("Config").lower()
            for exported_name in exported:
                if exported_name[: -len(store_suffix)].lower() == expected_prefix:
                    return cast("type[Any]", getattr(artifact_module, exported_name))
    if len(exported) == 1:
        return cast("type[Any]", getattr(artifact_module, exported[0]))
    if len(exported) > 1:
        mode_suffix = f"Async{store_suffix}" if bool(getattr(config, "is_async", False)) else f"Sync{store_suffix}"
        for exported_name in exported:
            if exported_name.endswith(mode_suffix):
                return cast("type[Any]", getattr(artifact_module, exported_name))
    return None


def _selected_stores(config: Any, adk_config: dict[str, Any]) -> list[Any]:
    """Resolve and instantiate enabled ADK session, memory, and artifact stores in canonical order."""
    stores: list[Any] = []
    try:
        if _adk_sessions_migration_enabled(config):
            session_store_cls = _adk_adapter_store_class(config, "ADKStore")
            stores.append(session_store_cls(config=config))
        if _adk_memory_migration_enabled(config):
            memory_store_cls = _adk_memory_migration_store_class(config)
            if memory_store_cls is None:
                _raise_missing_memory_store()
            stores.append(memory_store_cls(config=config))
        if adk_config.get("artifact_table"):
            artifact_store_cls = _adk_artifact_migration_store_class(config)
            if artifact_store_cls is not None:
                stores.append(artifact_store_cls(config=config))
    except MigrationError:
        raise
    except (SQLSpecError, ImportError, ValueError) as exc:
        msg = f"Failed to resolve ADK store for schema reconciliation: {exc}"
        raise MigrationError(msg) from exc
    return stores


def _validate_store_driver_mode(stores: list[Any], *, is_async_driver: bool) -> None:
    """Verify every selected ADK store matches the migration driver's sync/async mode."""
    for store in stores:
        if is_async_driver and not isinstance(
            store, (BaseAsyncADKStore, BaseAsyncADKMemoryStore, BaseAsyncADKArtifactStore)
        ):
            msg = f"ADK store '{type(store).__name__}' is synchronous, but MigrationContext supplied an async driver."
            raise MigrationError(msg)
        if not is_async_driver and not isinstance(
            store, (BaseSyncADKStore, BaseSyncADKMemoryStore, BaseSyncADKArtifactStore)
        ):
            msg = (
                f"ADK store '{type(store).__name__}' is asynchronous, "
                "but MigrationContext supplied a synchronous driver."
            )
            raise MigrationError(msg)


def _reconcile_sync(driver: Any, stores: list[Any], *, create_schema: bool) -> None:
    """Execute the four-phase ADK schema reconciliation pipeline on a synchronous driver."""
    for store in stores:
        store.prepare_schema_sync(driver)
    for store in stores:
        store.prepare_reconcile_sync(driver)

    targets: list[SchemaTarget] = []
    for store in stores:
        targets.extend(store.get_schema_targets_sync(driver))
    ensure_schema_sync(driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True)

    for store in stores:
        store.reconcile_companion_objects_sync(driver)
    for store in stores:
        store.finalize_reconcile_sync(driver)


async def _reconcile_async(driver: Any, stores: list[Any], *, create_schema: bool) -> None:
    """Execute the four-phase ADK schema reconciliation pipeline on an asynchronous driver."""
    for store in stores:
        await store.prepare_schema_async(driver)
    for store in stores:
        await store.prepare_reconcile_async(driver)

    targets: list[SchemaTarget] = []
    for store in stores:
        targets.extend(await store.get_schema_targets_async(driver))
    await ensure_schema_async(
        driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )

    for store in stores:
        await store.reconcile_companion_objects_async(driver)
    for store in stores:
        await store.finalize_reconcile_async(driver)


def reconcile(context: "MigrationContext") -> "Awaitable[None] | None":
    """Converge enabled ADK session and memory store schemas on the active migration driver.

    Args:
        context: Active migration execution context supplying config and driver.

    Raises:
        MigrationError: Context lacks config/driver, adapter is unsupported, or schema drift is unsafe.

    Returns:
        Awaitable resolving to ``None`` for async drivers, or ``None`` for sync drivers.
    """
    if context.config is None:
        msg = "ADK schema reconciliation requires context.config on MigrationContext."
        raise MigrationError(msg)
    if context.driver is None:
        msg = "ADK schema reconciliation requires context.driver on MigrationContext."
        raise MigrationError(msg)

    validate_builtin_reconcile_adapter(context.config)
    try:
        adk_config = _adk_config_from_extension(context.config)
    except MigrationError:
        raise
    except SQLSpecError as exc:
        raise MigrationError(str(exc)) from exc

    stores = _selected_stores(context.config, adk_config)
    is_async = context.is_async_driver
    _validate_store_driver_mode(stores, is_async_driver=is_async)
    if not stores:
        return _noop_async() if is_async else None

    manage_schema, create_schema = stores[0]._schema_management_flags()
    if not manage_schema:
        return _noop_async() if is_async else None

    if is_async:
        return _reconcile_async(context.driver, stores, create_schema=create_schema)
    _reconcile_sync(context.driver, stores, create_schema=create_schema)
    return None


def _store_downgrade_drops(store: Any) -> "list[DropStatement]":
    """Return the downgrade drops for an ADK session, memory, or artifact store."""
    if isinstance(store, (BaseSyncADKArtifactStore, BaseAsyncADKArtifactStore)):
        build_drop = getattr(store, "_build_drop_artifact_table_sql", None)
        if callable(build_drop):
            return [DropStatement(statement) for statement in cast("list[str]", build_drop())]
        return [DropStatement(f"DROP TABLE IF EXISTS {store.artifact_table}")]
    return list(store.downgrade_drops())


def _downgrade_sync(driver: Any, stores: list[Any]) -> None:
    """Drop enabled ADK store schemas in reverse dependency order on a synchronous driver."""
    for store in reversed(stores):
        run_drop_statements_sync(driver, _store_downgrade_drops(store))


async def _downgrade_async(driver: Any, stores: list[Any]) -> None:
    """Drop enabled ADK store schemas in reverse dependency order on an asynchronous driver."""
    for store in reversed(stores):
        await run_drop_statements_async(driver, _store_downgrade_drops(store))


def downgrade(context: "MigrationContext") -> "Awaitable[None] | None":
    """Drop enabled ADK session, memory, and artifact store schemas on the active migration driver.

    Args:
        context: Active migration execution context supplying config and driver.

    Raises:
        MigrationError: Context lacks config/driver or adapter is unsupported.

    Returns:
        Awaitable resolving to ``None`` for async drivers, or ``None`` for sync drivers.
    """
    if context.config is None:
        msg = "ADK schema reconciliation requires context.config on MigrationContext."
        raise MigrationError(msg)
    if context.driver is None:
        msg = "ADK schema reconciliation requires context.driver on MigrationContext."
        raise MigrationError(msg)

    validate_builtin_reconcile_adapter(context.config)
    try:
        adk_config = _adk_config_from_extension(context.config)
    except MigrationError:
        raise
    except SQLSpecError as exc:
        raise MigrationError(str(exc)) from exc

    stores = _selected_stores(context.config, adk_config)
    is_async = context.is_async_driver
    _validate_store_driver_mode(stores, is_async_driver=is_async)
    if not stores:
        return _noop_async() if is_async else None

    manage_schema, _ = stores[0]._schema_management_flags()
    if not manage_schema:
        return _noop_async() if is_async else None

    if is_async:
        return _downgrade_async(context.driver, stores)
    _downgrade_sync(context.driver, stores)
    return None


down = downgrade
