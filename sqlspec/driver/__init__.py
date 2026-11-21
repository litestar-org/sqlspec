"""Driver protocols and base classes for database adapters."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from typing import TypeAlias

    from sqlspec.core.cache import CachedQuery
    from sqlspec.driver._async import (
        AsyncDataDictionaryBase,
        AsyncDriverAdapterBase,
        AsyncPoolConnectionContext,
        AsyncPoolSessionFactory,
    )
    from sqlspec.driver._common import (
        AsyncCacheHitExecutor,
        AsyncExceptionHandler,
        CommonDriverAttributesMixin,
        DataDictionaryDialectMixin,
        DataDictionaryMixin,
        ExecutionResult,
        StackExecutionObserver,
        SyncCacheHitExecutor,
        SyncExceptionHandler,
        cache_hit_hooks_unchanged,
        describe_stack_statement,
        hash_stack_operations,
        parameter_value_needs_processing,
        parameter_values_need_processing,
        type_coercion_fallbacks,
        validate_savepoint_name,
    )
    from sqlspec.driver._deferred import DeferredAsyncSession, raise_async_connection_required
    from sqlspec.driver._exception_handler import BaseAsyncExceptionHandler, BaseSyncExceptionHandler
    from sqlspec.driver._routing import AsyncRoutingSession, SyncRoutingSession
    from sqlspec.driver._session import AsyncDriverSessionContext, SyncDriverSessionContext
    from sqlspec.driver._sql_helpers import convert_to_dialect
    from sqlspec.driver._stream import AsyncRowStream, SyncRowStream, rows_to_dicts
    from sqlspec.driver._sync import (
        SyncDataDictionaryBase,
        SyncDriverAdapterBase,
        SyncPoolConnectionContext,
        SyncPoolSessionFactory,
    )
    from sqlspec.driver._tenant import AsyncTenantLease, SyncTenantLease

    DriverAdapterProtocol: TypeAlias = SyncDriverAdapterBase | AsyncDriverAdapterBase

__all__ = (
    "AsyncCacheHitExecutor",
    "AsyncDataDictionaryBase",
    "AsyncDriverAdapterBase",
    "AsyncDriverSessionContext",
    "AsyncExceptionHandler",
    "AsyncPoolConnectionContext",
    "AsyncPoolSessionFactory",
    "AsyncRoutingSession",
    "AsyncRowStream",
    "AsyncTenantLease",
    "BaseAsyncExceptionHandler",
    "BaseSyncExceptionHandler",
    "CachedQuery",
    "CommonDriverAttributesMixin",
    "DataDictionaryDialectMixin",
    "DataDictionaryMixin",
    "DeferredAsyncSession",
    "DriverAdapterProtocol",
    "ExecutionResult",
    "StackExecutionObserver",
    "SyncCacheHitExecutor",
    "SyncDataDictionaryBase",
    "SyncDriverAdapterBase",
    "SyncDriverSessionContext",
    "SyncExceptionHandler",
    "SyncPoolConnectionContext",
    "SyncPoolSessionFactory",
    "SyncRoutingSession",
    "SyncRowStream",
    "SyncTenantLease",
    "cache_hit_hooks_unchanged",
    "convert_to_dialect",
    "describe_stack_statement",
    "hash_stack_operations",
    "parameter_value_needs_processing",
    "parameter_values_need_processing",
    "raise_async_connection_required",
    "rows_to_dicts",
    "type_coercion_fallbacks",
    "validate_savepoint_name",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncCacheHitExecutor": ("sqlspec.driver._common", "AsyncCacheHitExecutor"),
    "AsyncDataDictionaryBase": ("sqlspec.driver._async", "AsyncDataDictionaryBase"),
    "AsyncDriverAdapterBase": ("sqlspec.driver._async", "AsyncDriverAdapterBase"),
    "AsyncDriverSessionContext": ("sqlspec.driver._session", "AsyncDriverSessionContext"),
    "AsyncExceptionHandler": ("sqlspec.driver._common", "AsyncExceptionHandler"),
    "AsyncPoolConnectionContext": ("sqlspec.driver._async", "AsyncPoolConnectionContext"),
    "AsyncPoolSessionFactory": ("sqlspec.driver._async", "AsyncPoolSessionFactory"),
    "AsyncRoutingSession": ("sqlspec.driver._routing", "AsyncRoutingSession"),
    "AsyncRowStream": ("sqlspec.driver._stream", "AsyncRowStream"),
    "AsyncTenantLease": ("sqlspec.driver._tenant", "AsyncTenantLease"),
    "BaseAsyncExceptionHandler": ("sqlspec.driver._exception_handler", "BaseAsyncExceptionHandler"),
    "BaseSyncExceptionHandler": ("sqlspec.driver._exception_handler", "BaseSyncExceptionHandler"),
    "CachedQuery": ("sqlspec.core.cache", "CachedQuery"),
    "CommonDriverAttributesMixin": ("sqlspec.driver._common", "CommonDriverAttributesMixin"),
    "DataDictionaryDialectMixin": ("sqlspec.driver._common", "DataDictionaryDialectMixin"),
    "DataDictionaryMixin": ("sqlspec.driver._common", "DataDictionaryMixin"),
    "DeferredAsyncSession": ("sqlspec.driver._deferred", "DeferredAsyncSession"),
    "ExecutionResult": ("sqlspec.driver._common", "ExecutionResult"),
    "StackExecutionObserver": ("sqlspec.driver._common", "StackExecutionObserver"),
    "SyncCacheHitExecutor": ("sqlspec.driver._common", "SyncCacheHitExecutor"),
    "SyncDataDictionaryBase": ("sqlspec.driver._sync", "SyncDataDictionaryBase"),
    "SyncDriverAdapterBase": ("sqlspec.driver._sync", "SyncDriverAdapterBase"),
    "SyncDriverSessionContext": ("sqlspec.driver._session", "SyncDriverSessionContext"),
    "SyncExceptionHandler": ("sqlspec.driver._common", "SyncExceptionHandler"),
    "SyncPoolConnectionContext": ("sqlspec.driver._sync", "SyncPoolConnectionContext"),
    "SyncPoolSessionFactory": ("sqlspec.driver._sync", "SyncPoolSessionFactory"),
    "SyncRoutingSession": ("sqlspec.driver._routing", "SyncRoutingSession"),
    "SyncRowStream": ("sqlspec.driver._stream", "SyncRowStream"),
    "SyncTenantLease": ("sqlspec.driver._tenant", "SyncTenantLease"),
    "cache_hit_hooks_unchanged": ("sqlspec.driver._common", "cache_hit_hooks_unchanged"),
    "convert_to_dialect": ("sqlspec.driver._sql_helpers", "convert_to_dialect"),
    "describe_stack_statement": ("sqlspec.driver._common", "describe_stack_statement"),
    "hash_stack_operations": ("sqlspec.driver._common", "hash_stack_operations"),
    "parameter_value_needs_processing": ("sqlspec.driver._common", "parameter_value_needs_processing"),
    "parameter_values_need_processing": ("sqlspec.driver._common", "parameter_values_need_processing"),
    "raise_async_connection_required": ("sqlspec.driver._deferred", "raise_async_connection_required"),
    "rows_to_dicts": ("sqlspec.driver._stream", "rows_to_dicts"),
    "type_coercion_fallbacks": ("sqlspec.driver._common", "type_coercion_fallbacks"),
    "validate_savepoint_name": ("sqlspec.driver._common", "validate_savepoint_name"),
}


def __getattr__(name: str) -> Any:
    if name == "DriverAdapterProtocol":
        value = __getattr__("SyncDriverAdapterBase") | __getattr__("AsyncDriverAdapterBase")
        globals()[name] = value
        return value
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys() | {"DriverAdapterProtocol"})


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
