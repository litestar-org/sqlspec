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
        AsyncExceptionHandler,
        CommonDriverAttributesMixin,
        DataDictionaryDialectMixin,
        DataDictionaryMixin,
        ExecutionResult,
        StackExecutionObserver,
        SyncExceptionHandler,
        describe_stack_statement,
        hash_stack_operations,
        parameter_value_needs_processing,
        parameter_values_need_processing,
        type_coercion_fallbacks,
        validate_savepoint_name,
    )
    from sqlspec.driver._exception_handler import BaseAsyncExceptionHandler, BaseSyncExceptionHandler
    from sqlspec.driver._routing import AsyncRoutingSession, SyncRoutingSession
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
    "AsyncDataDictionaryBase",
    "AsyncDriverAdapterBase",
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
    "DriverAdapterProtocol",
    "ExecutionResult",
    "StackExecutionObserver",
    "SyncDataDictionaryBase",
    "SyncDriverAdapterBase",
    "SyncExceptionHandler",
    "SyncPoolConnectionContext",
    "SyncPoolSessionFactory",
    "SyncRoutingSession",
    "SyncRowStream",
    "SyncTenantLease",
    "convert_to_dialect",
    "describe_stack_statement",
    "hash_stack_operations",
    "parameter_value_needs_processing",
    "parameter_values_need_processing",
    "rows_to_dicts",
    "type_coercion_fallbacks",
    "validate_savepoint_name",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncDataDictionaryBase": ("sqlspec.driver._async", "AsyncDataDictionaryBase"),
    "AsyncDriverAdapterBase": ("sqlspec.driver._async", "AsyncDriverAdapterBase"),
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
    "ExecutionResult": ("sqlspec.driver._common", "ExecutionResult"),
    "StackExecutionObserver": ("sqlspec.driver._common", "StackExecutionObserver"),
    "SyncDataDictionaryBase": ("sqlspec.driver._sync", "SyncDataDictionaryBase"),
    "SyncDriverAdapterBase": ("sqlspec.driver._sync", "SyncDriverAdapterBase"),
    "SyncExceptionHandler": ("sqlspec.driver._common", "SyncExceptionHandler"),
    "SyncPoolConnectionContext": ("sqlspec.driver._sync", "SyncPoolConnectionContext"),
    "SyncPoolSessionFactory": ("sqlspec.driver._sync", "SyncPoolSessionFactory"),
    "SyncRoutingSession": ("sqlspec.driver._routing", "SyncRoutingSession"),
    "SyncRowStream": ("sqlspec.driver._stream", "SyncRowStream"),
    "SyncTenantLease": ("sqlspec.driver._tenant", "SyncTenantLease"),
    "convert_to_dialect": ("sqlspec.driver._sql_helpers", "convert_to_dialect"),
    "describe_stack_statement": ("sqlspec.driver._common", "describe_stack_statement"),
    "hash_stack_operations": ("sqlspec.driver._common", "hash_stack_operations"),
    "parameter_value_needs_processing": ("sqlspec.driver._common", "parameter_value_needs_processing"),
    "parameter_values_need_processing": ("sqlspec.driver._common", "parameter_values_need_processing"),
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
