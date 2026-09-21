"""Driver protocols and base classes for database adapters."""

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
from sqlspec.driver._query_cache import CachedQuery
from sqlspec.driver._sql_helpers import convert_to_dialect
from sqlspec.driver._stream import AsyncRowStream, SyncRowStream, rows_to_dicts
from sqlspec.driver._sync import (
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)

__all__ = (
    "AsyncDataDictionaryBase",
    "AsyncDriverAdapterBase",
    "AsyncExceptionHandler",
    "AsyncPoolConnectionContext",
    "AsyncPoolSessionFactory",
    "AsyncRowStream",
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
    "SyncRowStream",
    "convert_to_dialect",
    "describe_stack_statement",
    "hash_stack_operations",
    "parameter_value_needs_processing",
    "parameter_values_need_processing",
    "rows_to_dicts",
    "type_coercion_fallbacks",
    "validate_savepoint_name",
)

DriverAdapterProtocol = SyncDriverAdapterBase | AsyncDriverAdapterBase
