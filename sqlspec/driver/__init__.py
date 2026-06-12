"""Driver protocols and base classes for database adapters."""

from sqlspec.driver._async import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
)
from sqlspec.driver._common import (
    CommonDriverAttributesMixin,
    DataDictionaryDialectMixin,
    DataDictionaryMixin,
    ExecutionResult,
    StackExecutionObserver,
    describe_stack_statement,
    hash_stack_operations,
)
from sqlspec.driver._exception_handler import BaseAsyncExceptionHandler, BaseSyncExceptionHandler
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
    "AsyncPoolConnectionContext",
    "AsyncPoolSessionFactory",
    "AsyncRowStream",
    "BaseAsyncExceptionHandler",
    "BaseSyncExceptionHandler",
    "CommonDriverAttributesMixin",
    "DataDictionaryDialectMixin",
    "DataDictionaryMixin",
    "DriverAdapterProtocol",
    "ExecutionResult",
    "StackExecutionObserver",
    "SyncDataDictionaryBase",
    "SyncDriverAdapterBase",
    "SyncPoolConnectionContext",
    "SyncPoolSessionFactory",
    "SyncRowStream",
    "convert_to_dialect",
    "describe_stack_statement",
    "hash_stack_operations",
    "rows_to_dicts",
)

DriverAdapterProtocol = SyncDriverAdapterBase | AsyncDriverAdapterBase
