"""Driver protocols and base classes for database adapters."""

from sqlspec.driver._async import AsyncDataDictionaryBase, AsyncDriverAdapterBase
from sqlspec.driver._common import (
    CommonDriverAttributesMixin,
    DataDictionaryDialectMixin,
    DataDictionaryMixin,
    ExecutionResult,
    StackExecutionObserver,
    describe_stack_statement,
    hash_stack_operations,
)
from sqlspec.driver._sql_helpers import convert_to_dialect
from sqlspec.driver._sync import SyncDataDictionaryBase, SyncDriverAdapterBase

__all__ = (
    "AsyncDataDictionaryBase",
    "AsyncDriverAdapterBase",
    "CommonDriverAttributesMixin",
    "DataDictionaryDialectMixin",
    "DataDictionaryMixin",
    "DriverAdapterProtocol",
    "ExecutionResult",
    "StackExecutionObserver",
    "SyncDataDictionaryBase",
    "SyncDriverAdapterBase",
    "convert_to_dialect",
    "describe_stack_statement",
    "hash_stack_operations",
)

DriverAdapterProtocol = SyncDriverAdapterBase | AsyncDriverAdapterBase
