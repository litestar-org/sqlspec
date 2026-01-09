"""Driver protocols and base classes for database adapters."""

from sqlspec.driver._async import AsyncDataDictionaryBase, AsyncDriverAdapterBase
from sqlspec.driver._common import (
    ColumnMetadata,
    CommonDriverAttributesMixin,
    DataDictionaryMixin,
    ExecutionResult,
    ForeignKeyMetadata,
    IndexMetadata,
    StackExecutionObserver,
    TableMetadata,
    VersionInfo,
    describe_stack_statement,
    hash_stack_operations,
)
from sqlspec.driver._sync import SyncDataDictionaryBase, SyncDriverAdapterBase

__all__ = (
    "AsyncDataDictionaryBase",
    "AsyncDriverAdapterBase",
    "ColumnMetadata",
    "CommonDriverAttributesMixin",
    "DataDictionaryMixin",
    "DriverAdapterProtocol",
    "ExecutionResult",
    "ForeignKeyMetadata",
    "IndexMetadata",
    "StackExecutionObserver",
    "SyncDataDictionaryBase",
    "SyncDriverAdapterBase",
    "TableMetadata",
    "VersionInfo",
    "describe_stack_statement",
    "hash_stack_operations",
)

DriverAdapterProtocol = SyncDriverAdapterBase | AsyncDriverAdapterBase
