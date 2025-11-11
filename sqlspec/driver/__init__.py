"""Driver protocols and base classes for database adapters."""

from sqlspec.driver import mixins
from sqlspec.driver._async import AsyncDataDictionaryBase, AsyncDriverAdapterBase
from sqlspec.driver._common import (
    CommonDriverAttributesMixin,
    ExecutionResult,
    StackExecutionObserver,
    VersionInfo,
    describe_stack_statement,
)
from sqlspec.driver._sync import SyncDataDictionaryBase, SyncDriverAdapterBase

__all__ = (
    "AsyncDataDictionaryBase",
    "AsyncDriverAdapterBase",
    "CommonDriverAttributesMixin",
    "DriverAdapterProtocol",
    "ExecutionResult",
    "StackExecutionObserver",
    "SyncDataDictionaryBase",
    "SyncDriverAdapterBase",
    "VersionInfo",
    "describe_stack_statement",
    "mixins",
)

DriverAdapterProtocol = SyncDriverAdapterBase | AsyncDriverAdapterBase
