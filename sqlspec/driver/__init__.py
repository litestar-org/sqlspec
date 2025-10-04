"""Driver protocols and base classes for database adapters."""

from sqlspec.driver import mixins
from sqlspec.driver._async import AsyncDataDictionaryBase, AsyncDriverAdapterBase
from sqlspec.driver._common import CommonDriverAttributesMixin, ExecutionResult, VersionInfo
from sqlspec.driver._sync import SyncDataDictionaryBase, SyncDriverAdapterBase

__all__ = (
    "AsyncDataDictionaryBase",
    "AsyncDriverAdapterBase",
    "CommonDriverAttributesMixin",
    "DriverAdapterProtocol",
    "ExecutionResult",
    "SyncDataDictionaryBase",
    "SyncDriverAdapterBase",
    "VersionInfo",
    "mixins",
)

DriverAdapterProtocol = SyncDriverAdapterBase | AsyncDriverAdapterBase
