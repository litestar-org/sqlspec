"""Driver protocols and base classes for database adapters."""

from typing import Union

from sqlspec.driver import mixins
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver._sync import SyncDriverAdapterBase

__all__ = (
    "AsyncDriverAdapterBase",
    "CommonDriverAttributesMixin",
    "DriverAdapterProtocol",
    "SyncDriverAdapterBase",
    "mixins",
)

# Type alias for convenience
DriverAdapterProtocol = Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]
