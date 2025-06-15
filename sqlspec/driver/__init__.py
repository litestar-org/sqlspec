"""Driver protocols and base classes for database adapters."""

from typing import Union

from sqlspec.driver import mixins
from sqlspec.driver._async import AsyncDriverAdapterProtocol
from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver._sync import SyncDriverAdapterProtocol
from sqlspec.typing import ConnectionT, RowT

__all__ = (
    "AsyncDriverAdapterProtocol",
    "CommonDriverAttributesMixin",
    "DriverAdapterProtocol",
    "SyncDriverAdapterProtocol",
    "mixins",
)

# Type alias for convenience
DriverAdapterProtocol = Union[
    SyncDriverAdapterProtocol[ConnectionT, RowT], AsyncDriverAdapterProtocol[ConnectionT, RowT]
]
