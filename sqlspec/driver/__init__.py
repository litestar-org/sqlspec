"""Driver protocols and base classes for database adapters."""

from typing import Union

from sqlspec.driver import mixins
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.typing import ConnectionT, RowT

__all__ = (
    "AsyncDriverAdapterBase",
    "CommonDriverAttributesMixin",
    "DriverAdapterProtocol",
    "SyncDriverAdapterBase",
    "mixins",
)

# Type alias for convenience
DriverAdapterProtocol = Union[SyncDriverAdapterBase[ConnectionT, RowT], AsyncDriverAdapterBase[ConnectionT, RowT]]
