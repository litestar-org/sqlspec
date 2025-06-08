"""Driver protocols and base classes for database adapters."""

from typing import Union

from sqlspec.config import AsyncDatabaseConfig
from sqlspec.driver import mixins
from sqlspec.driver._async import AsyncDriverAdapterProtocol
from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver._sync import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncInstrumentationMixin, SyncInstrumentationMixin
from sqlspec.typing import ConnectionT, RowT

__all__ = (
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterProtocol",
    "AsyncInstrumentationMixin",
    "CommonDriverAttributesMixin",
    "DriverAdapterProtocol",
    "SyncDriverAdapterProtocol",
    "SyncInstrumentationMixin",
    "mixins",
)

# Type alias for convenience
DriverAdapterProtocol = Union[
    SyncDriverAdapterProtocol[ConnectionT, RowT], AsyncDriverAdapterProtocol[ConnectionT, RowT]
]
