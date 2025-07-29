from typing import TYPE_CHECKING, Union

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg.pool import PoolConnectionProxy

if TYPE_CHECKING:
    from asyncpg import Record
    from typing_extensions import TypeAlias


if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection = Union[AsyncpgNativeConnection, PoolConnectionProxy]


__all__ = ("AsyncpgConnection",)
