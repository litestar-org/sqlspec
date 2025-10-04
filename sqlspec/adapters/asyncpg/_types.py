from typing import TYPE_CHECKING

from asyncpg import Connection
from asyncpg.pool import PoolConnectionProxy

if TYPE_CHECKING:
    from typing import TypeAlias

    from asyncpg import Record


if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
else:
    AsyncpgConnection = Connection | PoolConnectionProxy


__all__ = ("AsyncpgConnection",)
