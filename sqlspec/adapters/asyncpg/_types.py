from typing import TYPE_CHECKING

from asyncpg.pool import PoolConnectionProxy

if TYPE_CHECKING:
    from typing import TypeAlias

    from asyncpg import Connection, Pool, Record


if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    AsyncpgPool: TypeAlias = Pool[Record]
else:
    from asyncpg import Pool

    AsyncpgConnection = PoolConnectionProxy
    AsyncpgPool = Pool


__all__ = ("AsyncpgConnection", "AsyncpgPool")
