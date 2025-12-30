from typing import TYPE_CHECKING

from asyncpg.pool import PoolConnectionProxy

if TYPE_CHECKING:
    from typing import TypeAlias

    from asyncpg import Connection, Pool, Record
    from asyncpg.prepared_stmt import PreparedStatement

    AsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    AsyncpgPool: TypeAlias = Pool[Record]
    AsyncpgPreparedStatement: TypeAlias = PreparedStatement[Record]
else:
    from asyncpg import Pool
    from asyncpg.prepared_stmt import PreparedStatement

    AsyncpgConnection = PoolConnectionProxy
    AsyncpgPool = Pool
    AsyncpgPreparedStatement = PreparedStatement


__all__ = ("AsyncpgConnection", "AsyncpgPool", "AsyncpgPreparedStatement")
