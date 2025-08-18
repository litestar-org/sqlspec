from typing import TYPE_CHECKING

from oracledb import AsyncConnection, Connection

if TYPE_CHECKING:
    from oracledb.pool import AsyncConnectionPool, ConnectionPool
    from typing_extensions import TypeAlias

    OracleSyncConnection: TypeAlias = Connection
    OracleAsyncConnection: TypeAlias = AsyncConnection
    OracleSyncConnectionPool: TypeAlias = ConnectionPool
    OracleAsyncConnectionPool: TypeAlias = AsyncConnectionPool
else:
    from oracledb.pool import AsyncConnectionPool, ConnectionPool

    OracleSyncConnection = Connection
    OracleAsyncConnection = AsyncConnection
    OracleSyncConnectionPool = ConnectionPool
    OracleAsyncConnectionPool = AsyncConnectionPool

__all__ = ("OracleAsyncConnection", "OracleAsyncConnectionPool", "OracleSyncConnection", "OracleSyncConnectionPool")
