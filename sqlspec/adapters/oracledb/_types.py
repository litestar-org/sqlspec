from typing import TYPE_CHECKING

from oracledb import AsyncConnection, Connection

if TYPE_CHECKING:
    from typing import TypeAlias

    from oracledb import DB_TYPE_VECTOR
    from oracledb.pool import AsyncConnectionPool, ConnectionPool

    OracleSyncConnection: TypeAlias = Connection
    OracleAsyncConnection: TypeAlias = AsyncConnection
    OracleSyncConnectionPool: TypeAlias = ConnectionPool
    OracleAsyncConnectionPool: TypeAlias = AsyncConnectionPool
    OracleVectorType: TypeAlias = int
else:
    from oracledb.pool import AsyncConnectionPool, ConnectionPool

    try:
        from oracledb import DB_TYPE_VECTOR

        OracleVectorType = int
    except ImportError:
        DB_TYPE_VECTOR = None
        OracleVectorType = int

    OracleSyncConnection = Connection
    OracleAsyncConnection = AsyncConnection
    OracleSyncConnectionPool = ConnectionPool
    OracleAsyncConnectionPool = AsyncConnectionPool

__all__ = (
    "DB_TYPE_VECTOR",
    "OracleAsyncConnection",
    "OracleAsyncConnectionPool",
    "OracleSyncConnection",
    "OracleSyncConnectionPool",
    "OracleVectorType",
)
