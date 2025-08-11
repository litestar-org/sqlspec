from typing import TYPE_CHECKING

from oracledb import AsyncConnection, Connection

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    OracleSyncConnection: TypeAlias = Connection
    OracleAsyncConnection: TypeAlias = AsyncConnection
else:
    OracleSyncConnection = Connection
    OracleAsyncConnection = AsyncConnection

__all__ = ("OracleAsyncConnection", "OracleSyncConnection")
