from typing import TYPE_CHECKING, Any, Protocol

from oracledb import AsyncConnection, Connection

if TYPE_CHECKING:
    from typing import TypeAlias

    from oracledb import DB_TYPE_VECTOR  # pyright: ignore[reportUnknownVariableType]
    from oracledb.pool import AsyncConnectionPool, ConnectionPool

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, Statement, StatementConfig

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


class OraclePipelineDriver(Protocol):
    """Protocol for Oracle pipeline driver methods used in stack execution."""

    statement_config: "StatementConfig"
    driver_features: "dict[str, Any]"

    def prepare_statement(
        self,
        statement: "str | Statement | QueryBuilder",
        parameters: "tuple[Any, ...] | dict[str, Any] | None",
        *,
        statement_config: "StatementConfig | None" = None,
        kwargs: "dict[str, Any] | None" = None,
    ) -> "SQL": ...

    def _get_compiled_sql(self, statement: "SQL", statement_config: "StatementConfig") -> "tuple[str, Any]": ...


__all__ = (
    "DB_TYPE_VECTOR",
    "OracleAsyncConnection",
    "OracleAsyncConnectionPool",
    "OraclePipelineDriver",
    "OracleSyncConnection",
    "OracleSyncConnectionPool",
    "OracleVectorType",
)
