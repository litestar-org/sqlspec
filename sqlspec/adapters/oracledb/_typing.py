"""Oracle adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any, Protocol

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
from oracledb.pool import AsyncConnectionPool, ConnectionPool

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from oracledb import DB_TYPE_VECTOR  # pyright: ignore[reportUnknownVariableType]

    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, Statement, StatementConfig

    OracleSyncConnection: TypeAlias = Connection
    OracleAsyncConnection: TypeAlias = AsyncConnection
    OracleSyncConnectionPool: TypeAlias = ConnectionPool
    OracleAsyncConnectionPool: TypeAlias = AsyncConnectionPool
    OracleSyncCursorType: TypeAlias = Cursor
    OracleAsyncCursorType: TypeAlias = AsyncCursor
    OracleVectorType: TypeAlias = int

if not TYPE_CHECKING:
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
    OracleSyncCursorType = Cursor
    OracleAsyncCursorType = AsyncCursor


class OracleSyncCursor:
    """Sync context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleSyncConnection) -> None:
        self.connection = connection
        self.cursor: Any = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            self.cursor.close()


class OracleAsyncCursor:
    """Async context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleAsyncConnection) -> None:
        self.connection = connection
        self.cursor: Any = None

    async def __aenter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        _ = (exc_type, exc_val, exc_tb)  # Mark as intentionally unused
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                # Oracle async cursors have a synchronous close method
                # but we need to ensure proper cleanup in the event loop context
                self.cursor.close()


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


class OracleSyncSessionContext:
    """Sync context manager for Oracle sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[OracleSyncDriver], OracleSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: OracleSyncDriver | None = None

    def __enter__(self) -> "OracleSyncDriver":
        from sqlspec.adapters.oracledb.driver import OracleSyncDriver

        self._connection = self._acquire_connection()
        self._driver = OracleSyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


class OracleAsyncSessionContext:
    """Async context manager for Oracle sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[OracleAsyncDriver], OracleAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: OracleAsyncDriver | None = None

    async def __aenter__(self) -> "OracleAsyncDriver":
        from sqlspec.adapters.oracledb.driver import OracleAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = OracleAsyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "DB_TYPE_VECTOR",
    "OracleAsyncConnection",
    "OracleAsyncConnectionPool",
    "OracleAsyncCursor",
    "OracleAsyncCursorType",
    "OracleAsyncSessionContext",
    "OraclePipelineDriver",
    "OracleSyncConnection",
    "OracleSyncConnectionPool",
    "OracleSyncCursor",
    "OracleSyncCursorType",
    "OracleSyncSessionContext",
    "OracleVectorType",
)
