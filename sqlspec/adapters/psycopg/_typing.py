"""Psycopg adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any, Protocol

from psycopg import AsyncConnection, AsyncCursor, Connection, Cursor
from psycopg import Error as PsycopgError
from psycopg import errors as psycopg_errors
from psycopg import sql as psycopg_sql
from psycopg.rows import DictRow as PsycopgDictRow
from psycopg.rows import dict_row as psycopg_dict_row
from psycopg.sql import SQL as PsycopgSQL  # noqa: N811
from psycopg.sql import Composed as PsycopgComposed
from psycopg.sql import Identifier as PsycopgIdentifier
from psycopg.types.json import Jsonb as PsycopgJsonb
from psycopg_pool import AsyncConnectionPool as PsycopgAsyncConnectionPool
from psycopg_pool import ConnectionPool as PsycopgConnectionPool

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, Statement, StatementConfig

    PsycopgSyncConnection: TypeAlias = Connection[PsycopgDictRow]
    PsycopgAsyncConnection: TypeAlias = AsyncConnection[PsycopgDictRow]
    PsycopgSyncRawCursor: TypeAlias = Cursor[PsycopgDictRow]
    PsycopgAsyncRawCursor: TypeAlias = AsyncCursor[PsycopgDictRow]

if not TYPE_CHECKING:
    PsycopgSyncConnection = Connection
    PsycopgAsyncConnection = AsyncConnection
    PsycopgSyncRawCursor = Cursor
    PsycopgAsyncRawCursor = AsyncCursor


class PsycopgSyncCursor:
    """Context manager for PostgreSQL psycopg cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "PsycopgSyncConnection") -> None:
        self.connection = connection
        self.cursor: PsycopgSyncRawCursor | None = None

    def __enter__(self) -> "PsycopgSyncRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class PsycopgAsyncCursor:
    """Async context manager for PostgreSQL psycopg cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "PsycopgAsyncConnection") -> None:
        self.connection = connection
        self.cursor: PsycopgAsyncRawCursor | None = None

    async def __aenter__(self) -> "PsycopgAsyncRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        _ = (exc_type, exc_val, exc_tb)
        if self.cursor is not None:
            await self.cursor.close()


class PsycopgPipelineDriver(Protocol):
    """Protocol for psycopg pipeline driver methods used in stack execution."""

    statement_config: "StatementConfig"

    def prepare_statement(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: Any,
        *,
        statement_config: "StatementConfig | None" = None,
        kwargs: "dict[str, Any] | None" = None,
    ) -> "SQL": ...

    def _get_compiled_sql(self, statement: "SQL", statement_config: "StatementConfig") -> "tuple[str, Any]": ...


class PsycopgSyncSessionContext:
    """Sync context manager for psycopg sessions.

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
        statement_config: "StatementConfig | Callable[[], StatementConfig]",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[PsycopgSyncDriver], PsycopgSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: PsycopgSyncDriver | None = None

    def __enter__(self) -> "PsycopgSyncDriver":
        from sqlspec.adapters.psycopg.driver import PsycopgSyncDriver

        self._connection = self._acquire_connection()
        statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
        self._driver = PsycopgSyncDriver(
            connection=self._connection, statement_config=statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


class PsycopgAsyncSessionContext:
    """Async context manager for psycopg sessions.

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
        statement_config: "StatementConfig | Callable[[], StatementConfig]",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[PsycopgAsyncDriver], PsycopgAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: PsycopgAsyncDriver | None = None

    async def __aenter__(self) -> "PsycopgAsyncDriver":
        from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver

        self._connection = await self._acquire_connection()
        statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
        self._driver = PsycopgAsyncDriver(
            connection=self._connection, statement_config=statement_config, driver_features=self._driver_features
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
    "PsycopgAsyncConnection",
    "PsycopgAsyncConnectionPool",
    "PsycopgAsyncCursor",
    "PsycopgAsyncRawCursor",
    "PsycopgAsyncSessionContext",
    "PsycopgComposed",
    "PsycopgConnectionPool",
    "PsycopgDictRow",
    "PsycopgError",
    "PsycopgIdentifier",
    "PsycopgJsonb",
    "PsycopgPipelineDriver",
    "PsycopgSQL",
    "PsycopgSyncConnection",
    "PsycopgSyncCursor",
    "PsycopgSyncRawCursor",
    "PsycopgSyncSessionContext",
    "psycopg_dict_row",
    "psycopg_errors",
    "psycopg_sql",
)
