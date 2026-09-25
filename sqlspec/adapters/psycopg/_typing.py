"""Psycopg adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any, Protocol

import psycopg as psycopg_module
from psycopg import AsyncConnection, AsyncCursor, Connection, Cursor
from psycopg import AsyncConnection as PsycopgNativeAsyncConnection
from psycopg import AsyncCursor as PsycopgNativeAsyncCursor
from psycopg import Connection as PsycopgConnection
from psycopg import Cursor as PsycopgCursor
from psycopg import ProgrammingError as PsycopgProgrammingError
from psycopg import errors as psycopg_errors
from psycopg import sql as psycopg_sql
from psycopg.abc import AdaptContext as PsycopgAdaptContext
from psycopg.rows import AsyncRowFactory as PsycopgAsyncRowFactory
from psycopg.rows import DictRow as PsycopgDictRow
from psycopg.rows import RowFactory as PsycopgRowFactory
from psycopg.rows import dict_row as psycopg_dict_row
from psycopg.sql import SQL as PsycopgSQL  # noqa: N811
from psycopg.sql import Composed as PsycopgComposed
from psycopg.sql import Identifier as PsycopgIdentifier
from psycopg.types.json import Jsonb as PsycopgJsonb
from psycopg_pool import AsyncConnectionPool as PsycopgAsyncConnectionPool
from psycopg_pool import AsyncNullConnectionPool as PsycopgAsyncNullConnectionPool
from psycopg_pool import ConnectionPool as PsycopgConnectionPool
from psycopg_pool import NullConnectionPool as PsycopgNullConnectionPool
from psycopg_pool.abc import AsyncConnectFailedCB as PsycopgAsyncConnectFailedCB
from psycopg_pool.abc import AsyncConnectionCB as PsycopgAsyncConnectionCB
from psycopg_pool.abc import ConnectFailedCB as PsycopgConnectFailedCB
from psycopg_pool.abc import ConnectionCB as PsycopgConnectionCB

from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from google.cloud.alloydb.connector import Connector as PsycopgAlloydbConnector

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


__all__ = (
    "PsycopgAdaptContext",
    "PsycopgAlloydbConnector",
    "PsycopgAsyncConnectFailedCB",
    "PsycopgAsyncConnection",
    "PsycopgAsyncConnectionCB",
    "PsycopgAsyncConnectionPool",
    "PsycopgAsyncCursor",
    "PsycopgAsyncNullConnectionPool",
    "PsycopgAsyncRawCursor",
    "PsycopgAsyncRowFactory",
    "PsycopgAsyncSessionContext",
    "PsycopgComposed",
    "PsycopgConnectFailedCB",
    "PsycopgConnection",
    "PsycopgConnectionCB",
    "PsycopgConnectionPool",
    "PsycopgCursor",
    "PsycopgDictRow",
    "PsycopgIdentifier",
    "PsycopgJsonb",
    "PsycopgNativeAsyncConnection",
    "PsycopgNativeAsyncCursor",
    "PsycopgNullConnectionPool",
    "PsycopgPipelineDriver",
    "PsycopgProgrammingError",
    "PsycopgRowFactory",
    "PsycopgSQL",
    "PsycopgSyncConnection",
    "PsycopgSyncCursor",
    "PsycopgSyncRawCursor",
    "PsycopgSyncSessionContext",
    "psycopg_dict_row",
    "psycopg_errors",
    "psycopg_module",
    "psycopg_sql",
)


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
            with contextlib.suppress(Exception):
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
            with contextlib.suppress(Exception):
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

    def prepare_driver_parameters(
        self,
        parameters: Any,
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Any | None = None,
    ) -> Any: ...

    def _compiled_sql(self, statement: "SQL", statement_config: "StatementConfig") -> "tuple[str, Any]": ...


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
        release_connection: "Callable[..., Any]",
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
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
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
        release_connection: "Callable[..., Any]",
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
            await self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


_LAZY_DRIVER_EXPORTS: dict[str, tuple[str, str]] = {
    "PsycopgAlloydbConnector": ("google.cloud.alloydb.connector", "Connector")
}


def __getattr__(name: str) -> Any:
    """Resolve optional driver symbols only when a consumer requests them."""
    target = _LAZY_DRIVER_EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = import_optional_attr(module_name, attribute)
    if value is None:
        msg = f"Cannot import {attribute!r} from {module_name!r}"
        raise ImportError(msg)
    return value
