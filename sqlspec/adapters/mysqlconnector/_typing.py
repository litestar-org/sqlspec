"""MysqlConnector adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

from sqlspec.typing import import_optional, import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType
    from typing import ClassVar, Protocol, TypeAlias

    import mysql.connector as _mysql_connector
    from mysql.connector import MySQLConnection as _MysqlConnectorSyncConnection
    from mysql.connector import pooling as _mysql_connector_pooling
    from mysql.connector.aio.pooling import MySQLConnectionPool as MysqlConnectorAsyncPool
    from mysql.connector.cursor import MySQLCursor as _MysqlConnectorSyncRawCursor

    from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
    from sqlspec.core import StatementConfig

    class MysqlConnectorAsyncRawCursorProtocol(Protocol):
        description: Any
        rowcount: int
        lastrowid: Any
        with_rows: bool

        async def execute(self, operation: Any, params: Any = None, multi: bool = False) -> Any: ...

        async def executemany(self, operation: Any, seq_params: Any) -> Any: ...

        async def fetchone(self) -> Any: ...

        async def fetchmany(self, size: int | None = None) -> list[Any]: ...

        async def fetchall(self) -> list[Any]: ...

        async def close(self) -> Any: ...

    class MysqlConnectorAsyncConnectionProtocol(Protocol):
        in_transaction: bool
        unread_result: bool

        def cursor(self, **kwargs: Any) -> "Awaitable[MysqlConnectorAsyncRawCursor]": ...

        async def commit(self) -> object: ...

        async def rollback(self) -> object: ...

        async def close(self) -> object: ...

        async def set_autocommit(self, value: bool) -> object: ...

        async def consume_results(self) -> object: ...

    class MysqlConnectorAioModuleProtocol(Protocol):
        def connect(self, *args: Any, **kwargs: Any) -> "Awaitable[MysqlConnectorAsyncConnection]": ...

    class MysqlConnectorFieldTypeProtocol(Protocol):
        JSON: int

    class MysqlConnectorConnectorModuleProtocol(Protocol):
        def connect(self, *args: Any, **kwargs: Any) -> "MysqlConnectorSyncConnection": ...

    class MysqlConnectorMysqlModuleProtocol(Protocol):
        connector: "ClassVar[MysqlConnectorConnectorModuleProtocol]"

    MysqlConnectorSyncConnection: TypeAlias = _MysqlConnectorSyncConnection
    MysqlConnectorAio: TypeAlias = MysqlConnectorAioModuleProtocol
    MysqlConnectorAsyncConnection: TypeAlias = MysqlConnectorAsyncConnectionProtocol
    MysqlConnectorSyncRawCursor: TypeAlias = _MysqlConnectorSyncRawCursor
    MysqlConnectorConnectionPool: TypeAlias = _mysql_connector_pooling.MySQLConnectionPool
    MysqlConnectorError: TypeAlias = _mysql_connector.Error
    MysqlConnectorFieldType: TypeAlias = MysqlConnectorFieldTypeProtocol
    MysqlConnectorMysqlModule: TypeAlias = MysqlConnectorMysqlModuleProtocol
    MysqlConnectorAsyncRawCursor: TypeAlias = MysqlConnectorAsyncRawCursorProtocol

if not TYPE_CHECKING:
    MysqlConnectorAsyncPool = import_optional_attr("mysql.connector.aio.pooling", "MySQLConnectionPool")
    MysqlConnectorAio = import_optional_attr("mysql.connector", "aio")
    MysqlConnectorSyncConnection = import_optional_attr("mysql.connector", "MySQLConnection")
    MysqlConnectorAsyncConnection = import_optional_attr("mysql.connector.aio", "MySQLConnection")
    MysqlConnectorConnectionPool = import_optional_attr("mysql.connector.pooling", "MySQLConnectionPool")
    MysqlConnectorError = import_optional_attr("mysql.connector", "Error")
    MysqlConnectorFieldType = import_optional_attr("mysql.connector.constants", "FieldType")
    MysqlConnectorMysqlModule = import_optional("mysql")
    MysqlConnectorSyncRawCursor = import_optional_attr("mysql.connector.cursor", "MySQLCursor")
    MysqlConnectorAsyncRawCursor = import_optional_attr("mysql.connector.aio.cursor", "MySQLCursor")

__all__ = (
    "MysqlConnectorAio",
    "MysqlConnectorAsyncConnection",
    "MysqlConnectorAsyncCursor",
    "MysqlConnectorAsyncPool",
    "MysqlConnectorAsyncRawCursor",
    "MysqlConnectorAsyncSessionContext",
    "MysqlConnectorConnectionPool",
    "MysqlConnectorError",
    "MysqlConnectorFieldType",
    "MysqlConnectorMysqlModule",
    "MysqlConnectorSyncConnection",
    "MysqlConnectorSyncCursor",
    "MysqlConnectorSyncRawCursor",
    "MysqlConnectorSyncSessionContext",
)

_resolved_sync_driver_type: "type[MysqlConnectorSyncDriver] | None" = None
_resolved_async_driver_type: "type[MysqlConnectorAsyncDriver] | None" = None


class MysqlConnectorSyncCursor:
    """Context manager for mysql-connector sync cursor operations."""

    __slots__ = ("connection", "cursor", "cursor_options")

    def __init__(
        self, connection: "MysqlConnectorSyncConnection", cursor_options: "dict[str, Any] | None" = None
    ) -> None:
        self.connection = connection
        self.cursor: MysqlConnectorSyncRawCursor | None = None
        self.cursor_options = cursor_options or {}

    def __enter__(self) -> "MysqlConnectorSyncRawCursor":
        self.cursor = self.connection.cursor(**self.cursor_options)
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            raw_connection = getattr(self.connection, "_cnx", None) or self.connection
            with contextlib.suppress(Exception):
                if getattr(raw_connection, "unread_result", False):
                    raw_connection.consume_results()
            with contextlib.suppress(Exception):
                self.cursor.close()


class MysqlConnectorAsyncCursor:
    """Async context manager for mysql-connector async cursor operations."""

    __slots__ = ("connection", "cursor", "cursor_options")

    def __init__(
        self, connection: "MysqlConnectorAsyncConnection", cursor_options: "dict[str, Any] | None" = None
    ) -> None:
        self.connection = connection
        self.cursor: MysqlConnectorAsyncRawCursor | None = None
        self.cursor_options = cursor_options or {}

    async def __aenter__(self) -> "MysqlConnectorAsyncRawCursor":
        self.cursor = await self.connection.cursor(**self.cursor_options)
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            raw_connection = getattr(self.connection, "_cnx", None) or self.connection
            with contextlib.suppress(Exception):
                if getattr(raw_connection, "unread_result", False):
                    await raw_connection.consume_results()
            with contextlib.suppress(Exception):
                await self.cursor.close()


class MysqlConnectorSyncSessionContext:
    """Sync context manager for mysql-connector sessions."""

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
        acquire_connection: "Callable[[], MysqlConnectorSyncConnection]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MysqlConnectorSyncDriver], MysqlConnectorSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: MysqlConnectorSyncConnection | None = None
        self._driver: MysqlConnectorSyncDriver | None = None

    def __enter__(self) -> "MysqlConnectorSyncDriver":
        driver_cls = _get_mysqlconnector_sync_driver_type()
        self._connection = self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


class MysqlConnectorAsyncSessionContext:
    """Async context manager for mysql-connector sessions."""

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
        acquire_connection: "Callable[[], Awaitable[MysqlConnectorAsyncConnection]]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MysqlConnectorAsyncDriver], MysqlConnectorAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: MysqlConnectorAsyncConnection | None = None
        self._driver: MysqlConnectorAsyncDriver | None = None

    async def __aenter__(self) -> "MysqlConnectorAsyncDriver":
        driver_cls = _get_mysqlconnector_async_driver_type()
        self._connection = await self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


def _get_mysqlconnector_sync_driver_type() -> "type[MysqlConnectorSyncDriver]":
    """Resolve and cache MysqlConnectorSyncDriver lazily to avoid circular imports."""
    global _resolved_sync_driver_type
    driver_cls = _resolved_sync_driver_type
    if driver_cls is None:
        driver_cls = import_optional_attr("sqlspec.adapters.mysqlconnector.driver", "MysqlConnectorSyncDriver")
        _resolved_sync_driver_type = driver_cls
    return driver_cls


def _get_mysqlconnector_async_driver_type() -> "type[MysqlConnectorAsyncDriver]":
    """Resolve and cache MysqlConnectorAsyncDriver lazily to avoid circular imports."""
    global _resolved_async_driver_type
    driver_cls = _resolved_async_driver_type
    if driver_cls is None:
        driver_cls = import_optional_attr("sqlspec.adapters.mysqlconnector.driver", "MysqlConnectorAsyncDriver")
        _resolved_async_driver_type = driver_cls
    return driver_cls
