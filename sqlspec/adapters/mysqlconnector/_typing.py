"""MysqlConnector adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

import mysql
import mysql.connector as _mysql_connector
from mysql.connector import MySQLConnection as _MysqlConnectorSyncConnection
from mysql.connector import aio as _mysql_connector_aio
from mysql.connector import constants as _mysql_connector_constants
from mysql.connector import cursor as _mysql_connector_cursor
from mysql.connector import pooling as _mysql_connector_pooling
from mysql.connector.aio import MySQLConnection as _MysqlConnectorAsyncConnection
from mysql.connector.aio import cursor as _mysql_connector_aio_cursor

from sqlspec.driver._session import AsyncDriverSessionContext, SyncDriverSessionContext

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from typing import ClassVar, Protocol, TypeAlias

    from mysql.connector.aio.pooling import MySQLConnectionPool as MysqlConnectorAsyncPool

    from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver

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
    MysqlConnectorSyncRawCursor: TypeAlias = _mysql_connector_cursor.MySQLCursor
    MysqlConnectorConnectionPool: TypeAlias = _mysql_connector_pooling.MySQLConnectionPool
    MysqlConnectorError: TypeAlias = _mysql_connector.Error
    MysqlConnectorFieldType: TypeAlias = MysqlConnectorFieldTypeProtocol
    MysqlConnectorMysqlModule: TypeAlias = MysqlConnectorMysqlModuleProtocol
    MysqlConnectorAsyncRawCursor: TypeAlias = MysqlConnectorAsyncRawCursorProtocol
else:
    try:
        from mysql.connector.aio.pooling import MySQLConnectionPool as MysqlConnectorAsyncPool
    except ModuleNotFoundError as exc:
        if exc.name != "mysql.connector.aio.pooling":
            raise
        MysqlConnectorAsyncPool = None
    MysqlConnectorAio = _mysql_connector_aio
    MysqlConnectorSyncConnection = _MysqlConnectorSyncConnection
    MysqlConnectorAsyncConnection = _MysqlConnectorAsyncConnection
    MysqlConnectorConnectionPool = _mysql_connector_pooling.MySQLConnectionPool
    MysqlConnectorError = _mysql_connector.Error
    MysqlConnectorFieldType = _mysql_connector_constants.FieldType
    MysqlConnectorMysqlModule = mysql
    MysqlConnectorSyncRawCursor = _mysql_connector_cursor.MySQLCursor
    MysqlConnectorAsyncRawCursor = _mysql_connector_aio_cursor.MySQLCursor

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


class MysqlConnectorSyncCursor:
    """Context manager for mysql-connector sync cursor operations."""

    __slots__ = ("connection", "cursor", "cursor_options")

    def __init__(
        self, connection: "MysqlConnectorSyncConnection", cursor_options: "dict[str, Any] | None" = None
    ) -> None:
        self.connection = connection
        self.cursor: MysqlConnectorSyncRawCursor | None = None
        self.cursor_options = cursor_options

    def __enter__(self) -> "MysqlConnectorSyncRawCursor":
        if self.cursor_options is None:
            self.cursor = self.connection.cursor()
        else:
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
        self.cursor_options = cursor_options

    async def __aenter__(self) -> "MysqlConnectorAsyncRawCursor":
        if self.cursor_options is None:
            self.cursor = await self.connection.cursor()
        else:
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


class MysqlConnectorSyncSessionContext(SyncDriverSessionContext):
    """Sync context manager for mysql-connector sessions."""

    __slots__ = ()

    def _resolve_driver_class(self) -> "type[MysqlConnectorSyncDriver]":
        from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorSyncDriver

        return MysqlConnectorSyncDriver


class MysqlConnectorAsyncSessionContext(AsyncDriverSessionContext):
    """Async context manager for mysql-connector sessions."""

    __slots__ = ()

    def _resolve_driver_class(self) -> "type[MysqlConnectorAsyncDriver]":
        from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver

        return MysqlConnectorAsyncDriver
