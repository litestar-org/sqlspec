"""MysqlConnector adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

import mysql as _mysql
import mysql.connector as _mysql_connector
from mysql.connector import MySQLConnection as _MysqlConnectorSyncConnection
from mysql.connector import aio as _mysql_connector_aio
from mysql.connector import pooling as _mysql_connector_pooling
from mysql.connector.aio import (
    MySQLConnection as _MysqlConnectorAsyncConnection,  # pyright: ignore[reportMissingImports]
)
from mysql.connector.aio.cursor import (
    MySQLCursor as _MysqlConnectorAsyncRawCursor,  # pyright: ignore[reportMissingImports]
)
from mysql.connector.constants import FieldType as _MysqlConnectorFieldType
from mysql.connector.cursor import MySQLCursor as _MysqlConnectorSyncRawCursor

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType
    from typing import ClassVar, Protocol, TypeAlias

    from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
    from sqlspec.core import StatementConfig

    class MysqlConnectorAsyncConnectionProtocol(Protocol):
        def cursor(self, **kwargs: Any) -> "Awaitable[MysqlConnectorAsyncRawCursor]": ...

        async def commit(self) -> object: ...

        async def rollback(self) -> object: ...

        async def close(self) -> object: ...

        async def set_autocommit(self, value: bool) -> object: ...

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
    MysqlConnectorAsyncRawCursor: TypeAlias = _MysqlConnectorAsyncRawCursor

if not TYPE_CHECKING:
    MysqlConnectorAio = _mysql_connector_aio
    MysqlConnectorSyncConnection = _MysqlConnectorSyncConnection
    MysqlConnectorAsyncConnection = _MysqlConnectorAsyncConnection
    MysqlConnectorConnectionPool = _mysql_connector_pooling.MySQLConnectionPool
    MysqlConnectorError = _mysql_connector.Error
    MysqlConnectorFieldType = _MysqlConnectorFieldType
    MysqlConnectorMysqlModule = _mysql
    MysqlConnectorSyncRawCursor = _MysqlConnectorSyncRawCursor
    MysqlConnectorAsyncRawCursor = _MysqlConnectorAsyncRawCursor

__all__ = (
    "MysqlConnectorAio",
    "MysqlConnectorAsyncConnection",
    "MysqlConnectorAsyncCursor",
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
        self.cursor_options = cursor_options or {}

    def __enter__(self) -> "MysqlConnectorSyncRawCursor":
        self.cursor = self.connection.cursor(**self.cursor_options)
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
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
        release_connection: "Callable[[MysqlConnectorSyncConnection], None]",
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
        from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorSyncDriver

        self._connection = self._acquire_connection()
        self._driver = MysqlConnectorSyncDriver(
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
        release_connection: "Callable[[MysqlConnectorAsyncConnection], Awaitable[None]]",
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
        from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = MysqlConnectorAsyncDriver(
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
