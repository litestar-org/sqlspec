"""MysqlConnector adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from mysql.connector import MySQLConnection as _MysqlConnectorSyncConnection

try:
    from mysql.connector.aio import (
        MySQLConnection as _MysqlConnectorAsyncConnection,  # pyright: ignore[reportMissingImports]
    )
except ImportError:  # pragma: no cover - optional async import
    _MysqlConnectorAsyncConnection = _MysqlConnectorSyncConnection  # type: ignore[assignment,misc]


if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Protocol, TypeAlias

    from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
    from sqlspec.core import StatementConfig

    class MysqlConnectorAsyncConnectionProtocol(Protocol):
        def cursor(self, **kwargs: Any) -> Any: ...

        async def commit(self) -> Any: ...

        async def rollback(self) -> Any: ...

        async def close(self) -> Any: ...

    MysqlConnectorSyncConnection: TypeAlias = _MysqlConnectorSyncConnection
    MysqlConnectorAsyncConnection: TypeAlias = MysqlConnectorAsyncConnectionProtocol
else:
    MysqlConnectorSyncConnection = _MysqlConnectorSyncConnection
    MysqlConnectorAsyncConnection = _MysqlConnectorAsyncConnection


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
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MysqlConnectorSyncDriver], MysqlConnectorSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: MysqlConnectorSyncDriver | None = None

    def __enter__(self) -> "MysqlConnectorSyncDriver":
        from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorSyncDriver

        self._connection = self._acquire_connection()
        self._driver = MysqlConnectorSyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
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
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MysqlConnectorAsyncDriver], MysqlConnectorAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: MysqlConnectorAsyncDriver | None = None

    async def __aenter__(self) -> "MysqlConnectorAsyncDriver":
        from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = MysqlConnectorAsyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "MysqlConnectorAsyncConnection",
    "MysqlConnectorAsyncSessionContext",
    "MysqlConnectorSyncConnection",
    "MysqlConnectorSyncSessionContext",
)
