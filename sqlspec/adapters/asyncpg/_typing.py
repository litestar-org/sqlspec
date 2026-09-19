"""AsyncPG adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

import asyncpg as asyncpg_module
from asyncpg import Connection as AsyncpgRawConnection
from asyncpg import Pool, PostgresError
from asyncpg import Record as AsyncpgRecord
from asyncpg import connect as asyncpg_connect
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import ConnectionMeta as AsyncpgConnectionMeta
from asyncpg.pool import Pool as AsyncpgNativePool
from asyncpg.pool import PoolConnectionProxy
from asyncpg.pool import PoolConnectionProxy as AsyncpgNativePoolConnectionProxy
from asyncpg.pool import PoolConnectionProxyMeta as AsyncpgPoolConnectionProxyMeta
from asyncpg.prepared_stmt import PreparedStatement

from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from asyncpg import Connection, Record
    from google.cloud.alloydb.connector import AsyncConnector as AsyncpgAlloydbAsyncConnector
    from google.cloud.sql.connector import Connector as AsyncpgCloudSqlConnector

    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
    from sqlspec.core import StatementConfig

    AsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    AsyncpgPool: TypeAlias = Pool[Record]
    AsyncpgPostgresError: TypeAlias = PostgresError
    AsyncpgPreparedStatement: TypeAlias = PreparedStatement[Record]

if not TYPE_CHECKING:
    AsyncpgConnection = PoolConnectionProxy
    AsyncpgPool = Pool
    AsyncpgPostgresError = PostgresError
    AsyncpgPreparedStatement = PreparedStatement


__all__ = (
    "AsyncpgAlloydbAsyncConnector",
    "AsyncpgCloudSqlConnector",
    "AsyncpgConnection",
    "AsyncpgConnectionMeta",
    "AsyncpgCursor",
    "AsyncpgNativePool",
    "AsyncpgNativePoolConnectionProxy",
    "AsyncpgPool",
    "AsyncpgPoolConnectionProxyMeta",
    "AsyncpgPostgresError",
    "AsyncpgPreparedStatement",
    "AsyncpgRawConnection",
    "AsyncpgRecord",
    "AsyncpgSessionContext",
    "asyncpg_connect",
    "asyncpg_create_pool",
    "asyncpg_module",
)


class AsyncpgCursor:
    """Context manager for AsyncPG cursor management."""

    __slots__ = ("connection",)

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "AsyncpgConnection":
        return self.connection

    async def __aexit__(self, *_: Any) -> None: ...


class AsyncpgSessionContext:
    """Async context manager for AsyncPG sessions.

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
        prepare_driver: "Callable[[AsyncpgDriver], AsyncpgDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: AsyncpgDriver | None = None

    async def __aenter__(self) -> "AsyncpgDriver":
        from sqlspec.adapters.asyncpg.driver import AsyncpgDriver

        self._connection = await self._acquire_connection()
        statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
        self._driver = AsyncpgDriver(
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
    "AsyncpgAlloydbAsyncConnector": ("google.cloud.alloydb.connector", "AsyncConnector"),
    "AsyncpgCloudSqlConnector": ("google.cloud.sql.connector", "Connector"),
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
