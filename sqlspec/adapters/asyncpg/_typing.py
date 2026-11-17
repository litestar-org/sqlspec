"""AsyncPG adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

import asyncpg as asyncpg_module
from asyncpg import Connection as AsyncpgRawConnection
from asyncpg import InterfaceError as AsyncpgInterfaceError
from asyncpg import Pool
from asyncpg import PostgresError as AsyncpgPostgresError
from asyncpg import Record as AsyncpgRecord
from asyncpg import connect as asyncpg_connect
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import ConnectionMeta as AsyncpgConnectionMeta
from asyncpg.pool import Pool as AsyncpgNativePool
from asyncpg.pool import PoolConnectionProxy
from asyncpg.pool import PoolConnectionProxy as AsyncpgNativePoolConnectionProxy
from asyncpg.pool import PoolConnectionProxyMeta as AsyncpgPoolConnectionProxyMeta
from asyncpg.prepared_stmt import PreparedStatement

from sqlspec.driver._session import AsyncDriverSessionContext
from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from typing import TypeAlias

    from asyncpg import Connection, Record
    from google.cloud.alloydb.connector import AsyncConnector as AsyncpgAlloydbAsyncConnector
    from google.cloud.sql.connector import Connector as AsyncpgCloudSqlConnector

    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver

    AsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    AsyncpgPool: TypeAlias = Pool[Record]
    AsyncpgPreparedStatement: TypeAlias = PreparedStatement[Record]
else:
    AsyncpgConnection = PoolConnectionProxy
    AsyncpgPool = Pool
    AsyncpgPreparedStatement = PreparedStatement


__all__ = (
    "AsyncpgAlloydbAsyncConnector",
    "AsyncpgCloudSqlConnector",
    "AsyncpgConnection",
    "AsyncpgConnectionMeta",
    "AsyncpgCursor",
    "AsyncpgInterfaceError",
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


class AsyncpgSessionContext(AsyncDriverSessionContext):
    """Async context manager for AsyncPG sessions."""

    __slots__ = ()

    def _resolve_driver_class(self) -> "type[AsyncpgDriver]":
        from sqlspec.adapters.asyncpg.driver import AsyncpgDriver

        return AsyncpgDriver


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
