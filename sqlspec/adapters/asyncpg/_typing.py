"""AsyncPG adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from asyncpg import Connection, Pool, PostgresError, Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import ConnectionMeta
from asyncpg.exceptions import UndefinedTableError
from asyncpg.pool import PoolConnectionProxy, PoolConnectionProxyMeta
from asyncpg.prepared_stmt import PreparedStatement

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
    from sqlspec.core import StatementConfig

    AsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    AsyncpgPool: TypeAlias = Pool[Record]
    AsyncpgPreparedStatement: TypeAlias = PreparedStatement[Record]

if not TYPE_CHECKING:  # pyright: ignore[reportUnreachable]
    AsyncpgConnection = PoolConnectionProxy
    AsyncpgPool = Pool
    AsyncpgPreparedStatement = PreparedStatement


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
        release_connection: "Callable[[Any], Any]",
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
        del exc_type, exc_val, exc_tb
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "AsyncpgConnection",
    "AsyncpgCursor",
    "AsyncpgPool",
    "AsyncpgPreparedStatement",
    "AsyncpgSessionContext",
    "Connection",
    "ConnectionMeta",
    "Pool",
    "PoolConnectionProxy",
    "PoolConnectionProxyMeta",
    "PostgresError",
    "Record",
    "UndefinedTableError",
    "asyncpg_create_pool",
)
