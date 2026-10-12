"""CockroachDB AsyncPG adapter type definitions."""

from typing import TYPE_CHECKING, Any, cast

import asyncpg as cockroach_asyncpg_module
from asyncpg import Pool
from asyncpg import PostgresError as CockroachAsyncpgPostgresError
from asyncpg import Record as CockroachAsyncpgRecord
from asyncpg import connect as cockroach_asyncpg_connect
from asyncpg import create_pool as cockroach_asyncpg_create_pool
from asyncpg.pool import PoolConnectionProxy

from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from asyncpg import Connection, Record

    from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver
    from sqlspec.core import StatementConfig

    CockroachAsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    CockroachAsyncpgPool: TypeAlias = Pool[Record]
else:
    CockroachAsyncpgConnection = PoolConnectionProxy
    CockroachAsyncpgPool = Pool

__all__ = (
    "CockroachAsyncpgConnection",
    "CockroachAsyncpgPool",
    "CockroachAsyncpgPostgresError",
    "CockroachAsyncpgRecord",
    "CockroachAsyncpgSessionContext",
    "cockroach_asyncpg_connect",
    "cockroach_asyncpg_create_pool",
    "cockroach_asyncpg_module",
)

_COCKROACH_ASYNCPG_DRIVER_CLS: "type[CockroachAsyncpgDriver] | None" = None


class CockroachAsyncpgSessionContext:
    """Async context manager for CockroachDB AsyncPG sessions."""

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
        prepare_driver: "Callable[[CockroachAsyncpgDriver], CockroachAsyncpgDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: CockroachAsyncpgDriver | None = None

    async def __aenter__(self) -> "CockroachAsyncpgDriver":
        driver_cls = _resolve_cockroach_asyncpg_driver()
        self._connection = await self._acquire_connection()
        statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
        self._driver = driver_cls(
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


def _resolve_cockroach_asyncpg_driver() -> "type[CockroachAsyncpgDriver]":
    global _COCKROACH_ASYNCPG_DRIVER_CLS
    driver_cls = _COCKROACH_ASYNCPG_DRIVER_CLS
    if driver_cls is None:
        resolved = import_optional_attr("sqlspec.adapters.cockroach_asyncpg.driver", "CockroachAsyncpgDriver")
        if resolved is None:
            msg = "Cannot import 'CockroachAsyncpgDriver' from 'sqlspec.adapters.cockroach_asyncpg.driver'"
            raise ImportError(msg)
        driver_cls = cast("type[CockroachAsyncpgDriver]", resolved)
        _COCKROACH_ASYNCPG_DRIVER_CLS = driver_cls
    return driver_cls
