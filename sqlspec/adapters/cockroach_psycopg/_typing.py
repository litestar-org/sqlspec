"""CockroachDB psycopg adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from psycopg import AsyncCursor, Cursor
from psycopg import Error as PsycopgError
from psycopg import crdb as psycopg_crdb
from psycopg import errors as psycopg_errors
from psycopg import sql as psycopg_sql
from psycopg.rows import DictRow as PsycopgDictRow
from psycopg.rows import dict_row as psycopg_dict_row
from psycopg.types.json import Jsonb as PsycopgJsonb
from psycopg_pool import AsyncConnectionPool as PsycopgAsyncConnectionPool
from psycopg_pool import ConnectionPool as PsycopgConnectionPool

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from psycopg.crdb import AsyncCrdbConnection, CrdbConnection

    from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
    from sqlspec.core import StatementConfig

    CockroachSyncConnection: TypeAlias = CrdbConnection[PsycopgDictRow]
    CockroachAsyncConnection: TypeAlias = AsyncCrdbConnection[PsycopgDictRow]
    CockroachSyncCursor: TypeAlias = Cursor[PsycopgDictRow]
    CockroachAsyncCursor: TypeAlias = AsyncCursor[PsycopgDictRow]

if not TYPE_CHECKING:
    CockroachSyncConnection = psycopg_crdb.CrdbConnection
    CockroachAsyncConnection = psycopg_crdb.AsyncCrdbConnection
    CockroachSyncCursor = Cursor
    CockroachAsyncCursor = AsyncCursor


class CockroachPsycopgSyncSessionContext:
    """Sync context manager for CockroachDB psycopg sessions."""

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
        prepare_driver: "Callable[[CockroachPsycopgSyncDriver], CockroachPsycopgSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: CockroachPsycopgSyncDriver | None = None

    def __enter__(self) -> "CockroachPsycopgSyncDriver":
        from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgSyncDriver

        self._connection = self._acquire_connection()
        self._driver = CockroachPsycopgSyncDriver(
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


class CockroachPsycopgAsyncSessionContext:
    """Async context manager for CockroachDB psycopg sessions."""

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
        prepare_driver: "Callable[[CockroachPsycopgAsyncDriver], CockroachPsycopgAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: CockroachPsycopgAsyncDriver | None = None

    async def __aenter__(self) -> "CockroachPsycopgAsyncDriver":
        from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = CockroachPsycopgAsyncDriver(
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


__all__ = (
    "CockroachAsyncConnection",
    "CockroachAsyncCursor",
    "CockroachPsycopgAsyncSessionContext",
    "CockroachPsycopgSyncSessionContext",
    "CockroachSyncConnection",
    "CockroachSyncCursor",
    "PsycopgAsyncConnectionPool",
    "PsycopgConnectionPool",
    "PsycopgDictRow",
    "PsycopgError",
    "PsycopgJsonb",
    "psycopg_crdb",
    "psycopg_dict_row",
    "psycopg_errors",
    "psycopg_sql",
)
