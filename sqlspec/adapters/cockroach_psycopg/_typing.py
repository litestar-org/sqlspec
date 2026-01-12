"""CockroachDB psycopg adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from psycopg import crdb as psycopg_crdb
from psycopg.rows import DictRow as PsycopgDictRow

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from psycopg.crdb import AsyncCrdbConnection, CrdbConnection

    from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
    from sqlspec.core import StatementConfig

    # Parametrize with DictRow so type system knows rows are dict-like
    CockroachSyncConnection: TypeAlias = CrdbConnection[PsycopgDictRow]
    CockroachAsyncConnection: TypeAlias = AsyncCrdbConnection[PsycopgDictRow]
else:
    CockroachSyncConnection = psycopg_crdb.CrdbConnection
    CockroachAsyncConnection = psycopg_crdb.AsyncCrdbConnection


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
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
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
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "CockroachAsyncConnection",
    "CockroachPsycopgAsyncSessionContext",
    "CockroachPsycopgSyncSessionContext",
    "CockroachSyncConnection",
    "PsycopgDictRow",
)
