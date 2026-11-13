"""CockroachDB psycopg adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any, cast

import psycopg as cockroach_psycopg_module
from psycopg import AsyncCursor, Cursor
from psycopg import crdb as cockroach_psycopg_crdb
from psycopg import errors as cockroach_psycopg_errors
from psycopg import pq as cockroach_psycopg_pq
from psycopg import sql as cockroach_psycopg_sql
from psycopg.adapt import AdaptersMap as CockroachPsycopgAdaptersMap
from psycopg.rows import DictRow as PsycopgDictRow
from psycopg.rows import dict_row as cockroach_psycopg_dict_row
from psycopg.rows import tuple_row as cockroach_psycopg_tuple_row
from psycopg.types.json import Jsonb as CockroachPsycopgJsonb
from psycopg.types.json import set_json_dumps as cockroach_psycopg_set_json_dumps
from psycopg.types.json import set_json_loads as cockroach_psycopg_set_json_loads
from psycopg_pool import AsyncConnectionPool as CockroachPsycopgAsyncConnectionPool
from psycopg_pool import ConnectionPool as CockroachPsycopgConnectionPool

from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from psycopg.crdb import AsyncCrdbConnection, CrdbConnection

    from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
    from sqlspec.core import StatementConfig

    CockroachSyncConnection: TypeAlias = CrdbConnection[Any]
    CockroachAsyncConnection: TypeAlias = AsyncCrdbConnection[Any]
    CockroachSyncCursor: TypeAlias = Cursor[Any]
    CockroachAsyncCursor: TypeAlias = AsyncCursor[Any]
else:
    CockroachSyncConnection = cockroach_psycopg_crdb.CrdbConnection
    CockroachAsyncConnection = cockroach_psycopg_crdb.AsyncCrdbConnection
    CockroachSyncCursor = Cursor
    CockroachAsyncCursor = AsyncCursor

__all__ = (
    "CockroachAsyncConnection",
    "CockroachAsyncCursor",
    "CockroachPsycopgAdaptersMap",
    "CockroachPsycopgAsyncConnectionPool",
    "CockroachPsycopgAsyncSessionContext",
    "CockroachPsycopgConnectionPool",
    "CockroachPsycopgJsonb",
    "CockroachPsycopgSyncSessionContext",
    "CockroachSyncConnection",
    "CockroachSyncCursor",
    "PsycopgDictRow",
    "cockroach_psycopg_crdb",
    "cockroach_psycopg_dict_row",
    "cockroach_psycopg_errors",
    "cockroach_psycopg_module",
    "cockroach_psycopg_pq",
    "cockroach_psycopg_set_json_dumps",
    "cockroach_psycopg_set_json_loads",
    "cockroach_psycopg_sql",
    "cockroach_psycopg_tuple_row",
)

_COCKROACH_PSYCOPG_SYNC_DRIVER_CLS: "type[CockroachPsycopgSyncDriver] | None" = None
_COCKROACH_PSYCOPG_ASYNC_DRIVER_CLS: "type[CockroachPsycopgAsyncDriver] | None" = None


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
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig | Callable[[], StatementConfig]",
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
        driver_cls = _resolve_cockroach_psycopg_sync_driver()
        self._connection = self._acquire_connection()
        try:
            statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
            self._driver = driver_cls(
                connection=self._connection, statement_config=statement_config, driver_features=self._driver_features
            )
            return self._prepare_driver(self._driver)
        except BaseException as exc:
            self.__exit__(type(exc), exc, exc.__traceback__)
            raise

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        rollback_exc: BaseException | None = None
        try:
            if self._driver is not None and (
                getattr(self._driver, "_transaction_active", False)
                or getattr(self._driver, "_connection_in_transaction", lambda: False)()
            ):
                try:
                    self._driver.rollback()
                except BaseException as exc:
                    rollback_exc = exc
                    if exc_type is None:
                        raise
        finally:
            self._driver = None
            if self._connection is not None:
                release_type = exc_type if exc_type is not None else (type(rollback_exc) if rollback_exc else None)
                release_val = exc_val if exc_val is not None else rollback_exc
                release_tb = exc_tb if exc_tb is not None else (rollback_exc.__traceback__ if rollback_exc else None)
                self._release_connection(
                    self._connection, exc_type=release_type, exc_val=release_val, exc_tb=release_tb
                )
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
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig | Callable[[], StatementConfig]",
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
        driver_cls = _resolve_cockroach_psycopg_async_driver()
        self._connection = await self._acquire_connection()
        try:
            statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
            self._driver = driver_cls(
                connection=self._connection, statement_config=statement_config, driver_features=self._driver_features
            )
            return self._prepare_driver(self._driver)
        except BaseException as exc:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
            raise

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        rollback_exc: BaseException | None = None
        try:
            if self._driver is not None and (
                getattr(self._driver, "_transaction_active", False)
                or getattr(self._driver, "_connection_in_transaction", lambda: False)()
            ):
                try:
                    await self._driver.rollback()
                except BaseException as exc:
                    rollback_exc = exc
                    if exc_type is None:
                        raise
        finally:
            self._driver = None
            if self._connection is not None:
                release_type = exc_type if exc_type is not None else (type(rollback_exc) if rollback_exc else None)
                release_val = exc_val if exc_val is not None else rollback_exc
                release_tb = exc_tb if exc_tb is not None else (rollback_exc.__traceback__ if rollback_exc else None)
                await self._release_connection(
                    self._connection, exc_type=release_type, exc_val=release_val, exc_tb=release_tb
                )
                self._connection = None
        return None


def _resolve_cockroach_psycopg_sync_driver() -> "type[CockroachPsycopgSyncDriver]":
    global _COCKROACH_PSYCOPG_SYNC_DRIVER_CLS
    driver_cls = _COCKROACH_PSYCOPG_SYNC_DRIVER_CLS
    if driver_cls is None:
        resolved = import_optional_attr("sqlspec.adapters.cockroach_psycopg.driver", "CockroachPsycopgSyncDriver")
        if resolved is None:
            msg = "Cannot import 'CockroachPsycopgSyncDriver' from 'sqlspec.adapters.cockroach_psycopg.driver'"
            raise ImportError(msg)
        driver_cls = cast("type[CockroachPsycopgSyncDriver]", resolved)
        _COCKROACH_PSYCOPG_SYNC_DRIVER_CLS = driver_cls
    return driver_cls


def _resolve_cockroach_psycopg_async_driver() -> "type[CockroachPsycopgAsyncDriver]":
    global _COCKROACH_PSYCOPG_ASYNC_DRIVER_CLS
    driver_cls = _COCKROACH_PSYCOPG_ASYNC_DRIVER_CLS
    if driver_cls is None:
        resolved = import_optional_attr("sqlspec.adapters.cockroach_psycopg.driver", "CockroachPsycopgAsyncDriver")
        if resolved is None:
            msg = "Cannot import 'CockroachPsycopgAsyncDriver' from 'sqlspec.adapters.cockroach_psycopg.driver'"
            raise ImportError(msg)
        driver_cls = cast("type[CockroachPsycopgAsyncDriver]", resolved)
        _COCKROACH_PSYCOPG_ASYNC_DRIVER_CLS = driver_cls
    return driver_cls
