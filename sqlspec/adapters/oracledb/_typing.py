"""Oracle adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import importlib
from typing import TYPE_CHECKING, Any, Literal, cast

import oracledb as oracledb_module
from oracledb import (
    DB_TYPE_BLOB,
    DB_TYPE_CHAR,
    DB_TYPE_CLOB,
    DB_TYPE_JSON,
    DB_TYPE_LONG,
    DB_TYPE_LONG_RAW,
    DB_TYPE_NCHAR,
    DB_TYPE_NVARCHAR,
    DB_TYPE_RAW,
    DB_TYPE_VARCHAR,
    DB_TYPE_VECTOR,
    DEQ_IMMEDIATE,
    DEQ_ON_COMMIT,
    AsyncConnection,
    AsyncCursor,
    Connection,
    Cursor,
    DatabaseError,
    Error,
)
from oracledb import AuthMode as OracleAuthMode
from oracledb import PoolGetMode as OraclePoolGetMode
from oracledb import Purity as OraclePurity
from oracledb import SparseVector as OracleSparseVector
from oracledb import create_pipeline as oracledb_create_pipeline
from oracledb.pool import AsyncConnectionPool, ConnectionPool

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
    from sqlspec.core import StatementConfig

OracleSyncConnection: "TypeAlias" = Connection
OracleAsyncConnection: "TypeAlias" = AsyncConnection
OracleSyncConnectionPool: "TypeAlias" = ConnectionPool
OracleAsyncConnectionPool: "TypeAlias" = AsyncConnectionPool
OracleSyncRawCursor: "TypeAlias" = Cursor
OracleAsyncRawCursor: "TypeAlias" = AsyncCursor

__all__ = (
    "DB_TYPE_BLOB",
    "DB_TYPE_CHAR",
    "DB_TYPE_CLOB",
    "DB_TYPE_JSON",
    "DB_TYPE_LONG",
    "DB_TYPE_LONG_RAW",
    "DB_TYPE_NCHAR",
    "DB_TYPE_NVARCHAR",
    "DB_TYPE_RAW",
    "DB_TYPE_VARCHAR",
    "DB_TYPE_VECTOR",
    "DEQ_IMMEDIATE",
    "DEQ_ON_COMMIT",
    "DatabaseError",
    "Error",
    "OracleAsyncConnection",
    "OracleAsyncConnectionPool",
    "OracleAsyncCursor",
    "OracleAsyncRawCursor",
    "OracleAsyncSessionContext",
    "OracleAsyncSessionlessTransactionContext",
    "OracleAuthMode",
    "OraclePoolGetMode",
    "OraclePurity",
    "OracleSparseVector",
    "OracleSyncConnection",
    "OracleSyncConnectionPool",
    "OracleSyncCursor",
    "OracleSyncRawCursor",
    "OracleSyncSessionContext",
    "OracleSyncSessionlessTransactionContext",
    "oracledb_create_pipeline",
    "oracledb_module",
)


class OracleSyncCursor:
    """Sync context manager for Oracle cursor management."""

    __slots__ = ("arraysize", "connection", "cursor", "on_enter", "prefetchrows")

    def __init__(
        self,
        connection: OracleSyncConnection,
        arraysize: "int | None" = None,
        prefetchrows: "int | None" = None,
        on_enter: "Callable[[Any], None] | None" = None,
    ) -> None:
        self.connection = connection
        self.cursor: OracleSyncRawCursor | None = None
        self.arraysize = arraysize
        self.prefetchrows = prefetchrows
        self.on_enter = on_enter

    def __enter__(self) -> "OracleSyncRawCursor":
        if self.on_enter is not None:
            self.on_enter(self.connection)
        self.cursor = self.connection.cursor()
        if self.arraysize is not None:
            self.cursor.arraysize = self.arraysize
        if self.prefetchrows is not None:
            self.cursor.prefetchrows = self.prefetchrows
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class OracleAsyncCursor:
    """Async context manager for Oracle cursor management."""

    __slots__ = ("arraysize", "connection", "cursor", "on_enter", "prefetchrows")

    def __init__(
        self,
        connection: OracleAsyncConnection,
        arraysize: "int | None" = None,
        prefetchrows: "int | None" = None,
        on_enter: "Callable[[Any], None] | None" = None,
    ) -> None:
        self.connection = connection
        self.cursor: OracleAsyncRawCursor | None = None
        self.arraysize = arraysize
        self.prefetchrows = prefetchrows
        self.on_enter = on_enter

    async def __aenter__(self) -> "OracleAsyncRawCursor":
        if self.on_enter is not None:
            self.on_enter(self.connection)
        self.cursor = self.connection.cursor()
        if self.arraysize is not None:
            self.cursor.arraysize = self.arraysize
        if self.prefetchrows is not None:
            self.cursor.prefetchrows = self.prefetchrows
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class OracleSyncSessionContext:
    """Sync context manager for Oracle sessions.

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
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[OracleSyncDriver], OracleSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: OracleSyncDriver | None = None

    def __enter__(self) -> "OracleSyncDriver":
        driver_cls = _resolve_sync_driver_type()
        self._connection = self._acquire_connection()
        try:
            self._driver = driver_cls(
                connection=self._connection,
                statement_config=self._statement_config,
                driver_features=self._driver_features,
            )
            self._driver = self._prepare_driver(self._driver)
            owner = getattr(self._acquire_connection, "__self__", None)
            prepare = getattr(owner, "prepare_driver", None)
            if callable(prepare):
                prepare(self._driver)
        except BaseException as exc:
            try:
                self._release_connection(self._connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
            finally:
                self._connection = None
            raise
        else:
            return self._driver

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


class OracleAsyncSessionContext:
    """Async context manager for Oracle sessions.

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
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[OracleAsyncDriver], OracleAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: OracleAsyncDriver | None = None

    async def __aenter__(self) -> "OracleAsyncDriver":
        driver_cls = _resolve_async_driver_type()
        self._connection = await self._acquire_connection()
        try:
            self._driver = driver_cls(
                connection=self._connection,
                statement_config=self._statement_config,
                driver_features=self._driver_features,
            )
            self._driver = self._prepare_driver(self._driver)
            owner = getattr(self._acquire_connection, "__self__", None)
            prepare = getattr(owner, "prepare_driver", None)
            if callable(prepare):
                prepare(self._driver)
        except BaseException as exc:
            try:
                await self._release_connection(
                    self._connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__
                )
            finally:
                self._connection = None
            raise
        else:
            return self._driver

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


def _resolve_sync_driver_type() -> "Callable[..., OracleSyncDriver]":
    """Resolve OracleSyncDriver without a function-scoped import."""
    return cast(
        "Callable[..., OracleSyncDriver]", importlib.import_module("sqlspec.adapters.oracledb.driver").OracleSyncDriver
    )


def _resolve_async_driver_type() -> "Callable[..., OracleAsyncDriver]":
    """Resolve OracleAsyncDriver without a function-scoped import."""
    return cast(
        "Callable[..., OracleAsyncDriver]",
        importlib.import_module("sqlspec.adapters.oracledb.driver").OracleAsyncDriver,
    )


class OracleSyncSessionlessTransactionContext:
    """Begin or resume a transaction and suspend it when its request scope exits."""

    __slots__ = ("_defer_round_trip", "_driver", "_resume", "_rollback_on_error", "_timeout", "_transaction_id")

    def __init__(
        self,
        driver: "OracleSyncDriver",
        *,
        transaction_id: str | bytes | None = None,
        timeout: int = 60,
        defer_round_trip: bool = True,
        resume: bool = False,
        rollback_on_error: bool = True,
    ) -> None:
        from sqlspec.adapters.oracledb.core import validate_sessionless_transaction_args
        from sqlspec.exceptions import ImproperConfigurationError

        if not isinstance(resume, bool) or not isinstance(rollback_on_error, bool):
            msg = "resume and rollback_on_error must be bool"
            raise ImproperConfigurationError(msg)
        self._transaction_id = validate_sessionless_transaction_args(
            transaction_id, timeout=timeout, is_resume=resume, defer_round_trip=defer_round_trip
        )
        self._driver = driver
        self._timeout = timeout
        self._defer_round_trip = defer_round_trip
        self._resume = resume
        self._rollback_on_error = rollback_on_error

    def __enter__(self) -> bytes:
        if self._resume:
            return self._driver.resume_sessionless_transaction(
                cast("str | bytes", self._transaction_id),
                timeout=self._timeout,
                defer_round_trip=self._defer_round_trip,
            )
        return self._driver.begin_sessionless_transaction(
            self._transaction_id, timeout=self._timeout, defer_round_trip=self._defer_round_trip
        )

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> Literal[False]:
        if self._driver.sessionless_transaction_id is None:
            return False
        if exc_type is None:
            self._driver.suspend_sessionless_transaction()
        else:
            with contextlib.suppress(Exception):
                if self._rollback_on_error:
                    self._driver.rollback()
                else:
                    self._driver.suspend_sessionless_transaction()
        return False


class OracleAsyncSessionlessTransactionContext:
    """Begin or resume a transaction and suspend it when its request scope exits."""

    __slots__ = ("_defer_round_trip", "_driver", "_resume", "_rollback_on_error", "_timeout", "_transaction_id")

    def __init__(
        self,
        driver: "OracleAsyncDriver",
        *,
        transaction_id: str | bytes | None = None,
        timeout: int = 60,
        defer_round_trip: bool = True,
        resume: bool = False,
        rollback_on_error: bool = True,
    ) -> None:
        from sqlspec.adapters.oracledb.core import validate_sessionless_transaction_args
        from sqlspec.exceptions import ImproperConfigurationError

        if not isinstance(resume, bool) or not isinstance(rollback_on_error, bool):
            msg = "resume and rollback_on_error must be bool"
            raise ImproperConfigurationError(msg)
        self._transaction_id = validate_sessionless_transaction_args(
            transaction_id, timeout=timeout, is_resume=resume, defer_round_trip=defer_round_trip
        )
        self._driver = driver
        self._timeout = timeout
        self._defer_round_trip = defer_round_trip
        self._resume = resume
        self._rollback_on_error = rollback_on_error

    async def __aenter__(self) -> bytes:
        if self._resume:
            return await self._driver.resume_sessionless_transaction(
                cast("str | bytes", self._transaction_id),
                timeout=self._timeout,
                defer_round_trip=self._defer_round_trip,
            )
        return await self._driver.begin_sessionless_transaction(
            self._transaction_id, timeout=self._timeout, defer_round_trip=self._defer_round_trip
        )

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> Literal[False]:
        if self._driver.sessionless_transaction_id is None:
            return False
        if exc_type is None:
            await self._driver.suspend_sessionless_transaction()
        else:
            with contextlib.suppress(Exception):
                if self._rollback_on_error:
                    await self._driver.rollback()
                else:
                    await self._driver.suspend_sessionless_transaction()
        return False
