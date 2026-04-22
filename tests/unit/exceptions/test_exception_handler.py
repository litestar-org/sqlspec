# pyright: reportPrivateUsage=false
"""Tests for shared exception handler bases and representative adapter handlers."""

import pytest

from sqlspec.driver import BaseAsyncExceptionHandler, BaseSyncExceptionHandler
from sqlspec.exceptions import SerializationConflictError

pytestmark = pytest.mark.xdist_group("driver")


def test_base_sync_exception_handler_defaults_to_passthrough() -> None:
    """Base sync handler should not suppress or map without an override."""
    handler = BaseSyncExceptionHandler()

    assert handler.__enter__() is handler
    assert handler.pending_exception is None
    assert handler.__exit__(None, None, None) is False
    assert handler.pending_exception is None


@pytest.mark.anyio
async def test_base_async_exception_handler_defaults_to_passthrough() -> None:
    """Base async handler should not suppress or map without an override."""
    handler = BaseAsyncExceptionHandler()

    assert await handler.__aenter__() is handler
    assert handler.pending_exception is None
    assert await handler.__aexit__(None, None, None) is False
    assert handler.pending_exception is None


def test_sync_exception_handlers_inherit_shared_base() -> None:
    """Representative sync handlers should inherit the shared base."""
    from sqlspec.adapters.bigquery.driver import BigQueryExceptionHandler
    from sqlspec.adapters.sqlite.driver import SqliteExceptionHandler

    assert issubclass(BigQueryExceptionHandler, BaseSyncExceptionHandler)
    assert issubclass(SqliteExceptionHandler, BaseSyncExceptionHandler)


def test_async_exception_handlers_inherit_shared_base() -> None:
    """Representative async handlers should inherit the shared base."""
    from sqlspec.adapters.aiosqlite.driver import AiosqliteExceptionHandler
    from sqlspec.adapters.asyncpg.driver import AsyncpgExceptionHandler

    assert issubclass(AiosqliteExceptionHandler, BaseAsyncExceptionHandler)
    assert issubclass(AsyncpgExceptionHandler, BaseAsyncExceptionHandler)


def test_duckdb_exception_handler_maps_any_present_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """DuckDB handler should map any exception when one is present."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import driver as duckdb_driver

    mapped = RuntimeError("mapped")
    seen: dict[str, object] = {}

    def fake_create_mapped_exception(exc_type: type[BaseException], exc_val: BaseException) -> Exception:
        seen["exc_type"] = exc_type
        seen["exc_val"] = exc_val
        return mapped

    monkeypatch.setattr(duckdb_driver, "create_mapped_exception", fake_create_mapped_exception)

    error = ValueError("boom")
    handler = duckdb_driver.DuckDBExceptionHandler()

    assert handler.__exit__(type(error), error, None) is True
    assert handler.pending_exception is mapped
    assert seen == {"exc_type": ValueError, "exc_val": error}


def test_mysqlconnector_sync_exception_handler_preserves_suppression(monkeypatch: pytest.MonkeyPatch) -> None:
    """mysql-connector sync handler should preserve migration-suppression sentinel values."""
    pytest.importorskip("mysql.connector")
    import mysql.connector

    from sqlspec.adapters.mysqlconnector import driver as mysqlconnector_driver

    monkeypatch.setattr(mysqlconnector_driver, "create_mapped_exception", lambda *args, **kwargs: True)

    error = mysql.connector.Error("skip mapping")
    handler = mysqlconnector_driver.MysqlConnectorSyncExceptionHandler()

    assert handler.__exit__(type(error), error, None) is True
    assert handler.pending_exception is None


@pytest.mark.anyio
async def test_mysqlconnector_async_exception_handler_preserves_suppression(monkeypatch: pytest.MonkeyPatch) -> None:
    """mysql-connector async handler should preserve migration-suppression sentinel values."""
    pytest.importorskip("mysql.connector")
    import mysql.connector

    from sqlspec.adapters.mysqlconnector import driver as mysqlconnector_driver

    monkeypatch.setattr(mysqlconnector_driver, "create_mapped_exception", lambda *args, **kwargs: True)

    error = mysql.connector.Error("skip mapping")
    handler = mysqlconnector_driver.MysqlConnectorAsyncExceptionHandler()

    assert await handler.__aexit__(type(error), error, None) is True
    assert handler.pending_exception is None


@pytest.mark.anyio
async def test_cockroach_asyncpg_exception_handler_preserves_serialization_conflicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cockroach asyncpg handler should keep serialization conflicts as dedicated errors."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.cockroach_asyncpg import driver as cockroach_asyncpg_driver

    class RetryableError(RuntimeError):
        pass

    monkeypatch.setattr(cockroach_asyncpg_driver, "has_sqlstate", lambda exc: True)

    error = RetryableError("retry")
    error.sqlstate = "40001"  # type: ignore[attr-defined]
    handler = cockroach_asyncpg_driver.CockroachAsyncpgExceptionHandler()

    assert await handler.__aexit__(type(error), error, None) is True
    assert isinstance(handler.pending_exception, SerializationConflictError)


# ─────────────────────────────────────────────────────────────────────────────
# SQLSpecError pass-through guard tests
# ─────────────────────────────────────────────────────────────────────────────


def test_sync_handler_does_not_remap_sqlspec_error() -> None:
    """Passing an already-mapped SQLSpecError through a sync handler should NOT re-map it."""
    from sqlspec.exceptions import IntegrityError

    handler = BaseSyncExceptionHandler()
    original = IntegrityError("already mapped")
    result = handler.__exit__(type(original), original, None)
    assert result is False
    assert handler.pending_exception is None


@pytest.mark.anyio
async def test_async_handler_does_not_remap_sqlspec_error() -> None:
    """Passing an already-mapped SQLSpecError through an async handler should NOT re-map it."""
    from sqlspec.exceptions import UniqueViolationError

    handler = BaseAsyncExceptionHandler()
    original = UniqueViolationError("already mapped")
    result = await handler.__aexit__(type(original), original, None)
    assert result is False
    assert handler.pending_exception is None


def test_sync_adapter_handler_does_not_remap_sqlspec_error() -> None:
    """Adapter-level sync handlers should also pass through SQLSpecError unchanged."""
    from sqlspec.adapters.sqlite.driver import SqliteExceptionHandler
    from sqlspec.exceptions import ForeignKeyViolationError

    handler = SqliteExceptionHandler()
    original = ForeignKeyViolationError("already mapped")
    result = handler.__exit__(type(original), original, None)
    assert result is False
    assert handler.pending_exception is None


@pytest.mark.anyio
async def test_async_adapter_handler_does_not_remap_sqlspec_error() -> None:
    """Adapter-level async handlers should also pass through SQLSpecError unchanged."""
    from sqlspec.adapters.aiosqlite.driver import AiosqliteExceptionHandler
    from sqlspec.exceptions import CheckViolationError

    handler = AiosqliteExceptionHandler()
    original = CheckViolationError("already mapped")
    result = await handler.__aexit__(type(original), original, None)
    assert result is False
    assert handler.pending_exception is None


# ─────────────────────────────────────────────────────────────────────────────
# Adapter exception mapping tests (synthetic — no real DB connections)
# ─────────────────────────────────────────────────────────────────────────────


def test_sqlite_handler_maps_integrity_error() -> None:
    """SQLite handler should map sqlite3.IntegrityError to a SQLSpecError."""
    import sqlite3

    from sqlspec.adapters.sqlite.driver import SqliteExceptionHandler
    from sqlspec.exceptions import SQLSpecError

    handler = SqliteExceptionHandler()
    error = sqlite3.IntegrityError("UNIQUE constraint failed: users.email")
    result = handler.__exit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is not None
    assert isinstance(handler.pending_exception, SQLSpecError)


def test_sqlite_handler_maps_operational_error() -> None:
    """SQLite handler should map sqlite3.OperationalError."""
    import sqlite3

    from sqlspec.adapters.sqlite.driver import SqliteExceptionHandler
    from sqlspec.exceptions import SQLSpecError

    handler = SqliteExceptionHandler()
    error = sqlite3.OperationalError("no such table: users")
    result = handler.__exit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is not None
    assert isinstance(handler.pending_exception, SQLSpecError)


def test_sqlite_handler_ignores_non_sqlite_errors() -> None:
    """SQLite handler should not map non-sqlite3 errors."""
    from sqlspec.adapters.sqlite.driver import SqliteExceptionHandler

    handler = SqliteExceptionHandler()
    error = ValueError("not a db error")
    result = handler.__exit__(type(error), error, None)
    assert result is False
    assert handler.pending_exception is None


@pytest.mark.anyio
async def test_aiosqlite_handler_maps_sqlite_error() -> None:
    """AIOSQLite handler should map sqlite3.Error variants."""
    import sqlite3

    from sqlspec.adapters.aiosqlite.driver import AiosqliteExceptionHandler
    from sqlspec.exceptions import SQLSpecError

    handler = AiosqliteExceptionHandler()
    error = sqlite3.IntegrityError("UNIQUE constraint failed")
    result = await handler.__aexit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is not None
    assert isinstance(handler.pending_exception, SQLSpecError)


def test_duckdb_handler_maps_constraint_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """DuckDB handler should map constraint errors to specific violation types."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import core as duckdb_core
    from sqlspec.adapters.duckdb.driver import DuckDBExceptionHandler
    from sqlspec.exceptions import UniqueViolationError

    def fake_create_mapped_exception(exc_type: "type[BaseException]", exc_val: "BaseException") -> Exception:
        return UniqueViolationError(str(exc_val))

    monkeypatch.setattr(duckdb_core, "create_mapped_exception", fake_create_mapped_exception)
    # Also patch the driver module's reference
    from sqlspec.adapters.duckdb import driver as duckdb_driver

    monkeypatch.setattr(duckdb_driver, "create_mapped_exception", fake_create_mapped_exception)

    handler = DuckDBExceptionHandler()
    error = RuntimeError("unique constraint violation")
    result = handler.__exit__(type(error), error, None)
    assert result is True
    assert isinstance(handler.pending_exception, UniqueViolationError)


def test_psycopg_sync_handler_maps_psycopg_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """psycopg sync handler should map psycopg.Error to SQLSpecError."""
    psycopg = pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg import driver as psycopg_driver
    from sqlspec.adapters.psycopg.driver import PsycopgSyncExceptionHandler
    from sqlspec.exceptions import SQLSpecError

    sentinel = SQLSpecError("mapped")
    monkeypatch.setattr(psycopg_driver, "create_mapped_exception", lambda exc_val: sentinel)

    handler = PsycopgSyncExceptionHandler()
    error = psycopg.Error("test error")
    result = handler.__exit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is sentinel


@pytest.mark.anyio
async def test_psycopg_async_handler_maps_psycopg_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """psycopg async handler should map psycopg.Error to SQLSpecError."""
    psycopg = pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg import driver as psycopg_driver
    from sqlspec.adapters.psycopg.driver import PsycopgAsyncExceptionHandler
    from sqlspec.exceptions import SQLSpecError

    sentinel = SQLSpecError("mapped")
    monkeypatch.setattr(psycopg_driver, "create_mapped_exception", lambda exc_val: sentinel)

    handler = PsycopgAsyncExceptionHandler()
    error = psycopg.Error("test error")
    result = await handler.__aexit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is sentinel


@pytest.mark.anyio
async def test_asyncpg_handler_maps_postgres_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """asyncpg handler should map asyncpg.PostgresError to SQLSpecError."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import driver as asyncpg_driver
    from sqlspec.adapters.asyncpg.driver import AsyncpgExceptionHandler
    from sqlspec.exceptions import IntegrityError

    sentinel = IntegrityError("mapped")
    monkeypatch.setattr(asyncpg_driver, "create_mapped_exception", lambda exc_val: sentinel)
    monkeypatch.setattr(asyncpg_driver, "has_sqlstate", lambda exc: True)

    error = RuntimeError("postgres-like error")
    error.sqlstate = "23505"  # type: ignore[attr-defined]
    handler = AsyncpgExceptionHandler()
    result = await handler.__aexit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is sentinel


def test_oracledb_sync_handler_maps_database_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """oracledb sync handler should map oracledb.DatabaseError."""
    oracledb = pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import driver as oracledb_driver
    from sqlspec.adapters.oracledb.driver import OracleSyncExceptionHandler
    from sqlspec.exceptions import SQLSpecError

    sentinel = SQLSpecError("mapped")
    monkeypatch.setattr(oracledb_driver, "create_mapped_exception", lambda exc_val: sentinel)

    handler = OracleSyncExceptionHandler()
    error = oracledb.DatabaseError("ORA-00001: unique constraint violated")
    result = handler.__exit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is sentinel


@pytest.mark.anyio
async def test_oracledb_async_handler_maps_database_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """oracledb async handler should map oracledb.DatabaseError."""
    oracledb = pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import driver as oracledb_driver
    from sqlspec.adapters.oracledb.driver import OracleAsyncExceptionHandler
    from sqlspec.exceptions import SQLSpecError

    sentinel = SQLSpecError("mapped")
    monkeypatch.setattr(oracledb_driver, "create_mapped_exception", lambda exc_val: sentinel)

    handler = OracleAsyncExceptionHandler()
    error = oracledb.DatabaseError("ORA-00001: unique constraint violated")
    result = await handler.__aexit__(type(error), error, None)
    assert result is True
    assert handler.pending_exception is sentinel


def test_handler_none_exc_val_returns_false() -> None:
    """All handlers should return False when exc_val is None (no exception)."""
    handler = BaseSyncExceptionHandler()
    assert handler.__exit__(None, None, None) is False


@pytest.mark.anyio
async def test_async_handler_none_exc_val_returns_false() -> None:
    """All async handlers should return False when exc_val is None."""
    handler = BaseAsyncExceptionHandler()
    assert await handler.__aexit__(None, None, None) is False


# ─────────────────────────────────────────────────────────────────────────────
# _check_pending_exception helper tests
# ─────────────────────────────────────────────────────────────────────────────


def test_check_pending_exception_raises_when_set() -> None:
    """_check_pending_exception should raise the pending exception from None."""
    from sqlspec.exceptions import UniqueViolationError

    handler = BaseSyncExceptionHandler()
    handler.pending_exception = UniqueViolationError("duplicate key")

    from sqlspec.driver._sync import SyncDriverAdapterBase

    with pytest.raises(UniqueViolationError, match="duplicate key"):
        SyncDriverAdapterBase._check_pending_exception(handler)


def test_check_pending_exception_noop_when_none() -> None:
    """_check_pending_exception should be a no-op when pending_exception is None."""
    handler = BaseSyncExceptionHandler()
    assert handler.pending_exception is None

    from sqlspec.driver._sync import SyncDriverAdapterBase

    SyncDriverAdapterBase._check_pending_exception(handler)


@pytest.mark.anyio
async def test_async_check_pending_exception_raises_when_set() -> None:
    """Async _check_pending_exception should raise the pending exception from None."""
    from sqlspec.exceptions import ForeignKeyViolationError

    handler = BaseAsyncExceptionHandler()
    handler.pending_exception = ForeignKeyViolationError("fk violation")

    from sqlspec.driver._async import AsyncDriverAdapterBase

    with pytest.raises(ForeignKeyViolationError, match="fk violation"):
        AsyncDriverAdapterBase._check_pending_exception(handler)


@pytest.mark.anyio
async def test_async_check_pending_exception_noop_when_none() -> None:
    """Async _check_pending_exception should be a no-op when pending_exception is None."""
    handler = BaseAsyncExceptionHandler()
    assert handler.pending_exception is None

    from sqlspec.driver._async import AsyncDriverAdapterBase

    AsyncDriverAdapterBase._check_pending_exception(handler)
