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
    from sqlspec.adapters.mock.driver import MockExceptionHandler
    from sqlspec.adapters.sqlite.driver import SqliteExceptionHandler

    assert issubclass(BigQueryExceptionHandler, BaseSyncExceptionHandler)
    assert issubclass(MockExceptionHandler, BaseSyncExceptionHandler)
    assert issubclass(SqliteExceptionHandler, BaseSyncExceptionHandler)


def test_async_exception_handlers_inherit_shared_base() -> None:
    """Representative async handlers should inherit the shared base."""
    from sqlspec.adapters.aiosqlite.driver import AiosqliteExceptionHandler
    from sqlspec.adapters.asyncpg.driver import AsyncpgExceptionHandler
    from sqlspec.adapters.mock.driver import MockAsyncExceptionHandler

    assert issubclass(AiosqliteExceptionHandler, BaseAsyncExceptionHandler)
    assert issubclass(AsyncpgExceptionHandler, BaseAsyncExceptionHandler)
    assert issubclass(MockAsyncExceptionHandler, BaseAsyncExceptionHandler)


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
