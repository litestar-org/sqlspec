# pyright: reportPrivateUsage = false
"""Tests for SQL query caching functionality."""

from typing import Any
from unittest.mock import Mock

import pytest

from sqlspec.core import SQL, OperationProfile, OperationType, ParameterProfile, ProcessedState
from sqlspec.driver._query_cache import CachedQuery
from sqlspec.exceptions import SQLSpecError


def _make_cached(
    compiled_sql: str = "SELECT 1",
    param_count: int = 0,
    operation_type: OperationType = "SELECT",
    column_names: list[str] | None = None,
    operation_profile: OperationProfile | None = None,
    processed_state: ProcessedState | None = None,
) -> CachedQuery:
    if operation_profile is None:
        operation_profile = OperationProfile(returns_rows=True, modifies_rows=False)
    if processed_state is None:
        processed_state = ProcessedState(
            compiled_sql=compiled_sql, execution_parameters=[], operation_type=operation_type
        )
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type=operation_type,
        operation_profile=operation_profile,
        param_count=param_count,
        processed_state=processed_state,
        column_names=column_names,
    )


def test_sync_stmt_cache_execute_direct_uses_fast_path(sqlite_sync_driver, monkeypatch) -> None:
    """Test that direct cache execution uses the fast path bypassing dispatch_execute."""
    sqlite_sync_driver.execute("CREATE TABLE t (id INTEGER)")

    # We want to verify it bypasses dispatch_execute
    def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        pytest.fail("dispatch_execute should not be called on fast path")

    monkeypatch.setattr(sqlite_sync_driver, "dispatch_execute", _fake_dispatch_execute)

    cached = _make_cached(
        compiled_sql="INSERT INTO t (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO t (id) VALUES (?)", execution_parameters=[1], operation_type="INSERT"
        ),
    )

    result = sqlite_sync_driver._stmt_cache_execute_direct("INSERT INTO t (id) VALUES (?)", (1,), cached)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 1


def test_execute_uses_fast_path_when_eligible(sqlite_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(sqlite_sync_driver, "_stmt_cache_lookup", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    result = sqlite_sync_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


def test_execute_skips_fast_path_with_statement_config_override(sqlite_sync_driver, monkeypatch) -> None:
    called = False

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(sqlite_sync_driver, "_stmt_cache_lookup", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    statement_config = sqlite_sync_driver.statement_config.replace()
    result = sqlite_sync_driver.execute("SELECT ?", (1,), statement_config=statement_config)

    assert called is False
    assert result.operation_type == "SELECT"


def test_execute_populates_fast_path_cache_on_normal_path(sqlite_sync_driver) -> None:
    sqlite_sync_driver._stmt_cache_enabled = True

    assert sqlite_sync_driver._stmt_cache.get("SELECT ?") is None

    result = sqlite_sync_driver.execute("SELECT ?", (1,))

    cached = sqlite_sync_driver._stmt_cache.get("SELECT ?")
    assert cached is not None
    assert cached.param_count == 1
    assert cached.operation_type == "SELECT"
    assert result.operation_type == "SELECT"


def test_sync_stmt_cache_execute_re_raises_mapped_exception(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    import sqlite3

    def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr(sqlite_sync_driver, "dispatch_execute", _fake_dispatch_execute)
    statement = SQL("SELECT ?", (1,), statement_config=sqlite_sync_driver.statement_config)
    statement.compile()

    with pytest.raises(SQLSpecError, match="SQLite database error: boom"):
        sqlite_sync_driver._stmt_cache_execute(statement)


def test_sync_stmt_cache_execute_direct_re_raises_mapped_exception(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    import sqlite3

    sqlite_sync_driver.execute("CREATE TABLE t (id INTEGER)")

    # Wrap connection to allow patching 'execute'
    wrapped_conn = Mock(wraps=sqlite_sync_driver.connection)
    wrapped_conn.execute.side_effect = sqlite3.OperationalError("boom")
    monkeypatch.setattr(sqlite_sync_driver, "connection", wrapped_conn)

    cached = _make_cached(
        compiled_sql="INSERT INTO t (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO t (id) VALUES (?)", execution_parameters=[1], operation_type="INSERT"
        ),
    )

    with pytest.raises(SQLSpecError, match="SQLite database error: boom"):
        sqlite_sync_driver._stmt_cache_execute_direct("INSERT INTO t (id) VALUES (?)", (1,), cached)


@pytest.mark.anyio
async def test_async_execute_uses_fast_path_when_eligible(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(aiosqlite_async_driver, "_stmt_cache_lookup", _fake_try)
    aiosqlite_async_driver._stmt_cache_enabled = True

    result = await aiosqlite_async_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


@pytest.mark.anyio
async def test_async_execute_skips_fast_path_with_statement_config_override(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    called = False

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(aiosqlite_async_driver, "_stmt_cache_lookup", _fake_try)
    aiosqlite_async_driver._stmt_cache_enabled = True

    statement_config = aiosqlite_async_driver.statement_config.replace()
    result = await aiosqlite_async_driver.execute("SELECT ?", (1,), statement_config=statement_config)

    assert called is False
    assert result.operation_type == "SELECT"


@pytest.mark.anyio
async def test_async_execute_populates_fast_path_cache_on_normal_path(aiosqlite_async_driver: Any) -> None:
    aiosqlite_async_driver._stmt_cache_enabled = True

    assert aiosqlite_async_driver._stmt_cache.get("SELECT ?") is None

    result = await aiosqlite_async_driver.execute("SELECT ?", (1,))

    cached = aiosqlite_async_driver._stmt_cache.get("SELECT ?")
    assert cached is not None
    assert cached.param_count == 1
    assert cached.operation_type == "SELECT"
    assert result.operation_type == "SELECT"


@pytest.mark.anyio
async def test_async_stmt_cache_execute_re_raises_mapped_exception(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    import aiosqlite

    async def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise aiosqlite.OperationalError("boom")

    monkeypatch.setattr(aiosqlite_async_driver, "dispatch_execute", _fake_dispatch_execute)
    statement = SQL("SELECT ?", (1,), statement_config=aiosqlite_async_driver.statement_config)
    statement.compile()

    with pytest.raises(SQLSpecError, match="AIOSQLite database error: boom"):
        await aiosqlite_async_driver._stmt_cache_execute(statement)


@pytest.mark.anyio
async def test_async_stmt_cache_execute_direct_re_raises_mapped_exception(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    import aiosqlite

    await aiosqlite_async_driver.execute("CREATE TABLE t (id INTEGER)")

    async def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise aiosqlite.OperationalError("boom")

    monkeypatch.setattr(aiosqlite_async_driver, "dispatch_execute", _fake_dispatch_execute)

    cached = _make_cached(
        compiled_sql="INSERT INTO t (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO t (id) VALUES (?)", execution_parameters=[1], operation_type="INSERT"
        ),
    )

    with pytest.raises(SQLSpecError, match="AIOSQLite database error: boom"):
        await aiosqlite_async_driver._stmt_cache_execute_direct("INSERT INTO t (id) VALUES (?)", (1,), cached)
