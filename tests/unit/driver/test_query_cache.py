# pyright: reportPrivateUsage = false
"""Tests for SQL query caching functionality."""

from typing import Any
from unittest.mock import Mock

import pytest

from sqlspec.core import (
    SQL,
    CachedStatement,
    OperationProfile,
    OperationType,
    ParameterInfo,
    ParameterProfile,
    ParameterStyle,
    ProcessedState,
    clear_all_caches,
    get_cache,
)
from sqlspec.driver._query_cache import CachedQuery, QueryCache
from sqlspec.exceptions import SQLSpecError


def _make_cached(
    compiled_sql: str = "SELECT 1",
    param_count: int = 0,
    operation_type: OperationType = "SELECT",
    column_names: list[str] | None = None,
    operation_profile: OperationProfile | None = None,
    parameter_profile: ParameterProfile | None = None,
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
        parameter_profile=parameter_profile or ParameterProfile(),
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


def test_compilation_cache_hit_skips_compile_and_stmt_cache_store(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    clear_all_caches()
    statement = SQL("SELECT :id", id=2, statement_config=sqlite_sync_driver.statement_config)
    cached_statement = CachedStatement(compiled_sql="SELECT :id", parameters={"id": 1}, expression=statement.expression)
    dialect_key = str(statement.dialect) if statement.dialect else None
    get_cache().put_statement("cache-key", cached_statement, dialect_key)

    monkeypatch.setattr(sqlite_sync_driver, "_generate_compilation_cache_key", lambda *_args, **_kwargs: "cache-key")
    monkeypatch.setattr(SQL, "compile", lambda *_args, **_kwargs: pytest.fail("cache hit should not compile"))
    monkeypatch.setattr(
        sqlite_sync_driver,
        "_stmt_cache_store",
        lambda *_args, **_kwargs: pytest.fail("cache hit should not rewrite the statement cache"),
    )

    compiled, prepared = sqlite_sync_driver._get_compiled_statement(
        statement, sqlite_sync_driver.statement_config, flatten_single_parameters=False
    )

    assert compiled.compiled_sql == "SELECT :id"
    assert prepared == {"id": 2}


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


def test_stmt_cache_prepare_direct_named_style_sets_needs_rebind(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    profile = ParameterProfile([ParameterInfo("id", ParameterStyle.NAMED_COLON, 0, 0, ":id")])
    cached = _make_cached(
        compiled_sql="SELECT :id",
        param_count=1,
        parameter_profile=profile,
        processed_state=ProcessedState(
            compiled_sql="SELECT :id", execution_parameters=[1], operation_type="SELECT", parameter_profile=profile
        ),
    )
    sqlite_sync_driver._stmt_cache.set("SELECT :id", cached)
    sqlite_sync_driver._stmt_cache_enabled = True
    called = False

    def _fake_rebind(params: tuple[Any, ...] | list[Any], cached_query: CachedQuery) -> tuple[Any, ...] | list[Any]:
        nonlocal called
        called = True
        assert params == (1,)
        assert cached_query is cached
        return params

    monkeypatch.setattr(sqlite_sync_driver, "stmt_cache_rebind", _fake_rebind)

    prepared = sqlite_sync_driver._stmt_cache_prepare_direct("SELECT :id", (1,))

    assert prepared is not None
    assert called is True


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


def test_cached_query_and_query_cache_are_final() -> None:
    """@final markers are present for mypyc devirtualization."""
    assert getattr(CachedQuery, "__final__", False) is True
    assert getattr(QueryCache, "__final__", False) is True

    cache = QueryCache()
    assert isinstance(cache, QueryCache)
    assert len(cache) == 0

    cached = CachedQuery.__new__(CachedQuery)
    assert isinstance(cached, CachedQuery)


def test_query_cache_lru_eviction_after_final() -> None:
    """QueryCache LRU eviction still works after final/native annotations."""
    cache = QueryCache(max_size=2)
    cache.set("SELECT 1", _make_cached("SELECT 1"))
    cache.set("SELECT 2", _make_cached("SELECT 2"))

    assert len(cache) == 2

    cache.set("SELECT 3", _make_cached("SELECT 3"))

    assert len(cache) == 2
    assert cache.get("SELECT 1") is None
    assert cache.get("SELECT 2") is not None
    assert cache.get("SELECT 3") is not None


def test_query_cache_zero_size_is_noop() -> None:
    """Zero-sized caches should ignore inserts instead of raising."""
    cache = QueryCache(max_size=0)

    cache.set("SELECT 1", _make_cached("SELECT 1"))

    assert len(cache) == 0
    assert cache.get("SELECT 1") is None


def test_release_pooled_statement_uses_direct_pooled_attribute(sqlite_sync_driver: Any) -> None:
    """_release_pooled_statement reads SQL._pooled directly."""
    statement = SQL("SELECT 1")

    sqlite_sync_driver._release_pooled_statement(statement)

    processed = ProcessedState(compiled_sql="SELECT 1", execution_parameters=[])
    pooled = SQL._create_cached_direct("SELECT 1", sqlite_sync_driver.statement_config, processed)

    sqlite_sync_driver._release_pooled_statement(pooled)
    assert pooled._pooled is True


def test_get_compiled_statement_compiled_from_cache_uses_direct_attribute(sqlite_sync_driver: Any) -> None:
    """Processed cache-copy statements use direct _compiled_from_cache access."""
    statement = SQL("SELECT ?", (1,), statement_config=sqlite_sync_driver.statement_config)
    statement.compile()
    copied = statement.copy(parameters=(2,))
    assert copied._compiled_from_cache is True

    compiled, prepared = sqlite_sync_driver._get_compiled_statement(
        copied, sqlite_sync_driver.statement_config, flatten_single_parameters=False
    )

    assert compiled.compiled_sql == "SELECT ?"
    assert prepared == [2]


def test_get_compiled_statement_cache_direct_uses_direct_attribute(sqlite_sync_driver: Any) -> None:
    """Cache-direct statements use direct _is_cache_direct access."""
    processed = ProcessedState(compiled_sql="SELECT ?", execution_parameters=(1,), operation_type="SELECT")
    statement = SQL._create_cached_direct("SELECT ?", sqlite_sync_driver.statement_config, processed)
    assert statement._is_cache_direct is True

    compiled, prepared = sqlite_sync_driver._get_compiled_statement(
        statement, sqlite_sync_driver.statement_config, flatten_single_parameters=False
    )

    assert compiled.compiled_sql == "SELECT ?"
    assert prepared == (1,)
