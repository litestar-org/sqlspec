# pyright: reportPrivateUsage = false
"""Tests for SQL query caching functionality."""

import gc
import sqlite3
import threading
from collections import UserDict
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from datetime import date
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from sqlglot import parse_one

from sqlspec import ObservabilityConfig
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.base import SQLSpec
from sqlspec.config import LifecycleConfig
from sqlspec.core import (
    SQL,
    CachedStatement,
    OperationProfile,
    OperationType,
    ParameterInfo,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    ProcessedState,
    StatementConfig,
    TypedParameter,
    build_null_pruning_transform,
    clear_all_caches,
    get_cache,
)
from sqlspec.core.cache import CachedQuery, QueryCache
from sqlspec.core.filters import LimitOffsetFilter
from sqlspec.core.parameters._declared import ParameterDeclaration
from sqlspec.core.parameters._processor import ParameterProcessor, type_coercion_fallbacks
from sqlspec.core.result import DMLResult, SQLResult
from sqlspec.driver import (
    AsyncCacheHitExecutor,
    BaseAsyncExceptionHandler,
    BaseSyncExceptionHandler,
    ExecutionResult,
    SyncCacheHitExecutor,
    cache_hit_hooks_unchanged,
)
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._common import _callable_cache_key, _check_declared_parameters, run_cached_execution
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.observability import LoggingConfig, ObservabilityRuntime, StatementEvent


def _make_cached(
    compiled_sql: str = "SELECT 1",
    param_count: int = 0,
    operation_type: OperationType = "SELECT",
    operation_profile: OperationProfile | None = None,
    parameter_profile: ParameterProfile | None = None,
    processed_state: ProcessedState | None = None,
    input_named_parameters: tuple[str, ...] = (),
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
        input_named_parameters=input_named_parameters,
        applied_wrap_types=False,
        parameter_casts={},
        operation_type=operation_type,
        operation_profile=operation_profile,
        param_count=param_count,
        processed_state=processed_state,
    )


def test_sync_execute_cache_hit_uses_fast_path(sqlite_sync_driver, monkeypatch) -> None:
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

    result = sqlite_sync_driver._execute_cache_hit("INSERT INTO t (id) VALUES (?)", (1,), cached)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 1


def test_execute_uses_fast_path_when_eligible(sqlite_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(driver: Any, statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr("sqlspec.driver._sync.run_cached_execution", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    result = sqlite_sync_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


def test_execute_uses_fast_path_with_dict_payload(sqlite_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(driver: Any, statement: str, params: tuple[Any, ...] | list[Any] | dict[str, Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr("sqlspec.driver._sync.run_cached_execution", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    result = sqlite_sync_driver.execute("SELECT :id", {"id": 1})

    assert result is sentinel
    assert called["args"] == ("SELECT :id", {"id": 1})


def test_execute_uses_fast_path_with_kwargs_payload(sqlite_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(driver: Any, statement: str, params: tuple[Any, ...] | list[Any] | dict[str, Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr("sqlspec.driver._sync.run_cached_execution", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    result = sqlite_sync_driver.execute("SELECT :id", id=1)

    assert result is sentinel
    assert called["args"] == ("SELECT :id", {"id": 1})


def test_execute_skips_fast_path_with_statement_config_override(sqlite_sync_driver, monkeypatch) -> None:
    called = False

    def _fake_try(driver: Any, statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr("sqlspec.driver._sync.run_cached_execution", _fake_try)
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


def test_execute_cache_hit_rebinds_type_coercion_subclass(sqlite_sync_driver: Any) -> None:
    import json
    from collections import defaultdict

    sqlite_sync_driver._stmt_cache_enabled = True
    sqlite_sync_driver.execute("CREATE TABLE t (data TEXT)")

    statement = "INSERT INTO t (data) VALUES (?)"
    sqlite_sync_driver.execute(statement, (defaultdict(int, a=1),))
    sqlite_sync_driver.execute(statement, (defaultdict(int, b=2),))

    rows = sqlite_sync_driver.connection.execute("SELECT data FROM t ORDER BY data").fetchall()
    assert [json.loads(row[0]) for row in rows] == [{"a": 1}, {"b": 2}]


def test_prepare_statement_sql_object_cache_is_bounded(sqlite_sync_driver: Any) -> None:
    sqlite_sync_driver._stmt_cache_max_size = 2

    first = sqlite_sync_driver.prepare_statement("SELECT 1")
    second = sqlite_sync_driver.prepare_statement("SELECT 2")
    third = sqlite_sync_driver.prepare_statement("SELECT 3")

    assert first.raw_sql == "SELECT 1"
    assert second.raw_sql == "SELECT 2"
    assert third.raw_sql == "SELECT 3"
    assert list(sqlite_sync_driver._statement_cache) == ["SELECT 2", "SELECT 3"]


def test_cache_statement_skips_clone_when_raw_sql_already_cached(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    statement = SQL("SELECT ?", 1, statement_config=sqlite_sync_driver.statement_config)
    sqlite_sync_driver._compiled_statement(statement, sqlite_sync_driver.statement_config)

    assert sqlite_sync_driver._stmt_cache.get("SELECT ?") is not None

    monkeypatch.setattr(
        "sqlspec.driver._common._clone_processed_state",
        lambda *_args, **_kwargs: pytest.fail("duplicate store should not clone processed state"),
    )

    sqlite_sync_driver._cache_statement(statement)


def test_stmt_cache_rebind_reuses_driver_owned_processor(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    parameter_profile = ParameterProfile([
        ParameterInfo("id", ParameterStyle.NAMED_COLON, position=31, ordinal=0, placeholder_text=":id")
    ])
    cached = _make_cached(
        compiled_sql="SELECT * FROM users WHERE id = ?",
        param_count=1,
        parameter_profile=parameter_profile,
        input_named_parameters=("id",),
    )
    processor = sqlite_sync_driver._stmt_cache_rebind_processor
    calls: list[object] = []
    original_transform = ParameterProcessor.transform_cached_parameters

    def wrapped_transform(self: ParameterProcessor, *args: Any, **kwargs: Any) -> Any:
        calls.append(self)
        return original_transform(self, *args, **kwargs)

    monkeypatch.setattr(ParameterProcessor, "transform_cached_parameters", wrapped_transform)

    sqlite_sync_driver.stmt_cache_rebind({"id": 1}, cached)
    sqlite_sync_driver.stmt_cache_rebind({"id": 2}, cached)

    assert calls == [processor, processor]
    assert sqlite_sync_driver._stmt_cache_rebind_processor is processor


def test_compilation_cache_hit_skips_compile_and_cache_statement(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    clear_all_caches()
    statement = SQL("SELECT :id", id=2, statement_config=sqlite_sync_driver.statement_config)
    cached_statement = CachedStatement(compiled_sql="SELECT :id", parameters={"id": 1}, expression=statement.expression)
    dialect_key = str(statement.dialect) if statement.dialect else None
    get_cache().put_statement("cache-key", cached_statement, dialect_key)

    monkeypatch.setattr(sqlite_sync_driver, "_compile_cache_key", lambda *_args, **_kwargs: "cache-key")
    monkeypatch.setattr(SQL, "compile", lambda *_args, **_kwargs: pytest.fail("cache hit should not compile"))
    monkeypatch.setattr(
        sqlite_sync_driver,
        "_cache_statement",
        lambda *_args, **_kwargs: pytest.fail("cache hit should not rewrite the statement cache"),
    )

    compiled, prepared = sqlite_sync_driver._compiled_statement(
        statement, sqlite_sync_driver.statement_config, flatten_single_parameters=False
    )

    assert compiled.compiled_sql == "SELECT :id"
    assert prepared == {"id": 2}


def test_compilation_cache_hit_rebinds_dynamic_named_parameters(sqlite_sync_driver: Any) -> None:
    from sqlspec import SQLFileLoader
    from sqlspec.adapters.asyncpg.core import default_statement_config as asyncpg_statement_config

    clear_all_caches()
    loader = SQLFileLoader()
    loader.add_named_sql(
        "get-workspace-member",
        "SELECT * FROM workspace_members AS wm WHERE wm.member_id = :member_id",
        dialect="postgres",
    )

    first = sqlite_sync_driver.prepare_statement(
        loader.get_sql("get-workspace-member").where("wm.account_id = :account_id"),
        statement_config=asyncpg_statement_config,
        kwargs={"member_id": 1, "account_id": 10},
    )
    first_compiled, first_prepared = sqlite_sync_driver._compiled_statement(
        first, asyncpg_statement_config, flatten_single_parameters=False
    )

    second = sqlite_sync_driver.prepare_statement(
        loader.get_sql("get-workspace-member").where("wm.account_id = :account_id"),
        statement_config=asyncpg_statement_config,
        kwargs={"member_id": 2, "account_id": 20},
    )
    second_compiled, second_prepared = sqlite_sync_driver._compiled_statement(
        second, asyncpg_statement_config, flatten_single_parameters=False
    )

    assert first_compiled.compiled_sql == second_compiled.compiled_sql
    assert first_prepared == (1, 10)
    assert second_prepared == (2, 20)


def test_compilation_cache_miss_maps_dynamic_named_parameters_by_placeholder_order(sqlite_sync_driver: Any) -> None:
    from sqlspec import SQLFileLoader
    from sqlspec.adapters.asyncpg.core import default_statement_config as asyncpg_statement_config

    clear_all_caches()
    loader = SQLFileLoader()
    loader.add_named_sql(
        "get-workspace-member",
        "SELECT * FROM workspace_members AS wm WHERE wm.member_id = :member_id",
        dialect="postgres",
    )

    statement = sqlite_sync_driver.prepare_statement(
        loader.get_sql("get-workspace-member").where("wm.account_id = :account_id"),
        statement_config=asyncpg_statement_config,
        kwargs={"account_id": 10, "member_id": 1},
    )
    compiled, prepared = sqlite_sync_driver._compiled_statement(
        statement, asyncpg_statement_config, flatten_single_parameters=False
    )

    assert compiled.compiled_sql.endswith("WHERE wm.member_id = $1 AND wm.account_id = $2")
    assert prepared == (1, 10)


def test_sync_execute_cached_statement_re_raises_mapped_exception(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    import sqlite3

    def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr(sqlite_sync_driver, "dispatch_execute", _fake_dispatch_execute)
    statement = SQL("SELECT ?", (1,), statement_config=sqlite_sync_driver.statement_config)
    statement.compile()

    with pytest.raises(SQLSpecError, match="SQLite operational error: boom"):
        sqlite_sync_driver._execute_cached_statement(statement)


def test_sync_execute_cache_hit_re_raises_mapped_exception(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
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

    with pytest.raises(SQLSpecError, match="SQLite operational error: boom"):
        sqlite_sync_driver._execute_cache_hit("INSERT INTO t (id) VALUES (?)", (1,), cached)


def test_cached_execution_named_style_sets_needs_rebind(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
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

    def _fake_rebind(params: tuple[Any, ...] | list[Any], cached_query: CachedQuery) -> dict[str, Any]:
        nonlocal called
        called = True
        assert params == (1,)
        assert cached_query is cached
        return {"id": params[0]}

    monkeypatch.setattr(sqlite_sync_driver, "stmt_cache_rebind", _fake_rebind)

    result = run_cached_execution(sqlite_sync_driver, "SELECT :id", (1,))

    assert result is not None
    assert called is True


@pytest.mark.anyio
async def test_async_execute_uses_fast_path_when_eligible(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    async def _fake_try(driver: Any, statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr("sqlspec.driver._async.run_cached_execution", _fake_try)
    aiosqlite_async_driver._stmt_cache_enabled = True

    result = await aiosqlite_async_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


@pytest.mark.anyio
async def test_async_execute_uses_fast_path_with_kwargs_payload(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    async def _fake_try(driver: Any, statement: str, params: tuple[Any, ...] | list[Any] | dict[str, Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr("sqlspec.driver._async.run_cached_execution", _fake_try)
    aiosqlite_async_driver._stmt_cache_enabled = True

    result = await aiosqlite_async_driver.execute("SELECT :id", id=1)

    assert result is sentinel
    assert called["args"] == ("SELECT :id", {"id": 1})


@pytest.mark.anyio
async def test_async_execute_skips_fast_path_with_statement_config_override(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    called = False

    async def _fake_try(driver: Any, statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr("sqlspec.driver._async.run_cached_execution", _fake_try)
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
async def test_async_execute_cache_hit_uses_cursor_fast_path(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    """Async direct cache execution should bypass adapter dispatch when cursor.execute is awaitable."""
    await aiosqlite_async_driver.execute("CREATE TABLE t (id INTEGER)")

    async def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        pytest.fail("dispatch_execute should not be called on async direct fast path")

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

    result = await aiosqlite_async_driver._execute_cache_hit("INSERT INTO t (id) VALUES (?)", (1,), cached)

    assert result.operation_type == "INSERT"
    assert result.rows_affected == 1


@pytest.fixture
def cached_sync_cursor(sqlite_sync_driver: Any, monkeypatch: Any) -> Generator[Any, None, None]:
    raw_cursor = sqlite_sync_driver.connection.cursor()
    cursor = Mock(spec=["execute", "fetchall", "rowcount", "description"])
    cursor.rowcount = 0

    def execute(sql: str, params: Any = ()) -> None:
        raw_cursor.execute(sql, params)
        cursor.description = raw_cursor.description
        cursor.rowcount = raw_cursor.rowcount

    cursor.execute = Mock(side_effect=execute)
    cursor.fetchall = raw_cursor.fetchall

    @contextmanager
    def with_cursor(_connection: Any) -> Generator[Any, None, None]:
        yield cursor

    dispatch = sqlite_sync_driver.dispatch_execute
    fallback = Mock(side_effect=lambda _cursor, statement: dispatch(raw_cursor, statement))
    monkeypatch.setattr(sqlite_sync_driver, "with_cursor", with_cursor)
    monkeypatch.setattr(sqlite_sync_driver, "dispatch_execute", fallback)
    statements: list[str] = []
    sqlite_sync_driver.connection.set_trace_callback(statements.append)
    try:
        yield sqlite_sync_driver, cursor, statements, fallback
    finally:
        sqlite_sync_driver.connection.set_trace_callback(None)
        raw_cursor.close()


@pytest.mark.parametrize("error_type", [AttributeError, NotImplementedError])
@pytest.mark.parametrize("stage", ["execute", "fetchall", "collect_rows", "resolve_rowcount", "build_statement_result"])
def test_sync_cache_hit_does_not_retry_after_execute(
    cached_sync_cursor: Any, monkeypatch: Any, error_type: type[Exception], stage: str
) -> None:
    driver, cursor, statements, fallback = cached_sync_cursor
    error = error_type("failure after SQL execution")
    execute = cursor.execute

    def execute_then_error(sql: str, params: Any) -> None:
        execute(sql, params)
        if stage == "execute":
            raise error

    cursor.execute = execute_then_error
    if stage == "fetchall":
        cursor.fetchall = Mock(side_effect=error)
    elif stage == "build_statement_result":
        monkeypatch.setattr(driver, "build_statement_result", Mock(side_effect=error))
        monkeypatch.setattr(driver, "build_cached_select_result", Mock(side_effect=error))
    elif stage != "execute":
        monkeypatch.setattr(driver, stage, Mock(side_effect=error))
    returns_rows = stage not in {"execute", "resolve_rowcount"}
    sql = "SELECT ? AS value" if returns_rows else "INSERT INTO users (name) VALUES (?)"
    cached = _make_cached(
        compiled_sql=sql,
        operation_type="SELECT" if returns_rows else "INSERT",
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
    )
    with pytest.raises(error_type) as exc_info:
        SyncDriverAdapterBase._execute_cache_hit(driver, sql, ("once",), cached)

    assert exc_info.value is error
    assert len([statement for statement in statements if statement.startswith(("SELECT", "INSERT"))]) == 1
    fallback.assert_not_called()


def test_sync_cache_hit_insert_executes_once(cached_sync_cursor: Any) -> None:
    driver, cursor, statements, fallback = cached_sync_cursor
    sql = "INSERT INTO users (name) VALUES (?)"
    cached = _make_cached(
        compiled_sql=sql,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
    )
    result = SyncDriverAdapterBase._execute_cache_hit(driver, sql, ("once",), cached)

    assert result.rows_affected == 1
    assert len([statement for statement in statements if statement.startswith("INSERT")]) == 1
    fallback.assert_not_called()
    cursor.execute("SELECT COUNT(*) FROM users WHERE name = ?", ("once",))
    assert cursor.fetchall() == [(1,)]


@pytest.mark.parametrize("row_format", ["tuple", "dict", "record"])
def test_sync_cache_hit_preserves_row_format(cached_sync_cursor: Any, row_format: str) -> None:
    driver, cursor, statements, fallback = cached_sync_cursor
    fetchall = cursor.fetchall

    def fetch_rows() -> list[Any]:
        rows: list[Any] = fetchall()
        if row_format == "tuple":
            return rows
        mappings = [{"name": row[1], "id": row[0]} for row in rows]
        return [UserDict(row) for row in mappings] if row_format == "record" else mappings

    cursor.fetchall = Mock(side_effect=fetch_rows)
    sql = "SELECT id, name FROM users ORDER BY id"
    result = SyncDriverAdapterBase._execute_cache_hit(driver, sql, (), _make_cached(compiled_sql=sql))

    assert result.get_data() == [{"id": 1, "name": "test"}, {"id": 2, "name": "example"}]
    assert result.column_names == ["id", "name"]
    assert statements == [sql]
    cursor.fetchall.assert_called_once_with()
    fallback.assert_not_called()


@pytest.mark.parametrize("missing_capability", ["execute", "fetchall", "rowcount"])
def test_sync_cache_hit_missing_cursor_capability_dispatches_once(
    cached_sync_cursor: Any, missing_capability: str
) -> None:
    driver, cursor, statements, fallback = cached_sync_cursor
    execute = cursor.execute
    delattr(cursor, missing_capability)
    returns_rows = missing_capability != "rowcount"
    sql = "SELECT ? AS value" if returns_rows else "INSERT INTO users (name) VALUES (?)"
    cached = _make_cached(
        compiled_sql=sql,
        operation_type="SELECT" if returns_rows else "INSERT",
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
    )
    result = SyncDriverAdapterBase._execute_cache_hit(driver, sql, ("once",), cached)

    assert len([statement for statement in statements if statement.startswith(("SELECT", "INSERT"))]) == 1
    fallback.assert_called_once()
    execute.assert_not_called()
    if returns_rows:
        assert result.get_data() == [{"value": "once"}]
    else:
        assert result.rows_affected == 1


@pytest.fixture
def cached_async_cursor(
    sqlite_sync_driver: Any, aiosqlite_async_driver: Any, monkeypatch: Any
) -> Generator[tuple[Any, Any, list[str], AsyncMock], None, None]:
    raw_cursor = sqlite_sync_driver.connection.cursor()
    cursor = Mock(spec=["execute", "fetchall", "rowcount", "description"])
    cursor.rowcount = 0

    def execute(sql: str, params: Any = ()) -> None:
        raw_cursor.execute(sql, params)
        cursor.description = raw_cursor.description
        cursor.rowcount = raw_cursor.rowcount

    cursor.execute = Mock(side_effect=execute)
    cursor.fetchall = raw_cursor.fetchall

    @asynccontextmanager
    async def with_cursor(_connection: Any) -> AsyncGenerator[Any, None]:
        yield cursor

    async def dispatch(_cursor: Any, statement: SQL) -> Any:
        return sqlite_sync_driver.dispatch_execute(raw_cursor, statement)

    fallback = AsyncMock(side_effect=dispatch)
    monkeypatch.setattr(aiosqlite_async_driver, "with_cursor", with_cursor)
    monkeypatch.setattr(aiosqlite_async_driver, "dispatch_execute", fallback)
    statements: list[str] = []
    sqlite_sync_driver.connection.set_trace_callback(statements.append)
    try:
        yield aiosqlite_async_driver, cursor, statements, fallback
    finally:
        sqlite_sync_driver.connection.set_trace_callback(None)
        raw_cursor.close()


@pytest.mark.anyio
@pytest.mark.parametrize("returns_rows", [True, False], ids=["select", "insert"])
@pytest.mark.parametrize("awaitable_result", [False, True], ids=["sync", "awaitable-wrapper"])
async def test_async_cache_hit_executes_once(
    cached_async_cursor: Any, returns_rows: bool, awaitable_result: bool
) -> None:
    driver, cursor, statements, fallback = cached_async_cursor
    if awaitable_result:
        execute = cursor.execute

        async def execute_async(sql: str, params: Any) -> None:
            execute(sql, params)

        cursor.execute = lambda sql, params: execute_async(sql, params)
        if returns_rows:
            fetchall = cursor.fetchall

            async def fetchall_async() -> Any:
                return fetchall()

            cursor.fetchall = fetchall_async

    sql = "SELECT ? AS value" if returns_rows else "INSERT INTO users (name) VALUES (?)"
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type="SELECT" if returns_rows else "INSERT",
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
    )
    result = await AsyncDriverAdapterBase._execute_cache_hit_with_cursor(driver, sql, ("once",), cached)

    executed = [statement for statement in statements if statement.startswith(("SELECT", "INSERT"))]
    assert len(executed) == 1
    fallback.assert_not_called()
    if returns_rows:
        assert result.get_data() == [{"value": "once"}]
    else:
        assert result.rows_affected == 1
        if awaitable_result:
            await cursor.execute("SELECT COUNT(*) FROM users WHERE name = ?", ("once",))
        else:
            cursor.execute("SELECT COUNT(*) FROM users WHERE name = ?", ("once",))
        assert cursor.fetchall() == [(1,)]


@pytest.mark.anyio
@pytest.mark.parametrize("error_type", [AttributeError, NotImplementedError])
@pytest.mark.parametrize("stage", ["execute", "fetchall", "collect_rows", "resolve_rowcount", "build_statement_result"])
async def test_async_cache_hit_does_not_retry_after_execute(
    cached_async_cursor: Any, monkeypatch: Any, error_type: type[Exception], stage: str
) -> None:
    driver, cursor, statements, fallback = cached_async_cursor
    error = error_type("failure after SQL execution")
    execute = cursor.execute

    async def execute_async(sql: str, params: Any) -> None:
        execute(sql, params)
        if stage == "execute":
            raise error

    cursor.execute = execute_async
    if stage == "fetchall":
        cursor.fetchall = Mock(side_effect=error)
    elif stage == "build_statement_result":
        monkeypatch.setattr(driver, "build_statement_result", Mock(side_effect=error))
        monkeypatch.setattr(driver, "build_cached_select_result", Mock(side_effect=error))
    elif stage != "execute":
        monkeypatch.setattr(driver, stage, Mock(side_effect=error))

    returns_rows = stage not in {"execute", "resolve_rowcount"}
    sql = "SELECT ? AS value" if returns_rows else "INSERT INTO users (name) VALUES (?)"
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type="SELECT" if returns_rows else "INSERT",
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
    )
    with pytest.raises(error_type) as exc_info:
        await AsyncDriverAdapterBase._execute_cache_hit_with_cursor(driver, sql, ("once",), cached)

    assert exc_info.value is error
    assert len([statement for statement in statements if statement.startswith(("SELECT", "INSERT"))]) == 1
    fallback.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("row_format", ["tuple", "dict", "record"])
async def test_async_cache_hit_preserves_row_format(cached_async_cursor: Any, row_format: str) -> None:
    driver, cursor, statements, fallback = cached_async_cursor
    fetchall = cursor.fetchall

    def fetch_rows() -> list[Any]:
        rows: list[Any] = fetchall()
        if row_format == "tuple":
            return rows
        mappings = [{"name": row[1], "id": row[0]} for row in rows]
        return [UserDict(row) for row in mappings] if row_format == "record" else mappings

    cursor.fetchall = Mock(side_effect=fetch_rows)
    sql = "SELECT id, name FROM users ORDER BY id"
    result = await AsyncDriverAdapterBase._execute_cache_hit_with_cursor(
        driver, sql, (), _make_cached(compiled_sql=sql)
    )

    assert result.get_data() == [{"id": 1, "name": "test"}, {"id": 2, "name": "example"}]
    assert result.column_names == ["id", "name"]
    assert statements == [sql]
    cursor.fetchall.assert_called_once_with()
    fallback.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("returns_rows", [True, False], ids=["select", "insert"])
async def test_async_cache_hit_missing_cursor_capability_dispatches_once(
    cached_async_cursor: Any, returns_rows: bool
) -> None:
    driver, cursor, statements, fallback = cached_async_cursor
    delattr(cursor, "fetchall" if returns_rows else "rowcount")
    sql = "SELECT ? AS value" if returns_rows else "INSERT INTO users (name) VALUES (?)"
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type="SELECT" if returns_rows else "INSERT",
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
    )
    result = await AsyncDriverAdapterBase._execute_cache_hit_with_cursor(driver, sql, ("once",), cached)

    assert len([statement for statement in statements if statement.startswith(("SELECT", "INSERT"))]) == 1
    fallback.assert_awaited_once()
    cursor.execute.assert_not_called()
    if returns_rows:
        assert result.get_data() == [{"value": "once"}]
    else:
        assert result.rows_affected == 1


@pytest.mark.anyio
async def test_aiosqlite_cached_dml_does_not_read_proxy_metadata(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    await aiosqlite_async_driver.execute("CREATE TABLE worker_metadata (id INTEGER PRIMARY KEY)")

    class MetadataForbiddenCursor:
        async def execute(self, _sql: str, _params: tuple[Any, ...]) -> None:
            return None

        @property
        def rowcount(self) -> int:
            raise AssertionError("proxy rowcount read on event-loop thread")

        @property
        def lastrowid(self) -> int:
            raise AssertionError("proxy lastrowid read on event-loop thread")

    class MetadataForbiddenContext:
        async def __aenter__(self) -> MetadataForbiddenCursor:
            return MetadataForbiddenCursor()

        async def __aexit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(aiosqlite_async_driver, "with_cursor", lambda _connection: MetadataForbiddenContext())
    cached = _make_cached(
        compiled_sql="INSERT INTO worker_metadata (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO worker_metadata (id) VALUES (?)",
            execution_parameters=[1],
            parsed_expression=parse_one("INSERT INTO worker_metadata (id) VALUES (?)", read="sqlite"),
            operation_type="INSERT",
        ),
    )

    result = await aiosqlite_async_driver._execute_cache_hit(
        "INSERT INTO worker_metadata (id) VALUES (?)", (1,), cached
    )

    assert result.last_inserted_id == 1


@pytest.mark.anyio
async def test_async_execute_cached_statement_re_raises_mapped_exception(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    import aiosqlite

    async def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise aiosqlite.OperationalError("boom")

    monkeypatch.setattr(aiosqlite_async_driver, "dispatch_execute", _fake_dispatch_execute)
    statement = SQL("SELECT ?", (1,), statement_config=aiosqlite_async_driver.statement_config)
    statement.compile()

    with pytest.raises(SQLSpecError, match="AIOSQLite operational error: boom"):
        await aiosqlite_async_driver._execute_cached_statement(statement)


@pytest.mark.anyio
async def test_async_execute_cache_hit_re_raises_mapped_exception(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    import aiosqlite

    await aiosqlite_async_driver.execute("CREATE TABLE t (id INTEGER)")

    def fail_on_worker(*_args: object, **_kwargs: object) -> object:
        raise aiosqlite.OperationalError("boom")

    monkeypatch.setattr("sqlspec.adapters.aiosqlite.driver._execute_and_resolve_metadata", fail_on_worker)

    cached = _make_cached(
        compiled_sql="INSERT INTO t (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO t (id) VALUES (?)", execution_parameters=[1], operation_type="INSERT"
        ),
    )

    with pytest.raises(SQLSpecError, match="AIOSQLite operational error: boom"):
        await aiosqlite_async_driver._execute_cache_hit("INSERT INTO t (id) VALUES (?)", (1,), cached)


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


def test_compiled_statement_compiled_from_cache_uses_direct_attribute(sqlite_sync_driver: Any) -> None:
    """Processed cache-copy statements use direct _compiled_from_cache access."""
    statement = SQL("SELECT ?", (1,), statement_config=sqlite_sync_driver.statement_config)
    statement.compile()
    copied = statement.copy(parameters=(2,))
    assert copied._compiled_from_cache is True

    compiled, prepared = sqlite_sync_driver._compiled_statement(
        copied, sqlite_sync_driver.statement_config, flatten_single_parameters=False
    )

    assert compiled.compiled_sql == "SELECT ?"
    assert prepared == [2]


def test_compiled_statement_cache_direct_uses_direct_attribute(sqlite_sync_driver: Any) -> None:
    """Cache-direct statements use direct _is_cache_direct access."""
    processed = ProcessedState(compiled_sql="SELECT ?", execution_parameters=(1,), operation_type="SELECT")
    statement = SQL._create_cached_direct("SELECT ?", sqlite_sync_driver.statement_config, processed)
    assert statement._is_cache_direct is True

    compiled, prepared = sqlite_sync_driver._compiled_statement(
        statement, sqlite_sync_driver.statement_config, flatten_single_parameters=False
    )

    assert compiled.compiled_sql == "SELECT ?"
    assert prepared == (1,)


def test_pooled_statement_cleared_when_attached_to_sql_result(sqlite_sync_driver: Any) -> None:
    """build_statement_result clears _pooled so result.statement is not returned to the pool."""
    processed = ProcessedState(compiled_sql="SELECT 1", execution_parameters=[], operation_type="SELECT")
    pooled = SQL._create_cached_direct("SELECT 1", sqlite_sync_driver.statement_config, processed)
    assert pooled._pooled is True

    result = sqlite_sync_driver.build_statement_result(pooled, sqlite_sync_driver.create_execution_result(None))
    assert result.statement is pooled
    assert pooled._pooled is False

    sqlite_sync_driver._release_pooled_statement(pooled)
    assert result.statement.raw_sql == "SELECT 1"


def test_refresh_statement_cache_state_allows_none_observability(sqlite_sync_driver: Any) -> None:
    """_refresh_statement_cache_state enables cache when _observability is None."""
    original_obs = sqlite_sync_driver._observability
    try:
        sqlite_sync_driver._observability = None
        sqlite_sync_driver._refresh_statement_cache_state()
        assert sqlite_sync_driver._stmt_cache_enabled is True
    finally:
        sqlite_sync_driver._observability = original_obs
        sqlite_sync_driver._refresh_statement_cache_state()


def test_cache_statement_command_not_cached(sqlite_sync_driver: Any) -> None:
    """_cache_statement must refuse statements with undetermined COMMAND operation or missing AST."""
    sqlite_sync_driver._stmt_cache_enabled = True

    unparsed = SQL("SELECT ?::INTEGER AS i", 1, statement_config=sqlite_sync_driver.statement_config)
    unparsed._processed_state = ProcessedState(
        compiled_sql="SELECT ?::INTEGER AS i", execution_parameters=[1], parsed_expression=None, operation_type="SELECT"
    )
    sqlite_sync_driver._cache_statement(unparsed)
    assert sqlite_sync_driver._stmt_cache.get("SELECT ?::INTEGER AS i") is None

    command_stmt = SQL("VACUUM", statement_config=sqlite_sync_driver.statement_config)
    command_stmt.compile()
    sqlite_sync_driver._cache_statement(command_stmt)
    assert sqlite_sync_driver._stmt_cache.get("VACUUM") is None


def test_fast_path_shapes_sync(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    """Parameterless and scalar/varargs positional calls route through the statement cache while StatementFilter bypasses it."""
    sqlite_sync_driver._stmt_cache_enabled = True
    sqlite_sync_driver.execute("CREATE TABLE t_shapes (id INTEGER, label TEXT)")
    sqlite_sync_driver.execute("INSERT INTO t_shapes (id, label) VALUES (?, ?)", (1, "one"))

    cache_hits: list[str] = []
    original_hit = sqlite_sync_driver._cache_hit_executor

    def tracked_hit(connection: Any, sql: str, params: Any, cached: CachedQuery, config: Any, state: Any) -> Any:
        cache_hits.append(sql)
        return original_hit(connection, sql, params, cached, config, state)

    monkeypatch.setattr(sqlite_sync_driver, "_cache_hit_executor", tracked_hit)

    r1_no_param = sqlite_sync_driver.execute("SELECT 1 AS v")
    r2_no_param = sqlite_sync_driver.execute("SELECT 1 AS v")
    assert r1_no_param.get_data() == r2_no_param.get_data() == [{"v": 1}]
    assert "SELECT 1 AS v" in cache_hits

    cache_hits.clear()
    assert sqlite_sync_driver.select_value("SELECT ? AS v", 42) == 42
    assert sqlite_sync_driver.select_value("SELECT ? AS v", 99) == 99
    assert "SELECT ? AS v" in cache_hits

    cache_hits.clear()
    assert sqlite_sync_driver.select_value("SELECT ? AS s", "hello") == "hello"
    assert sqlite_sync_driver.select_value("SELECT ? AS s", "world") == "world"
    assert "SELECT ? AS s" in cache_hits

    cache_hits.clear()
    assert sqlite_sync_driver.select_value("SELECT ? AS b", b"bytes") == b"bytes"
    assert sqlite_sync_driver.select_value("SELECT ? AS b", b"more") == b"more"
    assert "SELECT ? AS b" in cache_hits

    cache_hits.clear()
    assert sqlite_sync_driver.select_value("SELECT ? AS n", None) is None
    assert sqlite_sync_driver.select_value("SELECT ? AS n", None) is None
    assert "SELECT ? AS n" in cache_hits

    cache_hits.clear()
    assert sqlite_sync_driver.select_one("SELECT ? AS a, ? AS b", 1, "two") == {"a": 1, "b": "two"}
    assert sqlite_sync_driver.select_one("SELECT ? AS a, ? AS b", 3, "four") == {"a": 3, "b": "four"}
    assert "SELECT ? AS a, ? AS b" in cache_hits

    cache_hits.clear()
    r1_filtered = sqlite_sync_driver.execute("SELECT id, label FROM t_shapes", LimitOffsetFilter(10, 0))
    r2_filtered = sqlite_sync_driver.execute("SELECT id, label FROM t_shapes", LimitOffsetFilter(10, 0))
    assert r1_filtered.get_data() == r2_filtered.get_data() == [{"id": 1, "label": "one"}]
    assert cache_hits == []


@pytest.mark.anyio
async def test_fast_path_shapes_async(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    """Async parameterless and scalar/varargs positional calls route through the statement cache while StatementFilter bypasses it."""
    aiosqlite_async_driver._stmt_cache_enabled = True
    await aiosqlite_async_driver.execute("CREATE TABLE t_shapes (id INTEGER, label TEXT)")
    await aiosqlite_async_driver.execute("INSERT INTO t_shapes (id, label) VALUES (?, ?)", (1, "one"))

    cache_hits: list[str] = []
    original_hit = aiosqlite_async_driver._cache_hit_executor

    async def tracked_hit(connection: Any, sql: str, params: Any, cached: CachedQuery, config: Any, state: Any) -> Any:
        cache_hits.append(sql)
        return await original_hit(connection, sql, params, cached, config, state)

    monkeypatch.setattr(aiosqlite_async_driver, "_cache_hit_executor", tracked_hit)

    r1_no_param = await aiosqlite_async_driver.execute("SELECT 1 AS v")
    r2_no_param = await aiosqlite_async_driver.execute("SELECT 1 AS v")
    assert r1_no_param.get_data() == r2_no_param.get_data() == [{"v": 1}]
    assert "SELECT 1 AS v" in cache_hits

    cache_hits.clear()
    assert await aiosqlite_async_driver.select_value("SELECT ? AS v", 42) == 42
    assert await aiosqlite_async_driver.select_value("SELECT ? AS v", 99) == 99
    assert "SELECT ? AS v" in cache_hits

    cache_hits.clear()
    assert await aiosqlite_async_driver.select_value("SELECT ? AS s", "hello") == "hello"
    assert await aiosqlite_async_driver.select_value("SELECT ? AS s", "world") == "world"
    assert "SELECT ? AS s" in cache_hits

    cache_hits.clear()
    assert await aiosqlite_async_driver.select_value("SELECT ? AS b", b"bytes") == b"bytes"
    assert await aiosqlite_async_driver.select_value("SELECT ? AS b", b"more") == b"more"
    assert "SELECT ? AS b" in cache_hits

    cache_hits.clear()
    assert await aiosqlite_async_driver.select_value("SELECT ? AS n", None) is None
    assert await aiosqlite_async_driver.select_value("SELECT ? AS n", None) is None
    assert "SELECT ? AS n" in cache_hits

    cache_hits.clear()
    assert await aiosqlite_async_driver.select_one("SELECT ? AS a, ? AS b", 1, "two") == {"a": 1, "b": "two"}
    assert await aiosqlite_async_driver.select_one("SELECT ? AS a, ? AS b", 3, "four") == {"a": 3, "b": "four"}
    assert "SELECT ? AS a, ? AS b" in cache_hits

    cache_hits.clear()
    r1_filtered = await aiosqlite_async_driver.execute("SELECT id, label FROM t_shapes", LimitOffsetFilter(10, 0))
    r2_filtered = await aiosqlite_async_driver.execute("SELECT id, label FROM t_shapes", LimitOffsetFilter(10, 0))
    assert r1_filtered.get_data() == r2_filtered.get_data() == [{"id": 1, "label": "one"}]
    assert cache_hits == []


def test_statement_cache_shared_across_sessions(tmp_path: Any, monkeypatch: Any) -> None:
    """Sessions opened from the same database config share StatementCacheHolder, QueryCache, and rebind processor."""
    db_file = tmp_path / "shared_cache.db"
    spec = SQLSpec()
    config = SqliteConfig(connection_config={"database": str(db_file)})
    spec.add_config(config)

    try:
        with spec.provide_session(config) as session1:
            session1.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            session1.execute("INSERT INTO items (id, name) VALUES (?, ?)", (1, "alpha"))
            session1.execute("INSERT INTO items (id, name) VALUES (?, ?)", (2, "beta"))
            r1 = session1.select_one("SELECT id, name FROM items WHERE id = ?", 1)
            assert r1 == {"id": 1, "name": "alpha"}
            cache1 = session1._stmt_cache
            processor1 = session1._stmt_cache_rebind_processor
            holder1 = session1._cache_holder

        with spec.provide_session(config) as session2:
            assert session2._stmt_cache is cache1
            assert session2._stmt_cache_rebind_processor is processor1
            assert session2._cache_holder is holder1

            hit_calls: list[str] = []
            original_hit = cast("Any", session2._cache_hit_executor)

            def tracked_hit(
                connection: Any, sql: str, params: Any, cached: CachedQuery, config: Any, state: Any
            ) -> Any:
                hit_calls.append(sql)
                return original_hit(connection, sql, params, cached, config, state)

            monkeypatch.setattr(session2, "_cache_hit_executor", tracked_hit)
            r2 = session2.select_one("SELECT id, name FROM items WHERE id = ?", 2)
            assert r2 == {"id": 2, "name": "beta"}
            assert hit_calls == ["SELECT id, name FROM items WHERE id = ?"]
    finally:
        config.close_pool()


def test_statement_cache_shared_across_sessions_isolated_and_thread_safe(tmp_path: Any) -> None:
    """Different configs have isolated StatementCacheHolders, standalone drivers work, and concurrent access is thread-safe."""
    spec = SQLSpec()
    cfg_a = SqliteConfig(connection_config={"database": str(tmp_path / "a.db")})
    cfg_b = SqliteConfig(connection_config={"database": str(tmp_path / "b.db")})
    spec.add_config(cfg_a)
    spec.add_config(cfg_b)

    try:
        with spec.provide_session(cfg_a) as s_a, spec.provide_session(cfg_b) as s_b:
            assert s_a._cache_holder is not s_b._cache_holder
            assert s_a._stmt_cache is not s_b._stmt_cache
            s_a.execute("SELECT 1 AS a")
            assert s_b._stmt_cache.get("SELECT 1 AS a") is None

            errors: list[Exception] = []
            holder = s_a._cache_holder

            def worker(tid: int) -> None:
                try:
                    for idx in range(50):
                        sql = f"SELECT {idx % 16} AS v"
                        stmt = SQL(sql, statement_config=s_a.statement_config)
                        holder.set_statement(sql, stmt)
                        _ = holder.get_statement(sql)
                        holder.query_cache.set(sql, _make_cached(compiled_sql=sql))
                        _ = holder.query_cache.get(sql)
                        holder.set_row_count_hint(sql, tid + idx)
                        _ = holder.get_row_count_hint(sql)
                        holder.set_bulk_insert_sql(sql, ("t", "c"))
                        _ = holder.get_bulk_insert_sql(sql)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert errors == []
    finally:
        cfg_a.close_pool()
        cfg_b.close_pool()

    raw_conn = sqlite3.connect(":memory:")
    try:
        standalone = SqliteDriver(connection=raw_conn)
        assert standalone._cache_holder is not None
        assert standalone.select_value("SELECT ? AS v", 7) == 7
        assert standalone._stmt_cache.get("SELECT ? AS v") is not None
    finally:
        raw_conn.close()


def test_fast_path_with_observability_sync(tmp_path: Any, monkeypatch: Any) -> None:
    """Fast path stays enabled with observability active and emits matching events and error hooks."""
    events: list[StatementEvent] = []
    starts: list[dict[str, Any]] = []
    completes: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    obs = ObservabilityConfig(
        statement_observers=(events.append,),
        lifecycle=cast(
            "LifecycleConfig",
            {"on_query_start": [starts.append], "on_query_complete": [completes.append], "on_error": [errors.append]},
        ),
        logging=LoggingConfig(include_sql_hash=True),
    )
    spec = SQLSpec()
    config = SqliteConfig(connection_config={"database": str(tmp_path / "obs_sync.db")}, observability_config=obs)
    spec.add_config(config)

    try:
        with spec.provide_session(config) as session:
            assert session._stmt_cache_enabled is True
            session.execute("CREATE TABLE obs_items (id INTEGER PRIMARY KEY, label TEXT)")

            events.clear()
            starts.clear()
            completes.clear()
            errors.clear()

            hit_calls: list[str] = []
            original_hit = type(session)._execute_cache_hit

            def tracked_hit(self_driver: Any, sql: str, params: Any, cached: CachedQuery) -> Any:
                hit_calls.append(sql)
                return original_hit(self_driver, sql, params, cached)

            monkeypatch.setattr(type(session), "_execute_cache_hit", tracked_hit)

            insert_sql = "INSERT INTO obs_items (id, label) VALUES (?, ?)"
            session.execute(insert_sql, (1, "alpha"))
            session.execute(insert_sql, (2, "beta"))
            assert hit_calls == [insert_sql]
            assert len(events) == 2
            assert len(starts) == 2
            assert len(completes) == 2

            ev1, ev2 = events[0], events[1]
            assert ev1.sql == ev2.sql == insert_sql
            assert ev1.driver == ev2.driver == "SqliteDriver"
            assert ev1.operation == ev2.operation == "INSERT"
            assert ev1.execution_mode == ev2.execution_mode
            assert ev1.is_many == ev2.is_many is False
            assert ev1.is_script == ev2.is_script is False
            assert ev1.rows_affected == ev2.rows_affected == 1
            assert ev1.sql_hash is not None
            assert ev1.sql_hash == ev2.sql_hash
            assert type(ev1.parameters) is type(ev2.parameters)
            assert starts[0]["sql"] == starts[1]["sql"] == insert_sql
            assert starts[0]["operation"] == starts[1]["operation"] == "INSERT"
            assert completes[0]["rows_affected"] == completes[1]["rows_affected"] == 1

            events.clear()
            starts.clear()
            completes.clear()
            hit_calls.clear()

            select_sql = "SELECT id, label FROM obs_items WHERE id = ?"
            r1 = session.select_one(select_sql, (1,))
            r2 = session.select_one(select_sql, (1,))
            assert r1 == r2 == {"id": 1, "label": "alpha"}
            assert hit_calls == [select_sql]
            assert len(events) == 2
            s_ev1, s_ev2 = events[0], events[1]
            assert s_ev1.sql == s_ev2.sql == select_sql
            assert s_ev1.parameters == s_ev2.parameters == [1]
            assert s_ev1.driver == s_ev2.driver == "SqliteDriver"
            assert s_ev1.operation == s_ev2.operation == "SELECT"
            assert s_ev1.execution_mode == s_ev2.execution_mode
            assert s_ev1.is_many == s_ev2.is_many is False
            assert s_ev1.is_script == s_ev2.is_script is False
            assert s_ev1.rows_affected == s_ev2.rows_affected == 1
            assert s_ev1.sql_hash == s_ev2.sql_hash
            assert starts[0] == starts[1]
            assert completes[0] == completes[1]

            with pytest.raises(SQLSpecError):
                session.execute(insert_sql, (1, "duplicate"))
            assert len(errors) == 1
            assert isinstance(errors[0]["exception"], SQLSpecError)
            assert errors[0]["sql"] == insert_sql

            transformed_cfg = session.statement_config.replace(
                statement_transformers=((lambda expr, ctx: (expr, ctx)),)
            )
            transformed_drv = SqliteDriver(connection=session.connection, statement_config=transformed_cfg)
            assert transformed_drv._stmt_cache_enabled is False
    finally:
        config.close_pool()


@pytest.mark.anyio
async def test_fast_path_with_observability_async(tmp_path: Any, monkeypatch: Any) -> None:
    """Async fast path stays enabled with observability active and emits matching events and error hooks."""
    events: list[StatementEvent] = []
    starts: list[dict[str, Any]] = []
    completes: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    obs = ObservabilityConfig(
        statement_observers=(events.append,),
        lifecycle=cast(
            "LifecycleConfig",
            {"on_query_start": [starts.append], "on_query_complete": [completes.append], "on_error": [errors.append]},
        ),
        logging=LoggingConfig(include_sql_hash=True),
    )
    spec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "obs_async.db")}, observability_config=obs)
    spec.add_config(config)

    try:
        async with spec.provide_session(config) as session:
            assert session._stmt_cache_enabled is True
            await session.execute("CREATE TABLE obs_items (id INTEGER PRIMARY KEY, label TEXT)")

            events.clear()
            starts.clear()
            completes.clear()
            errors.clear()

            hit_calls: list[str] = []
            original_hit = type(session)._execute_cache_hit

            async def tracked_hit(self_driver: Any, sql: str, params: Any, cached: CachedQuery) -> Any:
                hit_calls.append(sql)
                return await original_hit(self_driver, sql, params, cached)

            monkeypatch.setattr(type(session), "_execute_cache_hit", tracked_hit)

            insert_sql = "INSERT INTO obs_items (id, label) VALUES (?, ?)"
            await session.execute(insert_sql, (1, "alpha"))
            await session.execute(insert_sql, (2, "beta"))
            assert hit_calls == [insert_sql]
            assert len(events) == 2

            select_sql = "SELECT id, label FROM obs_items WHERE id = ?"
            events.clear()
            starts.clear()
            completes.clear()
            hit_calls.clear()

            r1 = await session.select_one(select_sql, (1,))
            r2 = await session.select_one(select_sql, (1,))
            assert r1 == r2 == {"id": 1, "label": "alpha"}
            assert hit_calls == [select_sql]
            assert len(events) == 2
            s_ev1, s_ev2 = events[0], events[1]
            assert s_ev1.sql == s_ev2.sql == select_sql
            assert s_ev1.parameters == s_ev2.parameters == [1]
            assert s_ev1.driver == s_ev2.driver == "AiosqliteDriver"
            assert s_ev1.operation == s_ev2.operation == "SELECT"
            assert s_ev1.rows_affected == s_ev2.rows_affected == 1
            assert s_ev1.sql_hash == s_ev2.sql_hash
            assert starts[0] == starts[1]
            assert completes[0] == completes[1]

            with pytest.raises(SQLSpecError):
                await session.execute(insert_sql, (1, "duplicate"))
            assert len(errors) == 1
            assert isinstance(errors[0]["exception"], SQLSpecError)
            assert errors[0]["sql"] == insert_sql
    finally:
        await config.close_pool()


def test_fast_path_select_lazy_statement_materialization_sync(sqlite_sync_driver: Any) -> None:
    sqlite_sync_driver._stmt_cache_enabled = True
    sql = "SELECT ? AS val"

    warm_result = sqlite_sync_driver.execute(sql, (0,))
    assert warm_result.one() == {"val": 0}
    assert warm_result._statement is not None

    r1 = sqlite_sync_driver.execute(sql, (1,))
    r2 = sqlite_sync_driver.execute(sql, (2,))
    assert r1._statement is None
    assert r2._statement is None
    assert r1.one() == {"val": 1}
    assert r2.one() == {"val": 2}
    assert r1._statement is None
    assert r2._statement is None

    stmt1 = r1.statement
    stmt2 = r2.statement
    assert stmt1 is not stmt2
    assert r1._statement is stmt1
    assert r2._statement is stmt2
    assert stmt1.raw_sql == sql
    assert stmt2.raw_sql == sql
    assert stmt1.operation_type == "SELECT"
    assert stmt2.operation_type == "SELECT"
    assert stmt1._processed_state.execution_parameters == (1,)
    assert stmt2._processed_state.execution_parameters == (2,)
    assert stmt1.parameters == [1]
    assert stmt2.parameters == [2]


@pytest.mark.anyio
async def test_fast_path_select_lazy_statement_materialization_async(aiosqlite_async_driver: Any) -> None:
    aiosqlite_async_driver._stmt_cache_enabled = True
    sql = "SELECT ? AS val"

    warm_result = await aiosqlite_async_driver.execute(sql, (0,))
    assert warm_result.one() == {"val": 0}
    assert warm_result._statement is not None

    r1 = await aiosqlite_async_driver.execute(sql, (1,))
    r2 = await aiosqlite_async_driver.execute(sql, (2,))
    assert r1._statement is None
    assert r2._statement is None
    assert r1.one() == {"val": 1}
    assert r2.one() == {"val": 2}
    assert r1._statement is None
    assert r2._statement is None

    stmt1 = r1.statement
    stmt2 = r2.statement
    assert stmt1 is not stmt2
    assert stmt1.raw_sql == sql
    assert stmt2.raw_sql == sql
    assert stmt1.operation_type == "SELECT"
    assert stmt2.operation_type == "SELECT"
    assert stmt1._processed_state.execution_parameters == (1,)
    assert stmt2._processed_state.execution_parameters == (2,)


def test_fast_path_ambient_exception_isolation_sync(cached_sync_cursor: Any, monkeypatch: Any) -> None:
    driver, _cursor, _statements, _fallback = cached_sync_cursor
    sql = "SELECT ? AS value"
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
    )

    helper_calls = 0
    original_helper = driver._run_with_exception_handler

    def tracked_helper(exc_handler: Any, fn: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal helper_calls
        helper_calls += 1
        return original_helper(exc_handler, fn, *args, **kwargs)

    monkeypatch.setattr(driver, "_run_with_exception_handler", tracked_helper)

    res_normal = SyncDriverAdapterBase._execute_cache_hit(driver, sql, ("normal",), cached)
    assert res_normal.one() == {"value": "normal"}
    assert helper_calls == 0

    try:
        raise ValueError("ambient")
    except ValueError:
        res_ambient = SyncDriverAdapterBase._execute_cache_hit(driver, sql, ("ambient",), cached)
        assert res_ambient.one() == {"value": "ambient"}
        assert helper_calls == 1


@pytest.mark.anyio
async def test_fast_path_ambient_exception_isolation_async(cached_async_cursor: Any, monkeypatch: Any) -> None:
    driver, _cursor, _statements, _fallback = cached_async_cursor
    sql = "SELECT ? AS value"
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
    )

    helper_calls = 0
    original_helper = driver._run_with_exception_handler

    async def tracked_helper(exc_handler: Any, fn: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal helper_calls
        helper_calls += 1
        return await original_helper(exc_handler, fn, *args, **kwargs)

    monkeypatch.setattr(driver, "_run_with_exception_handler", tracked_helper)

    res_normal = await AsyncDriverAdapterBase._execute_cache_hit(driver, sql, ("normal",), cached)
    assert res_normal.one() == {"value": "normal"}
    assert helper_calls == 0

    try:
        raise ValueError("ambient")
    except ValueError:
        res_ambient = await AsyncDriverAdapterBase._execute_cache_hit(driver, sql, ("ambient",), cached)
        assert res_ambient.one() == {"value": "ambient"}
        assert helper_calls == 1


def test_precomputed_fast_path_coercion_and_rebind_processor(sqlite_sync_driver: Any) -> None:
    coercion_map: dict[type, Any] = {int: str, float: str}
    fallbacks_1 = type_coercion_fallbacks(coercion_map)
    fallbacks_2 = type_coercion_fallbacks(coercion_map)
    assert fallbacks_1 is fallbacks_2

    coercion_map[bool] = int
    fallbacks_3 = type_coercion_fallbacks(coercion_map)
    assert fallbacks_3 is not fallbacks_1
    assert len(fallbacks_3) == 3

    sqlite_sync_driver._stmt_cache_enabled = True
    sqlite_sync_driver.execute("SELECT :val AS v", {"val": 10})
    cached_named = sqlite_sync_driver._stmt_cache.get("SELECT :val AS v")
    assert cached_named is not None
    assert cached_named.needs_style_remap is True

    sqlite_sync_driver.execute("SELECT ? AS v", (10,))
    cached_qmark = sqlite_sync_driver._stmt_cache.get("SELECT ? AS v")
    assert cached_qmark is not None
    assert cached_qmark.needs_style_remap is False

    base_stmt = SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=sqlite_sync_driver.statement_config)
    base_stmt.compile()
    copy_1 = base_stmt.copy(parameters=(2,))
    copy_2 = base_stmt.copy(parameters=(3,))
    copy_1.compile()
    copy_2.compile()
    assert copy_1._rebind_processor is not None
    assert copy_1._rebind_processor is copy_2._rebind_processor


def test_execute_many_single_pass_parameter_preparation(sqlite_sync_driver: Any) -> None:
    coercion_calls = 0

    def coerce_decimal(val: Decimal) -> str:
        nonlocal coercion_calls
        coercion_calls += 1
        return str(val)

    def coerce_uuid(val: UUID) -> str:
        nonlocal coercion_calls
        coercion_calls += 1
        return val.hex

    def coerce_date(val: date) -> str:
        nonlocal coercion_calls
        coercion_calls += 1
        return val.isoformat()

    custom_cfg = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            default_execution_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
            supported_execution_parameter_styles={ParameterStyle.QMARK},
            type_coercion_map={Decimal: coerce_decimal, UUID: coerce_uuid, date: coerce_date},
        ),
    )

    sample_uuid = uuid4()
    sample_date = date(2026, 10, 13)
    tuple_rows = [
        (Decimal("10.50"), sample_uuid, sample_date, TypedParameter(7, int)),
        (Decimal("20.75"), sample_uuid, sample_date, TypedParameter(8, int)),
    ]
    stmt_tuples = SQL("INSERT INTO items VALUES (?, ?, ?, ?)", tuple_rows, statement_config=custom_cfg, is_many=True)
    stmt_tuples.compile()
    _, prepared_tuples = sqlite_sync_driver._compiled_statement(stmt_tuples, custom_cfg)
    assert coercion_calls == 6
    assert prepared_tuples == [("10.50", sample_uuid.hex, "2026-10-13", 7), ("20.75", sample_uuid.hex, "2026-10-13", 8)]

    coercion_calls = 0
    list_rows = [[Decimal("1.00"), sample_uuid, sample_date], [Decimal("2.00"), sample_uuid, sample_date]]
    stmt_lists = SQL("INSERT INTO items VALUES (?, ?, ?)", list_rows, statement_config=custom_cfg, is_many=True)
    stmt_lists.compile()
    _, prepared_lists = sqlite_sync_driver._compiled_statement(stmt_lists, custom_cfg)
    assert coercion_calls == 6
    assert prepared_lists == [["1.00", sample_uuid.hex, "2026-10-13"], ["2.00", sample_uuid.hex, "2026-10-13"]]

    coercion_calls = 0
    dict_rows = [{"price": Decimal("3.50"), "uid": sample_uuid}, {"price": Decimal("4.50"), "uid": sample_uuid}]
    stmt_dicts = SQL("INSERT INTO items VALUES (:price, :uid)", dict_rows, statement_config=custom_cfg, is_many=True)
    stmt_dicts.compile()
    _, prepared_dicts = sqlite_sync_driver._compiled_statement(stmt_dicts, custom_cfg)
    assert coercion_calls == 4
    assert prepared_dicts == [("3.50", sample_uuid.hex), ("4.50", sample_uuid.hex)]

    override_calls = 0

    class CustomDriver(SqliteDriver):
        def prepare_driver_parameters(
            self,
            parameters: Any,
            statement_config: StatementConfig,
            is_many: bool = False,
            prepared_statement: Any | None = None,
        ) -> Any:
            nonlocal override_calls
            override_calls += 1
            return super().prepare_driver_parameters(
                parameters, statement_config, is_many=is_many, prepared_statement=prepared_statement
            )

    custom_driver = CustomDriver(
        connection=sqlite_sync_driver.connection,
        statement_config=custom_cfg,
        driver_features=sqlite_sync_driver.driver_features,
    )
    stmt_override = SQL("INSERT INTO items VALUES (?, ?, ?)", list_rows, statement_config=custom_cfg, is_many=True)
    stmt_override.compile()
    custom_driver._compiled_statement(stmt_override, custom_cfg)
    assert override_calls == 1


def test_declared_parameters_execute_many_copies_only_when_default_missing() -> None:
    declarations = (
        ParameterDeclaration(name="id", type_str="int", required=True),
        ParameterDeclaration(name="note", type_str="str", required=False),
    )
    row_complete = {"id": 1, "note": "present"}
    row_missing = {"id": 2}
    stmt = SQL(
        "INSERT INTO items VALUES (:id, :note)",
        [row_complete, row_missing],
        is_many=True,
        declared_parameters=declarations,
    )
    _check_declared_parameters(stmt)
    rows = stmt.positional_parameters
    assert rows[0] is row_complete
    assert rows[1] is not row_missing
    assert rows[1] == {"id": 2, "note": None}
    assert "note" not in row_missing


def test_cached_pragma_returns_rows_on_repeated_execution(sqlite_sync_driver: SqliteDriver) -> None:
    """Ensure repeated PRAGMA executions preserve row-returning behavior on statement cache hit."""
    sqlite_sync_driver.execute("CREATE TABLE pragma_test (id INTEGER PRIMARY KEY, name TEXT)")

    first_result = sqlite_sync_driver.execute("PRAGMA table_info(pragma_test)")
    second_result = sqlite_sync_driver.execute("PRAGMA table_info(pragma_test)")

    first_columns = {row["name"] for row in first_result.get_data()}
    second_columns = {row["name"] for row in second_result.get_data()}

    assert first_columns == {"id", "name"}
    assert second_columns == {"id", "name"}


@pytest.mark.parametrize("auto_begin", [False, True], ids=["disabled", "enabled"])
@pytest.mark.parametrize(
    ("query_sql", "params", "expects_begin"),
    [
        ("SELECT ?", (1,), False),
        ("INSERT INTO lazy_cache_items VALUES (?)", (1,), True),
        ("INSERT INTO lazy_cache_items VALUES (:id)", {"id": 2}, True),
    ],
)
def test_sync_statement_cache_respects_auto_begin_on_write(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, auto_begin: bool, query_sql: str, params: Any, expects_begin: bool
) -> None:
    cfg = SqliteConfig(connection_config={"database": str(tmp_path / "cache_sync.sqlite"), "autocommit": True})
    try:
        with cfg.provide_session() as setup:
            setup.execute("CREATE TABLE lazy_cache_items (id INTEGER)")
            setup.execute(query_sql, params)
            setup.execute("DELETE FROM lazy_cache_items")

        begins = 0
        with cfg.provide_session() as session:
            driver_cls = type(session)
            orig_begin = driver_cls.begin

            def tracked_begin(driver: Any) -> None:
                nonlocal begins
                if driver is session:
                    begins += 1
                orig_begin(driver)

            monkeypatch.setattr(driver_cls, "begin", tracked_begin)
            session._auto_begin_on_write = auto_begin
            session.execute(query_sql, params)
            if auto_begin and expects_begin:
                assert begins == 1
                session.rollback()
            else:
                assert begins == 0
    finally:
        cfg.close_pool()


@pytest.mark.parametrize("auto_begin", [False, True], ids=["disabled", "enabled"])
@pytest.mark.parametrize(
    ("query_sql", "params", "expects_begin"),
    [
        ("SELECT ?", (1,), False),
        ("INSERT INTO lazy_cache_items VALUES (?)", (1,), True),
        ("INSERT INTO lazy_cache_items VALUES (:id)", {"id": 2}, True),
    ],
)
async def test_async_statement_cache_respects_auto_begin_on_write(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, auto_begin: bool, query_sql: str, params: Any, expects_begin: bool
) -> None:
    cfg = AiosqliteConfig(connection_config={"database": str(tmp_path / "cache_async.sqlite"), "autocommit": True})
    try:
        async with cfg.provide_session() as setup:
            await setup.execute("CREATE TABLE lazy_cache_items (id INTEGER)")
            await setup.execute(query_sql, params)
            await setup.execute("DELETE FROM lazy_cache_items")

        begins = 0
        async with cfg.provide_session() as session:
            driver_cls = type(session)
            orig_begin = driver_cls.begin

            async def tracked_begin(driver: Any) -> None:
                nonlocal begins
                if driver is session:
                    begins += 1
                await orig_begin(driver)

            monkeypatch.setattr(driver_cls, "begin", tracked_begin)
            session._auto_begin_on_write = auto_begin
            await session.execute(query_sql, params)
            if auto_begin and expects_begin:
                assert begins == 1
                await session.rollback()
            else:
                assert begins == 0
    finally:
        await cfg.close_pool()


def test_prepare_statement_cache_respects_statement_config_override(tmp_path: Path) -> None:
    """prepare_statement does not read or populate the default cache under an override StatementConfig."""
    cfg = SqliteConfig(connection_config={"database": str(tmp_path / "override_cache.sqlite")})
    postgres_override = StatementConfig(dialect="postgres")
    unparsed_override = StatementConfig(enable_parsing=False)
    try:
        with cfg.provide_session() as session:
            default_first = session.prepare_statement("SELECT :y AS v", ({"y": 1},))
            assert default_first.statement_config is session.statement_config

            dialect_override = session.prepare_statement(
                "SELECT :y AS v", ({"y": 1},), statement_config=postgres_override
            )
            assert dialect_override.statement_config is postgres_override

            unparsed = session.prepare_statement("SELECT :y AS v", ({"y": 1},), statement_config=unparsed_override)
            assert unparsed.statement_config is unparsed_override
            assert unparsed.statement_config.enable_parsing is False
            assert unparsed.is_read_only() is False

            fresh_sql = "SELECT :z AS v"
            override_first = session.prepare_statement(fresh_sql, ({"z": 1},), statement_config=postgres_override)
            assert override_first.statement_config is postgres_override

            default_after = session.prepare_statement(fresh_sql, ({"z": 2},))
            assert default_after.statement_config is session.statement_config
    finally:
        cfg.close_pool()


def _static_output_transformer(sql: str, parameters: Any, alias: str = "tag") -> tuple[str, Any]:
    return f"SELECT 1 AS {alias}", parameters


def _static_statement_transformer(expr: Any, parameters: Any) -> tuple[Any, Any]:
    return expr, parameters


class _IdentityCallable:
    def __init__(self, marker: str) -> None:
        self.marker = marker

    def __call__(self, sql: str, parameters: Any) -> tuple[str, Any]:
        return f"SELECT '{self.marker}' AS marker", parameters


def test_callable_cache_key_handles_static_vs_dynamic_callables() -> None:
    """Static functions stay cacheable while closures, lambdas, and default-hash instances get dynamic keys."""
    assert _callable_cache_key(None) is None
    assert _callable_cache_key(_static_statement_transformer) is not None

    label = "dyn"

    def local_closure(sql: str, parameters: Any) -> tuple[str, Any]:
        return sql, parameters

    def local_stmt_closure(expr: Any, parameters: Any) -> tuple[Any, Any]:
        return expr, parameters

    def second_closure(sql: str, parameters: Any) -> tuple[str, Any]:
        return sql, parameters

    k1 = _callable_cache_key(local_closure)
    k2 = _callable_cache_key(second_closure)
    k3 = _callable_cache_key(lambda sql, params: (f"{sql} -- {label}", params))
    k4 = _callable_cache_key(_IdentityCallable("a"))
    k5 = _callable_cache_key(partial(_static_output_transformer, alias="x"))

    assert k1 != k2
    assert "dynamic" in k1
    assert "dynamic" in k3
    assert "dynamic" in k4
    assert "dynamic" in k5


def test_statement_and_output_transformer_closures_bypass_cache(tmp_path: Path) -> None:
    """Dynamic closure and stateful callable transformers bypass statement cache safely across GC cycles."""

    def make_output_transform(marker: str) -> Any:
        def transform(sql: str, parameters: Any) -> tuple[str, Any]:
            return f"SELECT '{marker}' AS marker", parameters

        return transform

    cfg = SqliteConfig(connection_config={"database": str(tmp_path / "closure_cache.sqlite")})
    try:
        with cfg.provide_session() as session:
            t1 = make_output_transform("first")
            sc1 = session.statement_config.replace(output_transformer=t1)
            r1 = session.execute("SELECT 'orig' AS marker", statement_config=sc1).get_data()
            assert r1 == [{"marker": "first"}]
            del t1, sc1
            gc.collect()

            t2 = make_output_transform("second")
            sc2 = session.statement_config.replace(output_transformer=t2)
            r2 = session.execute("SELECT 'orig' AS marker", statement_config=sc2).get_data()
            assert r2 == [{"marker": "second"}]

            c1 = _IdentityCallable("obj-1")
            r3 = session.execute(
                "SELECT 'orig' AS marker", statement_config=session.statement_config.replace(output_transformer=c1)
            ).get_data()
            assert r3 == [{"marker": "obj-1"}]
            del c1
            gc.collect()

            c2 = _IdentityCallable("obj-2")
            r4 = session.execute(
                "SELECT 'orig' AS marker", statement_config=session.statement_config.replace(output_transformer=c2)
            ).get_data()
            assert r4 == [{"marker": "obj-2"}]

            def make_stmt_transform(literal_val: int) -> Any:
                def transform(expr: Any, params: Any) -> tuple[Any, Any]:
                    return parse_one(f"SELECT {literal_val} AS v"), params

                return transform

            st1 = make_stmt_transform(101)
            sr1 = session.execute(
                "SELECT 0 AS v", statement_config=session.statement_config.replace(statement_transformers=(st1,))
            ).get_data()
            assert sr1 == [{"v": 101}]
            del st1
            gc.collect()

            st2 = make_stmt_transform(202)
            sr2 = session.execute(
                "SELECT 0 AS v", statement_config=session.statement_config.replace(statement_transformers=(st2,))
            ).get_data()
            assert sr2 == [{"v": 202}]
    finally:
        cfg.close_pool()


def test_ast_transformer_bypasses_statement_cache_with_named_null_pruning(tmp_path: Path) -> None:
    """Drivers with parameter ast_transformer bypass statement caching even when pruned param counts vary."""
    clear_all_caches()
    adbc_cfg = AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite", "uri": ":memory:"})
    base_pc = adbc_cfg.statement_config.parameter_config
    pruning_sc = adbc_cfg.statement_config.replace(
        parameter_config=base_pc.replace(ast_transformer=build_null_pruning_transform(dialect="sqlite"))
    )
    adbc_pruned = AdbcConfig(
        connection_config={"driver_name": "adbc_driver_sqlite", "uri": str(tmp_path / "adbc_null_prune.sqlite")},
        statement_config=pruning_sc,
    )
    sql_text = "SELECT :a AS c0, :p1 AS c1, :c0 AS c2, :a AS c3"
    try:
        with adbc_pruned.provide_session() as session:
            assert session._stmt_cache_enabled is False
            first = session.execute(sql_text, {"a": "A1", "p1": None, "c0": "C1"}).get_data()
            second = session.execute(sql_text, {"a": "A2", "p1": "P2", "c0": "C2"}).get_data()
            third = session.execute(sql_text, {"a": "A3", "p1": None, "c0": "C3"}).get_data()
            assert first == [{"c0": "A1", "c1": None, "c2": "C1", "c3": "A1"}]
            assert second == [{"c0": "A2", "c1": "P2", "c2": "C2", "c3": "A2"}]
            assert third == [{"c0": "A3", "c1": None, "c2": "C3", "c3": "A3"}]
    finally:
        adbc_cfg.close_pool()
        adbc_pruned.close_pool()

    sqlite_base = SqliteConfig(connection_config={"database": str(tmp_path / "sqlite_null_prune.sqlite")})
    sqlite_pc = sqlite_base.statement_config.parameter_config.replace(
        default_execution_parameter_style=ParameterStyle.QMARK,
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        ast_transformer=build_null_pruning_transform(dialect="sqlite"),
    )
    sqlite_pruned = SqliteConfig(
        connection_config={"database": str(tmp_path / "sqlite_null_prune.sqlite")},
        statement_config=sqlite_base.statement_config.replace(parameter_config=sqlite_pc),
    )
    try:
        with sqlite_pruned.provide_session() as session:
            assert session._stmt_cache_enabled is False
            r1 = session.execute(sql_text, {"a": "A1", "p1": None, "c0": "C1"}).get_data()
            r2 = session.execute(sql_text, {"a": "A2", "p1": "P2", "c0": "C2"}).get_data()
            assert r1 == [{"c0": "A1", "c1": None, "c2": "C1", "c3": "A1"}]
            assert r2 == [{"c0": "A2", "c1": "P2", "c2": "C2", "c3": "A2"}]
    finally:
        sqlite_base.close_pool()
        sqlite_pruned.close_pool()


def test_warm_cache_mapping_parameters_with_numeric_and_qmark_styles(tmp_path: Path) -> None:
    """Warm statement cache aligns positional mapping keys identically to cold compilation across DuckDB and SQLite."""
    clear_all_caches()
    duck_cfg = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with duck_cfg.provide_session() as session:
            cold_rev = session.execute("SELECT $1 AS first_col, $2 AS second_col", {"2": "two", "1": "one"}).get_data()
            warm_rev = session.execute(
                "SELECT $1 AS first_col, $2 AS second_col", {"2": "two_w", "1": "one_w"}
            ).get_data()
            assert cold_rev == [{"first_col": "one", "second_col": "two"}]
            assert warm_rev == [{"first_col": "one_w", "second_col": "two_w"}]

            eleven_sql = ", ".join(f"${idx} AS c{idx}" for idx in range(1, 12))
            eleven_query = f"SELECT {eleven_sql}"
            reversed_eleven = {str(idx): f"val_{idx}" for idx in range(11, 0, -1)}
            cold_eleven = session.execute(eleven_query, reversed_eleven).get_data()
            warm_eleven = session.execute(eleven_query, reversed_eleven).get_data()
            expected_eleven = [{f"c{idx}": f"val_{idx}" for idx in range(1, 12)}]
            assert cold_eleven == expected_eleven
            assert warm_eleven == expected_eleven

            dollar_cold = session.execute("SELECT $1 AS a, $2 AS b", {"$2": "second", "$1": "first"}).get_data()
            dollar_warm = session.execute("SELECT $1 AS a, $2 AS b", {"$2": "second_w", "$1": "first_w"}).get_data()
            assert dollar_cold == [{"a": "first", "b": "second"}]
            assert dollar_warm == [{"a": "first_w", "b": "second_w"}]

            param_cold = session.execute(
                "SELECT $1 AS a, $2 AS b", {"param_1": "p_second", "param_0": "p_first"}
            ).get_data()
            param_warm = session.execute(
                "SELECT $1 AS a, $2 AS b", {"param_1": "p_second_w", "param_0": "p_first_w"}
            ).get_data()
            assert param_cold == [{"a": "p_first", "b": "p_second"}]
            assert param_warm == [{"a": "p_first_w", "b": "p_second_w"}]
    finally:
        duck_cfg.close_pool()

    sqlite_cfg = SqliteConfig(connection_config={"database": str(tmp_path / "sqlite_pos_map.sqlite")})
    try:
        with sqlite_cfg.provide_session() as session:
            qmark_tuple = session.execute("SELECT ? AS first_col, ? AS second_col", ("one", "two")).get_data()
            qmark_map = session.execute(
                "SELECT ? AS first_col, ? AS second_col", {"2": "two_w", "1": "one_w"}
            ).get_data()
            assert qmark_tuple == [{"first_col": "one", "second_col": "two"}]
            assert qmark_map == [{"first_col": "one_w", "second_col": "two_w"}]

            clear_all_caches()
            qmark_cold_map = session.execute(
                "SELECT ? AS first_col, ? AS second_col", {"2": "two_cold", "1": "one_cold"}
            ).get_data()
            qmark_warm_map = session.execute(
                "SELECT ? AS first_col, ? AS second_col", {"2": "two_warm", "1": "one_warm"}
            ).get_data()
            assert qmark_cold_map == [{"first_col": "one_cold", "second_col": "two_cold"}]
            assert qmark_warm_map == [{"first_col": "one_warm", "second_col": "two_warm"}]

            direct_num_qmark = SQL(
                "SELECT ?1 AS first_col, ?2 AS second_col",
                {"2": "two_nq", "1": "one_nq"},
                statement_config=sqlite_cfg.statement_config,
            ).compile()
            clear_all_caches()
            prepared_num_qmark = session.prepare_statement(
                "SELECT ?1 AS first_col, ?2 AS second_col", ({"2": "two_nq", "1": "one_nq"},)
            )
            driver_num_qmark = session.prepare_driver_parameters(
                {"2": "two_nq", "1": "one_nq"}, sqlite_cfg.statement_config, prepared_statement=prepared_num_qmark
            )
            assert direct_num_qmark[1] == ["one_nq", "two_nq"]
            assert driver_num_qmark == ["one_nq", "two_nq"]
    finally:
        sqlite_cfg.close_pool()


def test_subclass_cache_hit_override_runs_for_repeated_statements(sqlite_sync_driver: Any) -> None:
    hits: list[str] = []

    class TrackingDriver(SqliteDriver):
        def _execute_cache_hit(self, sql: str, params: Any, cached: CachedQuery) -> Any:
            hits.append(sql)
            return super()._execute_cache_hit(sql, params, cached)

    driver = TrackingDriver(
        connection=sqlite_sync_driver.connection,
        statement_config=sqlite_sync_driver.statement_config,
        driver_features=sqlite_sync_driver.driver_features,
    )
    driver._stmt_cache_enabled = True

    assert driver.execute("SELECT ? AS v", (1,)).get_data() == [{"v": 1}]
    assert driver.execute("SELECT ? AS v", (2,)).get_data() == [{"v": 2}]
    assert hits == ["SELECT ? AS v"]


def test_declined_cache_hit_runs_driver_cache_hit_method(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
    declined: list[str] = []

    def decline(connection: Any, sql: str, params: Any, cached: CachedQuery, config: Any, state: Any) -> None:
        declined.append(sql)

    sqlite_sync_driver._stmt_cache_enabled = True
    assert sqlite_sync_driver.execute("SELECT ? AS v", (1,)).get_data() == [{"v": 1}]
    monkeypatch.setattr(sqlite_sync_driver, "_cache_hit_executor", decline)

    assert sqlite_sync_driver.execute("SELECT ? AS v", (2,)).get_data() == [{"v": 2}]
    assert declined == ["SELECT ? AS v"]


@pytest.mark.anyio
async def test_declined_async_cache_hit_runs_driver_cache_hit_method(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    declined: list[str] = []

    async def decline(connection: Any, sql: str, params: Any, cached: CachedQuery, config: Any, state: Any) -> None:
        declined.append(sql)

    aiosqlite_async_driver._stmt_cache_enabled = True
    assert (await aiosqlite_async_driver.execute("SELECT ? AS v", (1,))).get_data() == [{"v": 1}]
    monkeypatch.setattr(aiosqlite_async_driver, "_cache_hit_executor", decline)

    assert (await aiosqlite_async_driver.execute("SELECT ? AS v", (2,))).get_data() == [{"v": 2}]
    assert declined == ["SELECT ? AS v"]


def test_run_cached_execution_hits_fast_path_for_numeric_positional_params(
    sqlite_sync_driver: Any, monkeypatch: Any
) -> None:
    """Contiguous $1 positional cached queries take execute_cache_hit while mapping params still rebind."""
    param_profile = ParameterProfile([
        ParameterInfo(name="1", style=ParameterStyle.NUMERIC, position=7, ordinal=1, placeholder_text="$1")
    ])
    cached = CachedQuery(
        compiled_sql="SELECT $1 AS val",
        parameter_profile=param_profile,
        input_named_parameters=("1",),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=1,
        processed_state=ProcessedState(
            compiled_sql="SELECT $1 AS val", execution_parameters=[1], operation_type="SELECT"
        ),
        simple_wire_fast_path=True,
    )
    sqlite_sync_driver._stmt_cache_enabled = True
    sqlite_sync_driver._stmt_cache.set("SELECT $1 AS val", cached)

    fast_calls: list[Any] = []
    rebind_calls: list[Any] = []

    def fake_executor(_conn: Any, sql: str, params: Any, _cached: Any, _cfg: Any, _state: Any) -> Any:
        fast_calls.append((sql, params))
        return SQLResult.create_cached_select(
            sql=sql,
            config=sqlite_sync_driver.statement_config,
            cached_state=cached.processed_state,
            compiled_sql=cached.compiled_sql,
            params=params,
            data=[(params[0],)],
            column_names=["val"],
            rows_affected=1,
            row_format="tuple",
        )

    def fake_rebind(params: Any, _cached: Any) -> Any:
        rebind_calls.append(params)
        return [params["1"]]

    monkeypatch.setattr(sqlite_sync_driver, "_cache_hit_executor", fake_executor)
    monkeypatch.setattr(sqlite_sync_driver, "stmt_cache_rebind", fake_rebind)

    result_pos = run_cached_execution(sqlite_sync_driver, "SELECT $1 AS val", (7,))
    assert isinstance(result_pos, SQLResult)
    assert result_pos.scalar() == 7
    assert fast_calls == [("SELECT $1 AS val", (7,))]
    assert rebind_calls == []

    monkeypatch.setattr(sqlite_sync_driver, "_cache_hit_executor", None)
    monkeypatch.setattr(
        sqlite_sync_driver,
        "_execute_cached_statement",
        lambda prepared: SQLResult(statement=prepared, data=[{"val": 9}], rows_affected=1),
    )
    result_map = run_cached_execution(sqlite_sync_driver, "SELECT $1 AS val", {"1": 9})
    assert isinstance(result_map, SQLResult)
    assert result_map.scalar() == 9
    assert rebind_calls == [{"1": 9}]


def test_compile_statement_rejects_mismatched_source_guard_on_cache_hit(sqlite_sync_driver: Any) -> None:
    """L2 CachedStatement source guard rejects entries whose raw SQL disagrees with the requested statement."""
    clear_all_caches()
    config = sqlite_sync_driver.statement_config
    stmt = SQL("SELECT 1 AS ok", statement_config=config)
    cache_key = sqlite_sync_driver._compile_cache_key(stmt, config, False)
    poisoned = CachedStatement(
        compiled_sql="SELECT 999 AS ok",
        parameters=(),
        expression=None,
        _source_guard=("SELECT 999 AS ok", False, False, False, config),
    )
    get_cache().put_statement(cache_key, poisoned, None)

    compiled_stmt, _ = sqlite_sync_driver._compiled_statement(stmt, config)
    assert "999" not in compiled_stmt.compiled_sql
    assert "1" in compiled_stmt.compiled_sql
    clear_all_caches()


def test_open_session_cached_statements_see_query_hooks_registered_later() -> None:
    """A query hook registered while a session is open fires for that session's cached statements."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    seen: list[str] = []

    with spec.provide_session(config) as session:
        for _ in range(2):
            session.execute("SELECT ?", (1,))
        config.get_observability_runtime().register_lifecycle_hook("on_query_start", lambda _context: seen.append("q"))
        for _ in range(3):
            session.execute("SELECT ?", (1,))

    assert seen == ["q", "q", "q"]


class _NoopSyncExceptionHandler(BaseSyncExceptionHandler):
    def _handle_exception(self, exc_type: type[BaseException] | None, exc_val: BaseException) -> bool:
        _ = (exc_type, exc_val)
        return False


class _NoopAsyncExceptionHandler(BaseAsyncExceptionHandler):
    def _handle_exception(self, exc_type: type[BaseException] | None, exc_val: BaseException) -> bool:
        _ = (exc_type, exc_val)
        return False


class _ExternalSyncCursor:
    def __init__(self, rowcount: int, lastrowid: Any, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rowcount = rowcount
        self.lastrowid = lastrowid
        self._rows = rows or []
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _ExternalAsyncCursor:
    def __init__(self, rowcount: int, lastrowid: Any, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rowcount = rowcount
        self.lastrowid = lastrowid
        self._rows = rows or []
        self.executed: list[tuple[str, Any]] = []

    async def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _ExternalSyncDriver(SyncDriverAdapterBase):
    """Minimal synchronous external adapter implementing the abstract driver surface."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: Any,
        *,
        cursor: Any | None = None,
        fallback_rowcount: int = 1,
        fallback_last_id: int | str | None = None,
    ) -> None:
        config = StatementConfig(
            dialect="sqlite",
            enable_caching=True,
            parameter_config=ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK},
                default_execution_parameter_style=ParameterStyle.QMARK,
                supported_execution_parameter_styles={ParameterStyle.QMARK},
            ),
        )
        super().__init__(connection=connection, statement_config=config)
        self._cursor = cursor
        self._fallback_rowcount = fallback_rowcount
        self._fallback_last_id = fallback_last_id
        self.cursor_used = 0
        self.dispatch_calls = 0

    @property
    def data_dictionary(self) -> Any:
        return cast("Any", None)

    @contextmanager
    def with_cursor(self, connection: Any) -> Generator[Any, None, None]:
        self.cursor_used += 1
        yield connection if self._cursor is None else self._cursor

    def handle_database_exceptions(self) -> _NoopSyncExceptionHandler:
        return _NoopSyncExceptionHandler()

    def collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        _ = cursor
        return fetched, ["v"], len(fetched)

    def resolve_rowcount(self, cursor: Any) -> int:
        return int(cursor.rowcount) if isinstance(cursor.rowcount, int) and cursor.rowcount >= 0 else 0

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        self.dispatch_calls += 1
        self._compiled_sql(statement, self.statement_config)
        if statement.returns_rows():
            return self.create_execution_result(
                cursor,
                selected_data=[(1,)],
                column_names=["v"],
                data_row_count=1,
                is_select_result=True,
                row_format="tuple",
            )
        return self.create_execution_result(
            cursor, rowcount_override=self._fallback_rowcount, last_inserted_id=self._fallback_last_id
        )

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = statement
        return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = statement
        return self.create_execution_result(cursor, statement_count=1, successful_statements=1, is_script_result=True)

    def begin(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def _connection_in_transaction(self) -> bool:
        return False


class _ExternalAsyncDriver(AsyncDriverAdapterBase):
    """Minimal asynchronous external adapter implementing the abstract driver surface."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: Any,
        *,
        cursor: Any | None = None,
        fallback_rowcount: int = 1,
        fallback_last_id: int | str | None = None,
    ) -> None:
        config = StatementConfig(
            dialect="sqlite",
            enable_caching=True,
            parameter_config=ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK},
                default_execution_parameter_style=ParameterStyle.QMARK,
                supported_execution_parameter_styles={ParameterStyle.QMARK},
            ),
        )
        super().__init__(connection=connection, statement_config=config)
        self._cursor = cursor
        self._fallback_rowcount = fallback_rowcount
        self._fallback_last_id = fallback_last_id
        self.cursor_used = 0
        self.dispatch_calls = 0

    @property
    def data_dictionary(self) -> Any:
        return cast("Any", None)

    @asynccontextmanager
    async def with_cursor(self, connection: Any) -> AsyncGenerator[Any, None]:
        self.cursor_used += 1
        yield connection if self._cursor is None else self._cursor

    def handle_database_exceptions(self) -> _NoopAsyncExceptionHandler:
        return _NoopAsyncExceptionHandler()

    def collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        _ = cursor
        return fetched, ["v"], len(fetched)

    def resolve_rowcount(self, cursor: Any) -> int:
        return int(cursor.rowcount) if isinstance(cursor.rowcount, int) and cursor.rowcount >= 0 else 0

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        self.dispatch_calls += 1
        self._compiled_sql(statement, self.statement_config)
        if statement.returns_rows():
            return self.create_execution_result(
                cursor,
                selected_data=[(1,)],
                column_names=["v"],
                data_row_count=1,
                is_select_result=True,
                row_format="tuple",
            )
        return self.create_execution_result(
            cursor, rowcount_override=self._fallback_rowcount, last_inserted_id=self._fallback_last_id
        )

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = statement
        return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = statement
        return self.create_execution_result(cursor, statement_count=1, successful_statements=1, is_script_result=True)

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    def _connection_in_transaction(self) -> bool:
        return False


def test_public_cache_hit_driver_and_core_exports() -> None:
    """Public driver and core modules expose cache-hit registration protocols and helpers."""
    assert SyncCacheHitExecutor is not None
    assert AsyncCacheHitExecutor is not None
    assert callable(cache_hit_hooks_unchanged)
    assert callable(SQLResult.create_cached_select)
    assert DMLResult("INSERT", 1, 7).last_inserted_id == 7
    assert cache_hit_hooks_unchanged(int, str) is False
    assert cache_hit_hooks_unchanged(cast("Any", "not_a_type"), _ExternalSyncDriver) is False
    assert cache_hit_hooks_unchanged(_ExternalSyncDriver, _ExternalSyncDriver) is True


@pytest.mark.parametrize(
    ("operation", "sql", "rowcount", "lastrowid", "expected_last_id"),
    [
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, 42, 42),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, "row-9", "row-9"),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 0, 42, None),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, True, None),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, object(), None),
        ("UPDATE", "UPDATE items SET name = ?", 2, 42, None),
        ("DELETE", "DELETE FROM items WHERE name = ?", 1, 42, None),
    ],
    ids=["insert-int", "insert-str", "insert-zero-rc", "insert-bool", "insert-object", "update", "delete"],
)
def test_sync_cache_hit_direct_cursor_preserves_last_inserted_id(
    operation: OperationType, sql: str, rowcount: int, lastrowid: Any, expected_last_id: int | str | None
) -> None:
    """Generic sync cache-hit direct cursor path preserves last_inserted_id only on successful INSERT."""
    cursor = _ExternalSyncCursor(rowcount=rowcount, lastrowid=lastrowid)
    driver = _ExternalSyncDriver(object(), cursor=cursor)
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type=operation,
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
    )

    result = driver._execute_cache_hit_with_cursor(sql, ("alpha",), cached)

    assert isinstance(result, DMLResult)
    assert result.rows_affected == rowcount
    assert result.last_inserted_id == expected_last_id


@pytest.mark.parametrize(
    ("operation", "sql", "rowcount", "last_id", "expected_last_id"),
    [
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, 11, 11),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, "abc-1", "abc-1"),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 0, 11, None),
        ("UPDATE", "UPDATE items SET name = ?", 2, 11, None),
    ],
    ids=["insert-int", "insert-str", "insert-zero-rc", "update"],
)
def test_sync_cache_hit_dispatch_fallback_preserves_last_inserted_id(
    operation: OperationType, sql: str, rowcount: int, last_id: int | str | None, expected_last_id: int | str | None
) -> None:
    """Generic sync cache-hit dispatch_execute fallback preserves execution_result.last_inserted_id on INSERT."""
    driver = _ExternalSyncDriver(object(), fallback_rowcount=rowcount, fallback_last_id=last_id)
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type=operation,
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
    )

    result = driver._execute_cache_hit_with_cursor(sql, ("alpha",), cached)

    assert isinstance(result, DMLResult)
    assert driver.dispatch_calls == 1
    assert result.rows_affected == rowcount
    assert result.last_inserted_id == expected_last_id


def test_sync_cache_hit_custom_resolve_last_inserted_id_hook() -> None:
    """Overriding resolve_last_inserted_id controls last_inserted_id on the generic sync cache-hit path."""

    class _CustomLastIdDriver(_ExternalSyncDriver):
        def resolve_last_inserted_id(self, cursor: Any) -> int | str | None:
            _ = cursor
            return "custom-id"

    cursor = _ExternalSyncCursor(rowcount=1, lastrowid=99)
    driver = _CustomLastIdDriver(object(), cursor=cursor)
    cached = _make_cached(
        compiled_sql="INSERT INTO items (name) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
    )

    result = driver._execute_cache_hit_with_cursor("INSERT INTO items (name) VALUES (?)", ("alpha",), cached)

    assert result.last_inserted_id == "custom-id"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("operation", "sql", "rowcount", "lastrowid", "expected_last_id"),
    [
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, 42, 42),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, "row-9", "row-9"),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 0, 42, None),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, True, None),
        ("UPDATE", "UPDATE items SET name = ?", 2, 42, None),
        ("DELETE", "DELETE FROM items WHERE name = ?", 1, 42, None),
    ],
    ids=["insert-int", "insert-str", "insert-zero-rc", "insert-bool", "update", "delete"],
)
async def test_async_cache_hit_direct_cursor_preserves_last_inserted_id(
    operation: OperationType, sql: str, rowcount: int, lastrowid: Any, expected_last_id: int | str | None
) -> None:
    """Generic async cache-hit direct cursor path preserves last_inserted_id only on successful INSERT."""
    cursor = _ExternalAsyncCursor(rowcount=rowcount, lastrowid=lastrowid)
    driver = _ExternalAsyncDriver(object(), cursor=cursor)
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type=operation,
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
    )

    result = await driver._execute_cache_hit_with_cursor(sql, ("alpha",), cached)

    assert isinstance(result, DMLResult)
    assert result.rows_affected == rowcount
    assert result.last_inserted_id == expected_last_id


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("operation", "sql", "rowcount", "last_id", "expected_last_id"),
    [
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, 19, 19),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 1, "async-id", "async-id"),
        ("INSERT", "INSERT INTO items (name) VALUES (?)", 0, 19, None),
        ("UPDATE", "UPDATE items SET name = ?", 2, 19, None),
    ],
    ids=["insert-int", "insert-str", "insert-zero-rc", "update"],
)
async def test_async_cache_hit_dispatch_fallback_preserves_last_inserted_id(
    operation: OperationType, sql: str, rowcount: int, last_id: int | str | None, expected_last_id: int | str | None
) -> None:
    """Generic async cache-hit dispatch_execute fallback preserves execution_result.last_inserted_id on INSERT."""
    driver = _ExternalAsyncDriver(object(), fallback_rowcount=rowcount, fallback_last_id=last_id)
    cached = _make_cached(
        compiled_sql=sql,
        param_count=1,
        operation_type=operation,
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
    )

    result = await driver._execute_cache_hit_with_cursor(sql, ("alpha",), cached)

    assert isinstance(result, DMLResult)
    assert driver.dispatch_calls == 1
    assert result.rows_affected == rowcount
    assert result.last_inserted_id == expected_last_id


def test_sync_end_to_end_cached_insert_matches_uncached_last_inserted_id() -> None:
    """Repeated INSERT executions preserve last_inserted_id on both cache miss and cache hit."""
    cursor = _ExternalSyncCursor(rowcount=1, lastrowid=101)
    driver = _ExternalSyncDriver(object(), cursor=cursor, fallback_rowcount=1, fallback_last_id=101)

    first = driver.execute("INSERT INTO items (name) VALUES (?)", ("first",))
    cursor.lastrowid = 102
    second = driver.execute("INSERT INTO items (name) VALUES (?)", ("second",))

    assert first.last_inserted_id == 101
    assert second.last_inserted_id == 102
    assert isinstance(second, DMLResult)


def test_register_sync_cache_hit_executor_public_api() -> None:
    """External sync adapters can register, inspect, run, decline, observe, and clear executors."""
    calls: list[tuple[Any, str, Any, Any]] = []

    def custom_executor(
        conn: Any, raw_sql: str, params: Any, cached_entry: CachedQuery, cfg: StatementConfig, custom_state: Any
    ) -> SQLResult | None:
        calls.append((conn, raw_sql, params, custom_state))
        if params == ("decline",):
            return None
        if not cached_entry.operation_profile.returns_rows:
            return DMLResult(cached_entry.operation_type, 1, 777)
        return SQLResult.create_cached_select(
            raw_sql, cfg, cached_entry.processed_state, cached_entry.compiled_sql, params, [(9,)], ["v"], 1, "tuple"
        )

    connection = object()
    state_marker = {"tag": "sync-state"}
    cursor = _ExternalSyncCursor(rowcount=1, lastrowid=55, rows=[(3,)])
    driver = _ExternalSyncDriver(connection, cursor=cursor)
    registered = driver.register_cache_hit_executor(custom_executor, state=state_marker, owner=_ExternalSyncDriver)

    assert registered is True
    assert driver.cache_hit_executor is custom_executor
    assert driver.cache_hit_state is state_marker

    driver.execute("SELECT ? AS v", ("warm",))
    hit = driver.execute("SELECT ? AS v", ("fast",))
    assert hit.get_data() == [{"v": 9}]
    assert len(calls) == 1
    assert calls[0][0] is connection
    assert calls[0][3] is state_marker

    driver.execute("INSERT INTO items (name) VALUES (?)", ("warm",))
    insert_hit = driver.execute("INSERT INTO items (name) VALUES (?)", ("fast-insert",))
    assert isinstance(insert_hit, DMLResult)
    assert insert_hit.last_inserted_id == 777
    assert len(calls) == 2

    fallback = driver.execute("SELECT ? AS v", ("decline",))
    assert fallback.get_data() == [{"v": 3}]
    assert len(calls) == 3

    observed_events: list[StatementEvent] = []
    driver.attach_observability(
        ObservabilityRuntime(
            config=ObservabilityConfig(statement_observers=(observed_events.append,)), config_name="external"
        )
    )
    observed = driver.execute("SELECT ? AS v", ("obs",))
    assert observed.get_data() == [{"v": 3}]
    assert len(calls) == 3
    assert len(observed_events) == 1

    driver.clear_cache_hit_executor()
    assert driver.cache_hit_executor is None
    assert driver.cache_hit_state is None


@pytest.mark.anyio
async def test_register_async_cache_hit_executor_public_api() -> None:
    """External async adapters can register single-call executors, handle declines, and route observability."""
    calls: list[tuple[str, Any, Any]] = []

    async def custom_executor(
        conn: Any, raw_sql: str, params: Any, cached_entry: CachedQuery, cfg: StatementConfig, custom_state: Any
    ) -> SQLResult | None:
        _ = conn
        calls.append((raw_sql, params, custom_state))
        if params == ("decline",):
            return None
        return SQLResult.create_cached_select(
            raw_sql, cfg, cached_entry.processed_state, cached_entry.compiled_sql, params, [(8,)], ["v"], 1, "tuple"
        )

    state_marker = ("async-state",)
    cursor = _ExternalAsyncCursor(rowcount=1, lastrowid=1, rows=[(4,)])
    driver = _ExternalAsyncDriver(object(), cursor=cursor)
    registered = driver.register_cache_hit_executor(custom_executor, state=state_marker, owner=_ExternalAsyncDriver)

    assert registered is True
    assert driver.cache_hit_executor is custom_executor
    assert driver.cache_hit_state is state_marker

    await driver.execute("SELECT ? AS v", ("warm",))
    hit = await driver.execute("SELECT ? AS v", ("fast",))
    assert hit.get_data() == [{"v": 8}]
    assert calls == [("SELECT ? AS v", ("fast",), state_marker)]

    fallback = await driver.execute("SELECT ? AS v", ("decline",))
    assert fallback.get_data() == [{"v": 4}]
    assert len(calls) == 2

    observed_events: list[StatementEvent] = []
    driver.attach_observability(
        ObservabilityRuntime(
            config=ObservabilityConfig(statement_observers=(observed_events.append,)), config_name="external-async"
        )
    )
    observed = await driver.execute("SELECT ? AS v", ("obs",))
    assert observed.get_data() == [{"v": 4}]
    assert len(calls) == 2
    assert len(observed_events) == 1


@pytest.mark.parametrize(
    "hook_name",
    [
        "with_cursor",
        "collect_rows",
        "resolve_rowcount",
        "resolve_last_inserted_id",
        "handle_database_exceptions",
        "build_cached_select_result",
        "_execute_cache_hit",
        "_execute_cache_hit_with_cursor",
    ],
)
def test_register_cache_hit_executor_respects_subclass_hook_overrides(hook_name: str) -> None:
    """Subclass overriding any watched cache-hit hook causes owner-checked registration to decline."""

    def dummy_executor(*_args: Any) -> SQLResult | None:
        return None

    def dummy_override(self: Any, *args: Any, **kwargs: Any) -> Any:
        return getattr(_ExternalSyncDriver, hook_name)(self, *args, **kwargs)

    sub_cls = type(f"Sub_{hook_name}", (_ExternalSyncDriver,), {hook_name: dummy_override})
    driver = sub_cls(object())

    assert driver.register_cache_hit_executor(dummy_executor, state="kept", owner=_ExternalSyncDriver) is False
    assert driver.cache_hit_executor is None
    assert driver.cache_hit_state == "kept"

    assert driver.register_cache_hit_executor(dummy_executor, state="own", owner=sub_cls) is True
    assert driver.cache_hit_executor is dummy_executor
    assert driver.cache_hit_state == "own"

    driver.clear_cache_hit_executor()
    assert driver.register_cache_hit_executor(dummy_executor, state="direct") is True
    assert driver.cache_hit_executor is dummy_executor
    assert driver.cache_hit_state == "direct"


def test_register_cache_hit_executor_validates_arguments() -> None:
    """Non-callable executors or mismatched owner classes raise TypeError."""
    driver = _ExternalSyncDriver(object())

    with pytest.raises(TypeError, match="callable"):
        driver.register_cache_hit_executor(cast("Any", "not_callable"))

    def valid_executor(*_args: Any) -> SQLResult | None:
        return None

    with pytest.raises(TypeError, match="owner"):
        driver.register_cache_hit_executor(valid_executor, owner=cast("Any", SqliteDriver))

    with pytest.raises(TypeError, match="owner"):
        driver.register_cache_hit_executor(valid_executor, owner=cast("Any", "not_a_class"))


def test_pymssql_and_sybase_resolve_last_inserted_id_returns_none() -> None:
    """Pymssql and Sybase drivers return None from resolve_last_inserted_id to match dispatch_execute."""
    pytest.importorskip("pymssql")
    from sqlspec.adapters.pymssql.driver import PymssqlDriver
    from sqlspec.adapters.sybase.driver import SybaseDriver

    fake_cursor = _ExternalSyncCursor(rowcount=1, lastrowid=99)
    assert PymssqlDriver(cast("Any", object())).resolve_last_inserted_id(fake_cursor) is None
    assert SybaseDriver(cast("Any", object())).resolve_last_inserted_id(fake_cursor) is None
