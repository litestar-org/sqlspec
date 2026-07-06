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
from sqlspec.core.parameters._processor import ParameterProcessor
from sqlspec.driver import _common as driver_common
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
        column_names=column_names,
    )


def test_driver_common_dead_script_and_version_helpers_stay_removed() -> None:
    """Retired private helper names should not reappear in the compiled driver module."""
    for name in (
        "EXEC_CURSOR_RESULT",
        "EXEC_ROWCOUNT_OVERRIDE",
        "EXEC_SPECIAL_DATA",
        "ScriptExecutionResult",
        "get_cached_version_for_driver",
        "cache_version_for_driver",
        "detect_version_with_queries",
        "parse_version_string",
    ):
        assert not hasattr(driver_common, name)


def test_private_statement_cache_helper_names_are_cleaned_up() -> None:
    """Statement-cache helpers should use purpose-oriented private names."""
    assert hasattr(driver_common.CommonDriverAttributesMixin, "_cached_execution")
    assert not hasattr(driver_common.CommonDriverAttributesMixin, "_stmt_cache_lookup")


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

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(sqlite_sync_driver, "_cached_execution", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    result = sqlite_sync_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


def test_execute_uses_fast_path_with_dict_payload(sqlite_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any] | dict[str, Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(sqlite_sync_driver, "_cached_execution", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    result = sqlite_sync_driver.execute("SELECT :id", {"id": 1})

    assert result is sentinel
    assert called["args"] == ("SELECT :id", {"id": 1})


def test_execute_uses_fast_path_with_kwargs_payload(sqlite_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any] | dict[str, Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(sqlite_sync_driver, "_cached_execution", _fake_try)
    sqlite_sync_driver._stmt_cache_enabled = True

    result = sqlite_sync_driver.execute("SELECT :id", id=1)

    assert result is sentinel
    assert called["args"] == ("SELECT :id", {"id": 1})


def test_execute_skips_fast_path_with_statement_config_override(sqlite_sync_driver, monkeypatch) -> None:
    called = False

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(sqlite_sync_driver, "_cached_execution", _fake_try)
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
    original_transform = ParameterProcessor._transform_cached_parameters

    def wrapped_transform(self: ParameterProcessor, *args: Any, **kwargs: Any) -> Any:
        calls.append(self)
        return original_transform(self, *args, **kwargs)

    monkeypatch.setattr(ParameterProcessor, "_transform_cached_parameters", wrapped_transform)

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

    with pytest.raises(SQLSpecError, match="SQLite database error: boom"):
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

    with pytest.raises(SQLSpecError, match="SQLite database error: boom"):
        sqlite_sync_driver._execute_cache_hit("INSERT INTO t (id) VALUES (?)", (1,), cached)


def test_prepare_cached_statement_named_style_sets_needs_rebind(sqlite_sync_driver: Any, monkeypatch: Any) -> None:
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

    prepared = sqlite_sync_driver._prepare_cached_statement("SELECT :id", (1,))

    assert prepared is not None
    assert called is True


@pytest.mark.anyio
async def test_async_execute_uses_fast_path_when_eligible(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(aiosqlite_async_driver, "_cached_execution", _fake_try)
    aiosqlite_async_driver._stmt_cache_enabled = True

    result = await aiosqlite_async_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


@pytest.mark.anyio
async def test_async_execute_uses_fast_path_with_kwargs_payload(aiosqlite_async_driver: Any, monkeypatch: Any) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any] | dict[str, Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(aiosqlite_async_driver, "_cached_execution", _fake_try)
    aiosqlite_async_driver._stmt_cache_enabled = True

    result = await aiosqlite_async_driver.execute("SELECT :id", id=1)

    assert result is sentinel
    assert called["args"] == ("SELECT :id", {"id": 1})


@pytest.mark.anyio
async def test_async_execute_skips_fast_path_with_statement_config_override(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    called = False

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(aiosqlite_async_driver, "_cached_execution", _fake_try)
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

    with pytest.raises(SQLSpecError, match="AIOSQLite database error: boom"):
        await aiosqlite_async_driver._execute_cached_statement(statement)


@pytest.mark.anyio
async def test_async_execute_cache_hit_re_raises_mapped_exception(
    aiosqlite_async_driver: Any, monkeypatch: Any
) -> None:
    import aiosqlite

    await aiosqlite_async_driver.execute("CREATE TABLE t (id INTEGER)")

    class FailingCursor:
        async def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            _ = (sql, params)
            raise aiosqlite.OperationalError("boom")

    class FailingCursorContext:
        async def __aenter__(self) -> FailingCursor:
            return FailingCursor()

        async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> None:
            _ = (exc_type, exc, tb)

    monkeypatch.setattr(aiosqlite_async_driver, "with_cursor", lambda _connection: FailingCursorContext())

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
        await aiosqlite_async_driver._execute_cache_hit("INSERT INTO t (id) VALUES (?)", (1,), cached)


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
