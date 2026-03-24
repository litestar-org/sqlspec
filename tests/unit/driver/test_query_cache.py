# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for fast-path query cache behavior."""

from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal, cast

import pytest

from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.core.compiler import OperationProfile, OperationType
from sqlspec.core.parameters import ParameterInfo, ParameterProfile
from sqlspec.core.statement import ProcessedState
from sqlspec.driver._common import CachedQuery, CommonDriverAttributesMixin
from sqlspec.driver._query_cache import QueryCache
from sqlspec.exceptions import SQLSpecError

_EMPTY_PS = ProcessedState("", [], None, "COMMAND")


def _make_cached(
    compiled_sql: str = "SQL",
    param_count: int = 0,
    operation_type: "OperationType" = "COMMAND",
    operation_profile: "OperationProfile | None" = None,
    parameter_profile: "ParameterProfile | None" = None,
    processed_state: "ProcessedState | None" = None,
) -> CachedQuery:
    """Helper to create CachedQuery instances with sensible defaults."""
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=parameter_profile or ParameterProfile.empty(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type=operation_type,
        operation_profile=operation_profile or OperationProfile.empty(),
        param_count=param_count,
        processed_state=processed_state or _EMPTY_PS,
    )


class _FakeDriver(CommonDriverAttributesMixin):
    __slots__ = ()

    def _stmt_cache_execute(self, statement: Any) -> Any:
        return statement


def test_stmt_cache_lru_eviction() -> None:
    cache = QueryCache(max_size=2)

    cache.set("a", _make_cached("SQL_A", 1))
    cache.set("b", _make_cached("SQL_B", 1))
    assert cache.get("a") is not None

    cache.set("c", _make_cached("SQL_C", 1))

    assert cache.get("b") is None
    assert cache.get("a") is not None
    assert cache.get("c") is not None


def test_stmt_cache_update_moves_to_end() -> None:
    cache = QueryCache(max_size=2)

    cache.set("a", _make_cached("SQL_A", 1))
    cache.set("b", _make_cached("SQL_B", 1))
    cache.set("a", _make_cached("SQL_A2", 2))
    cache.set("c", _make_cached("SQL_C", 1))

    assert cache.get("b") is None
    entry = cache.get("a")
    assert entry is not None
    assert entry.compiled_sql == "SQL_A2"
    assert entry.param_count == 2


def test_stmt_cache_lookup_cache_hit_rebinds() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        )
    )
    driver = _FakeDriver(object(), config)

    profile = ParameterProfile((ParameterInfo(None, ParameterStyle.QMARK, 0, 0, "?"),))
    ps = ProcessedState(compiled_sql="SELECT * FROM t WHERE id = ?", execution_parameters=[1], operation_type="SELECT")
    cached = CachedQuery(
        compiled_sql="SELECT * FROM t WHERE id = ?",
        parameter_profile=profile,
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=1,
        processed_state=ps,
    )
    driver._stmt_cache.set("SELECT * FROM t WHERE id = ?", cached)

    result = driver._stmt_cache_lookup("SELECT * FROM t WHERE id = ?", (1,))

    assert result is not None
    # Result is the SQL statement with processed state
    statement = cast("Any", result)
    assert statement.operation_type == "SELECT"
    compiled_sql, params = statement.compile()
    assert compiled_sql == "SELECT * FROM t WHERE id = ?"
    assert params == (1,)


def test_stmt_cache_store_snapshots_processed_state() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        )
    )
    driver = _FakeDriver(object(), config)
    statement = SQL("SELECT ?", (1,), statement_config=config)
    statement.compile()

    driver._stmt_cache_store(statement)
    cached = driver._stmt_cache.get("SELECT ?")
    assert cached is not None

    # Mutate/reset the original state after cache storage; cached metadata
    # should remain stable and independent.
    processed = cast("ProcessedState", statement.get_processed_state())
    processed.reset()

    assert cached.compiled_sql == "SELECT ?"
    assert cached.processed_state.compiled_sql == "SELECT ?"
    assert cached.processed_state.operation_type == "SELECT"


def test_prepare_driver_parameters_many_passes_through_irrelevant_coercion_map() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
            type_coercion_map={bool: lambda value: 1 if value else 0},
        )
    )
    driver = _FakeDriver(object(), config)
    parameters = [("a",), ("b",), ("c",)]

    prepared = driver.prepare_driver_parameters(parameters, config, is_many=True)

    assert prepared is parameters


def test_prepare_driver_parameters_many_coerces_rows_when_needed() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
            type_coercion_map={bool: lambda value: 1 if value else 0},
        )
    )
    driver = _FakeDriver(object(), config)
    parameters = [(True,), ("b",)]

    prepared = driver.prepare_driver_parameters(parameters, config, is_many=True)

    assert isinstance(prepared, list)
    assert prepared is not parameters
    assert tuple(prepared[0]) == (1,)
    assert tuple(prepared[1]) == ("b",)


def test_prepare_driver_parameters_many_coerces_subclass_rows_when_needed() -> None:
    class MyInt(int):
        pass

    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
            type_coercion_map={int: lambda value: value + 1},
        )
    )
    driver = _FakeDriver(object(), config)
    parameters = [(MyInt(2),), ("b",)]

    prepared = driver.prepare_driver_parameters(parameters, config, is_many=True)

    assert isinstance(prepared, list)
    assert prepared is not parameters
    assert tuple(prepared[0]) == (3,)
    assert tuple(prepared[1]) == ("b",)


def test_prepare_driver_parameters_many_coerces_virtual_abc_rows_when_needed() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
            type_coercion_map={Sequence: lambda value: tuple(value)},
        )
    )
    driver = _FakeDriver(object(), config)
    fallback_items = ((Sequence, lambda value: tuple(value)),)

    prepared = driver._apply_coercion_with_fallback(  # pyright: ignore[reportPrivateUsage]
        [1, 2], config.parameter_config.type_coercion_map, fallback_items
    )

    assert prepared == (1, 2)


def test_sync_stmt_cache_execute_direct_uses_dispatch_path(mock_sync_driver, monkeypatch) -> None:
    class _CursorManager:
        def __enter__(self) -> object:
            return object()

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> "Literal[False]":
            _ = (exc_type, exc_val, exc_tb)
            return False

    def _fake_with_cursor(_connection: Any) -> _CursorManager:
        return _CursorManager()

    def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        # Regression test: direct cache execution should not require cursor.execute().
        assert not hasattr(cursor, "execute")
        return mock_sync_driver.create_execution_result(cursor, rowcount_override=7)

    monkeypatch.setattr(mock_sync_driver, "with_cursor", _fake_with_cursor)
    monkeypatch.setattr(mock_sync_driver, "dispatch_execute", _fake_dispatch_execute)

    cached = _make_cached(
        compiled_sql="INSERT INTO t (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO t (id) VALUES (?)", execution_parameters=[1], operation_type="INSERT"
        ),
    )

    result = mock_sync_driver._stmt_cache_execute_direct("INSERT INTO t (id) VALUES (?)", (1,), cached)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 7


def test_execute_uses_fast_path_when_eligible(mock_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(mock_sync_driver, "_stmt_cache_lookup", _fake_try)
    mock_sync_driver._stmt_cache_enabled = True

    result = mock_sync_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


def test_execute_skips_fast_path_with_statement_config_override(mock_sync_driver, monkeypatch) -> None:
    called = False

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(mock_sync_driver, "_stmt_cache_lookup", _fake_try)
    mock_sync_driver._stmt_cache_enabled = True

    statement_config = mock_sync_driver.statement_config.replace()
    result = mock_sync_driver.execute("SELECT ?", (1,), statement_config=statement_config)

    assert called is False
    assert result.operation_type == "SELECT"


def test_execute_populates_fast_path_cache_on_normal_path(mock_sync_driver) -> None:
    mock_sync_driver._stmt_cache_enabled = True

    assert mock_sync_driver._stmt_cache.get("SELECT ?") is None

    result = mock_sync_driver.execute("SELECT ?", (1,))

    cached = mock_sync_driver._stmt_cache.get("SELECT ?")
    assert cached is not None
    assert cached.param_count == 1
    assert cached.operation_type == "SELECT"
    assert result.operation_type == "SELECT"


def test_sync_stmt_cache_execute_re_raises_mapped_exception(mock_sync_driver: Any, monkeypatch: Any) -> None:
    def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise ValueError("boom")

    monkeypatch.setattr(mock_sync_driver, "dispatch_execute", _fake_dispatch_execute)
    statement = SQL("SELECT ?", (1,), statement_config=mock_sync_driver.statement_config)
    statement.compile()

    with pytest.raises(SQLSpecError, match="Mock database error: boom"):
        mock_sync_driver._stmt_cache_execute(statement)


def test_sync_stmt_cache_execute_direct_re_raises_mapped_exception(mock_sync_driver: Any, monkeypatch: Any) -> None:
    def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise ValueError("boom")

    monkeypatch.setattr(mock_sync_driver, "dispatch_execute", _fake_dispatch_execute)
    cached = _make_cached(
        compiled_sql="INSERT INTO t (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO t (id) VALUES (?)", execution_parameters=[1], operation_type="INSERT"
        ),
    )

    with pytest.raises(SQLSpecError, match="Mock database error: boom"):
        mock_sync_driver._stmt_cache_execute_direct("INSERT INTO t (id) VALUES (?)", (1,), cached)


@pytest.mark.anyio
async def test_async_execute_uses_fast_path_when_eligible(mock_async_driver: Any, monkeypatch: Any) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(mock_async_driver, "_stmt_cache_lookup", _fake_try)
    mock_async_driver._stmt_cache_enabled = True

    result = await mock_async_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


@pytest.mark.anyio
async def test_async_execute_skips_fast_path_with_statement_config_override(
    mock_async_driver: Any, monkeypatch: Any
) -> None:
    called = False

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(mock_async_driver, "_stmt_cache_lookup", _fake_try)
    mock_async_driver._stmt_cache_enabled = True

    statement_config = mock_async_driver.statement_config.replace()
    result = await mock_async_driver.execute("SELECT ?", (1,), statement_config=statement_config)

    assert called is False
    assert result.operation_type == "SELECT"


@pytest.mark.anyio
async def test_async_execute_populates_fast_path_cache_on_normal_path(mock_async_driver: Any) -> None:
    mock_async_driver._stmt_cache_enabled = True

    assert mock_async_driver._stmt_cache.get("SELECT ?") is None

    result = await mock_async_driver.execute("SELECT ?", (1,))

    cached = mock_async_driver._stmt_cache.get("SELECT ?")
    assert cached is not None
    assert cached.param_count == 1
    assert cached.operation_type == "SELECT"
    assert result.operation_type == "SELECT"


@pytest.mark.anyio
async def test_async_stmt_cache_execute_re_raises_mapped_exception(mock_async_driver: Any, monkeypatch: Any) -> None:
    async def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise ValueError("boom")

    monkeypatch.setattr(mock_async_driver, "dispatch_execute", _fake_dispatch_execute)
    statement = SQL("SELECT ?", (1,), statement_config=mock_async_driver.statement_config)
    statement.compile()

    with pytest.raises(SQLSpecError, match="Mock async database error: boom"):
        await mock_async_driver._stmt_cache_execute(statement)


@pytest.mark.anyio
async def test_async_stmt_cache_execute_direct_re_raises_mapped_exception(
    mock_async_driver: Any, monkeypatch: Any
) -> None:
    async def _fake_dispatch_execute(cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise ValueError("boom")

    monkeypatch.setattr(mock_async_driver, "dispatch_execute", _fake_dispatch_execute)
    cached = _make_cached(
        compiled_sql="INSERT INTO t (id) VALUES (?)",
        param_count=1,
        operation_type="INSERT",
        operation_profile=OperationProfile(returns_rows=False, modifies_rows=True),
        processed_state=ProcessedState(
            compiled_sql="INSERT INTO t (id) VALUES (?)", execution_parameters=[1], operation_type="INSERT"
        ),
    )

    with pytest.raises(SQLSpecError, match="Mock async database error: boom"):
        await mock_async_driver._stmt_cache_execute_direct("INSERT INTO t (id) VALUES (?)", (1,), cached)


def test_stmt_cache_thread_safety() -> None:
    cache = QueryCache(max_size=32)
    cached = _make_cached()
    for idx in range(16):
        cache.set(str(idx), cached)

    def worker(seed: int) -> None:
        for i in range(200):
            key = str((seed + i) % 16)
            cache.get(key)
            if i % 5 == 0:
                cache.set(key, cached)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(worker, range(4)))
