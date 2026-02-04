# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for fast-path query cache behavior."""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

import pytest

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.core.compiler import OperationProfile
from sqlspec.core.parameters import ParameterInfo, ParameterProfile
from sqlspec.driver._common import CachedQuery, CommonDriverAttributesMixin
from sqlspec.driver._query_cache import QueryCache


class _FakeDriver(CommonDriverAttributesMixin):
    __slots__ = ()

    def _qc_execute(self, statement: Any) -> Any:
        return statement


def test_qc_lru_eviction() -> None:
    cache = QueryCache(max_size=2)

    cache.set(
        "a", CachedQuery("SQL_A", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1)
    )
    cache.set(
        "b", CachedQuery("SQL_B", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1)
    )
    assert cache.get("a") is not None

    cache.set(
        "c", CachedQuery("SQL_C", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1)
    )

    assert cache.get("b") is None
    assert cache.get("a") is not None
    assert cache.get("c") is not None


def test_qc_update_moves_to_end() -> None:
    cache = QueryCache(max_size=2)

    cache.set(
        "a", CachedQuery("SQL_A", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1)
    )
    cache.set(
        "b", CachedQuery("SQL_B", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1)
    )
    cache.set(
        "a", CachedQuery("SQL_A2", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 2)
    )
    cache.set(
        "c", CachedQuery("SQL_C", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1)
    )

    assert cache.get("b") is None
    entry = cache.get("a")
    assert entry is not None
    assert entry.compiled_sql == "SQL_A2"
    assert entry.param_count == 2


def test_qc_lookup_cache_hit_rebinds() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        )
    )
    driver = _FakeDriver(object(), config)

    profile = ParameterProfile((ParameterInfo(None, ParameterStyle.QMARK, 0, 0, "?"),))
    cached = CachedQuery(
        compiled_sql="SELECT * FROM t WHERE id = ?",
        parameter_profile=profile,
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=1,
    )
    driver._qc.set("SELECT * FROM t WHERE id = ?", cached)

    result = driver._qc_lookup("SELECT * FROM t WHERE id = ?", (1,))

    assert result is not None
    # Result is the SQL statement with processed state
    statement = cast("Any", result)
    assert statement.operation_type == "SELECT"
    compiled_sql, params = statement.compile()
    assert compiled_sql == "SELECT * FROM t WHERE id = ?"
    assert params == (1,)


def test_execute_uses_fast_path_when_eligible(mock_sync_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(mock_sync_driver, "_qc_lookup", _fake_try)
    mock_sync_driver._qc_enabled = True

    result = mock_sync_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


def test_execute_skips_fast_path_with_statement_config_override(mock_sync_driver, monkeypatch) -> None:
    called = False

    def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(mock_sync_driver, "_qc_lookup", _fake_try)
    mock_sync_driver._qc_enabled = True

    statement_config = mock_sync_driver.statement_config.replace()
    result = mock_sync_driver.execute("SELECT ?", (1,), statement_config=statement_config)

    assert called is False
    assert result.operation_type == "SELECT"


def test_execute_populates_fast_path_cache_on_normal_path(mock_sync_driver) -> None:
    mock_sync_driver._qc_enabled = True

    assert mock_sync_driver._qc.get("SELECT ?") is None

    result = mock_sync_driver.execute("SELECT ?", (1,))

    cached = mock_sync_driver._qc.get("SELECT ?")
    assert cached is not None
    assert cached.param_count == 1
    assert cached.operation_type == "SELECT"
    assert result.operation_type == "SELECT"


@pytest.mark.asyncio
async def test_async_execute_uses_fast_path_when_eligible(mock_async_driver, monkeypatch) -> None:
    sentinel = object()
    called: dict[str, object] = {}

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        called["args"] = (statement, params)
        return sentinel

    monkeypatch.setattr(mock_async_driver, "_qc_lookup", _fake_try)
    mock_async_driver._qc_enabled = True

    result = await mock_async_driver.execute("SELECT ?", (1,))

    assert result is sentinel
    assert called["args"] == ("SELECT ?", (1,))


@pytest.mark.asyncio
async def test_async_execute_skips_fast_path_with_statement_config_override(mock_async_driver, monkeypatch) -> None:
    called = False

    async def _fake_try(statement: str, params: tuple[Any, ...] | list[Any]) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(mock_async_driver, "_qc_lookup", _fake_try)
    mock_async_driver._qc_enabled = True

    statement_config = mock_async_driver.statement_config.replace()
    result = await mock_async_driver.execute("SELECT ?", (1,), statement_config=statement_config)

    assert called is False
    assert result.operation_type == "SELECT"


@pytest.mark.asyncio
async def test_async_execute_populates_fast_path_cache_on_normal_path(mock_async_driver) -> None:
    mock_async_driver._qc_enabled = True

    assert mock_async_driver._qc.get("SELECT ?") is None

    result = await mock_async_driver.execute("SELECT ?", (1,))

    cached = mock_async_driver._qc.get("SELECT ?")
    assert cached is not None
    assert cached.param_count == 1
    assert cached.operation_type == "SELECT"
    assert result.operation_type == "SELECT"


def test_qc_thread_safety() -> None:
    cache = QueryCache(max_size=32)
    cached = CachedQuery("SQL", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 0)
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
