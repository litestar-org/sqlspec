"""Unit tests for the Oracle compiled cache-hit executors."""

import inspect
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import oracledb
import pytest

from sqlspec.adapters.oracledb.core import OracleVersionCache, execute_cache_hit, execute_cache_hit_async
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.result._base import DMLResult
from sqlspec.exceptions import UniqueViolationError

SELECT_SQL = "SELECT id, body FROM articles"
SELECT_BY_ID_SQL = "SELECT id, body FROM articles WHERE id = :1"
UPDATE_SQL = "UPDATE articles SET body = 'x'"


class _Lob:
    """LOB locator that must be read before use."""

    def __init__(self, payload: str) -> None:
        self.payload = payload

    def read(self) -> str:
        return self.payload


class _AsyncLob:
    """Async LOB locator that must be awaited before use."""

    def __init__(self, payload: str) -> None:
        self.payload = payload

    async def read(self) -> str:
        return self.payload


class _Session:
    """Oracle driver over a mocked connection, driven the same way in sync and async mode."""

    def __init__(self, is_async: bool, driver_features: "dict[str, Any] | None" = None) -> None:
        self.is_async = is_async
        self.cursor = MagicMock()
        self.cursor.description = [("id", "NUMBER"), ("body", "VARCHAR2")]
        self.cursor.rowcount = 0
        self.cursor.execute = AsyncMock() if is_async else MagicMock()
        self.cursor.fetchall = AsyncMock(return_value=[]) if is_async else MagicMock(return_value=[])
        self.connection = MagicMock()
        self.connection.cursor.return_value = self.cursor
        driver_class = OracleAsyncDriver if is_async else OracleSyncDriver
        self.driver: Any = driver_class(cast("Any", self.connection), driver_features=driver_features)
        self.executor: Any = execute_cache_hit_async if is_async else execute_cache_hit

    async def execute(self, statement: str, *params: Any) -> Any:
        return await _resolve(self.driver.execute(statement, *params))

    async def run_executor(self, statement: str, params: Any) -> Any:
        driver = self.driver
        cached = driver._stmt_cache.get(statement)
        return await _resolve(
            self.executor(
                driver.connection, statement, params, cached, driver.statement_config, driver._cache_hit_state
            )
        )

    def lob(self, payload: str) -> Any:
        return _AsyncLob(payload) if self.is_async else _Lob(payload)


async def _resolve(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@pytest.fixture(params=["sync", "async"])
def is_async(request: pytest.FixtureRequest) -> bool:
    return bool(request.param == "async")


async def test_repeated_select_runs_through_cache_hit_executor(is_async: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    session = _Session(is_async)
    session.cursor.fetchall.return_value = [(1, "first")]
    calls: list[tuple[Any, ...]] = []

    def recording_executor(*args: Any) -> Any:
        calls.append(args)
        return session.executor(*args)

    monkeypatch.setattr(session.driver, "_cache_hit_executor", recording_executor)

    await session.execute(SELECT_SQL)
    session.cursor.close.reset_mock()
    result = await session.execute(SELECT_SQL)

    assert len(calls) == 1
    assert result.get_data() == [{"id": 1, "body": "first"}]
    assert result.column_names == ["id", "body"]
    assert result.rows_affected == 1
    assert result.statement.sql == SELECT_SQL
    session.cursor.execute.assert_called_with(SELECT_SQL, {})
    session.cursor.close.assert_called_once_with()


async def test_cache_hit_passes_parameters_fetch_options_and_cursor_tuning(is_async: bool) -> None:
    session = _Session(is_async, {"fetch_lobs": False, "fetch_decimals": True, "arraysize": 50, "prefetchrows": 10})
    session.cursor.fetchall.return_value = [(7, "seventh")]
    await session.execute(SELECT_BY_ID_SQL, 7)

    result = await session.run_executor(SELECT_BY_ID_SQL, (7,))

    assert result.get_data() == [{"id": 7, "body": "seventh"}]
    session.cursor.execute.assert_called_with(SELECT_BY_ID_SQL, (7,), fetch_lobs=False, fetch_decimals=True)
    assert session.cursor.arraysize == 50
    assert session.cursor.prefetchrows == 10


async def test_cache_hit_reads_lob_locators(is_async: bool) -> None:
    session = _Session(is_async)
    await session.execute(SELECT_SQL)
    session.cursor.fetchall.return_value = [(1, session.lob("large body"))]

    result = await session.run_executor(SELECT_SQL, ())

    assert result.get_data() == [{"id": 1, "body": "large body"}]


async def test_cache_hit_dml_returns_affected_rows(is_async: bool) -> None:
    session = _Session(is_async)
    session.cursor.description = None
    session.cursor.rowcount = 4
    await session.execute(UPDATE_SQL)

    result = await session.run_executor(UPDATE_SQL, ())

    assert isinstance(result, DMLResult)
    assert result.operation_type == "UPDATE"
    assert result.rows_affected == 4
    session.cursor.fetchall.assert_not_called()


async def test_cache_hit_maps_database_errors_and_closes_cursor(is_async: bool) -> None:
    session = _Session(is_async)
    session.cursor.description = None
    await session.execute(UPDATE_SQL)
    native = oracledb.DatabaseError("ORA-00001: unique constraint violated")
    session.cursor.execute.side_effect = native
    session.cursor.close.reset_mock()

    with pytest.raises(UniqueViolationError) as exc_info:
        await session.execute(UPDATE_SQL)

    assert exc_info.value.__cause__ is native
    session.cursor.close.assert_called_once_with()


async def test_cache_hit_propagates_errors_outside_database_error_unchanged(is_async: bool) -> None:
    session = _Session(is_async)
    session.cursor.description = None
    await session.execute(UPDATE_SQL)
    native = oracledb.InterfaceError("DPY-1001: not connected to database")
    session.cursor.execute.side_effect = native

    with pytest.raises(oracledb.InterfaceError) as exc_info:
        await session.run_executor(UPDATE_SQL, ())

    assert exc_info.value is native


async def test_cache_hit_applies_session_tracing_before_the_statement(is_async: bool) -> None:
    session = _Session(is_async, {"enable_session_tracing": True})
    session.cursor.fetchall.return_value = [(1, "first")]
    await session.execute(SELECT_SQL)
    session.connection.module = None

    result = await session.run_executor(SELECT_SQL, ())

    assert result.get_data() == [{"id": 1, "body": "first"}]
    assert session.connection.module == "sqlspec"
    session.cursor.execute.assert_called_with(SELECT_SQL, {})


async def test_cache_hit_propagates_trace_context_when_enabled(is_async: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlspec.adapters.oracledb.core as oracle_core

    session = _Session(is_async)
    await session.execute(SELECT_SQL)
    session.driver._otel_tracing_enabled = True
    applied: list[dict[str, Any]] = []

    def recording_trace_attributes(connection: Any, **kwargs: Any) -> None:
        applied.append(kwargs)

    monkeypatch.setattr(oracle_core, "apply_oracle_session_trace_attributes", recording_trace_attributes)

    await session.run_executor(SELECT_SQL, ())

    assert applied == [
        {
            "module": "sqlspec",
            "action": None,
            "preserve_client_identifier": False,
            "enable_session_attributes": False,
            "enable_otel_trace_context": True,
        }
    ]


async def test_cache_hit_strips_sqlcommenter_comment_under_session_tracing(is_async: bool) -> None:
    from sqlspec.adapters.oracledb.core import default_statement_config

    config = default_statement_config.replace(enable_sqlcommenter=True, sqlcommenter_attributes={"route": "/articles"})
    session = _Session(is_async)
    session.driver = type(session.driver)(cast("Any", session.connection), statement_config=config)
    session.cursor.fetchall.return_value = [(1, "first")]
    await session.execute(SELECT_SQL)
    session.cursor.execute.reset_mock()

    result = await session.execute(SELECT_SQL)

    assert result.get_data() == [{"id": 1, "body": "first"}]
    session.cursor.execute.assert_called_once_with(SELECT_SQL, {})
    assert session.connection.action == "/articles"


async def test_cache_hit_uses_version_cache_attached_after_construction(
    is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sqlspec.adapters.oracledb.core as oracle_core

    session = _Session(is_async)
    session.cursor.description = None
    await session.execute(SELECT_BY_ID_SQL, 1)
    version_cache = OracleVersionCache()
    session.driver._oracle_version_cache = version_cache
    seen: list[Any] = []
    binding_state = oracle_core._OracleJsonBindingState

    def recording_binding_state(connection: Any, cache: Any) -> Any:
        seen.append(cache)
        return binding_state(connection, cache)

    monkeypatch.setattr(oracle_core, "_OracleJsonBindingState", recording_binding_state)

    await session.run_executor(SELECT_BY_ID_SQL, (1,))

    assert seen == [version_cache]
    assert session.driver._oracle_version_cache is version_cache
