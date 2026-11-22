"""Unit tests for the Oracle compiled cache-hit executors."""

import inspect
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import oracledb
import pytest

from sqlspec.adapters.oracledb.core import (
    OracleVersionCache,
    execute_cache_hit,
    execute_cache_hit_async,
    normalize_lastrowid,
    resolve_rowcount,
)
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.result._base import DMLResult
from sqlspec.exceptions import UniqueViolationError

SELECT_SQL = "SELECT id, body FROM articles"
SELECT_BY_ID_SQL = "SELECT id, body FROM articles WHERE id = :1"
INSERT_SQL = "INSERT INTO articles (body) VALUES (:1)"
UPDATE_SQL = "UPDATE articles SET body = 'x'"
DELETE_SQL = "DELETE FROM articles WHERE id = :1"
_MISSING = object()


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
    session.cursor.lastrowid = "AAAR3qAAEAAAACTAAA"
    first = await session.execute(UPDATE_SQL)

    result = await session.run_executor(UPDATE_SQL, ())

    assert first.operation_type == "UPDATE"
    assert first.rows_affected == 4
    assert first.last_inserted_id is None
    assert isinstance(result, DMLResult)
    assert result.operation_type == "UPDATE"
    assert result.rows_affected == 4
    assert result.last_inserted_id is None
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


class _CountingCursorBase:
    """Base fake cursor tracking ``lastrowid`` descriptor accesses."""

    def __init__(self, rowcount: int, lastrowid_value: Any) -> None:
        self.description: Any = None
        self.rowcount = rowcount
        self._lastrowid_value = lastrowid_value
        self.lastrowid_reads = 0

    @property
    def lastrowid(self) -> Any:
        self.lastrowid_reads += 1
        return self._lastrowid_value

    def close(self) -> None:
        return None


class _SyncCountingCursor(_CountingCursorBase):
    """Sync fake cursor tracking ``lastrowid`` descriptor accesses."""

    def execute(self, sql: str, params: Any = None, **kwargs: Any) -> None:
        _ = (sql, params, kwargs)


class _AsyncCountingCursor(_CountingCursorBase):
    """Async fake cursor tracking ``lastrowid`` descriptor accesses."""

    async def execute(self, sql: str, params: Any = None, **kwargs: Any) -> None:
        _ = (sql, params, kwargs)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("AAAR3qAAEAAAACTAAA", "AAAR3qAAEAAAACTAAA"),
        ("", None),
        (42, 42),
        (0, 0),
        (True, None),
        (False, None),
        (None, None),
        (object(), None),
        (_MISSING, None),
    ],
)
def test_normalize_lastrowid_and_resolve_last_inserted_id(is_async: bool, raw_value: Any, expected: Any) -> None:
    cursor = SimpleNamespace() if raw_value is _MISSING else SimpleNamespace(lastrowid=raw_value)
    session = _Session(is_async)

    normalized = normalize_lastrowid(cursor)
    resolved = session.driver.resolve_last_inserted_id(cursor)

    assert normalized == expected
    assert resolved == expected
    if expected is None:
        assert normalized is None
        assert resolved is None
    elif isinstance(expected, int):
        assert type(normalized) is int
        assert type(resolved) is int


def test_normalize_lastrowid_ignores_magic_mock_default() -> None:
    mock_cursor = MagicMock()
    assert normalize_lastrowid(mock_cursor) is None


@pytest.mark.parametrize(
    ("raw_value", "expected"), [(True, 0), (False, 0), (-1, 0), (0, 0), (3, 3), (None, 0), ("5", 0), (_MISSING, 0)]
)
def test_resolve_rowcount_rejects_bool_and_invalid_values(is_async: bool, raw_value: Any, expected: int) -> None:
    cursor = SimpleNamespace() if raw_value is _MISSING else SimpleNamespace(rowcount=raw_value)
    session = _Session(is_async)

    direct = resolve_rowcount(cursor)
    via_driver = session.driver.resolve_rowcount(cursor)

    assert direct == expected
    assert type(direct) is int
    assert via_driver == expected
    assert type(via_driver) is int


@pytest.mark.parametrize(
    ("lastrowid_value", "rowcount", "expected"),
    [
        ("AAAR3qAAEAAAACTAAA", 1, "AAAR3qAAEAAAACTAAA"),
        ("AAAR3qAAEAAAACTAAA", 0, None),
        ("", 1, None),
        ("", 0, None),
        (42, 1, 42),
        (42, 0, None),
        (True, 1, None),
        (True, 0, None),
        (None, 1, None),
        (None, 0, None),
    ],
)
async def test_insert_last_inserted_id_parity_across_all_execution_paths(
    is_async: bool, lastrowid_value: Any, rowcount: int, expected: Any
) -> None:
    session = _Session(is_async)
    session.cursor.description = None
    session.cursor.rowcount = rowcount
    session.cursor.lastrowid = lastrowid_value

    uncached = await session.execute(INSERT_SQL, "hello")
    compiled_cache_hit = await session.execute(INSERT_SQL, "hello")
    assert session.driver.set_cached_statement_executor(None) is True
    assert session.driver._cache_hit_executor is None
    fallback_cache_hit = await session.execute(INSERT_SQL, "hello")

    assert uncached.rows_affected == rowcount
    assert compiled_cache_hit.rows_affected == rowcount
    assert fallback_cache_hit.rows_affected == rowcount
    assert uncached.last_inserted_id == expected
    assert compiled_cache_hit.last_inserted_id == expected
    assert fallback_cache_hit.last_inserted_id == expected
    assert isinstance(compiled_cache_hit, DMLResult)
    assert isinstance(fallback_cache_hit, DMLResult)


@pytest.mark.parametrize(
    ("sql", "params", "rowcount", "expected_reads", "expected_id"),
    [
        (INSERT_SQL, ("hello",), 1, 1, "AAAR3qAAEAAAACTAAA"),
        (INSERT_SQL, ("hello",), 0, 0, None),
        (UPDATE_SQL, (), 1, 0, None),
        (DELETE_SQL, (7,), 1, 0, None),
    ],
)
async def test_lastrowid_descriptor_read_count_across_all_paths(
    is_async: bool, sql: str, params: tuple[Any, ...], rowcount: int, expected_reads: int, expected_id: str | None
) -> None:
    cursor_cls = _AsyncCountingCursor if is_async else _SyncCountingCursor
    cursor = cursor_cls(rowcount=rowcount, lastrowid_value="AAAR3qAAEAAAACTAAA")
    session = _Session(is_async)
    session.connection.cursor.return_value = cursor

    uncached = await session.execute(sql, *params)
    assert cursor.lastrowid_reads == expected_reads
    assert uncached.last_inserted_id == expected_id

    cursor.lastrowid_reads = 0
    cached = await session.execute(sql, *params)
    assert cursor.lastrowid_reads == expected_reads
    assert cached.last_inserted_id == expected_id

    cursor.lastrowid_reads = 0
    assert session.driver.set_cached_statement_executor(None) is True
    fallback = await session.execute(sql, *params)
    assert cursor.lastrowid_reads == expected_reads
    assert fallback.last_inserted_id == expected_id


def test_normalize_lastrowid_reads_descriptor_once() -> None:
    cursor = _SyncCountingCursor(rowcount=1, lastrowid_value="AAAR3qAAEAAAACTAAA")
    assert normalize_lastrowid(cursor) == "AAAR3qAAEAAAACTAAA"
    assert cursor.lastrowid_reads == 1


@pytest.mark.parametrize(("sql", "params", "expected_op"), [(UPDATE_SQL, (), "UPDATE"), (DELETE_SQL, (1,), "DELETE")])
async def test_non_insert_dml_never_leaks_rowid(
    is_async: bool, sql: str, params: tuple[Any, ...], expected_op: str
) -> None:
    session = _Session(is_async)
    session.cursor.description = None
    session.cursor.rowcount = 2
    session.cursor.lastrowid = "AAAR3qAAEAAAACTAAA"

    uncached = await session.execute(sql, *params)
    cached = await session.execute(sql, *params)
    assert session.driver.set_cached_statement_executor(None) is True
    fallback = await session.execute(sql, *params)

    for res in (uncached, cached, fallback):
        assert res.operation_type == expected_op
        assert res.rows_affected == 2
        assert res.last_inserted_id is None


async def test_subclass_cache_hit_executor_and_resolve_last_inserted_id_override(is_async: bool) -> None:
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 1
    cursor.lastrowid = "AAAR3qAAEAAAACTAAA"
    cursor.execute = AsyncMock() if is_async else MagicMock()
    connection.cursor.return_value = cursor

    if is_async:

        class _EmptyAsyncDriver(OracleAsyncDriver):
            pass

        class _CustomAsyncDriver(OracleAsyncDriver):
            def resolve_last_inserted_id(self, cursor: Any) -> int | str | None:
                _ = cursor
                return "custom-rowid"

        empty_driver: Any = _EmptyAsyncDriver(cast("Any", connection))
        assert empty_driver._cache_hit_executor is execute_cache_hit_async
        sub_driver: Any = _CustomAsyncDriver(cast("Any", connection))
    else:

        class _EmptySyncDriver(OracleSyncDriver):
            pass

        class _CustomSyncDriver(OracleSyncDriver):
            def resolve_last_inserted_id(self, cursor: Any) -> int | str | None:
                _ = cursor
                return "custom-rowid"

        empty_driver = _EmptySyncDriver(cast("Any", connection))
        assert empty_driver._cache_hit_executor is execute_cache_hit
        sub_driver = _CustomSyncDriver(cast("Any", connection))

    assert sub_driver._cache_hit_executor is None

    first = await _resolve(sub_driver.execute(INSERT_SQL, "first"))
    second = await _resolve(sub_driver.execute(INSERT_SQL, "second"))

    assert first.rows_affected == 1
    assert first.last_inserted_id == "AAAR3qAAEAAAACTAAA"
    assert isinstance(second, DMLResult)
    assert second.rows_affected == 1
    assert second.last_inserted_id == "custom-rowid"


def test_clear_cache_hit_executor_preserves_oracle_session_state(is_async: bool) -> None:
    session = _Session(is_async)
    version_cache = OracleVersionCache()
    session.driver._oracle_version_cache = version_cache
    session.driver._otel_tracing_enabled = True

    assert session.driver.set_cached_statement_executor(None) is True
    assert session.driver._cache_hit_executor is None
    assert session.driver._cache_hit_state is None
    assert session.driver._oracle_version_cache is version_cache
    assert session.driver._otel_tracing_enabled is True

    replacement_cache = OracleVersionCache()
    session.driver._oracle_version_cache = replacement_cache
    session.driver._otel_tracing_enabled = False
    assert session.driver._oracle_version_cache is replacement_cache
    assert session.driver._otel_tracing_enabled is False
