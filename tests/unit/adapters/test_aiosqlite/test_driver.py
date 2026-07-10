import asyncio
import sqlite3
import time
from collections.abc import Callable, Iterator
from threading import Thread
from typing import Any, cast

import aiosqlite
import pytest

from sqlspec.adapters.aiosqlite._typing import AiosqliteCursor
from sqlspec.adapters.aiosqlite.core import (
    build_profile,
    build_statement_config,
    default_statement_config,
    driver_profile,
    execute_and_resolve_rowcount,
    execute_fetchall_with_description,
    run_on_worker_thread,
)
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool
from sqlspec.core import SQL, ParameterStyle
from sqlspec.core.result import DMLResult
from sqlspec.exceptions import SQLSpecError


def test_rowid_eligibility_falls_back_when_table_list_is_unavailable() -> None:
    from sqlspec.adapters.aiosqlite.core import _target_supports_rowid

    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE rowid_target (id INTEGER PRIMARY KEY)")
    connection.execute("CREATE TABLE without_rowid_target (id TEXT PRIMARY KEY) WITHOUT ROWID")
    connection.execute("CREATE TABLE shadowed_without_rowid (rowid TEXT PRIMARY KEY) WITHOUT ROWID")

    class LegacyConnection:
        def execute(self, sql: str, parameters: object = ()) -> sqlite3.Cursor:
            if sql == "PRAGMA table_list":
                raise sqlite3.OperationalError
            return connection.execute(sql, cast("Any", parameters))

    legacy_connection = LegacyConnection()
    try:
        assert _target_supports_rowid(legacy_connection, (None, "rowid_target"))
        assert not _target_supports_rowid(legacy_connection, (None, "without_rowid_target"))
        assert not _target_supports_rowid(legacy_connection, (None, "shadowed_without_rowid"))
    finally:
        connection.close()


async def test_execute_fetchall_with_description_preserves_exported_contract() -> None:
    connection = await aiosqlite.connect(":memory:")
    try:
        result = await run_on_worker_thread(
            connection, execute_fetchall_with_description, connection, "SELECT 1 AS value", ()
        )
        rows, description = result
        assert rows == [(1,)]
        assert description[0][0] == "value"
    finally:
        await connection.close()


async def test_execute_and_resolve_rowcount_preserves_exported_contract() -> None:
    connection = await aiosqlite.connect(":memory:")
    try:
        await connection.execute("CREATE TABLE helper_contract (id INTEGER PRIMARY KEY)")
        result = await run_on_worker_thread(
            connection, execute_and_resolve_rowcount, connection, "INSERT INTO helper_contract (id) VALUES (?)", (1,)
        )
        assert result == 1
    finally:
        await connection.close()


async def test_cursor_lifecycle_cursor_closed_after_normal_exit() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        async with AiosqliteCursor(conn) as cursor:
            await cursor.execute("SELECT 1")
        with pytest.raises(sqlite3.ProgrammingError, match="closed cursor"):
            cursor._cursor.execute("SELECT 1")
    finally:
        await conn.close()


async def test_cursor_lifecycle_cursor_closed_after_exception_in_block() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        captured_cursor = None
        with pytest.raises(aiosqlite.OperationalError):
            async with AiosqliteCursor(conn) as cursor:
                captured_cursor = cursor
                await cursor.execute("SELECT * FROM nonexistent_table_xyz")
        assert captured_cursor is not None
        with pytest.raises(sqlite3.ProgrammingError, match="closed cursor"):
            captured_cursor._cursor.execute("SELECT 1")
    finally:
        await conn.close()


async def test_cursor_lifecycle_cursor_closed_after_generic_exception_in_block() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        captured_cursor = None
        with pytest.raises(ValueError, match="simulated error"):
            async with AiosqliteCursor(conn) as cursor:
                captured_cursor = cursor
                raise ValueError("simulated error")
        assert captured_cursor is not None
        with pytest.raises(sqlite3.ProgrammingError, match="closed cursor"):
            captured_cursor._cursor.execute("SELECT 1")
    finally:
        await conn.close()


async def test_cursor_lifecycle_aexit_does_not_suppress_exception() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        with pytest.raises(aiosqlite.OperationalError):
            async with AiosqliteCursor(conn) as cursor:
                await cursor.execute("SELECT * FROM nonexistent_table_xyz")
    finally:
        await conn.close()


class _MappingRow:
    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def keys(self) -> object:
        return self._data.keys()

    def __getitem__(self, key: str) -> object:
        return self._data[key]


class _SelectCursor:
    def __init__(self) -> None:
        self.description = [("id",), ("name",)]
        self.rowcount = 2
        self.closed = False

    def fetchall(self) -> list[_MappingRow]:
        return [_MappingRow({"id": 1, "name": "alice"})]

    def close(self) -> None:
        self.closed = True


class _WorkerConnection:
    def __init__(self) -> None:
        self._conn = self
        self.cursor = _SelectCursor()
        self.executemany_calls: list[tuple[str, object]] = []

    def execute(self, sql: str, parameters: object) -> _SelectCursor:
        _ = sql, parameters
        return self.cursor

    async def executemany(self, sql: str, parameters: object) -> _SelectCursor:
        self.executemany_calls.append((sql, parameters))
        return self.cursor


class _BeginConnection:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.in_transaction = False
        self.execute_calls = 0

    async def execute(self, sql: str) -> None:
        assert sql == "BEGIN IMMEDIATE"
        self.execute_calls += 1
        if self.execute_calls <= self.failures:
            raise aiosqlite.Error("database is locked")


async def test_begin_retries_native_error_until_success(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _BeginConnection(failures=2)
    driver = AiosqliteDriver(connection=cast("Any", connection), statement_config=default_statement_config)

    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    monkeypatch.setattr("sqlspec.adapters.aiosqlite.driver.random.uniform", lambda *_args: 0.0)

    await driver.begin()

    assert connection.execute_calls == 3


async def test_begin_raises_original_error_after_retry_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _BeginConnection(failures=4)
    driver = AiosqliteDriver(connection=cast("Any", connection), statement_config=default_statement_config)

    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    monkeypatch.setattr("sqlspec.adapters.aiosqlite.driver.random.uniform", lambda *_args: 0.0)

    with pytest.raises(SQLSpecError, match="Failed to begin transaction after retries") as exc_info:
        await driver.begin()

    assert connection.execute_calls == 4
    assert isinstance(exc_info.value.__cause__, aiosqlite.Error)


async def test_dispatch_execute_detects_record_row_format(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run_on_worker_thread(
        _connection: object, function: Callable[..., object], *args: object, **kwargs: object
    ) -> object:
        return function(*args, **kwargs)

    monkeypatch.setattr("sqlspec.adapters.aiosqlite.driver.run_on_worker_thread", _run_on_worker_thread)
    monkeypatch.setattr(AiosqliteDriver, "_compiled_sql", lambda *_args, **_kwargs: ("SELECT id, name FROM users", []))

    driver = AiosqliteDriver(connection=cast("Any", _WorkerConnection()), statement_config=default_statement_config)

    result = await driver.dispatch_execute(
        cast("Any", object()), SQL("SELECT id, name FROM users", statement_config=default_statement_config)
    )

    assert result.row_format == "record"
    selected_data = result.selected_data
    assert selected_data is not None
    assert dict(selected_data[0]) == {"id": 1, "name": "alice"}


async def test_dispatch_execute_clears_rowid_cache_after_queued_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    driver = AiosqliteDriver(connection=cast("Any", _WorkerConnection()), statement_config=default_statement_config)

    async def _run_on_worker_thread(
        _connection: object, _function: Callable[..., object], *_args: object, **_kwargs: object
    ) -> tuple[int, None]:
        assert driver._rowid_target_cache == {}
        driver._rowid_target_cache[(None, "stale_target")] = True
        return 0, None

    monkeypatch.setattr("sqlspec.adapters.aiosqlite.driver.run_on_worker_thread", _run_on_worker_thread)
    monkeypatch.setattr(
        AiosqliteDriver, "_compiled_sql", lambda *_args, **_kwargs: ("CREATE TABLE queued_ddl (id)", [])
    )

    await driver.dispatch_execute(
        cast("Any", object()), SQL("CREATE TABLE queued_ddl (id)", statement_config=default_statement_config)
    )

    assert driver._rowid_target_cache == {}


async def test_execute_many_uses_thin_qmark_path() -> None:
    connection = _WorkerConnection()
    driver = AiosqliteDriver(connection=cast("Any", connection), statement_config=default_statement_config)

    result = await driver.execute_many("INSERT INTO test_thin_path (value) VALUES (?)", [("a",), ("b",)])

    assert isinstance(result, DMLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 2
    assert connection.executemany_calls == [("INSERT INTO test_thin_path (value) VALUES (?)", [("a",), ("b",)])]


def test_execute_many_thin_path_rejects_subclass_coercion_values() -> None:
    from collections import defaultdict

    assert (
        AiosqliteDriver._thin_path_parameters_are_eligible(
            [(defaultdict(int, a=1),)], default_statement_config.parameter_config.type_coercion_map
        )
        is False
    )


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.scripted: list[str] = []

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

    async def executescript(self, script: str) -> None:
        self.scripted.append(script)

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeConnectProxy:
    def __init__(self, connection: _FakeConnection) -> None:
        self._thread = Thread(target=lambda: None)
        self._connection = connection

    def __await__(self) -> Any:

        async def _resolve() -> _FakeConnection:
            return self._connection

        return _resolve().__await__()


async def test_pool_acquire_acquire_does_not_sleep_when_wal_not_initialized(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    proxy = _FakeConnectProxy(_FakeConnection())
    pool = AiosqliteConnectionPool({"database": "file:test_acquire?mode=memory&cache=shared", "uri": True}, pool_size=3)
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: proxy)
    pool_conn = await pool._create_connection()
    pool._queue.put_nowait(pool_conn)
    pool._wal_initialized = False
    start = time.monotonic()
    connection = await pool.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.005
    assert connection is pool_conn
    await pool._retire_connection(pool_conn, reason="test_cleanup")


async def test_pool_acquire_acquire_multiple_concurrent_no_sleep_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    pool_size = 5
    pool = AiosqliteConnectionPool(
        {"database": "file:concurrent_test?mode=memory&cache=shared", "uri": True}, pool_size=pool_size
    )
    proxies: Iterator[_FakeConnectProxy] = iter(_FakeConnectProxy(_FakeConnection()) for _ in range(pool_size))
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: next(proxies))
    for _ in range(pool_size):
        pool_conn = await pool._create_connection()
        pool._queue.put_nowait(pool_conn)
    pool._wal_initialized = False

    async def _acquire_and_release() -> float:
        t0 = time.monotonic()
        connection = await pool.acquire()
        t1 = time.monotonic()
        await pool.release(connection)
        return t1 - t0

    start = time.monotonic()
    times = await asyncio.gather(*[_acquire_and_release() for _ in range(pool_size)])
    total = time.monotonic() - start
    assert total < pool_size * 0.005, f"total={total:.4f}s per-call={times}"


async def test_pool_acquire_wal_ready_event_set_after_shared_cache_create_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    pool = AiosqliteConnectionPool({"database": "file:wal_event_test?mode=memory&cache=shared", "uri": True})
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FakeConnectProxy(_FakeConnection()))
    assert not pool._wal_ready_event.is_set()
    pool_conn = await pool._create_connection()
    try:
        assert pool._wal_initialized is True
        assert pool._wal_ready_event.is_set()
    finally:
        await pool._retire_connection(pool_conn, reason="test_cleanup")


async def test_pool_acquire_wal_ready_event_not_set_for_non_shared_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    pool = AiosqliteConnectionPool({"database": ":memory:"})
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FakeConnectProxy(_FakeConnection()))
    pool_conn = await pool._create_connection()
    try:
        assert pool._wal_initialized is False
        assert not pool._wal_ready_event.is_set()
    finally:
        await pool._retire_connection(pool_conn, reason="test_cleanup")


def test_profile_build_profile_includes_named_colon_in_supported_styles() -> None:
    profile = build_profile()
    assert ParameterStyle.NAMED_COLON in profile.supported_styles


def test_profile_build_profile_includes_named_colon_in_supported_execution_styles() -> None:
    profile = build_profile()
    assert profile.supported_execution_styles is not None
    assert ParameterStyle.NAMED_COLON in profile.supported_execution_styles


def test_profile_build_profile_default_style_remains_qmark() -> None:
    profile = build_profile()
    assert profile.default_style == ParameterStyle.QMARK
    assert profile.default_execution_style == ParameterStyle.QMARK


def test_profile_build_profile_qmark_still_supported() -> None:
    profile = build_profile()
    assert ParameterStyle.QMARK in profile.supported_styles
    assert profile.supported_execution_styles is not None
    assert ParameterStyle.QMARK in profile.supported_execution_styles


def test_profile_build_statement_config_disables_parameter_type_wrapping() -> None:
    config = build_statement_config()
    assert config.enable_parameter_type_wrapping is False


def test_profile_default_statement_config_disables_parameter_type_wrapping() -> None:
    assert default_statement_config.enable_parameter_type_wrapping is False


def test_profile_driver_profile_module_singleton_has_named_colon() -> None:
    assert ParameterStyle.NAMED_COLON in driver_profile.supported_styles
    assert driver_profile.supported_execution_styles is not None
    assert ParameterStyle.NAMED_COLON in driver_profile.supported_execution_styles


def test_profile_aiosqlite_profile_parity_with_sqlite_profile() -> None:
    from sqlspec.adapters.sqlite.core import build_profile as sqlite_build_profile

    aio_profile = build_profile()
    sqlite_profile = sqlite_build_profile()
    assert aio_profile.supported_styles == sqlite_profile.supported_styles
    assert aio_profile.supported_execution_styles == sqlite_profile.supported_execution_styles
    assert aio_profile.default_style == sqlite_profile.default_style
    assert aio_profile.default_execution_style == sqlite_profile.default_execution_style


def test_profile_aiosqlite_statement_config_parity_with_sqlite() -> None:
    from sqlspec.adapters.sqlite.core import build_statement_config as sqlite_build_statement_config

    aio_config = build_statement_config()
    sqlite_config = sqlite_build_statement_config()
    assert aio_config.enable_parameter_type_wrapping == sqlite_config.enable_parameter_type_wrapping
