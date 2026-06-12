import asyncio
import sqlite3
import sys
import time
from collections.abc import Iterator
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
)
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool
from sqlspec.core import ParameterStyle
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError


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


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

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


@pytest.mark.skipif(sys.version_info >= (3, 11), reason="gate only raises below 3.11")
async def test_serialize_gate_below_311() -> None:
    driver = AiosqliteDriver(cast(Any, object()))

    with pytest.raises(ImproperConfigurationError, match=r"3\.11"):
        await driver.serialize()


@pytest.mark.skipif(sys.version_info >= (3, 11), reason="gate only raises below 3.11")
async def test_deserialize_gate_below_311() -> None:
    driver = AiosqliteDriver(cast(Any, object()))

    with pytest.raises(ImproperConfigurationError, match=r"3\.11"):
        await driver.deserialize(b"")


@pytest.mark.skipif(sys.version_info >= (3, 11), reason="gate only raises below 3.11")
async def test_blob_open_gate_below_311() -> None:
    driver = AiosqliteDriver(cast(Any, object()))

    with pytest.raises(ImproperConfigurationError, match=r"3\.11"):
        await driver.blob_open("t", "c", 1)


async def test_wal_checkpoint_invalid_mode_raises() -> None:
    driver = AiosqliteDriver(cast(Any, object()))

    with pytest.raises(SQLSpecError, match="Invalid WAL checkpoint mode"):
        await driver.wal_checkpoint(cast(Any, "BOGUS"))


def test_profile_aiosqlite_statement_config_parity_with_sqlite() -> None:
    from sqlspec.adapters.sqlite.core import build_statement_config as sqlite_build_statement_config

    aio_config = build_statement_config()
    sqlite_config = sqlite_build_statement_config()
    assert aio_config.enable_parameter_type_wrapping == sqlite_config.enable_parameter_type_wrapping
