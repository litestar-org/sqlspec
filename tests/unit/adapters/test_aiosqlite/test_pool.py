"""Unit tests for the aiosqlite connection pool acquisition contract."""

import asyncio
import sqlite3
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest

from sqlspec.adapters.aiosqlite.pool import (
    SQLITE_BUSY_TIMEOUT,
    SQLITE_DEFAULT_ENABLE_FOREIGN_KEYS,
    SQLITE_DEFAULT_ENABLE_OPTIMIZATIONS,
    SQLITE_MEMORY_CACHE_SIZE,
    AiosqliteConnectionPool,
    AiosqliteConnectTimeoutError,
)
from sqlspec.adapters.sqlite.pool import SQLITE_BUSY_TIMEOUT as SYNC_SQLITE_BUSY_TIMEOUT
from sqlspec.adapters.sqlite.pool import SQLITE_DEFAULT_ENABLE_FOREIGN_KEYS as SYNC_SQLITE_DEFAULT_ENABLE_FOREIGN_KEYS
from sqlspec.adapters.sqlite.pool import SQLITE_DEFAULT_ENABLE_OPTIMIZATIONS as SYNC_SQLITE_DEFAULT_ENABLE_OPTIMIZATIONS
from sqlspec.adapters.sqlite.pool import SQLITE_MEMORY_CACHE_SIZE as SYNC_SQLITE_MEMORY_CACHE_SIZE

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection

pytest.importorskip("aiosqlite", reason="aiosqlite adapter requires the aiosqlite package")


def test_sqlite_family_pragma_defaults_are_equal() -> None:
    assert SQLITE_BUSY_TIMEOUT == SYNC_SQLITE_BUSY_TIMEOUT == 5000
    assert SQLITE_MEMORY_CACHE_SIZE == SYNC_SQLITE_MEMORY_CACHE_SIZE == -16000
    assert SQLITE_DEFAULT_ENABLE_OPTIMIZATIONS is SYNC_SQLITE_DEFAULT_ENABLE_OPTIMIZATIONS is True
    assert SQLITE_DEFAULT_ENABLE_FOREIGN_KEYS is SYNC_SQLITE_DEFAULT_ENABLE_FOREIGN_KEYS is False


async def _read_pragma(
    connection: "AiosqliteConnection",
    statement: Literal[
        "PRAGMA foreign_keys",
        "PRAGMA busy_timeout",
        "PRAGMA cache_size",
        "PRAGMA journal_mode",
        "PRAGMA synchronous",
        "PRAGMA temp_store",
        "PRAGMA read_uncommitted",
    ],
) -> int | str:
    cursor = await connection.execute(statement)
    try:
        row = await cursor.fetchone()
        assert row is not None
        return cast("int | str", row[0])
    finally:
        await cursor.close()


async def test_default_pool_uses_native_like_shared_pragma_profile() -> None:
    pool = AiosqliteConnectionPool({"database": ":memory:", "timeout": 30.0})
    pool_connection = await pool.acquire()
    try:
        connection = pool_connection.connection
        assert await _read_pragma(connection, "PRAGMA foreign_keys") == 0
        assert await _read_pragma(connection, "PRAGMA busy_timeout") == 5000
        assert await _read_pragma(connection, "PRAGMA cache_size") == -16000
        assert await _read_pragma(connection, "PRAGMA journal_mode") == "memory"
        assert await _read_pragma(connection, "PRAGMA synchronous") == 0
        assert await _read_pragma(connection, "PRAGMA temp_store") == 2
    finally:
        await pool.release(pool_connection)
        await pool.close()


async def test_disable_optimizations_preserves_native_memory_profile() -> None:
    native_connection = sqlite3.connect(":memory:", timeout=30.0)
    pool = AiosqliteConnectionPool({"database": ":memory:", "timeout": 30.0}, enable_optimizations=False)
    pool_connection = await pool.acquire()
    try:
        connection = pool_connection.connection
        assert (
            await _read_pragma(connection, "PRAGMA journal_mode")
            == native_connection.execute("PRAGMA journal_mode").fetchone()[0]
        )
        assert (
            await _read_pragma(connection, "PRAGMA synchronous")
            == native_connection.execute("PRAGMA synchronous").fetchone()[0]
        )
        assert (
            await _read_pragma(connection, "PRAGMA temp_store")
            == native_connection.execute("PRAGMA temp_store").fetchone()[0]
        )
        assert (
            await _read_pragma(connection, "PRAGMA cache_size")
            == native_connection.execute("PRAGMA cache_size").fetchone()[0]
        )
        assert (
            await _read_pragma(connection, "PRAGMA busy_timeout")
            == native_connection.execute("PRAGMA busy_timeout").fetchone()[0]
        )
        assert (
            await _read_pragma(connection, "PRAGMA foreign_keys")
            == native_connection.execute("PRAGMA foreign_keys").fetchone()[0]
        )
    finally:
        await pool.release(pool_connection)
        await pool.close()
        native_connection.close()


async def test_file_pool_uses_wal_normal_and_shared_busy_timeout(tmp_path: Path) -> None:
    pool = AiosqliteConnectionPool({"database": tmp_path / "profile.db", "timeout": 30.0})
    pool_connection = await pool.acquire()
    try:
        connection = pool_connection.connection
        assert await _read_pragma(connection, "PRAGMA journal_mode") == "wal"
        assert await _read_pragma(connection, "PRAGMA synchronous") == 1
        assert await _read_pragma(connection, "PRAGMA busy_timeout") == 5000
        assert await _read_pragma(connection, "PRAGMA foreign_keys") == 0
    finally:
        await pool.release(pool_connection)
        await pool.close()


@pytest.mark.parametrize(
    ("enable_optimizations", "enable_foreign_keys", "expected_foreign_keys", "expected_cache_size"),
    [(False, True, 1, None), (True, False, 0, -16000)],
)
async def test_optimization_and_foreign_key_flags_are_independent(
    enable_optimizations: bool, enable_foreign_keys: bool, expected_foreign_keys: int, expected_cache_size: int | None
) -> None:
    pool = AiosqliteConnectionPool(
        {"database": ":memory:"}, enable_optimizations=enable_optimizations, enable_foreign_keys=enable_foreign_keys
    )
    pool_connection = await pool.acquire()
    try:
        connection = pool_connection.connection
        assert await _read_pragma(connection, "PRAGMA foreign_keys") == expected_foreign_keys
        cache_size = await _read_pragma(connection, "PRAGMA cache_size")
        if expected_cache_size is None:
            assert cache_size != -16000
        else:
            assert cache_size == expected_cache_size
    finally:
        await pool.release(pool_connection)
        await pool.close()


async def test_shared_memory_preserves_read_uncommitted() -> None:
    pool = AiosqliteConnectionPool({"database": "file:pragma_contract?mode=memory&cache=shared", "uri": True})
    pool_connection = await pool.acquire()
    try:
        assert await _read_pragma(pool_connection.connection, "PRAGMA read_uncommitted") == 1
    finally:
        await pool.release(pool_connection)
        await pool.close()


class _FallbackConnection:
    def __init__(self, fail_statement: str | None = None, *, script_fails: bool = True) -> None:
        self.executed: list[str] = []
        self.fail_statement = fail_statement
        self.script_fails = script_fails
        self.closed = False

    async def executescript(self, _script: str) -> None:
        if self.script_fails:
            raise RuntimeError("script unavailable")

    async def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if statement == self.fail_statement:
            raise RuntimeError("individual pragma failed")

    async def commit(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


class _FallbackConnectProxy:
    def __init__(self, connection: _FallbackConnection) -> None:
        self._thread = Thread(target=lambda: None)
        self._connection = connection

    def __await__(self) -> Any:
        async def resolve() -> _FallbackConnection:
            return self._connection

        return resolve().__await__()


async def test_pragma_script_fallback_uses_native_like_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    connection = _FallbackConnection()
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FallbackConnectProxy(connection))
    pool = AiosqliteConnectionPool({"database": ":memory:"})

    await pool._create_connection()

    assert connection.executed == [
        "PRAGMA journal_mode = MEMORY",
        "PRAGMA synchronous = OFF",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA cache_size = -16000",
        "PRAGMA busy_timeout = 5000",
    ]
    await pool.close()


async def test_pragma_script_fallback_honors_independent_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    connection = _FallbackConnection()
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FallbackConnectProxy(connection))
    pool = AiosqliteConnectionPool({"database": ":memory:"}, enable_optimizations=False, enable_foreign_keys=True)

    await pool._create_connection()

    assert connection.executed == ["PRAGMA foreign_keys = ON"]
    await pool.close()


async def test_pragma_script_fallback_surfaces_explicit_foreign_key_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    connection = _FallbackConnection(fail_statement="PRAGMA foreign_keys = ON")
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FallbackConnectProxy(connection))
    pool = AiosqliteConnectionPool({"database": ":memory:"}, enable_foreign_keys=True)

    with pytest.raises(RuntimeError, match="individual pragma failed"):
        await pool._create_connection()

    assert connection.executed == [
        "PRAGMA journal_mode = MEMORY",
        "PRAGMA synchronous = OFF",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA cache_size = -16000",
        "PRAGMA busy_timeout = 5000",
        "PRAGMA foreign_keys = ON",
    ]
    await pool.close()


async def test_pragma_script_fallback_continues_after_optimization_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    connection = _FallbackConnection(fail_statement="PRAGMA synchronous = OFF")
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FallbackConnectProxy(connection))
    pool = AiosqliteConnectionPool({"database": ":memory:"})

    await pool._create_connection()

    assert connection.executed == [
        "PRAGMA journal_mode = MEMORY",
        "PRAGMA synchronous = OFF",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA cache_size = -16000",
        "PRAGMA busy_timeout = 5000",
    ]
    await pool.close()


async def test_file_pragma_script_fallback_replays_file_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    connection = _FallbackConnection()
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FallbackConnectProxy(connection))
    pool = AiosqliteConnectionPool({"database": "profile.db", "timeout": 30.0})

    await pool._create_connection()

    assert connection.executed == [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA busy_timeout = 5000",
    ]
    await pool.close()


async def test_shared_memory_pragma_script_fallback_replays_read_uncommitted(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    connection = _FallbackConnection()
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FallbackConnectProxy(connection))
    pool = AiosqliteConnectionPool({"database": "file:fallback?mode=memory&cache=shared", "uri": True, "timeout": 30.0})

    await pool._create_connection()

    assert connection.executed == [
        "PRAGMA journal_mode = MEMORY",
        "PRAGMA synchronous = OFF",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA cache_size = -16000",
        "PRAGMA busy_timeout = 5000",
        "PRAGMA read_uncommitted = ON",
    ]
    await pool.close()


@pytest.mark.parametrize("error", [RuntimeError("hook failed"), asyncio.CancelledError()])
async def test_post_connect_failure_closes_raw_connection(
    error: BaseException, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    connection = _FallbackConnection(script_fails=False)
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FallbackConnectProxy(connection))

    async def failing_hook(_connection: Any) -> None:
        raise error

    pool = AiosqliteConnectionPool({"database": ":memory:"}, on_connection_create=failing_hook)

    with pytest.raises(type(error)):
        await pool._create_connection()

    assert connection.closed is True


async def test_acquire_enforces_connect_timeout_on_pool_exhaustion() -> None:
    """A saturated pool raises AiosqliteConnectTimeoutError instead of blocking.

    The acquire is bounded by an outer wait_for so the test fails fast rather than
    hanging the suite if the pool blocks.
    """
    pool = AiosqliteConnectionPool({"database": ":memory:"}, pool_size=1, connect_timeout=0.2)
    held = await pool.acquire()
    try:
        with pytest.raises(AiosqliteConnectTimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=5.0)
    finally:
        await pool.release(held)
        await pool.close()
