"""Tests for aiosqlite pool shutdown behavior."""

# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false

import asyncio
import sqlite3
import time
from pathlib import Path
from threading import Event, Thread, current_thread
from typing import TYPE_CHECKING, Any, Literal, cast

import anyio
import pytest

from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool, AiosqlitePoolClosedError, AiosqlitePoolConnection

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection


class _FakeAiosqliteConnection:
    """Minimal async connection stub used by pool tests."""

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.stop_called = 0

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def stop(self) -> None:
        self.stop_called += 1


class _LegacyConnectProxy(Thread):
    """aiosqlite <=0.21 style connect proxy (thread subclass)."""

    def __init__(self, connection: _FakeAiosqliteConnection) -> None:
        super().__init__(target=lambda: None)
        self._connection = connection

    def __await__(self) -> Any:
        async def _resolve() -> _FakeAiosqliteConnection:
            return self._connection

        return _resolve().__await__()


class _ModernConnectProxy:
    """aiosqlite >=0.22 style connect proxy (has internal _thread)."""

    def __init__(self, connection: _FakeAiosqliteConnection) -> None:
        self._thread = Thread(target=lambda: None)
        self._connection = connection

    def __await__(self) -> Any:
        async def _resolve() -> _FakeAiosqliteConnection:
            return self._connection

        return _resolve().__await__()


async def test_create_connection_sets_daemon_for_legacy_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool should set daemon mode for pre-0.22 thread-based connect proxy."""
    from sqlspec.adapters.aiosqlite import pool as pool_module

    fake_connection = _FakeAiosqliteConnection()
    connect_proxy = _LegacyConnectProxy(fake_connection)
    pool = AiosqliteConnectionPool({"database": ":memory:"})

    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: connect_proxy)

    pool_connection = await pool._create_connection()
    try:
        assert connect_proxy.daemon is True
    finally:
        await pool._retire_connection(pool_connection, reason="test_cleanup")


async def test_create_connection_sets_daemon_for_modern_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool should set daemon mode for 0.22+ connect proxy internal worker thread."""
    from sqlspec.adapters.aiosqlite import pool as pool_module

    fake_connection = _FakeAiosqliteConnection()
    connect_proxy = _ModernConnectProxy(fake_connection)
    pool = AiosqliteConnectionPool({"database": ":memory:"})

    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: connect_proxy)

    pool_connection = await pool._create_connection()
    try:
        assert connect_proxy._thread.daemon is True
    finally:
        await pool._retire_connection(pool_connection, reason="test_cleanup")


async def test_pool_close_uses_force_stop_when_close_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool should trigger force-stop fallback when graceful close times out."""
    from sqlspec.adapters.aiosqlite import pool as pool_module

    calls: list[tuple[str, str]] = []

    async def _hanging_close(self: AiosqlitePoolConnection) -> None:
        await asyncio.sleep(0.05)

    async def _capture_force_stop(
        self: AiosqliteConnectionPool, connection: AiosqlitePoolConnection, *, reason: str
    ) -> None:
        calls.append((connection.id, reason))

    monkeypatch.setattr(pool_module.AiosqlitePoolConnection, "close", _hanging_close)
    monkeypatch.setattr(pool_module.AiosqliteConnectionPool, "_force_stop_connection", _capture_force_stop)

    pool = AiosqliteConnectionPool({"database": ":memory:"}, operation_timeout=0.001)
    connection = AiosqlitePoolConnection(cast("AiosqliteConnection", _FakeAiosqliteConnection()))
    pool._connection_registry[connection.id] = connection

    await pool.close()

    assert calls == [(connection.id, "pool_close_timeout")]


@pytest.mark.parametrize("cancellation", ["cancel_scope", "task_cancel"])
async def test_close_joins_worker_of_connection_cancelled_while_opening(
    cancellation: Literal["cancel_scope", "task_cancel"], tmp_path: Path
) -> None:
    """A connection cancelled while opening has its worker thread joined by pool close."""
    opening = Event()
    opening_threads: list[Thread] = []

    class _SlowOpenConnection(sqlite3.Connection):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            opening_threads.append(current_thread())
            opening.set()
            time.sleep(0.2)
            super().__init__(*args, **kwargs)

    pool = AiosqliteConnectionPool({"database": str(tmp_path / "pool.db"), "factory": _SlowOpenConnection})

    if cancellation == "cancel_scope":
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(pool.acquire)
            await anyio.to_thread.run_sync(opening.wait)
            task_group.cancel_scope.cancel()
    else:
        acquire = asyncio.ensure_future(pool.acquire())
        await anyio.to_thread.run_sync(opening.wait)
        acquire.cancel()
        await asyncio.wait({acquire}, timeout=5)
        assert acquire.cancelled()

    await pool.close()

    assert pool.size() == 0
    assert not opening_threads[0].is_alive()


async def test_close_stops_worker_of_connection_retired_in_cancelled_scope(tmp_path: Path) -> None:
    """Retiring a connection inside a cancelled scope still stops and joins its worker."""
    pool = AiosqliteConnectionPool({"database": str(tmp_path / "pool.db")})
    pooled = await pool.acquire()
    worker = pooled.connection._thread

    with anyio.CancelScope() as scope:
        scope.cancel()
        await pool._retire_connection(pooled)

    await pool.close()

    assert not worker.is_alive()


async def test_connection_created_after_close_is_discarded_and_its_worker_joined(tmp_path: Path) -> None:
    """A creation that finishes after close() raises and leaves no live worker thread."""
    opening = Event()
    proceed = Event()
    opening_threads: list[Thread] = []

    class _GatedOpenConnection(sqlite3.Connection):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            opening_threads.append(current_thread())
            opening.set()
            proceed.wait(timeout=5)
            super().__init__(*args, **kwargs)

    pool = AiosqliteConnectionPool({"database": str(tmp_path / "pool.db"), "factory": _GatedOpenConnection})
    acquire = asyncio.ensure_future(pool.acquire())
    await anyio.to_thread.run_sync(opening.wait)

    await pool.close()
    proceed.set()

    with pytest.raises(AiosqlitePoolClosedError, match="connection creation"):
        await acquire
    assert pool.size() == 0
    assert not opening_threads[0].is_alive()
