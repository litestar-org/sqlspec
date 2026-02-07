"""Tests for aiosqlite pool shutdown behavior."""

# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false

import asyncio
from threading import Thread
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool, AiosqlitePoolConnection

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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
