"""Tests for aiosqlite pool acquire hot path."""

import asyncio
import time
from collections.abc import Iterator
from threading import Thread
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool


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


async def test_acquire_does_not_sleep_when_wal_not_initialized(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    proxy = _FakeConnectProxy(_FakeConnection())
    pool = AiosqliteConnectionPool({"database": "file:test_acquire?mode=memory&cache=shared", "uri": True}, pool_size=3)

    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: proxy)

    pool_conn = await pool._create_connection()  # pyright: ignore[reportPrivateUsage]
    pool._queue.put_nowait(pool_conn)  # pyright: ignore[reportPrivateUsage]
    pool._wal_initialized = False  # pyright: ignore[reportPrivateUsage]

    start = time.monotonic()
    connection = await pool.acquire()
    elapsed = time.monotonic() - start

    assert elapsed < 0.005
    assert connection is pool_conn

    await pool._retire_connection(pool_conn, reason="test_cleanup")  # pyright: ignore[reportPrivateUsage]


async def test_acquire_multiple_concurrent_no_sleep_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    pool_size = 5
    pool = AiosqliteConnectionPool(
        {"database": "file:concurrent_test?mode=memory&cache=shared", "uri": True}, pool_size=pool_size
    )

    proxies: Iterator[_FakeConnectProxy] = iter(_FakeConnectProxy(_FakeConnection()) for _ in range(pool_size))
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: next(proxies))

    for _ in range(pool_size):
        pool_conn = await pool._create_connection()  # pyright: ignore[reportPrivateUsage]
        pool._queue.put_nowait(pool_conn)  # pyright: ignore[reportPrivateUsage]

    pool._wal_initialized = False  # pyright: ignore[reportPrivateUsage]

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


async def test_wal_ready_event_set_after_shared_cache_create_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    pool = AiosqliteConnectionPool({"database": "file:wal_event_test?mode=memory&cache=shared", "uri": True})
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FakeConnectProxy(_FakeConnection()))

    assert not pool._wal_ready_event.is_set()  # pyright: ignore[reportPrivateUsage]

    pool_conn = await pool._create_connection()  # pyright: ignore[reportPrivateUsage]
    try:
        assert pool._wal_initialized is True  # pyright: ignore[reportPrivateUsage]
        assert pool._wal_ready_event.is_set()  # pyright: ignore[reportPrivateUsage]
    finally:
        await pool._retire_connection(pool_conn, reason="test_cleanup")  # pyright: ignore[reportPrivateUsage]


async def test_wal_ready_event_not_set_for_non_shared_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.aiosqlite import pool as pool_module

    pool = AiosqliteConnectionPool({"database": ":memory:"})
    monkeypatch.setattr(pool_module.aiosqlite, "connect", lambda **_: _FakeConnectProxy(_FakeConnection()))

    pool_conn = await pool._create_connection()  # pyright: ignore[reportPrivateUsage]
    try:
        assert pool._wal_initialized is False  # pyright: ignore[reportPrivateUsage]
        assert not pool._wal_ready_event.is_set()  # pyright: ignore[reportPrivateUsage]
    finally:
        await pool._retire_connection(pool_conn, reason="test_cleanup")  # pyright: ignore[reportPrivateUsage]
