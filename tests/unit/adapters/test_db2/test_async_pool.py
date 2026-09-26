"""Tests for the bounded asyncio Db2 connection pool."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest

import sqlspec.adapters.db2.pool as pool_module
from sqlspec.adapters.db2.pool import Db2AsyncConnectionPool
from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError
from tests.unit.adapters.test_db2._fakes import (
    FakeDb2AsyncConnection,
    FakeDb2Connection,
    FakeDb2Cursor,
    FakeDb2OperationalError,
    FakeIbmDbDbiModule,
    FakeIbmDbModule,
    db2_error,
)

FakeModules = tuple[FakeIbmDbModule, FakeIbmDbDbiModule]

pytestmark = pytest.mark.anyio


class FakeClock:
    """Replacement for the pool module's ``time`` with a controllable monotonic clock."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return time.time()

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _as_fake(connection: Any) -> FakeDb2AsyncConnection:
    return cast(FakeDb2AsyncConnection, connection)


async def test_acquire_creates_and_reuses_lifo(fake_ibm_db: FakeModules) -> None:
    """Released connections are reused most-recent first and no extra connection is opened."""
    _, module = fake_ibm_db
    pool = Db2AsyncConnectionPool({"database": "TESTDB"})
    try:
        connection = await pool.acquire()
        await pool.release(connection)
        assert await pool.acquire() is connection
        assert pool.size() == 1

        other = await pool.acquire()
        await pool.release(connection)
        await pool.release(other)
        assert await pool.acquire() is other
        assert await pool.acquire() is connection
        assert (pool.size(), pool.checked_out(), len(module.connect_calls)) == (2, 2, 2)

        await pool.release(FakeDb2AsyncConnection())
        assert (pool.size(), pool.checked_out()) == (2, 2)
    finally:
        await pool.close()


async def test_get_connection_releases_on_exit(fake_ibm_db: FakeModules) -> None:
    """The async context manager checks a connection out and returns it to the pool."""
    pool = Db2AsyncConnectionPool({"database": "TESTDB"})
    try:
        async with pool.get_connection() as connection:
            assert pool.checked_out() == 1
        assert pool.checked_out() == 0
        assert await pool.acquire() is connection
    finally:
        await pool.close()


async def test_max_size_bounds_open_connections_and_times_out(fake_ibm_db: FakeModules) -> None:
    """A second waiter times out while the only connection is held, then succeeds after release."""
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, max_size=1, acquire_timeout=0.05)
    try:
        held = await pool.acquire()
        with pytest.raises(ConnectionTimeoutError, match=r"Timed out after 0\.05s"):
            await pool.acquire()
        assert pool.size() == 1
        await pool.release(held)
        assert await pool.acquire() is held
    finally:
        await pool.close()


async def test_concurrent_acquires_never_exceed_max_size(fake_ibm_db: FakeModules) -> None:
    """Concurrent sessions share at most ``max_size`` physical connections."""
    _, module = fake_ibm_db
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, max_size=2, acquire_timeout=2.0)
    errors: list[BaseException] = []
    peak = 0

    async def worker() -> None:
        nonlocal peak
        try:
            connection = await pool.acquire()
            peak = max(peak, pool.size())
            await asyncio.sleep(0.01)
            await pool.release(connection)
        except Exception as exc:
            errors.append(exc)

    try:
        await asyncio.gather(*(worker() for _ in range(6)))
        assert not errors, f"workers raised: {errors}"
        assert (peak, len(module.connect_calls), pool.checked_out()) == (2, 2, 0)
    finally:
        await pool.close()


async def test_cancelled_waiter_does_not_leak_capacity(fake_ibm_db: FakeModules) -> None:
    """Cancelling a blocked acquire leaves the single slot available to the next caller."""
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, max_size=1, acquire_timeout=5.0)
    try:
        held = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0.01)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        await pool.release(held)

        assert await asyncio.wait_for(pool.acquire(), 1.0) is held
        assert pool.checked_out() == 1
    finally:
        await pool.close()


async def test_cancelled_connect_releases_capacity_and_closes_connection(fake_ibm_db: FakeModules) -> None:
    """Cancelling an acquire while the new connection is being set up frees its slot and closes it."""
    gate = asyncio.Event()
    calls = 0

    async def slow_first_hook(_: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            await gate.wait()

    pool = Db2AsyncConnectionPool(
        {"database": "TESTDB"}, max_size=1, acquire_timeout=1.0, on_connection_create=slow_first_hook
    )
    try:
        pending = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0.01)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

        connection = _as_fake(await pool.acquire())
        assert (pool.size(), pool.checked_out(), calls) == (1, 1, 2)
        assert connection.sync_connection.closed is False
    finally:
        await pool.close()


async def test_failed_connect_releases_capacity(fake_ibm_db: FakeModules) -> None:
    """A connect error surfaces to the caller without consuming the slot."""
    _, module = fake_ibm_db
    module.connect_errors.append(db2_error(-30081, "08001", "A communication error", FakeDb2OperationalError))
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, max_size=1, acquire_timeout=0.05)
    try:
        with pytest.raises(FakeDb2OperationalError):
            await pool.acquire()
        await pool.acquire()
        assert (pool.size(), len(module.connect_calls)) == (1, 2)
    finally:
        await pool.close()


async def test_failed_create_hook_closes_connection(fake_ibm_db: FakeModules) -> None:
    """A failing connection hook closes the new connection and frees the slot."""
    _, module = fake_ibm_db
    first = FakeDb2Connection()
    module.pending_connections.append(first)
    failures = [RuntimeError("hook failed")]

    def hook(_: Any) -> None:
        if failures:
            raise failures.pop()

    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, max_size=1, acquire_timeout=0.05, on_connection_create=hook)
    try:
        with pytest.raises(RuntimeError, match="hook failed"):
            await pool.acquire()
        assert first.closed is True
        await pool.acquire()
        assert pool.size() == 1
    finally:
        await pool.close()


async def test_recycle_and_health_check(fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch) -> None:
    """Old connections are recycled, idle ones are pinged, and failed pings replace the connection."""
    _, module = fake_ibm_db
    clock = FakeClock()
    monkeypatch.setattr(pool_module, "time", clock)
    recycled = FakeDb2Connection()
    unhealthy = FakeDb2Connection([
        FakeDb2Cursor(error=db2_error(-30108, "08506", "A connection failed", FakeDb2OperationalError))
    ])
    healthy = FakeDb2Connection()
    module.pending_connections.extend([recycled, unhealthy, healthy])
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, recycle_seconds=100, health_check_interval=10.0)
    try:
        await pool.release(await pool.acquire())
        clock.advance(101)
        second = _as_fake(await pool.acquire())
        assert second.sync_connection is unhealthy
        assert recycled.closed is True
        assert recycled.cursors == []

        await pool.release(second)
        clock.advance(11)
        third = _as_fake(await pool.acquire())
        assert third.sync_connection is healthy
        assert unhealthy.closed is True
        assert unhealthy.cursors[0].closed is True

        await pool.release(third)
        clock.advance(11)
        assert await pool.acquire() is third
        assert healthy.cursors[0].executed == [("SELECT 1 FROM SYSIBM.SYSDUMMY1", None)]
        assert healthy.cursors[0].closed is True

        await pool.release(third)
        clock.advance(5)
        assert await pool.acquire() is third
        assert len(healthy.cursors) == 1
        assert (pool.size(), len(module.connect_calls)) == (1, 3)
    finally:
        await pool.close()


async def test_close_closes_idle_and_released_connections(fake_ibm_db: FakeModules) -> None:
    """Closing closes idle connections now, checked-out ones on release, and rejects new acquires."""
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, max_size=2)
    idle = _as_fake(await pool.acquire())
    busy = _as_fake(await pool.acquire())
    await pool.release(idle)

    await pool.close()
    assert idle.sync_connection.closed is True
    assert busy.sync_connection.closed is False

    await pool.release(busy)
    assert busy.sync_connection.closed is True
    assert (pool.size(), pool.checked_out()) == (0, 0)
    with pytest.raises(DatabaseConnectionError, match="Db2 async connection pool is closed"):
        await pool.acquire()


async def test_close_fails_blocked_waiters_once_capacity_returns(fake_ibm_db: FakeModules) -> None:
    """A waiter blocked on a full pool gets the closed-pool error instead of a new connection."""
    _, module = fake_ibm_db
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, max_size=1, acquire_timeout=5.0)
    held = _as_fake(await pool.acquire())
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0.01)

    await pool.close()
    await pool.release(held)

    with pytest.raises(DatabaseConnectionError, match="closed"):
        await asyncio.wait_for(waiter, 1.0)
    assert held.sync_connection.closed is True
    assert (len(module.connect_calls), pool.size()) == (1, 0)


@pytest.mark.parametrize(("autocommit", "expected_mode"), [(None, 1), (True, 1), (False, 0)])
async def test_connect_passes_rendered_dsn_and_autocommit_options(
    fake_ibm_db: FakeModules, autocommit: "bool | None", expected_mode: int
) -> None:
    """Connections open through ``AsyncConnection.connect`` with the escaped DSN and autocommit baseline."""
    _, module = fake_ibm_db
    parameters: dict[str, Any] = {"database": "CUSTOM", "hostname": "h", "port": 50000, "user": "u", "password": "p;x"}
    if autocommit is not None:
        parameters["autocommit"] = autocommit
    pool = Db2AsyncConnectionPool(parameters)

    connection = _as_fake(await pool.new_connection())

    assert module.connect_calls == [
        ("DATABASE=CUSTOM;HOSTNAME=h;PORT=50000;UID=u;PWD={p;x};", "", "", "", "", {102: expected_mode})
    ]
    assert connection.sync_connection.autocommit is bool(expected_mode)
    assert pool.size() == 0
    await pool.close()
    assert connection.sync_connection.closed is False


def _sync_hook(seen: "list[Any]") -> "Callable[[Any], None]":
    return seen.append


def _async_hook(seen: "list[Any]") -> "Callable[[Any], Awaitable[None]]":
    async def hook(connection: Any) -> None:
        await asyncio.sleep(0)
        seen.append(connection)

    return hook


@pytest.mark.parametrize("make_hook", [_sync_hook, _async_hook], ids=["sync", "async"])
async def test_async_connection_create_hook_is_awaited(
    fake_ibm_db: FakeModules, make_hook: "Callable[[list[Any]], Callable[[Any], Awaitable[None] | None]]"
) -> None:
    """Sync and async connection hooks both run before the connection is handed out."""
    seen: list[Any] = []
    pool = Db2AsyncConnectionPool({"database": "TESTDB"}, on_connection_create=make_hook(seen))
    try:
        connection = await pool.acquire()
        assert seen == [connection]
    finally:
        await pool.close()
