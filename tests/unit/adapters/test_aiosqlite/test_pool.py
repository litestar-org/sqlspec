"""Unit tests for the aiosqlite connection pool acquisition contract."""

import asyncio

import pytest

from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectTimeoutError, AiosqliteConnectionPool

pytest.importorskip("aiosqlite", reason="aiosqlite adapter requires the aiosqlite package")


async def test_acquire_enforces_connect_timeout_on_pool_exhaustion() -> None:
    """A saturated pool must raise AiosqliteConnectTimeoutError, not hang forever."""
    pool = AiosqliteConnectionPool({"database": ":memory:"}, pool_size=1, connect_timeout=0.2)
    held = await pool.acquire()
    try:
        with pytest.raises(AiosqliteConnectTimeoutError):
            # Outer guard: a regression (unbounded wait) fails the test instead of hanging the suite.
            await asyncio.wait_for(pool.acquire(), timeout=5.0)
    finally:
        await pool.release(held)
        await pool.close()
