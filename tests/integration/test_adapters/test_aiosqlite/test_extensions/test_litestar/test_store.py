"""Integration tests for AioSQLite session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar.store import AiosqliteStore

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.aiosqlite, pytest.mark.integration]


@pytest.fixture
async def aiosqlite_store() -> "AsyncGenerator[AiosqliteStore, None]":
    """Create AioSQLite store with in-memory database."""
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"litestar": {"session_table": "test_sessions"}}
    )
    store = AiosqliteStore(config)
    await store.create_table()
    yield store
    await store.delete_all()


async def test_store_create_table(aiosqlite_store: AiosqliteStore) -> None:
    """Test table creation."""
    assert aiosqlite_store.table_name == "test_sessions"


async def test_store_set_and_get(aiosqlite_store: AiosqliteStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await aiosqlite_store.set("session_123", test_data)

    result = await aiosqlite_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(aiosqlite_store: AiosqliteStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await aiosqlite_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(aiosqlite_store: AiosqliteStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await aiosqlite_store.set("session_str", "string data")

    result = await aiosqlite_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(aiosqlite_store: AiosqliteStore) -> None:
    """Test delete operation."""
    await aiosqlite_store.set("session_to_delete", b"data")

    assert await aiosqlite_store.exists("session_to_delete")

    await aiosqlite_store.delete("session_to_delete")

    assert not await aiosqlite_store.exists("session_to_delete")
    assert await aiosqlite_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(aiosqlite_store: AiosqliteStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await aiosqlite_store.delete("nonexistent")


async def test_store_expiration_with_int(aiosqlite_store: AiosqliteStore) -> None:
    """Test session expiration with integer seconds."""
    await aiosqlite_store.set("expiring_session", b"data", expires_in=1)

    assert await aiosqlite_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await aiosqlite_store.get("expiring_session")
    assert result is None
    assert not await aiosqlite_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(aiosqlite_store: AiosqliteStore) -> None:
    """Test session expiration with timedelta."""
    await aiosqlite_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await aiosqlite_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await aiosqlite_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(aiosqlite_store: AiosqliteStore) -> None:
    """Test session without expiration persists."""
    await aiosqlite_store.set("permanent_session", b"data")

    expires_in = await aiosqlite_store.expires_in("permanent_session")
    assert expires_in is None

    assert await aiosqlite_store.exists("permanent_session")


async def test_store_expires_in(aiosqlite_store: AiosqliteStore) -> None:
    """Test expires_in returns correct time."""
    await aiosqlite_store.set("timed_session", b"data", expires_in=10)

    expires_in = await aiosqlite_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(aiosqlite_store: AiosqliteStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await aiosqlite_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await aiosqlite_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(aiosqlite_store: AiosqliteStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await aiosqlite_store.set("active_session", b"data", expires_in=60)
    await aiosqlite_store.set("expired_session_1", b"data", expires_in=1)
    await aiosqlite_store.set("expired_session_2", b"data", expires_in=1)
    await aiosqlite_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await aiosqlite_store.delete_expired()
    assert count == 2

    assert await aiosqlite_store.exists("active_session")
    assert await aiosqlite_store.exists("permanent_session")
    assert not await aiosqlite_store.exists("expired_session_1")
    assert not await aiosqlite_store.exists("expired_session_2")


async def test_store_upsert(aiosqlite_store: AiosqliteStore) -> None:
    """Test updating existing session (UPSERT)."""
    await aiosqlite_store.set("session_upsert", b"original data")

    result = await aiosqlite_store.get("session_upsert")
    assert result == b"original data"

    await aiosqlite_store.set("session_upsert", b"updated data")

    result = await aiosqlite_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(aiosqlite_store: AiosqliteStore) -> None:
    """Test updating session expiration."""
    await aiosqlite_store.set("session_exp", b"data", expires_in=60)

    expires_in = await aiosqlite_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await aiosqlite_store.set("session_exp", b"data", expires_in=10)

    expires_in = await aiosqlite_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(aiosqlite_store: AiosqliteStore) -> None:
    """Test renewing session expiration on get."""
    await aiosqlite_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await aiosqlite_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await aiosqlite_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await aiosqlite_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(aiosqlite_store: AiosqliteStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await aiosqlite_store.set("large_session", large_data)

    result = await aiosqlite_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(aiosqlite_store: AiosqliteStore) -> None:
    """Test delete_all removes all sessions."""
    await aiosqlite_store.set("session1", b"data1")
    await aiosqlite_store.set("session2", b"data2")
    await aiosqlite_store.set("session3", b"data3")

    assert await aiosqlite_store.exists("session1")
    assert await aiosqlite_store.exists("session2")
    assert await aiosqlite_store.exists("session3")

    await aiosqlite_store.delete_all()

    assert not await aiosqlite_store.exists("session1")
    assert not await aiosqlite_store.exists("session2")
    assert not await aiosqlite_store.exists("session3")


async def test_store_exists(aiosqlite_store: AiosqliteStore) -> None:
    """Test exists method."""
    assert not await aiosqlite_store.exists("test_session")

    await aiosqlite_store.set("test_session", b"data")

    assert await aiosqlite_store.exists("test_session")


async def test_store_context_manager(aiosqlite_store: AiosqliteStore) -> None:
    """Test store can be used as async context manager."""
    async with aiosqlite_store:
        await aiosqlite_store.set("ctx_session", b"data")

    result = await aiosqlite_store.get("ctx_session")
    assert result == b"data"
