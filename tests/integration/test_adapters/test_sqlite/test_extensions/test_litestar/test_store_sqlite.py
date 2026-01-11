"""Integration tests for SQLite sync session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.litestar.store import SQLiteStore

pytestmark = [pytest.mark.sqlite, pytest.mark.integration, pytest.mark.xdist_group("sqlite")]


@pytest.fixture
async def sqlite_store() -> AsyncGenerator[SQLiteStore, None]:
    """Create SQLite store with shared in-memory database."""
    config = SqliteConfig(
        connection_config={"database": "file:test_sessions_mem?mode=memory&cache=shared", "uri": True},
        extension_config={"litestar": {"session_table": "test_sessions"}},
    )
    store = SQLiteStore(config)
    await store.create_table()
    yield store
    await store.delete_all()


async def test_store_create_table(sqlite_store: SQLiteStore) -> None:
    """Test table creation."""
    assert sqlite_store.table_name == "test_sessions"


async def test_store_set_and_get(sqlite_store: SQLiteStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await sqlite_store.set("session_123", test_data)

    result = await sqlite_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(sqlite_store: SQLiteStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await sqlite_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(sqlite_store: SQLiteStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await sqlite_store.set("session_str", "string data")

    result = await sqlite_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(sqlite_store: SQLiteStore) -> None:
    """Test delete operation."""
    await sqlite_store.set("session_to_delete", b"data")

    assert await sqlite_store.exists("session_to_delete")

    await sqlite_store.delete("session_to_delete")

    assert not await sqlite_store.exists("session_to_delete")
    assert await sqlite_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(sqlite_store: SQLiteStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await sqlite_store.delete("nonexistent")


async def test_store_expiration_with_int(sqlite_store: SQLiteStore) -> None:
    """Test session expiration with integer seconds."""
    await sqlite_store.set("expiring_session", b"data", expires_in=1)

    assert await sqlite_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await sqlite_store.get("expiring_session")
    assert result is None
    assert not await sqlite_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(sqlite_store: SQLiteStore) -> None:
    """Test session expiration with timedelta."""
    await sqlite_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await sqlite_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await sqlite_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(sqlite_store: SQLiteStore) -> None:
    """Test session without expiration persists."""
    await sqlite_store.set("permanent_session", b"data")

    expires_in = await sqlite_store.expires_in("permanent_session")
    assert expires_in is None

    assert await sqlite_store.exists("permanent_session")


async def test_store_expires_in(sqlite_store: SQLiteStore) -> None:
    """Test expires_in returns correct time."""
    await sqlite_store.set("timed_session", b"data", expires_in=10)

    expires_in = await sqlite_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(sqlite_store: SQLiteStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await sqlite_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await sqlite_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(sqlite_store: SQLiteStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await sqlite_store.set("active_session", b"data", expires_in=60)
    await sqlite_store.set("expired_session_1", b"data", expires_in=1)
    await sqlite_store.set("expired_session_2", b"data", expires_in=1)
    await sqlite_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await sqlite_store.delete_expired()
    assert count == 2

    assert await sqlite_store.exists("active_session")
    assert await sqlite_store.exists("permanent_session")
    assert not await sqlite_store.exists("expired_session_1")
    assert not await sqlite_store.exists("expired_session_2")


async def test_store_upsert(sqlite_store: SQLiteStore) -> None:
    """Test updating existing session (UPSERT)."""
    await sqlite_store.set("session_upsert", b"original data")

    result = await sqlite_store.get("session_upsert")
    assert result == b"original data"

    await sqlite_store.set("session_upsert", b"updated data")

    result = await sqlite_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(sqlite_store: SQLiteStore) -> None:
    """Test updating session expiration."""
    await sqlite_store.set("session_exp", b"data", expires_in=60)

    expires_in = await sqlite_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await sqlite_store.set("session_exp", b"data", expires_in=10)

    expires_in = await sqlite_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(sqlite_store: SQLiteStore) -> None:
    """Test renewing session expiration on get."""
    await sqlite_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await sqlite_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await sqlite_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await sqlite_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(sqlite_store: SQLiteStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await sqlite_store.set("large_session", large_data)

    result = await sqlite_store.get("large_session")
    assert result == large_data
    assert result is not None
    assert len(result) > 1024 * 1024


async def test_store_delete_all(sqlite_store: SQLiteStore) -> None:
    """Test delete_all removes all sessions."""
    await sqlite_store.set("session1", b"data1")
    await sqlite_store.set("session2", b"data2")
    await sqlite_store.set("session3", b"data3")

    assert await sqlite_store.exists("session1")
    assert await sqlite_store.exists("session2")
    assert await sqlite_store.exists("session3")

    await sqlite_store.delete_all()

    assert not await sqlite_store.exists("session1")
    assert not await sqlite_store.exists("session2")
    assert not await sqlite_store.exists("session3")


async def test_store_exists(sqlite_store: SQLiteStore) -> None:
    """Test exists method."""
    assert not await sqlite_store.exists("test_session")

    await sqlite_store.set("test_session", b"data")

    assert await sqlite_store.exists("test_session")


async def test_store_context_manager(sqlite_store: SQLiteStore) -> None:
    """Test store can be used as async context manager."""
    async with sqlite_store:
        await sqlite_store.set("ctx_session", b"data")

    result = await sqlite_store.get("ctx_session")
    assert result == b"data"


async def test_sync_to_thread_concurrency(sqlite_store: SQLiteStore) -> None:
    """Test concurrent access via sync_to_thread wrapper.

    SQLite has write serialization, so we test sequential writes
    followed by concurrent reads which is the typical session store pattern.
    """
    for i in range(10):
        await sqlite_store.set(f"session_{i}", f"data_{i}".encode())

    async def read_session(session_id: int) -> "bytes | None":
        return await sqlite_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
