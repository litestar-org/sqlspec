"""Integration tests for ADBC session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.litestar.store import ADBCStore

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.adbc, pytest.mark.integration]


@pytest.fixture
async def adbc_store(adbc_postgres_config: AdbcConfig) -> AsyncGenerator[ADBCStore, None]:
    """Create ADBC store with PostgreSQL backend."""
    adbc_postgres_config.extension_config = {"litestar": {"session_table": "test_adbc_sessions"}}
    store = ADBCStore(adbc_postgres_config)
    await store.create_table()
    try:
        yield store
    finally:
        try:
            await store.delete_all()
        except Exception:  # pragma: no cover - best effort cleanup
            pass


async def test_store_create_table(adbc_store: ADBCStore) -> None:
    """Test table creation."""
    assert adbc_store.table_name == "test_adbc_sessions"


async def test_store_set_and_get(adbc_store: ADBCStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await adbc_store.set("session_123", test_data)

    result = await adbc_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(adbc_store: ADBCStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await adbc_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(adbc_store: ADBCStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await adbc_store.set("session_str", "string data")

    result = await adbc_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(adbc_store: ADBCStore) -> None:
    """Test delete operation."""
    await adbc_store.set("session_to_delete", b"data")

    assert await adbc_store.exists("session_to_delete")

    await adbc_store.delete("session_to_delete")

    assert not await adbc_store.exists("session_to_delete")
    assert await adbc_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(adbc_store: ADBCStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await adbc_store.delete("nonexistent")


async def test_store_expiration_with_int(adbc_store: ADBCStore) -> None:
    """Test session expiration with integer seconds."""
    await adbc_store.set("expiring_session", b"data", expires_in=1)

    assert await adbc_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await adbc_store.get("expiring_session")
    assert result is None
    assert not await adbc_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(adbc_store: ADBCStore) -> None:
    """Test session expiration with timedelta."""
    await adbc_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await adbc_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await adbc_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(adbc_store: ADBCStore) -> None:
    """Test session without expiration persists."""
    await adbc_store.set("permanent_session", b"data")

    expires_in = await adbc_store.expires_in("permanent_session")
    assert expires_in is None

    assert await adbc_store.exists("permanent_session")


async def test_store_expires_in(adbc_store: ADBCStore) -> None:
    """Test expires_in returns correct time."""
    await adbc_store.set("timed_session", b"data", expires_in=10)

    expires_in = await adbc_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(adbc_store: ADBCStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await adbc_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await adbc_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(adbc_store: ADBCStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await adbc_store.set("active_session", b"data", expires_in=60)
    await adbc_store.set("expired_session_1", b"data", expires_in=1)
    await adbc_store.set("expired_session_2", b"data", expires_in=1)
    await adbc_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await adbc_store.delete_expired()
    assert count == 2

    assert await adbc_store.exists("active_session")
    assert await adbc_store.exists("permanent_session")
    assert not await adbc_store.exists("expired_session_1")
    assert not await adbc_store.exists("expired_session_2")


async def test_store_upsert(adbc_store: ADBCStore) -> None:
    """Test updating existing session (UPSERT)."""
    await adbc_store.set("session_upsert", b"original data")

    result = await adbc_store.get("session_upsert")
    assert result == b"original data"

    await adbc_store.set("session_upsert", b"updated data")

    result = await adbc_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(adbc_store: ADBCStore) -> None:
    """Test updating session expiration."""
    await adbc_store.set("session_exp", b"data", expires_in=60)

    expires_in = await adbc_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await adbc_store.set("session_exp", b"data", expires_in=10)

    expires_in = await adbc_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(adbc_store: ADBCStore) -> None:
    """Test renewing session expiration on get."""
    await adbc_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await adbc_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await adbc_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await adbc_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(adbc_store: ADBCStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await adbc_store.set("large_session", large_data)

    result = await adbc_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(adbc_store: ADBCStore) -> None:
    """Test delete_all removes all sessions."""
    await adbc_store.set("session1", b"data1")
    await adbc_store.set("session2", b"data2")
    await adbc_store.set("session3", b"data3")

    assert await adbc_store.exists("session1")
    assert await adbc_store.exists("session2")
    assert await adbc_store.exists("session3")

    await adbc_store.delete_all()

    assert not await adbc_store.exists("session1")
    assert not await adbc_store.exists("session2")
    assert not await adbc_store.exists("session3")


async def test_store_exists(adbc_store: ADBCStore) -> None:
    """Test exists method."""
    assert not await adbc_store.exists("test_session")

    await adbc_store.set("test_session", b"data")

    assert await adbc_store.exists("test_session")


async def test_store_context_manager(adbc_store: ADBCStore) -> None:
    """Test store can be used as async context manager."""
    async with adbc_store:
        await adbc_store.set("ctx_session", b"data")

    result = await adbc_store.get("ctx_session")
    assert result == b"data"


async def test_sync_to_thread_concurrency(adbc_store: ADBCStore) -> None:
    """Test concurrent access via sync_to_thread wrapper.

    ADBC with PostgreSQL supports concurrent reads and writes.
    We test concurrent writes followed by concurrent reads.
    """

    async def write_session(session_id: int) -> None:
        await adbc_store.set(f"session_{session_id}", f"data_{session_id}".encode())

    await asyncio.gather(*[write_session(i) for i in range(10)])

    async def read_session(session_id: int) -> "bytes | None":
        return await adbc_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
