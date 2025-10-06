"""Integration tests for BigQuery session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.litestar.store import BigQueryStore

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture
async def bigquery_store(bigquery_config: BigQueryConfig) -> AsyncGenerator[BigQueryStore, None]:
    """Create BigQuery store with test database.

    Note:
        Uses the bigquery_config fixture from conftest.py which provides
        a configured BigQuery connection. The test table will be created
        and cleaned up automatically.
    """
    store = BigQueryStore(bigquery_config, table_name="test_sessions")
    try:
        await store.create_table()
        yield store
        await store.delete_all()
    finally:
        pass


async def test_store_create_table(bigquery_store: BigQueryStore) -> None:
    """Test table creation."""
    assert bigquery_store.table_name == "test_sessions"


async def test_store_set_and_get(bigquery_store: BigQueryStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await bigquery_store.set("session_123", test_data)

    result = await bigquery_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(bigquery_store: BigQueryStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await bigquery_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(bigquery_store: BigQueryStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await bigquery_store.set("session_str", "string data")

    result = await bigquery_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(bigquery_store: BigQueryStore) -> None:
    """Test delete operation."""
    await bigquery_store.set("session_to_delete", b"data")

    assert await bigquery_store.exists("session_to_delete")

    await bigquery_store.delete("session_to_delete")

    assert not await bigquery_store.exists("session_to_delete")
    assert await bigquery_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(bigquery_store: BigQueryStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await bigquery_store.delete("nonexistent")


async def test_store_expiration_with_int(bigquery_store: BigQueryStore) -> None:
    """Test session expiration with integer seconds."""
    await bigquery_store.set("expiring_session", b"data", expires_in=1)

    assert await bigquery_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await bigquery_store.get("expiring_session")
    assert result is None
    assert not await bigquery_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(bigquery_store: BigQueryStore) -> None:
    """Test session expiration with timedelta."""
    await bigquery_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await bigquery_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await bigquery_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(bigquery_store: BigQueryStore) -> None:
    """Test session without expiration persists."""
    await bigquery_store.set("permanent_session", b"data")

    expires_in = await bigquery_store.expires_in("permanent_session")
    assert expires_in is None

    assert await bigquery_store.exists("permanent_session")


async def test_store_expires_in(bigquery_store: BigQueryStore) -> None:
    """Test expires_in returns correct time."""
    await bigquery_store.set("timed_session", b"data", expires_in=10)

    expires_in = await bigquery_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(bigquery_store: BigQueryStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await bigquery_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await bigquery_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(bigquery_store: BigQueryStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await bigquery_store.set("active_session", b"data", expires_in=60)
    await bigquery_store.set("expired_session_1", b"data", expires_in=1)
    await bigquery_store.set("expired_session_2", b"data", expires_in=1)
    await bigquery_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await bigquery_store.delete_expired()
    assert count == 2

    assert await bigquery_store.exists("active_session")
    assert await bigquery_store.exists("permanent_session")
    assert not await bigquery_store.exists("expired_session_1")
    assert not await bigquery_store.exists("expired_session_2")


async def test_store_upsert(bigquery_store: BigQueryStore) -> None:
    """Test updating existing session (UPSERT)."""
    await bigquery_store.set("session_upsert", b"original data")

    result = await bigquery_store.get("session_upsert")
    assert result == b"original data"

    await bigquery_store.set("session_upsert", b"updated data")

    result = await bigquery_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(bigquery_store: BigQueryStore) -> None:
    """Test updating session expiration."""
    await bigquery_store.set("session_exp", b"data", expires_in=60)

    expires_in = await bigquery_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await bigquery_store.set("session_exp", b"data", expires_in=10)

    expires_in = await bigquery_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(bigquery_store: BigQueryStore) -> None:
    """Test renewing session expiration on get."""
    await bigquery_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await bigquery_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await bigquery_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await bigquery_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(bigquery_store: BigQueryStore) -> None:
    """Test storing large session data (>1MB).

    Note:
        BigQuery supports up to 10MB per cell, so 1MB session data
        is well within limits.
    """
    large_data = b"x" * (1024 * 1024 + 100)

    await bigquery_store.set("large_session", large_data)

    result = await bigquery_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(bigquery_store: BigQueryStore) -> None:
    """Test delete_all removes all sessions."""
    await bigquery_store.set("session1", b"data1")
    await bigquery_store.set("session2", b"data2")
    await bigquery_store.set("session3", b"data3")

    assert await bigquery_store.exists("session1")
    assert await bigquery_store.exists("session2")
    assert await bigquery_store.exists("session3")

    await bigquery_store.delete_all()

    assert not await bigquery_store.exists("session1")
    assert not await bigquery_store.exists("session2")
    assert not await bigquery_store.exists("session3")


async def test_store_exists(bigquery_store: BigQueryStore) -> None:
    """Test exists method."""
    assert not await bigquery_store.exists("test_session")

    await bigquery_store.set("test_session", b"data")

    assert await bigquery_store.exists("test_session")


async def test_store_context_manager(bigquery_store: BigQueryStore) -> None:
    """Test store can be used as async context manager."""
    async with bigquery_store:
        await bigquery_store.set("ctx_session", b"data")

    result = await bigquery_store.get("ctx_session")
    assert result == b"data"


async def test_sync_to_thread_concurrency(bigquery_store: BigQueryStore) -> None:
    """Test concurrent access via sync_to_thread wrapper.

    BigQuery is a cloud data warehouse optimized for concurrent reads
    and can handle multiple concurrent operations efficiently.
    """
    for i in range(10):
        await bigquery_store.set(f"session_{i}", f"data_{i}".encode())

    async def read_session(session_id: int) -> "bytes | None":
        return await bigquery_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
