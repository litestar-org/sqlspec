"""Integration tests for DuckDB sync session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta
from pathlib import Path

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.duckdb.litestar.store import DuckdbStore

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
async def duckdb_store(tmp_path: Path, worker_id: str) -> AsyncGenerator[DuckdbStore, None]:
    """Create DuckDB store with temporary file-based database.

    Args:
        tmp_path: Pytest fixture providing unique temporary directory per test.
        worker_id: Pytest-xdist fixture providing unique worker identifier.

    Note:
        DuckDB in-memory databases are connection-local, not process-wide.
        Since the thread-local connection pool creates separate connection
        objects for each thread, we must use a file-based database to ensure
        all threads share the same data.

        Worker ID ensures parallel pytest-xdist workers use separate database
        files, preventing file locking conflicts.
    """
    db_path = tmp_path / f"test_sessions_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={"litestar": {"session_table": "test_sessions"}},
        )
        store = DuckdbStore(config)
        await store.create_table()
        yield store
        await store.delete_all()
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_store_create_table(duckdb_store: DuckdbStore) -> None:
    """Test table creation."""
    assert duckdb_store.table_name == "test_sessions"


async def test_store_set_and_get(duckdb_store: DuckdbStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await duckdb_store.set("session_123", test_data)

    result = await duckdb_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(duckdb_store: DuckdbStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await duckdb_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(duckdb_store: DuckdbStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await duckdb_store.set("session_str", "string data")

    result = await duckdb_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(duckdb_store: DuckdbStore) -> None:
    """Test delete operation."""
    await duckdb_store.set("session_to_delete", b"data")

    assert await duckdb_store.exists("session_to_delete")

    await duckdb_store.delete("session_to_delete")

    assert not await duckdb_store.exists("session_to_delete")
    assert await duckdb_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(duckdb_store: DuckdbStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await duckdb_store.delete("nonexistent")


async def test_store_expiration_with_int(duckdb_store: DuckdbStore) -> None:
    """Test session expiration with integer seconds."""
    await duckdb_store.set("expiring_session", b"data", expires_in=1)

    assert await duckdb_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await duckdb_store.get("expiring_session")
    assert result is None
    assert not await duckdb_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(duckdb_store: DuckdbStore) -> None:
    """Test session expiration with timedelta."""
    await duckdb_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await duckdb_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await duckdb_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(duckdb_store: DuckdbStore) -> None:
    """Test session without expiration persists."""
    await duckdb_store.set("permanent_session", b"data")

    expires_in = await duckdb_store.expires_in("permanent_session")
    assert expires_in is None

    assert await duckdb_store.exists("permanent_session")


async def test_store_expires_in(duckdb_store: DuckdbStore) -> None:
    """Test expires_in returns correct time."""
    await duckdb_store.set("timed_session", b"data", expires_in=10)

    expires_in = await duckdb_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(duckdb_store: DuckdbStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await duckdb_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await duckdb_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(duckdb_store: DuckdbStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await duckdb_store.set("active_session", b"data", expires_in=60)
    await duckdb_store.set("expired_session_1", b"data", expires_in=1)
    await duckdb_store.set("expired_session_2", b"data", expires_in=1)
    await duckdb_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await duckdb_store.delete_expired()
    assert count == 2

    assert await duckdb_store.exists("active_session")
    assert await duckdb_store.exists("permanent_session")
    assert not await duckdb_store.exists("expired_session_1")
    assert not await duckdb_store.exists("expired_session_2")


async def test_store_upsert(duckdb_store: DuckdbStore) -> None:
    """Test updating existing session (UPSERT)."""
    await duckdb_store.set("session_upsert", b"original data")

    result = await duckdb_store.get("session_upsert")
    assert result == b"original data"

    await duckdb_store.set("session_upsert", b"updated data")

    result = await duckdb_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(duckdb_store: DuckdbStore) -> None:
    """Test updating session expiration."""
    await duckdb_store.set("session_exp", b"data", expires_in=60)

    expires_in = await duckdb_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await duckdb_store.set("session_exp", b"data", expires_in=10)

    expires_in = await duckdb_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(duckdb_store: DuckdbStore) -> None:
    """Test renewing session expiration on get."""
    await duckdb_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await duckdb_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await duckdb_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await duckdb_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(duckdb_store: DuckdbStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await duckdb_store.set("large_session", large_data)

    result = await duckdb_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(duckdb_store: DuckdbStore) -> None:
    """Test delete_all removes all sessions."""
    await duckdb_store.set("session1", b"data1")
    await duckdb_store.set("session2", b"data2")
    await duckdb_store.set("session3", b"data3")

    assert await duckdb_store.exists("session1")
    assert await duckdb_store.exists("session2")
    assert await duckdb_store.exists("session3")

    await duckdb_store.delete_all()

    assert not await duckdb_store.exists("session1")
    assert not await duckdb_store.exists("session2")
    assert not await duckdb_store.exists("session3")


async def test_store_exists(duckdb_store: DuckdbStore) -> None:
    """Test exists method."""
    assert not await duckdb_store.exists("test_session")

    await duckdb_store.set("test_session", b"data")

    assert await duckdb_store.exists("test_session")


async def test_store_context_manager(duckdb_store: DuckdbStore) -> None:
    """Test store can be used as async context manager."""
    async with duckdb_store:
        await duckdb_store.set("ctx_session", b"data")

    result = await duckdb_store.get("ctx_session")
    assert result == b"data"


async def test_sync_to_thread_concurrency(duckdb_store: DuckdbStore) -> None:
    """Test concurrent access via sync_to_thread wrapper.

    DuckDB has write serialization, so we test sequential writes
    followed by concurrent reads which is the typical session store pattern.
    """
    for i in range(10):
        await duckdb_store.set(f"session_{i}", f"data_{i}".encode())

    async def read_session(session_id: int) -> "bytes | None":
        return await duckdb_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
