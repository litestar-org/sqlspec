"""Integration tests for Psycopg async session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
from sqlspec.adapters.psycopg.litestar.store import PsycopgAsyncStore

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.psycopg, pytest.mark.integration]


@pytest.fixture
async def psycopg_async_store(psycopg_async_config: PsycopgAsyncConfig) -> "AsyncGenerator[PsycopgAsyncStore, None]":
    """Create Psycopg async store with test database."""
    psycopg_async_config.extension_config = {"litestar": {"session_table": "test_psycopg_async_sessions"}}
    store = PsycopgAsyncStore(psycopg_async_config)
    await store.create_table()
    try:
        yield store
    finally:
        try:
            await store.delete_all()
        except Exception:
            pass


async def test_store_create_table(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test table creation."""
    assert psycopg_async_store.table_name == "test_psycopg_async_sessions"


async def test_store_set_and_get(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await psycopg_async_store.set("session_123", test_data)

    result = await psycopg_async_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await psycopg_async_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await psycopg_async_store.set("session_str", "string data")

    result = await psycopg_async_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test delete operation."""
    await psycopg_async_store.set("session_to_delete", b"data")

    assert await psycopg_async_store.exists("session_to_delete")

    await psycopg_async_store.delete("session_to_delete")

    assert not await psycopg_async_store.exists("session_to_delete")
    assert await psycopg_async_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await psycopg_async_store.delete("nonexistent")


async def test_store_expiration_with_int(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test session expiration with integer seconds."""
    await psycopg_async_store.set("expiring_session", b"data", expires_in=1)

    assert await psycopg_async_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await psycopg_async_store.get("expiring_session")
    assert result is None
    assert not await psycopg_async_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test session expiration with timedelta."""
    await psycopg_async_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await psycopg_async_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await psycopg_async_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test session without expiration persists."""
    await psycopg_async_store.set("permanent_session", b"data")

    expires_in = await psycopg_async_store.expires_in("permanent_session")
    assert expires_in is None

    assert await psycopg_async_store.exists("permanent_session")


async def test_store_expires_in(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test expires_in returns correct time."""
    await psycopg_async_store.set("timed_session", b"data", expires_in=10)

    expires_in = await psycopg_async_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await psycopg_async_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await psycopg_async_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await psycopg_async_store.set("active_session", b"data", expires_in=60)
    await psycopg_async_store.set("expired_session_1", b"data", expires_in=1)
    await psycopg_async_store.set("expired_session_2", b"data", expires_in=1)
    await psycopg_async_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await psycopg_async_store.delete_expired()
    assert count == 2

    assert await psycopg_async_store.exists("active_session")
    assert await psycopg_async_store.exists("permanent_session")
    assert not await psycopg_async_store.exists("expired_session_1")
    assert not await psycopg_async_store.exists("expired_session_2")


async def test_store_upsert(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test updating existing session (UPSERT)."""
    await psycopg_async_store.set("session_upsert", b"original data")

    result = await psycopg_async_store.get("session_upsert")
    assert result == b"original data"

    await psycopg_async_store.set("session_upsert", b"updated data")

    result = await psycopg_async_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test updating session expiration."""
    await psycopg_async_store.set("session_exp", b"data", expires_in=60)

    expires_in = await psycopg_async_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await psycopg_async_store.set("session_exp", b"data", expires_in=10)

    expires_in = await psycopg_async_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test renewing session expiration on get."""
    await psycopg_async_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await psycopg_async_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await psycopg_async_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await psycopg_async_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await psycopg_async_store.set("large_session", large_data)

    result = await psycopg_async_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test delete_all removes all sessions."""
    await psycopg_async_store.set("session1", b"data1")
    await psycopg_async_store.set("session2", b"data2")
    await psycopg_async_store.set("session3", b"data3")

    assert await psycopg_async_store.exists("session1")
    assert await psycopg_async_store.exists("session2")
    assert await psycopg_async_store.exists("session3")

    await psycopg_async_store.delete_all()

    assert not await psycopg_async_store.exists("session1")
    assert not await psycopg_async_store.exists("session2")
    assert not await psycopg_async_store.exists("session3")


async def test_store_exists(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test exists method."""
    assert not await psycopg_async_store.exists("test_session")

    await psycopg_async_store.set("test_session", b"data")

    assert await psycopg_async_store.exists("test_session")


async def test_store_context_manager(psycopg_async_store: PsycopgAsyncStore) -> None:
    """Test store can be used as async context manager."""
    async with psycopg_async_store:
        await psycopg_async_store.set("ctx_session", b"data")

    result = await psycopg_async_store.get("ctx_session")
    assert result == b"data"
