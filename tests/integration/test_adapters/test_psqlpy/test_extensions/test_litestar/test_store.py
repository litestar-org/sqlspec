"""Integration tests for Psqlpy session store."""

import asyncio
from datetime import timedelta

import pytest

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.litestar.store import PsqlpyStore

pytestmark = [pytest.mark.psqlpy, pytest.mark.integration]


@pytest.fixture
async def psqlpy_store(postgres_service):
    """Create Psqlpy store with test database."""
    config = PsqlpyConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "username": postgres_service.user,
            "password": postgres_service.password,
            "db_name": postgres_service.database,
        }
    )
    store = PsqlpyStore(config, table_name="test_psqlpy_sessions")
    try:
        await store.create_table()
        yield store
        await store.delete_all()
    finally:
        if config.pool_instance:
            await config.close_pool()


@pytest.mark.asyncio
async def test_store_create_table(psqlpy_store):
    """Test table creation."""
    assert psqlpy_store.table_name == "test_psqlpy_sessions"


@pytest.mark.asyncio
async def test_store_set_and_get(psqlpy_store):
    """Test basic set and get operations."""
    test_data = b"test session data"
    await psqlpy_store.set("session_123", test_data)

    result = await psqlpy_store.get("session_123")
    assert result == test_data


@pytest.mark.asyncio
async def test_store_get_nonexistent(psqlpy_store):
    """Test getting a non-existent session returns None."""
    result = await psqlpy_store.get("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_store_set_with_string_value(psqlpy_store):
    """Test setting a string value (should be converted to bytes)."""
    await psqlpy_store.set("session_str", "string data")

    result = await psqlpy_store.get("session_str")
    assert result == b"string data"


@pytest.mark.asyncio
async def test_store_delete(psqlpy_store):
    """Test delete operation."""
    await psqlpy_store.set("session_to_delete", b"data")

    assert await psqlpy_store.exists("session_to_delete")

    await psqlpy_store.delete("session_to_delete")

    assert not await psqlpy_store.exists("session_to_delete")
    assert await psqlpy_store.get("session_to_delete") is None


@pytest.mark.asyncio
async def test_store_delete_nonexistent(psqlpy_store):
    """Test deleting a non-existent session is a no-op."""
    await psqlpy_store.delete("nonexistent")


@pytest.mark.asyncio
async def test_store_expiration_with_int(psqlpy_store):
    """Test session expiration with integer seconds."""
    await psqlpy_store.set("expiring_session", b"data", expires_in=1)

    assert await psqlpy_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await psqlpy_store.get("expiring_session")
    assert result is None
    assert not await psqlpy_store.exists("expiring_session")


@pytest.mark.asyncio
async def test_store_expiration_with_timedelta(psqlpy_store):
    """Test session expiration with timedelta."""
    await psqlpy_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await psqlpy_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await psqlpy_store.get("expiring_session")
    assert result is None


@pytest.mark.asyncio
async def test_store_no_expiration(psqlpy_store):
    """Test session without expiration persists."""
    await psqlpy_store.set("permanent_session", b"data")

    expires_in = await psqlpy_store.expires_in("permanent_session")
    assert expires_in is None

    assert await psqlpy_store.exists("permanent_session")


@pytest.mark.asyncio
async def test_store_expires_in(psqlpy_store):
    """Test expires_in returns correct time."""
    await psqlpy_store.set("timed_session", b"data", expires_in=10)

    expires_in = await psqlpy_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


@pytest.mark.asyncio
async def test_store_expires_in_expired(psqlpy_store):
    """Test expires_in returns 0 for expired session."""
    await psqlpy_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await psqlpy_store.expires_in("expired_session")
    assert expires_in == 0


@pytest.mark.asyncio
async def test_store_cleanup(psqlpy_store):
    """Test delete_expired removes only expired sessions."""
    await psqlpy_store.set("active_session", b"data", expires_in=60)
    await psqlpy_store.set("expired_session_1", b"data", expires_in=1)
    await psqlpy_store.set("expired_session_2", b"data", expires_in=1)
    await psqlpy_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await psqlpy_store.delete_expired()
    assert count == 2

    assert await psqlpy_store.exists("active_session")
    assert await psqlpy_store.exists("permanent_session")
    assert not await psqlpy_store.exists("expired_session_1")
    assert not await psqlpy_store.exists("expired_session_2")


@pytest.mark.asyncio
async def test_store_upsert(psqlpy_store):
    """Test updating existing session (UPSERT)."""
    await psqlpy_store.set("session_upsert", b"original data")

    result = await psqlpy_store.get("session_upsert")
    assert result == b"original data"

    await psqlpy_store.set("session_upsert", b"updated data")

    result = await psqlpy_store.get("session_upsert")
    assert result == b"updated data"


@pytest.mark.asyncio
async def test_store_upsert_with_expiration_change(psqlpy_store):
    """Test updating session expiration."""
    await psqlpy_store.set("session_exp", b"data", expires_in=60)

    expires_in = await psqlpy_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await psqlpy_store.set("session_exp", b"data", expires_in=10)

    expires_in = await psqlpy_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


@pytest.mark.asyncio
async def test_store_renew_for(psqlpy_store):
    """Test renewing session expiration on get."""
    await psqlpy_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await psqlpy_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await psqlpy_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await psqlpy_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


@pytest.mark.asyncio
async def test_store_large_data(psqlpy_store):
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await psqlpy_store.set("large_session", large_data)

    result = await psqlpy_store.get("large_session")
    assert result == large_data
    assert len(result) > 1024 * 1024


@pytest.mark.asyncio
async def test_store_delete_all(psqlpy_store):
    """Test delete_all removes all sessions."""
    await psqlpy_store.set("session1", b"data1")
    await psqlpy_store.set("session2", b"data2")
    await psqlpy_store.set("session3", b"data3")

    assert await psqlpy_store.exists("session1")
    assert await psqlpy_store.exists("session2")
    assert await psqlpy_store.exists("session3")

    await psqlpy_store.delete_all()

    assert not await psqlpy_store.exists("session1")
    assert not await psqlpy_store.exists("session2")
    assert not await psqlpy_store.exists("session3")


@pytest.mark.asyncio
async def test_store_exists(psqlpy_store):
    """Test exists method."""
    assert not await psqlpy_store.exists("test_session")

    await psqlpy_store.set("test_session", b"data")

    assert await psqlpy_store.exists("test_session")


@pytest.mark.asyncio
async def test_store_context_manager(psqlpy_store):
    """Test store can be used as async context manager."""
    async with psqlpy_store:
        await psqlpy_store.set("ctx_session", b"data")

    result = await psqlpy_store.get("ctx_session")
    assert result == b"data"
