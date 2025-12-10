"""Integration tests for AsyncMy session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.adapters.asyncmy.litestar.store import AsyncmyStore

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.asyncmy, pytest.mark.integration]


@pytest.fixture
async def asyncmy_store(mysql_service: MySQLService) -> "AsyncGenerator[AsyncmyStore, None]":
    """Create AsyncMy store with test database."""
    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
        },
        extension_config={"litestar": {"session_table": "test_asyncmy_sessions"}},
    )
    store = AsyncmyStore(config)
    try:
        await store.create_table()
        yield store
        try:
            await store.delete_all()
        except Exception:
            pass
    finally:
        try:
            if config.connection_instance:
                await config.close_pool()
        except Exception:
            pass


async def test_store_create_table(asyncmy_store: AsyncmyStore) -> None:
    """Test table creation."""
    assert asyncmy_store.table_name == "test_asyncmy_sessions"


async def test_store_set_and_get(asyncmy_store: AsyncmyStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await asyncmy_store.set("session_123", test_data)

    result = await asyncmy_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(asyncmy_store: AsyncmyStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await asyncmy_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(asyncmy_store: AsyncmyStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await asyncmy_store.set("session_str", "string data")

    result = await asyncmy_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(asyncmy_store: AsyncmyStore) -> None:
    """Test delete operation."""
    await asyncmy_store.set("session_to_delete", b"data")

    assert await asyncmy_store.exists("session_to_delete")

    await asyncmy_store.delete("session_to_delete")

    assert not await asyncmy_store.exists("session_to_delete")
    assert await asyncmy_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(asyncmy_store: AsyncmyStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await asyncmy_store.delete("nonexistent")


async def test_store_expiration_with_int(asyncmy_store: AsyncmyStore) -> None:
    """Test session expiration with integer seconds."""
    await asyncmy_store.set("expiring_session", b"data", expires_in=1)

    assert await asyncmy_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await asyncmy_store.get("expiring_session")
    assert result is None
    assert not await asyncmy_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(asyncmy_store: AsyncmyStore) -> None:
    """Test session expiration with timedelta."""
    await asyncmy_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await asyncmy_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await asyncmy_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(asyncmy_store: AsyncmyStore) -> None:
    """Test session without expiration persists."""
    await asyncmy_store.set("permanent_session", b"data")

    expires_in = await asyncmy_store.expires_in("permanent_session")
    assert expires_in is None

    assert await asyncmy_store.exists("permanent_session")


async def test_store_expires_in(asyncmy_store: AsyncmyStore) -> None:
    """Test expires_in returns correct time."""
    await asyncmy_store.set("timed_session", b"data", expires_in=10)

    expires_in = await asyncmy_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(asyncmy_store: AsyncmyStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await asyncmy_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await asyncmy_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(asyncmy_store: AsyncmyStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await asyncmy_store.set("active_session", b"data", expires_in=60)
    await asyncmy_store.set("expired_session_1", b"data", expires_in=1)
    await asyncmy_store.set("expired_session_2", b"data", expires_in=1)
    await asyncmy_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await asyncmy_store.delete_expired()
    assert count == 2

    assert await asyncmy_store.exists("active_session")
    assert await asyncmy_store.exists("permanent_session")
    assert not await asyncmy_store.exists("expired_session_1")
    assert not await asyncmy_store.exists("expired_session_2")


async def test_store_upsert(asyncmy_store: AsyncmyStore) -> None:
    """Test updating existing session (UPSERT)."""
    await asyncmy_store.set("session_upsert", b"original data")

    result = await asyncmy_store.get("session_upsert")
    assert result == b"original data"

    await asyncmy_store.set("session_upsert", b"updated data")

    result = await asyncmy_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(asyncmy_store: AsyncmyStore) -> None:
    """Test updating session expiration."""
    await asyncmy_store.set("session_exp", b"data", expires_in=60)

    expires_in = await asyncmy_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await asyncmy_store.set("session_exp", b"data", expires_in=10)

    expires_in = await asyncmy_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(asyncmy_store: AsyncmyStore) -> None:
    """Test renewing session expiration on get."""
    await asyncmy_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await asyncmy_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await asyncmy_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await asyncmy_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(asyncmy_store: AsyncmyStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await asyncmy_store.set("large_session", large_data)

    result = await asyncmy_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(asyncmy_store: AsyncmyStore) -> None:
    """Test delete_all removes all sessions."""
    await asyncmy_store.set("session1", b"data1")
    await asyncmy_store.set("session2", b"data2")
    await asyncmy_store.set("session3", b"data3")

    assert await asyncmy_store.exists("session1")
    assert await asyncmy_store.exists("session2")
    assert await asyncmy_store.exists("session3")

    await asyncmy_store.delete_all()

    assert not await asyncmy_store.exists("session1")
    assert not await asyncmy_store.exists("session2")
    assert not await asyncmy_store.exists("session3")


async def test_store_exists(asyncmy_store: AsyncmyStore) -> None:
    """Test exists method."""
    assert not await asyncmy_store.exists("test_session")

    await asyncmy_store.set("test_session", b"data")

    assert await asyncmy_store.exists("test_session")


async def test_store_context_manager(asyncmy_store: AsyncmyStore) -> None:
    """Test store can be used as async context manager."""
    async with asyncmy_store:
        await asyncmy_store.set("ctx_session", b"data")

    result = await asyncmy_store.get("ctx_session")
    assert result == b"data"
