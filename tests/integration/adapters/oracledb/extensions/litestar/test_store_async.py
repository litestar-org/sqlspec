"""Integration tests for Oracle session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb.config import OracleAsyncConfig
from sqlspec.adapters.oracledb.litestar.store import OracleAsyncStore

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture
async def oracle_store(oracle_23ai_service: OracleService) -> "AsyncGenerator[OracleAsyncStore, None]":
    """Create Oracle store with test database."""
    config = OracleAsyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 5,
        },
        extension_config={"litestar": {"session_table": "test_sessions"}},
    )
    store = OracleAsyncStore(config)
    try:
        await store.create_table()
        yield store
        await store.delete_all()
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_store_create_table(oracle_store: OracleAsyncStore) -> None:
    """Test table creation."""
    assert oracle_store.table_name == "test_sessions"


async def test_store_set_and_get(oracle_store: OracleAsyncStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await oracle_store.set("session_123", test_data)

    result = await oracle_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(oracle_store: OracleAsyncStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await oracle_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(oracle_store: OracleAsyncStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await oracle_store.set("session_str", "string data")

    result = await oracle_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(oracle_store: OracleAsyncStore) -> None:
    """Test delete operation."""
    await oracle_store.set("session_to_delete", b"data")

    assert await oracle_store.exists("session_to_delete")

    await oracle_store.delete("session_to_delete")

    assert not await oracle_store.exists("session_to_delete")
    assert await oracle_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(oracle_store: OracleAsyncStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await oracle_store.delete("nonexistent")


async def test_store_expiration_with_int(oracle_store: OracleAsyncStore) -> None:
    """Test session expiration with integer seconds."""
    await oracle_store.set("expiring_session", b"data", expires_in=2)

    assert await oracle_store.exists("expiring_session")

    await asyncio.sleep(2.1)

    result = await oracle_store.get("expiring_session")
    assert result is None
    assert not await oracle_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(oracle_store: OracleAsyncStore) -> None:
    """Test session expiration with timedelta."""
    await oracle_store.set("expiring_session", b"data", expires_in=timedelta(seconds=2))

    assert await oracle_store.exists("expiring_session")

    await asyncio.sleep(2.1)

    result = await oracle_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(oracle_store: OracleAsyncStore) -> None:
    """Test session without expiration persists."""
    await oracle_store.set("permanent_session", b"data")

    expires_in = await oracle_store.expires_in("permanent_session")
    assert expires_in is None

    assert await oracle_store.exists("permanent_session")


async def test_store_expires_in(oracle_store: OracleAsyncStore) -> None:
    """Test expires_in returns correct time."""
    await oracle_store.set("timed_session", b"data", expires_in=10)

    expires_in = await oracle_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(oracle_store: OracleAsyncStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await oracle_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await oracle_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(oracle_store: OracleAsyncStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await oracle_store.set("active_session", b"data", expires_in=60)
    await oracle_store.set("expired_session_1", b"data", expires_in=1)
    await oracle_store.set("expired_session_2", b"data", expires_in=1)
    await oracle_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await oracle_store.delete_expired()
    assert count == 2

    assert await oracle_store.exists("active_session")
    assert await oracle_store.exists("permanent_session")
    assert not await oracle_store.exists("expired_session_1")
    assert not await oracle_store.exists("expired_session_2")


async def test_store_upsert(oracle_store: OracleAsyncStore) -> None:
    """Test updating existing session (UPSERT)."""
    await oracle_store.set("session_upsert", b"original data")

    result = await oracle_store.get("session_upsert")
    assert result == b"original data"

    await oracle_store.set("session_upsert", b"updated data")

    result = await oracle_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(oracle_store: OracleAsyncStore) -> None:
    """Test updating session expiration."""
    await oracle_store.set("session_exp", b"data", expires_in=60)

    expires_in = await oracle_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await oracle_store.set("session_exp", b"data", expires_in=10)

    expires_in = await oracle_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(oracle_store: OracleAsyncStore) -> None:
    """Test renewing session expiration on get."""
    await oracle_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await oracle_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await oracle_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await oracle_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after >= 8  # Use >= to avoid timing race conditions


async def test_store_large_data(oracle_store: OracleAsyncStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await oracle_store.set("large_session", large_data)

    result = await oracle_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(oracle_store: OracleAsyncStore) -> None:
    """Test delete_all removes all sessions."""
    await oracle_store.set("session1", b"data1")
    await oracle_store.set("session2", b"data2")
    await oracle_store.set("session3", b"data3")

    assert await oracle_store.exists("session1")
    assert await oracle_store.exists("session2")
    assert await oracle_store.exists("session3")

    await oracle_store.delete_all()

    assert not await oracle_store.exists("session1")
    assert not await oracle_store.exists("session2")
    assert not await oracle_store.exists("session3")


async def test_store_exists(oracle_store: OracleAsyncStore) -> None:
    """Test exists method."""
    assert not await oracle_store.exists("test_session")

    await oracle_store.set("test_session", b"data")

    assert await oracle_store.exists("test_session")


async def test_store_context_manager(oracle_store: OracleAsyncStore) -> None:
    """Test store can be used as async context manager."""
    async with oracle_store:
        await oracle_store.set("ctx_session", b"data")

    result = await oracle_store.get("ctx_session")
    assert result == b"data"
