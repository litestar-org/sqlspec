"""Integration tests for aiomysql session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql.config import AiomysqlConfig
from sqlspec.adapters.aiomysql.litestar.store import AiomysqlStore

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.aiomysql, pytest.mark.integration]


@pytest.fixture
async def aiomysql_store(mysql_service: MySQLService) -> "AsyncGenerator[AiomysqlStore, None]":
    """Create aiomysql store with test database."""
    config = AiomysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "db": mysql_service.db,
        },
        extension_config={"litestar": {"session_table": "test_aiomysql_sessions"}},
    )
    store = AiomysqlStore(config)
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


async def test_store_create_table(aiomysql_store: AiomysqlStore) -> None:
    """Test table creation."""
    assert aiomysql_store.table_name == "test_aiomysql_sessions"


async def test_store_set_and_get(aiomysql_store: AiomysqlStore) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await aiomysql_store.set("session_123", test_data)

    result = await aiomysql_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(aiomysql_store: AiomysqlStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await aiomysql_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(aiomysql_store: AiomysqlStore) -> None:
    """Test setting a string value (should be converted to bytes)."""
    await aiomysql_store.set("session_str", "string data")

    result = await aiomysql_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(aiomysql_store: AiomysqlStore) -> None:
    """Test delete operation."""
    await aiomysql_store.set("session_to_delete", b"data")

    assert await aiomysql_store.exists("session_to_delete")

    await aiomysql_store.delete("session_to_delete")

    assert not await aiomysql_store.exists("session_to_delete")
    assert await aiomysql_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(aiomysql_store: AiomysqlStore) -> None:
    """Test deleting a non-existent session is a no-op."""
    await aiomysql_store.delete("nonexistent")


async def test_store_expiration_with_int(aiomysql_store: AiomysqlStore) -> None:
    """Test session expiration with integer seconds."""
    await aiomysql_store.set("expiring_session", b"data", expires_in=1)

    assert await aiomysql_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await aiomysql_store.get("expiring_session")
    assert result is None
    assert not await aiomysql_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(aiomysql_store: AiomysqlStore) -> None:
    """Test session expiration with timedelta."""
    await aiomysql_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await aiomysql_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await aiomysql_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(aiomysql_store: AiomysqlStore) -> None:
    """Test session without expiration persists."""
    await aiomysql_store.set("permanent_session", b"data")

    expires_in = await aiomysql_store.expires_in("permanent_session")
    assert expires_in is None

    assert await aiomysql_store.exists("permanent_session")


async def test_store_expires_in(aiomysql_store: AiomysqlStore) -> None:
    """Test expires_in returns correct time."""
    await aiomysql_store.set("timed_session", b"data", expires_in=10)

    expires_in = await aiomysql_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(aiomysql_store: AiomysqlStore) -> None:
    """Test expires_in returns 0 for expired session."""
    await aiomysql_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await aiomysql_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(aiomysql_store: AiomysqlStore) -> None:
    """Test delete_expired removes only expired sessions."""
    await aiomysql_store.set("active_session", b"data", expires_in=60)
    await aiomysql_store.set("expired_session_1", b"data", expires_in=1)
    await aiomysql_store.set("expired_session_2", b"data", expires_in=1)
    await aiomysql_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await aiomysql_store.delete_expired()
    assert count == 2

    assert await aiomysql_store.exists("active_session")
    assert await aiomysql_store.exists("permanent_session")
    assert not await aiomysql_store.exists("expired_session_1")
    assert not await aiomysql_store.exists("expired_session_2")


async def test_store_upsert(aiomysql_store: AiomysqlStore) -> None:
    """Test updating existing session (UPSERT)."""
    await aiomysql_store.set("session_upsert", b"original data")

    result = await aiomysql_store.get("session_upsert")
    assert result == b"original data"

    await aiomysql_store.set("session_upsert", b"updated data")

    result = await aiomysql_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(aiomysql_store: AiomysqlStore) -> None:
    """Test updating session expiration."""
    await aiomysql_store.set("session_exp", b"data", expires_in=60)

    expires_in = await aiomysql_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await aiomysql_store.set("session_exp", b"data", expires_in=10)

    expires_in = await aiomysql_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(aiomysql_store: AiomysqlStore) -> None:
    """Test renewing session expiration on get."""
    await aiomysql_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await aiomysql_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await aiomysql_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await aiomysql_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(aiomysql_store: AiomysqlStore) -> None:
    """Test storing large session data (>1MB)."""
    large_data = b"x" * (1024 * 1024 + 100)

    await aiomysql_store.set("large_session", large_data)

    result = await aiomysql_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(aiomysql_store: AiomysqlStore) -> None:
    """Test delete_all removes all sessions."""
    await aiomysql_store.set("session1", b"data1")
    await aiomysql_store.set("session2", b"data2")
    await aiomysql_store.set("session3", b"data3")

    assert await aiomysql_store.exists("session1")
    assert await aiomysql_store.exists("session2")
    assert await aiomysql_store.exists("session3")

    await aiomysql_store.delete_all()

    assert not await aiomysql_store.exists("session1")
    assert not await aiomysql_store.exists("session2")
    assert not await aiomysql_store.exists("session3")


async def test_store_exists(aiomysql_store: AiomysqlStore) -> None:
    """Test exists method."""
    assert not await aiomysql_store.exists("test_session")

    await aiomysql_store.set("test_session", b"data")

    assert await aiomysql_store.exists("test_session")


async def test_store_context_manager(aiomysql_store: AiomysqlStore) -> None:
    """Test store can be used as async context manager."""
    async with aiomysql_store:
        await aiomysql_store.set("ctx_session", b"data")

    result = await aiomysql_store.get("ctx_session")
    assert result == b"data"
