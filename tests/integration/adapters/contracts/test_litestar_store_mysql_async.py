"""MySQL async-family Litestar store contract tests."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql.litestar.store import AiomysqlStore
from sqlspec.adapters.asyncmy.litestar.store import AsyncmyStore
from tests.integration.adapters.contracts._mysql_async import (
    MYSQL_ASYNC_ADAPTERS,
    close_mysql_async_config,
    mysql_async_config,
)

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.integration]


def _litestar_store_type(adapter: str) -> type[Any]:
    if adapter == "aiomysql":
        return AiomysqlStore
    return AsyncmyStore


@pytest.fixture(params=MYSQL_ASYNC_ADAPTERS)
def mysql_async_litestar_adapter(request: pytest.FixtureRequest) -> str:
    """Return the MySQL async-family Litestar store adapter under test."""
    return str(request.param)


@pytest.fixture
async def mysql_async_litestar_store(
    mysql_async_litestar_adapter: str, mysql_service: MySQLService
) -> AsyncGenerator[Any, None]:
    """Create a MySQL async-family Litestar store with test database."""
    table_name = f"test_{mysql_async_litestar_adapter}_sessions"
    config = mysql_async_config(
        mysql_async_litestar_adapter, mysql_service, extension_config={"litestar": {"session_table": table_name}}
    )
    store = _litestar_store_type(mysql_async_litestar_adapter)(config)
    try:
        async with config.provide_session() as driver:
            await driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")
        await store.create_table()
        yield store
        await store.delete_all()
        async with config.provide_session() as driver:
            await driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")
    finally:
        await close_mysql_async_config(config)


async def test_store_create_table(mysql_async_litestar_adapter: str, mysql_async_litestar_store: Any) -> None:
    """Test table creation."""
    assert mysql_async_litestar_store.table_name == f"test_{mysql_async_litestar_adapter}_sessions"


async def test_store_set_and_get(mysql_async_litestar_store: Any) -> None:
    """Test basic set and get operations."""
    test_data = b"test session data"
    await mysql_async_litestar_store.set("session_123", test_data)

    result = await mysql_async_litestar_store.get("session_123")
    assert result == test_data


async def test_store_get_nonexistent(mysql_async_litestar_store: Any) -> None:
    """Test getting a non-existent session returns None."""
    result = await mysql_async_litestar_store.get("nonexistent")
    assert result is None


async def test_store_set_with_string_value(mysql_async_litestar_store: Any) -> None:
    """Test setting a string value."""
    await mysql_async_litestar_store.set("session_str", "string data")

    result = await mysql_async_litestar_store.get("session_str")
    assert result == b"string data"


async def test_store_delete(mysql_async_litestar_store: Any) -> None:
    """Test delete operation."""
    await mysql_async_litestar_store.set("session_to_delete", b"data")

    assert await mysql_async_litestar_store.exists("session_to_delete")

    await mysql_async_litestar_store.delete("session_to_delete")

    assert not await mysql_async_litestar_store.exists("session_to_delete")
    assert await mysql_async_litestar_store.get("session_to_delete") is None


async def test_store_delete_nonexistent(mysql_async_litestar_store: Any) -> None:
    """Test deleting a non-existent session is a no-op."""
    await mysql_async_litestar_store.delete("nonexistent")


async def test_store_expiration_with_int(mysql_async_litestar_store: Any) -> None:
    """Test session expiration with integer seconds."""
    await mysql_async_litestar_store.set("expiring_session", b"data", expires_in=1)

    assert await mysql_async_litestar_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await mysql_async_litestar_store.get("expiring_session")
    assert result is None
    assert not await mysql_async_litestar_store.exists("expiring_session")


async def test_store_expiration_with_timedelta(mysql_async_litestar_store: Any) -> None:
    """Test session expiration with timedelta."""
    await mysql_async_litestar_store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))

    assert await mysql_async_litestar_store.exists("expiring_session")

    await asyncio.sleep(1.1)

    result = await mysql_async_litestar_store.get("expiring_session")
    assert result is None


async def test_store_no_expiration(mysql_async_litestar_store: Any) -> None:
    """Test session without expiration persists."""
    await mysql_async_litestar_store.set("permanent_session", b"data")

    expires_in = await mysql_async_litestar_store.expires_in("permanent_session")
    assert expires_in is None

    assert await mysql_async_litestar_store.exists("permanent_session")


async def test_store_expires_in(mysql_async_litestar_store: Any) -> None:
    """Test expires_in returns correct time."""
    await mysql_async_litestar_store.set("timed_session", b"data", expires_in=10)

    expires_in = await mysql_async_litestar_store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def test_store_expires_in_expired(mysql_async_litestar_store: Any) -> None:
    """Test expires_in returns 0 for expired session."""
    await mysql_async_litestar_store.set("expired_session", b"data", expires_in=1)

    await asyncio.sleep(1.1)

    expires_in = await mysql_async_litestar_store.expires_in("expired_session")
    assert expires_in == 0


async def test_store_cleanup(mysql_async_litestar_store: Any) -> None:
    """Test delete_expired removes only expired sessions."""
    await mysql_async_litestar_store.set("active_session", b"data", expires_in=60)
    await mysql_async_litestar_store.set("expired_session_1", b"data", expires_in=1)
    await mysql_async_litestar_store.set("expired_session_2", b"data", expires_in=1)
    await mysql_async_litestar_store.set("permanent_session", b"data")

    await asyncio.sleep(1.1)

    count = await mysql_async_litestar_store.delete_expired()
    assert count == 2

    assert await mysql_async_litestar_store.exists("active_session")
    assert await mysql_async_litestar_store.exists("permanent_session")
    assert not await mysql_async_litestar_store.exists("expired_session_1")
    assert not await mysql_async_litestar_store.exists("expired_session_2")


async def test_store_upsert(mysql_async_litestar_store: Any) -> None:
    """Test updating existing session."""
    await mysql_async_litestar_store.set("session_upsert", b"original data")

    result = await mysql_async_litestar_store.get("session_upsert")
    assert result == b"original data"

    await mysql_async_litestar_store.set("session_upsert", b"updated data")

    result = await mysql_async_litestar_store.get("session_upsert")
    assert result == b"updated data"


async def test_store_upsert_with_expiration_change(mysql_async_litestar_store: Any) -> None:
    """Test updating session expiration."""
    await mysql_async_litestar_store.set("session_exp", b"data", expires_in=60)

    expires_in = await mysql_async_litestar_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50

    await mysql_async_litestar_store.set("session_exp", b"data", expires_in=10)

    expires_in = await mysql_async_litestar_store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def test_store_renew_for(mysql_async_litestar_store: Any) -> None:
    """Test renewing session expiration on get."""
    await mysql_async_litestar_store.set("session_renew", b"data", expires_in=5)

    await asyncio.sleep(3)

    expires_before = await mysql_async_litestar_store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2

    result = await mysql_async_litestar_store.get("session_renew", renew_for=10)
    assert result == b"data"

    expires_after = await mysql_async_litestar_store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def test_store_large_data(mysql_async_litestar_store: Any) -> None:
    """Test storing large session data."""
    large_data = b"x" * (1024 * 1024 + 100)

    await mysql_async_litestar_store.set("large_session", large_data)

    result = await mysql_async_litestar_store.get("large_session")
    assert result is not None
    assert result == large_data
    assert len(result) > 1024 * 1024


async def test_store_delete_all(mysql_async_litestar_store: Any) -> None:
    """Test delete_all removes all sessions."""
    await mysql_async_litestar_store.set("session1", b"data1")
    await mysql_async_litestar_store.set("session2", b"data2")
    await mysql_async_litestar_store.set("session3", b"data3")

    assert await mysql_async_litestar_store.exists("session1")
    assert await mysql_async_litestar_store.exists("session2")
    assert await mysql_async_litestar_store.exists("session3")

    await mysql_async_litestar_store.delete_all()

    assert not await mysql_async_litestar_store.exists("session1")
    assert not await mysql_async_litestar_store.exists("session2")
    assert not await mysql_async_litestar_store.exists("session3")


async def test_store_exists(mysql_async_litestar_store: Any) -> None:
    """Test exists method."""
    assert not await mysql_async_litestar_store.exists("test_session")

    await mysql_async_litestar_store.set("test_session", b"data")

    assert await mysql_async_litestar_store.exists("test_session")


async def test_store_context_manager(mysql_async_litestar_store: Any) -> None:
    """Test store can be used as async context manager."""
    async with mysql_async_litestar_store:
        await mysql_async_litestar_store.set("ctx_session", b"data")

    result = await mysql_async_litestar_store.get("ctx_session")
    assert result == b"data"
