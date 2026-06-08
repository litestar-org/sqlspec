"""Behavior helpers for shared Litestar session-store contract tests."""

import asyncio
from datetime import timedelta
from typing import Any


async def assert_store_create_table_contract(store: Any) -> None:
    """Stores expose the configured session table name."""
    assert store.table_name == "litestar_contract_sessions"


async def assert_store_set_and_get_contract(store: Any) -> None:
    """Stores round-trip byte payloads through set and get."""
    payload = b"test session data"
    await store.set("session_123", payload)
    assert await store.get("session_123") == payload


async def assert_store_get_nonexistent_contract(store: Any) -> None:
    """Stores return None for a missing key."""
    assert await store.get("nonexistent") is None


async def assert_store_set_string_value_contract(store: Any) -> None:
    """Stores coerce string values to bytes."""
    await store.set("session_str", "string data")
    assert await store.get("session_str") == b"string data"


async def assert_store_delete_contract(store: Any) -> None:
    """Stores remove a stored key on delete."""
    await store.set("session_to_delete", b"data")
    assert await store.exists("session_to_delete")
    await store.delete("session_to_delete")
    assert not await store.exists("session_to_delete")
    assert await store.get("session_to_delete") is None


async def assert_store_delete_nonexistent_contract(store: Any) -> None:
    """Deleting a missing key is a no-op."""
    await store.delete("nonexistent")


async def assert_store_expiration_int_contract(store: Any) -> None:
    """Stores expire entries after an integer-second TTL."""
    await store.set("expiring_session", b"data", expires_in=1)
    assert await store.exists("expiring_session")
    await asyncio.sleep(1.1)
    assert await store.get("expiring_session") is None
    assert not await store.exists("expiring_session")


async def assert_store_expiration_timedelta_contract(store: Any) -> None:
    """Stores expire entries after a timedelta TTL."""
    await store.set("expiring_session", b"data", expires_in=timedelta(seconds=1))
    assert await store.exists("expiring_session")
    await asyncio.sleep(1.1)
    assert await store.get("expiring_session") is None


async def assert_store_no_expiration_contract(store: Any) -> None:
    """Entries without a TTL persist and report no expiry."""
    await store.set("permanent_session", b"data")
    assert await store.expires_in("permanent_session") is None
    assert await store.exists("permanent_session")


async def assert_store_expires_in_contract(store: Any) -> None:
    """expires_in reports the remaining TTL window."""
    await store.set("timed_session", b"data", expires_in=10)
    expires_in = await store.expires_in("timed_session")
    assert expires_in is not None
    assert 8 <= expires_in <= 10


async def assert_store_expires_in_expired_contract(store: Any) -> None:
    """expires_in reports zero for an expired entry."""
    await store.set("expired_session", b"data", expires_in=1)
    await asyncio.sleep(1.1)
    assert await store.expires_in("expired_session") == 0


async def assert_store_cleanup_contract(store: Any) -> None:
    """delete_expired removes only expired entries."""
    await store.set("active_session", b"data", expires_in=60)
    await store.set("expired_session_1", b"data", expires_in=1)
    await store.set("expired_session_2", b"data", expires_in=1)
    await store.set("permanent_session", b"data")
    await asyncio.sleep(1.1)
    assert await store.delete_expired() == 2
    assert await store.exists("active_session")
    assert await store.exists("permanent_session")
    assert not await store.exists("expired_session_1")
    assert not await store.exists("expired_session_2")


async def assert_store_upsert_contract(store: Any) -> None:
    """Re-setting a key replaces the stored value."""
    await store.set("session_upsert", b"original data")
    assert await store.get("session_upsert") == b"original data"
    await store.set("session_upsert", b"updated data")
    assert await store.get("session_upsert") == b"updated data"


async def assert_store_upsert_expiration_change_contract(store: Any) -> None:
    """Re-setting a key updates its expiration window."""
    await store.set("session_exp", b"data", expires_in=60)
    expires_in = await store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in > 50
    await store.set("session_exp", b"data", expires_in=10)
    expires_in = await store.expires_in("session_exp")
    assert expires_in is not None
    assert expires_in <= 10


async def assert_store_renew_for_contract(store: Any) -> None:
    """get with renew_for extends the expiration window."""
    await store.set("session_renew", b"data", expires_in=5)
    await asyncio.sleep(3)
    expires_before = await store.expires_in("session_renew")
    assert expires_before is not None
    assert expires_before <= 2
    assert await store.get("session_renew", renew_for=10) == b"data"
    expires_after = await store.expires_in("session_renew")
    assert expires_after is not None
    assert expires_after > 8


async def assert_store_large_data_contract(store: Any) -> None:
    """Stores round-trip payloads larger than 1MB."""
    large_data = b"x" * (1024 * 1024 + 100)
    await store.set("large_session", large_data)
    result = await store.get("large_session")
    assert result == large_data
    assert result is not None
    assert len(result) > 1024 * 1024


async def assert_store_delete_all_contract(store: Any) -> None:
    """delete_all removes every stored entry."""
    await store.set("session1", b"data1")
    await store.set("session2", b"data2")
    await store.set("session3", b"data3")
    assert await store.exists("session1")
    await store.delete_all()
    assert not await store.exists("session1")
    assert not await store.exists("session2")
    assert not await store.exists("session3")


async def assert_store_exists_contract(store: Any) -> None:
    """exists reflects presence of a key."""
    assert not await store.exists("test_session")
    await store.set("test_session", b"data")
    assert await store.exists("test_session")


async def assert_store_context_manager_contract(store: Any) -> None:
    """Stores support async context-manager usage."""
    async with store:
        await store.set("ctx_session", b"data")
    assert await store.get("ctx_session") == b"data"
