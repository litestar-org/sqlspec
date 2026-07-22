"""Shared Litestar session-store contracts."""

from tests.integration.adapters._shared._store_cases import StoreCaseContext
from tests.integration.adapters._shared.store_behaviors import (
    assert_store_cleanup_contract,
    assert_store_context_manager_contract,
    assert_store_create_table_contract,
    assert_store_delete_all_contract,
    assert_store_delete_contract,
    assert_store_delete_nonexistent_contract,
    assert_store_exists_contract,
    assert_store_expiration_int_contract,
    assert_store_expiration_timedelta_contract,
    assert_store_expires_in_contract,
    assert_store_expires_in_expired_contract,
    assert_store_get_nonexistent_contract,
    assert_store_large_data_contract,
    assert_store_no_expiration_contract,
    assert_store_renew_for_contract,
    assert_store_set_and_get_contract,
    assert_store_set_string_value_contract,
    assert_store_upsert_contract,
    assert_store_upsert_expiration_change_contract,
)


async def test_store_create_table_contract(store_case: StoreCaseContext) -> None:
    """Stores expose the configured session table name."""
    await assert_store_create_table_contract(store_case.store)


async def test_store_set_and_get_contract(store_case: StoreCaseContext) -> None:
    """Stores round-trip byte payloads through set and get."""
    await assert_store_set_and_get_contract(store_case.store)


async def test_store_get_nonexistent_contract(store_case: StoreCaseContext) -> None:
    """Stores return None for a missing key."""
    await assert_store_get_nonexistent_contract(store_case.store)


async def test_store_set_string_value_contract(store_case: StoreCaseContext) -> None:
    """Stores coerce string values to bytes."""
    await assert_store_set_string_value_contract(store_case.store)


async def test_store_delete_contract(store_case: StoreCaseContext) -> None:
    """Stores remove a stored key on delete."""
    await assert_store_delete_contract(store_case.store)


async def test_store_delete_nonexistent_contract(store_case: StoreCaseContext) -> None:
    """Deleting a missing key is a no-op."""
    await assert_store_delete_nonexistent_contract(store_case.store)


async def test_store_expiration_int_contract(store_case: StoreCaseContext) -> None:
    """Stores expire entries after an integer-second TTL."""
    await assert_store_expiration_int_contract(store_case.store)


async def test_store_expiration_timedelta_contract(store_case: StoreCaseContext) -> None:
    """Stores expire entries after a timedelta TTL."""
    await assert_store_expiration_timedelta_contract(store_case.store)


async def test_store_no_expiration_contract(store_case: StoreCaseContext) -> None:
    """Entries without a TTL persist and report no expiry."""
    await assert_store_no_expiration_contract(store_case.store)


async def test_store_expires_in_contract(store_case: StoreCaseContext) -> None:
    """expires_in reports the remaining TTL window."""
    await assert_store_expires_in_contract(store_case.store)


async def test_store_expires_in_expired_contract(store_case: StoreCaseContext) -> None:
    """expires_in reports zero for an expired entry."""
    await assert_store_expires_in_expired_contract(store_case.store)


async def test_store_cleanup_contract(store_case: StoreCaseContext) -> None:
    """delete_expired removes only expired entries."""
    await assert_store_cleanup_contract(store_case.store)


async def test_store_upsert_contract(store_case: StoreCaseContext) -> None:
    """Re-setting a key replaces the stored value."""
    await assert_store_upsert_contract(store_case.store)


async def test_store_upsert_expiration_change_contract(store_case: StoreCaseContext) -> None:
    """Re-setting a key updates its expiration window."""
    await assert_store_upsert_expiration_change_contract(store_case.store)


async def test_store_renew_for_contract(store_case: StoreCaseContext) -> None:
    """get with renew_for extends the expiration window."""
    await assert_store_renew_for_contract(store_case.store)


async def test_store_large_data_contract(store_case: StoreCaseContext) -> None:
    """Stores round-trip payloads larger than 1MB."""
    await assert_store_large_data_contract(store_case.store)


async def test_store_delete_all_contract(store_case: StoreCaseContext) -> None:
    """delete_all removes every stored entry."""
    await assert_store_delete_all_contract(store_case.store)


async def test_store_exists_contract(store_case: StoreCaseContext) -> None:
    """exists reflects presence of a key."""
    await assert_store_exists_contract(store_case.store)


async def test_store_context_manager_contract(store_case: StoreCaseContext) -> None:
    """Stores support async context-manager usage."""
    await assert_store_context_manager_contract(store_case.store)
