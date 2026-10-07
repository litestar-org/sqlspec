"""Spanner Litestar session store integration tests for the sync and async adapters."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from sqlspec.adapters.spanner.litestar import SpannerAsyncStore, SpannerSyncStore
from tests.integration.adapters._shared.store_behaviors import (
    assert_store_cleanup_contract,
    assert_store_delete_all_contract,
    assert_store_delete_contract,
    assert_store_delete_nonexistent_contract,
    assert_store_exists_contract,
    assert_store_expiration_int_contract,
    assert_store_expiration_timedelta_contract,
    assert_store_expires_in_contract,
    assert_store_get_nonexistent_contract,
    assert_store_large_data_contract,
    assert_store_no_expiration_contract,
    assert_store_renew_for_contract,
    assert_store_set_and_get_contract,
    assert_store_set_string_value_contract,
    assert_store_upsert_contract,
    assert_store_upsert_expiration_change_contract,
)

pytestmark = [pytest.mark.spanner, pytest.mark.integration, pytest.mark.anyio]

STORE_CONTRACTS = (
    assert_store_set_and_get_contract,
    assert_store_get_nonexistent_contract,
    assert_store_set_string_value_contract,
    assert_store_delete_contract,
    assert_store_delete_nonexistent_contract,
    assert_store_expiration_int_contract,
    assert_store_expiration_timedelta_contract,
    assert_store_no_expiration_contract,
    assert_store_expires_in_contract,
    assert_store_cleanup_contract,
    assert_store_upsert_contract,
    assert_store_upsert_expiration_change_contract,
    assert_store_renew_for_contract,
    assert_store_large_data_contract,
    assert_store_delete_all_contract,
    assert_store_exists_contract,
)


@pytest.mark.parametrize(
    "contract", STORE_CONTRACTS, ids=lambda contract: contract.__name__.removeprefix("assert_store_")
)
async def test_spanner_store_contract(
    spanner_store: "SpannerSyncStore | SpannerAsyncStore", contract: "Callable[[Any], Awaitable[None]]"
) -> None:
    """Spanner session stores satisfy the shared Litestar store contracts."""
    await contract(spanner_store)


async def test_delete_expired_returns_count(spanner_store: "SpannerSyncStore | SpannerAsyncStore") -> None:
    """delete_expired() reports how many expired sessions it removed."""
    await spanner_store.set("exp1", b"x", expires_in=1)
    await spanner_store.set("exp2", b"x", expires_in=1)
    await spanner_store.set("kept", b"x", expires_in=60)
    await asyncio.sleep(1.5)
    assert await spanner_store.delete_expired() == 2
    assert await spanner_store.get("kept") == b"x"
