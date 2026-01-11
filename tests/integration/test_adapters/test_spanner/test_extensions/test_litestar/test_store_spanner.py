"""Integration tests for Spanner session store."""

import asyncio

import pytest

from sqlspec.adapters.spanner.litestar import SpannerSyncStore

pytestmark = [pytest.mark.spanner, pytest.mark.integration]


async def test_store_set_and_get(spanner_store: SpannerSyncStore) -> None:
    data = b"payload"
    await spanner_store.set("s1", data)
    assert await spanner_store.get("s1") == data


async def test_store_expiration(spanner_store: SpannerSyncStore) -> None:
    await spanner_store.set("expiring", b"data", expires_in=1)
    assert await spanner_store.exists("expiring")
    await asyncio.sleep(1.1)
    assert await spanner_store.get("expiring") is None


async def test_store_delete(spanner_store: SpannerSyncStore) -> None:
    await spanner_store.set("todelete", b"d")
    await spanner_store.delete("todelete")
    assert await spanner_store.get("todelete") is None


async def test_store_renew(spanner_store: SpannerSyncStore) -> None:
    await spanner_store.set("renew", b"r", expires_in=1)
    await asyncio.sleep(0.5)
    await spanner_store.get("renew", renew_for=1)
    await asyncio.sleep(0.7)
    assert await spanner_store.get("renew") == b"r"


async def test_delete_expired_returns_count(spanner_store: SpannerSyncStore) -> None:
    await spanner_store.set("exp1", b"x", expires_in=1)
    await spanner_store.set("exp2", b"x", expires_in=1)
    await asyncio.sleep(1.1)
    deleted = await spanner_store.delete_expired()
    assert deleted >= 2
