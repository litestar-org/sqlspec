"""Spanner ADK store integration tests for the sync and async adapters.

Spanner is not part of the shared ADK contract matrix because its tables need
admin-API DDL. These tests run the shared contract behaviors against both
Spanner stores through adapter-local factories instead.
"""

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from inspect import isawaitable
from typing import Any, TypeVar

import pytest

from sqlspec.extensions.adk.memory import StoredMemory
from tests.integration.adapters._shared.adk_behaviors import (
    assert_adk_append_and_get_events_contract,
    assert_adk_append_event_and_update_state_contract,
    assert_adk_create_tables_idempotent_contract,
    assert_adk_delete_session_cascade_contract,
    assert_adk_get_events_filtering_contract,
    assert_adk_get_nonexistent_session_contract,
    assert_adk_list_sessions_contract,
    assert_adk_session_round_trip_contract,
    assert_adk_update_session_state_contract,
)

pytestmark = [pytest.mark.spanner, pytest.mark.integration, pytest.mark.anyio]

T = TypeVar("T")

ADK_CONTRACTS = (
    assert_adk_create_tables_idempotent_contract,
    assert_adk_session_round_trip_contract,
    assert_adk_get_nonexistent_session_contract,
    assert_adk_update_session_state_contract,
    assert_adk_list_sessions_contract,
    assert_adk_delete_session_cascade_contract,
    assert_adk_append_and_get_events_contract,
    assert_adk_append_event_and_update_state_contract,
    assert_adk_get_events_filtering_contract,
)


async def _resolve(result: "T | Awaitable[T]") -> T:
    if isawaitable(result):
        return await result
    return result


@pytest.mark.parametrize("contract", ADK_CONTRACTS, ids=lambda contract: contract.__name__.removeprefix("assert_adk_"))
async def test_spanner_adk_store_contract(
    spanner_adk_store_factory: "Callable[[], tuple[Any, Any]]", contract: "Callable[[Any], Awaitable[None]]"
) -> None:
    """Spanner ADK session stores satisfy the shared session and event contracts."""
    await contract(spanner_adk_store_factory)


async def test_spanner_adk_memory_store_round_trip(
    spanner_adk_memory_store_factory: "Callable[[], tuple[Any, Any]]",
) -> None:
    """Spanner ADK memory stores insert, search, and delete entries by session."""
    config, store = spanner_adk_memory_store_factory()
    now = datetime.now(timezone.utc)
    entry: StoredMemory = {
        "id": "memory-1",
        "session_id": "session-memory",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-1",
        "author": "user",
        "timestamp": now,
        "content_json": {"parts": [{"text": "spanner remembers coffee orders"}]},
        "content_text": "spanner remembers coffee orders",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }
    try:
        await _resolve(store.create_tables())
        assert await _resolve(store.insert_memory_entries([entry])) == 1

        matches = await _resolve(store.search_entries("coffee", "app", "user"))
        assert [match["id"] for match in matches] == ["memory-1"]

        assert await _resolve(store.delete_entries_by_session("session-memory")) == 1
        assert await _resolve(store.search_entries("coffee", "app", "user")) == []
    finally:
        await _resolve(config.close_pool())
