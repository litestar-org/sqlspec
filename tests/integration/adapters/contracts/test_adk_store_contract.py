"""Shared ADK session/event store contracts."""

import pytest

from tests.integration.adapters.contracts._adk_cases import AdkStoreCaseContext
from tests.integration.adapters.contracts.adk_behaviors import (
    assert_adk_append_and_get_events_contract,
    assert_adk_append_event_and_update_state_contract,
    assert_adk_create_tables_idempotent_contract,
    assert_adk_delete_session_cascade_contract,
    assert_adk_get_events_filtering_contract,
    assert_adk_get_nonexistent_session_contract,
    assert_adk_list_sessions_contract,
    assert_adk_reads_empty_when_tables_missing_contract,
    assert_adk_session_round_trip_contract,
    assert_adk_update_session_state_contract,
)


async def test_adk_create_tables_idempotent_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """Creating ADK tables twice is a safe no-op."""
    await assert_adk_create_tables_idempotent_contract(adk_store_case.make_store)


async def test_adk_session_round_trip_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """Sessions persist empty and populated state through a round-trip."""
    await assert_adk_session_round_trip_contract(adk_store_case.make_store)


async def test_adk_get_nonexistent_session_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """Reading a missing session returns None."""
    await assert_adk_get_nonexistent_session_contract(adk_store_case.make_store)


async def test_adk_update_session_state_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """update_session_state replaces the durable session state."""
    await assert_adk_update_session_state_contract(adk_store_case.make_store)


async def test_adk_list_sessions_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """list_sessions filters by app and optional user."""
    await assert_adk_list_sessions_contract(adk_store_case.make_store)


async def test_adk_delete_session_cascade_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """Deleting a session removes the session and its events."""
    await assert_adk_delete_session_cascade_contract(adk_store_case.make_store)


async def test_adk_append_and_get_events_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """Appended events round-trip through get_events."""
    await assert_adk_append_and_get_events_contract(adk_store_case.make_store)


async def test_adk_append_event_and_update_state_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """Atomic append updates state and stores the event together."""
    if not adk_store_case.case.supports_atomic_state_update:
        pytest.skip("adapter cannot update a session row referenced by an event foreign key")
    await assert_adk_append_event_and_update_state_contract(adk_store_case.make_store)


async def test_adk_get_events_filtering_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """get_events honors after_timestamp and limit."""
    await assert_adk_get_events_filtering_contract(adk_store_case.make_store)


async def test_adk_reads_empty_when_tables_missing_contract(adk_store_case: AdkStoreCaseContext) -> None:
    """Read paths return None/empty when the ADK tables do not exist."""
    await assert_adk_reads_empty_when_tables_missing_contract(adk_store_case.make_store)
