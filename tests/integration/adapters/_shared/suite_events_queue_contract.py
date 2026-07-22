"""Shared event-channel queue-backend contracts."""

from tests.integration.adapters._shared._events_cases import EventsCaseContext
from tests.integration.adapters._shared.events_behaviors import (
    assert_async_events_queue_attempts_contract,
    assert_async_events_queue_channel_filter_contract,
    assert_async_events_queue_lifecycle_contract,
    assert_async_events_queue_metadata_contract,
    assert_async_events_queue_multiple_messages_contract,
    assert_async_events_queue_nack_contract,
    assert_async_events_queue_telemetry_contract,
    assert_sync_events_queue_attempts_contract,
    assert_sync_events_queue_channel_filter_contract,
    assert_sync_events_queue_lifecycle_contract,
    assert_sync_events_queue_metadata_contract,
    assert_sync_events_queue_multiple_messages_contract,
    assert_sync_events_queue_nack_contract,
    assert_sync_events_queue_telemetry_contract,
)


def test_sync_events_queue_lifecycle_contract(sync_events_case: EventsCaseContext) -> None:
    """Sync queue backends publish, consume, ack, and persist status."""
    assert_sync_events_queue_lifecycle_contract(sync_events_case.make_config, sync_events_case.case)


async def test_async_events_queue_lifecycle_contract(async_events_case: EventsCaseContext) -> None:
    """Async queue backends publish, consume, ack, and persist status."""
    await assert_async_events_queue_lifecycle_contract(async_events_case.make_config, async_events_case.case)


def test_sync_events_queue_channel_filter_contract(sync_events_case: EventsCaseContext) -> None:
    """Sync queue backends deliver only events for the requested channel."""
    assert_sync_events_queue_channel_filter_contract(sync_events_case.make_config, sync_events_case.case)


async def test_async_events_queue_channel_filter_contract(async_events_case: EventsCaseContext) -> None:
    """Async queue backends deliver only events for the requested channel."""
    await assert_async_events_queue_channel_filter_contract(async_events_case.make_config, async_events_case.case)


def test_sync_events_queue_metadata_contract(sync_events_case: EventsCaseContext) -> None:
    """Sync queue backends preserve metadata through publish and consume."""
    assert_sync_events_queue_metadata_contract(sync_events_case.make_config, sync_events_case.case)


async def test_async_events_queue_metadata_contract(async_events_case: EventsCaseContext) -> None:
    """Async queue backends preserve metadata through publish and consume."""
    await assert_async_events_queue_metadata_contract(async_events_case.make_config, async_events_case.case)


def test_sync_events_queue_attempts_contract(sync_events_case: EventsCaseContext) -> None:
    """Sync queue backends increment the attempts counter on dequeue."""
    assert_sync_events_queue_attempts_contract(sync_events_case.make_config, sync_events_case.case)


async def test_async_events_queue_attempts_contract(async_events_case: EventsCaseContext) -> None:
    """Async queue backends increment the attempts counter on dequeue."""
    await assert_async_events_queue_attempts_contract(async_events_case.make_config, async_events_case.case)


def test_sync_events_queue_telemetry_contract(sync_events_case: EventsCaseContext) -> None:
    """Sync queue backends record publish and ack telemetry counters."""
    assert_sync_events_queue_telemetry_contract(sync_events_case.make_config, sync_events_case.case)


async def test_async_events_queue_telemetry_contract(async_events_case: EventsCaseContext) -> None:
    """Async queue backends record publish and ack telemetry counters."""
    await assert_async_events_queue_telemetry_contract(async_events_case.make_config, async_events_case.case)


def test_sync_events_queue_multiple_messages_contract(sync_events_case: EventsCaseContext) -> None:
    """Sync queue backends deliver every queued message on a channel exactly once."""
    assert_sync_events_queue_multiple_messages_contract(sync_events_case.make_config, sync_events_case.case)


async def test_async_events_queue_multiple_messages_contract(async_events_case: EventsCaseContext) -> None:
    """Async queue backends deliver every queued message on a channel exactly once."""
    await assert_async_events_queue_multiple_messages_contract(async_events_case.make_config, async_events_case.case)


def test_sync_events_queue_nack_contract(sync_events_case: EventsCaseContext) -> None:
    """Sync queue backends return nacked events to a redeliverable pending state."""
    assert_sync_events_queue_nack_contract(sync_events_case.make_config, sync_events_case.case)


async def test_async_events_queue_nack_contract(async_events_case: EventsCaseContext) -> None:
    """Async queue backends return nacked events to a redeliverable pending state."""
    await assert_async_events_queue_nack_contract(async_events_case.make_config, async_events_case.case)
