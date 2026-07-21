"""Shared native PostgreSQL LISTEN/NOTIFY contracts."""

from tests.integration.adapters._shared._events_cases import ListenNotifyCaseContext
from tests.integration.adapters._shared.events_behaviors import (
    assert_async_listen_notify_delivery_contract,
    assert_sync_listen_notify_delivery_contract,
)


def test_sync_listen_notify_delivery_contract(sync_listen_notify_case: ListenNotifyCaseContext) -> None:
    """Sync native LISTEN/NOTIFY backends deliver payloads and metadata."""
    assert_sync_listen_notify_delivery_contract(sync_listen_notify_case.make_config, sync_listen_notify_case.case)


async def test_async_listen_notify_delivery_contract(async_listen_notify_case: ListenNotifyCaseContext) -> None:
    """Async native LISTEN/NOTIFY backends deliver payloads and metadata."""
    await assert_async_listen_notify_delivery_contract(
        async_listen_notify_case.make_config, async_listen_notify_case.case
    )
