"""Behavior helpers for shared event-channel queue-backend contract tests."""

import asyncio
import contextlib
import time
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from tests.integration.adapters.contracts._events_cases import EventsCase, ListenNotifyCase

_LISTEN_NOTIFY_CHANNEL_PREFIX = "contract_listen_notify"
_LISTEN_NOTIFY_POLL_INTERVAL = 0.05
_LISTEN_NOTIFY_SUBSCRIBE_WAIT = 0.5
_LISTEN_NOTIFY_ATTEMPTS = 200


async def setup_async_event_channel(config: Any) -> 'tuple["SQLSpec", Any]':
    """Run async migrations and return SQLSpec + event channel."""
    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    return spec, spec.event_channel(config)


def setup_sync_event_channel(config: Any) -> 'tuple["SQLSpec", Any]':
    """Run sync migrations and return SQLSpec + event channel."""
    commands = SyncMigrationCommands(config)
    commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    return spec, spec.event_channel(config)


def _events_extension_config(case: EventsCase, behavior: str) -> "tuple[str, dict[str, Any], str]":
    token = f"{case.id}_{behavior}".replace("-", "_")
    queue_table = f"evq_{token}"
    events_config: dict[str, Any] = {"queue_table": queue_table}
    if case.force_table_queue:
        events_config["backend"] = "poll_queue"
    return queue_table, {"events": events_config}, token


def _listen_notify_channel(case: ListenNotifyCase, behavior: str) -> str:
    return f"{_LISTEN_NOTIFY_CHANNEL_PREFIX}_{case.id}_{behavior}".replace("-", "_")


def _wait_for_sync_message(received: "list[Any]") -> None:
    for _ in range(_LISTEN_NOTIFY_ATTEMPTS):
        if received:
            return
        time.sleep(_LISTEN_NOTIFY_POLL_INTERVAL)


async def _wait_for_async_message(received: "list[Any]") -> None:
    for _ in range(_LISTEN_NOTIFY_ATTEMPTS):
        if received:
            return
        await asyncio.sleep(_LISTEN_NOTIFY_POLL_INTERVAL)


def _consume_sync(channel: Any, channel_name: str) -> Any:
    iterator = channel.iter_events(channel_name, poll_interval=0.05)
    try:
        return next(iterator)
    finally:
        iterator.close()


async def _consume_async(channel: Any, channel_name: str) -> Any:
    iterator = channel.iter_events(channel_name, poll_interval=0.05)
    try:
        return await asyncio.wait_for(iterator.__anext__(), timeout=10)
    finally:
        await iterator.aclose()


def _status_row_sync(config: Any, queue_table: str, event_id: Any, columns: str = "status") -> Any:
    with config.provide_session() as driver:
        return driver.select_one(
            f"SELECT {columns} FROM {queue_table} WHERE event_id = :event_id", {"event_id": event_id}
        )


async def _status_row_async(config: Any, queue_table: str, event_id: Any, columns: str = "status") -> Any:
    async with config.provide_session() as driver:
        return await driver.select_one(
            f"SELECT {columns} FROM {queue_table} WHERE event_id = :event_id", {"event_id": event_id}
        )


def assert_sync_listen_notify_delivery_contract(make_config: Any, case: ListenNotifyCase) -> None:
    """Sync native LISTEN/NOTIFY backends select the native backend and deliver payload metadata."""
    channel_name = _listen_notify_channel(case, "delivery")
    config = make_config(suffix=f"{case.id}_listen_notify".replace("-", "_"))
    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)
    listener: Any | None = None
    try:
        assert channel._backend_name == "notify"  # pyright: ignore[reportPrivateUsage]
        received: list[Any] = []
        listener = channel.listen(channel_name, received.append, poll_interval=_LISTEN_NOTIFY_POLL_INTERVAL)
        time.sleep(_LISTEN_NOTIFY_SUBSCRIBE_WAIT)
        event_id = channel.publish(
            channel_name, {"case": case.id, "mode": case.mode}, metadata={"source": "contract", "adapter": case.adapter}
        )
        _wait_for_sync_message(received)
        if not received:
            event_id = channel.publish(
                channel_name,
                {"case": case.id, "mode": case.mode},
                metadata={"source": "contract", "adapter": case.adapter},
            )
            _wait_for_sync_message(received)

        assert received, "listener did not receive native LISTEN/NOTIFY message"
        message = received[0]
        assert message.event_id == event_id
        assert message.channel == channel_name
        assert message.payload == {"case": case.id, "mode": case.mode}
        assert message.metadata == {"source": "contract", "adapter": case.adapter}
        channel.ack(message.event_id)
    finally:
        if listener is not None:
            with contextlib.suppress(Exception):
                channel.stop_listener(listener.id)
        with contextlib.suppress(Exception):
            channel.shutdown()
        config.close_pool()


async def assert_async_listen_notify_delivery_contract(make_config: Any, case: ListenNotifyCase) -> None:
    """Async native LISTEN/NOTIFY backends select the native backend and deliver payload metadata."""
    channel_name = _listen_notify_channel(case, "delivery")
    config = make_config(suffix=f"{case.id}_listen_notify".replace("-", "_"))
    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)
    listener: Any | None = None
    try:
        assert channel._backend_name == "notify"  # pyright: ignore[reportPrivateUsage]
        received: list[Any] = []

        async def _handler(message: Any) -> None:
            received.append(message)

        listener = channel.listen(channel_name, _handler, poll_interval=_LISTEN_NOTIFY_POLL_INTERVAL)
        await asyncio.sleep(_LISTEN_NOTIFY_SUBSCRIBE_WAIT)
        event_id = await channel.publish(
            channel_name, {"case": case.id, "mode": case.mode}, metadata={"source": "contract", "adapter": case.adapter}
        )
        await _wait_for_async_message(received)
        if not received:
            event_id = await channel.publish(
                channel_name,
                {"case": case.id, "mode": case.mode},
                metadata={"source": "contract", "adapter": case.adapter},
            )
            await _wait_for_async_message(received)

        assert received, "listener did not receive native LISTEN/NOTIFY message"
        message = received[0]
        assert message.event_id == event_id
        assert message.channel == channel_name
        assert message.payload == {"case": case.id, "mode": case.mode}
        assert message.metadata == {"source": "contract", "adapter": case.adapter}
        await channel.ack(message.event_id)
    finally:
        if listener is not None:
            with contextlib.suppress(Exception):
                await channel.stop_listener(listener.id)
        with contextlib.suppress(Exception):
            await channel.shutdown()
        await config.close_pool()


def assert_sync_events_queue_lifecycle_contract(make_config: Any, case: EventsCase) -> None:
    """Sync queue backends publish, consume, ack, and persist status to the custom table."""
    queue_table, extension_config, suffix = _events_extension_config(case, "lifecycle")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = setup_sync_event_channel(config)
        assert channel._backend_name == "poll_queue"  # pyright: ignore[reportPrivateUsage]
        event_id = channel.publish("notifications", {"action": "queue"})
        message = _consume_sync(channel, "notifications")
        assert message.event_id == event_id
        assert message.payload["action"] == "queue"
        assert message.channel == "notifications"
        channel.ack(message.event_id)
        assert _status_row_sync(config, queue_table, event_id)["status"] == "acked"
    finally:
        config.close_pool()


async def assert_async_events_queue_lifecycle_contract(make_config: Any, case: EventsCase) -> None:
    """Async queue backends publish, consume, ack, and persist status to the custom table."""
    queue_table, extension_config, suffix = _events_extension_config(case, "lifecycle")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = await setup_async_event_channel(config)
        assert channel._backend_name == "poll_queue"  # pyright: ignore[reportPrivateUsage]
        event_id = await channel.publish("notifications", {"action": "queue"})
        message = await _consume_async(channel, "notifications")
        assert message.event_id == event_id
        assert message.payload["action"] == "queue"
        assert message.channel == "notifications"
        await channel.ack(message.event_id)
        assert (await _status_row_async(config, queue_table, event_id))["status"] == "acked"
    finally:
        await config.close_pool()


def assert_sync_events_queue_channel_filter_contract(make_config: Any, case: EventsCase) -> None:
    """Sync queue backends deliver only events for the requested channel."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "channels")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = setup_sync_event_channel(config)
        alert_id = channel.publish("alerts", {"type": "alert"})
        channel.publish("notifications", {"type": "notification"})
        message = _consume_sync(channel, "alerts")
        assert message.event_id == alert_id
        assert message.channel == "alerts"
        assert message.payload["type"] == "alert"
    finally:
        config.close_pool()


async def assert_async_events_queue_channel_filter_contract(make_config: Any, case: EventsCase) -> None:
    """Async queue backends deliver only events for the requested channel."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "channels")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = await setup_async_event_channel(config)
        alert_id = await channel.publish("alerts", {"type": "alert"})
        await channel.publish("notifications", {"type": "notification"})
        message = await _consume_async(channel, "alerts")
        assert message.event_id == alert_id
        assert message.channel == "alerts"
        assert message.payload["type"] == "alert"
    finally:
        await config.close_pool()


def assert_sync_events_queue_metadata_contract(make_config: Any, case: EventsCase) -> None:
    """Sync queue backends preserve metadata through publish and consume."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "metadata")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("events", {"action": "create"}, metadata={"user_id": "user_123", "source": "api"})
        message = _consume_sync(channel, "events")
        assert message.event_id == event_id
        assert message.metadata is not None
        assert message.metadata["user_id"] == "user_123"
        assert message.metadata["source"] == "api"
    finally:
        config.close_pool()


async def assert_async_events_queue_metadata_contract(make_config: Any, case: EventsCase) -> None:
    """Async queue backends preserve metadata through publish and consume."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "metadata")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish(
            "events", {"action": "create"}, metadata={"user_id": "user_123", "source": "api"}
        )
        message = await _consume_async(channel, "events")
        assert message.event_id == event_id
        assert message.metadata is not None
        assert message.metadata["user_id"] == "user_123"
        assert message.metadata["source"] == "api"
    finally:
        await config.close_pool()


def assert_sync_events_queue_attempts_contract(make_config: Any, case: EventsCase) -> None:
    """Sync queue backends increment the attempts counter on dequeue."""
    queue_table, extension_config, suffix = _events_extension_config(case, "attempts")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("events", {"action": "test"})
        _consume_sync(channel, "events")
        assert _status_row_sync(config, queue_table, event_id, "attempts")["attempts"] >= 1
    finally:
        config.close_pool()


async def assert_async_events_queue_attempts_contract(make_config: Any, case: EventsCase) -> None:
    """Async queue backends increment the attempts counter on dequeue."""
    queue_table, extension_config, suffix = _events_extension_config(case, "attempts")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish("events", {"action": "test"})
        await _consume_async(channel, "events")
        assert (await _status_row_async(config, queue_table, event_id, "attempts"))["attempts"] >= 1
    finally:
        await config.close_pool()


def assert_sync_events_queue_telemetry_contract(make_config: Any, case: EventsCase) -> None:
    """Sync queue backends record publish and ack telemetry counters."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "telemetry")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        spec, channel = setup_sync_event_channel(config)
        channel.publish("events", {"action": "telemetry"})
        message = _consume_sync(channel, "events")
        channel.ack(message.event_id)
        snapshot = spec.telemetry_snapshot()
        config_name = type(config).__name__
        assert snapshot.get(f"{config_name}.events.publish") == pytest.approx(1.0)
        assert snapshot.get(f"{config_name}.events.ack") == pytest.approx(1.0)
    finally:
        config.close_pool()


async def assert_async_events_queue_telemetry_contract(make_config: Any, case: EventsCase) -> None:
    """Async queue backends record publish and ack telemetry counters."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "telemetry")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        spec, channel = await setup_async_event_channel(config)
        await channel.publish("events", {"action": "telemetry"})
        message = await _consume_async(channel, "events")
        await channel.ack(message.event_id)
        snapshot = spec.telemetry_snapshot()
        config_name = type(config).__name__
        assert snapshot.get(f"{config_name}.events.publish") == pytest.approx(1.0)
        assert snapshot.get(f"{config_name}.events.ack") == pytest.approx(1.0)
    finally:
        await config.close_pool()


def assert_sync_events_queue_multiple_messages_contract(make_config: Any, case: EventsCase) -> None:
    """Sync queue backends deliver every queued message on a channel exactly once."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "multi")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = setup_sync_event_channel(config)
        event_ids = {channel.publish("multi", {"index": index}) for index in range(3)}
        received = set()
        for _ in range(3):
            message = _consume_sync(channel, "multi")
            received.add(message.event_id)
            channel.ack(message.event_id)
        assert received == event_ids
    finally:
        config.close_pool()


async def assert_async_events_queue_multiple_messages_contract(make_config: Any, case: EventsCase) -> None:
    """Async queue backends deliver every queued message on a channel exactly once."""
    _queue_table, extension_config, suffix = _events_extension_config(case, "multi")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = await setup_async_event_channel(config)
        event_ids = {await channel.publish("multi", {"index": index}) for index in range(3)}
        received = set()
        for _ in range(3):
            message = await _consume_async(channel, "multi")
            received.add(message.event_id)
            await channel.ack(message.event_id)
        assert received == event_ids
    finally:
        await config.close_pool()


def assert_sync_events_queue_nack_contract(make_config: Any, case: EventsCase) -> None:
    """Sync queue backends return nacked events to a redeliverable pending state."""
    queue_table, extension_config, suffix = _events_extension_config(case, "nack")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("nack", {"retry": True})
        message = _consume_sync(channel, "nack")
        channel.nack(message.event_id)
        row = _status_row_sync(config, queue_table, event_id, "status, attempts")
        assert row["status"] == "pending"
        assert row["attempts"] >= 1
    finally:
        config.close_pool()


async def assert_async_events_queue_nack_contract(make_config: Any, case: EventsCase) -> None:
    """Async queue backends return nacked events to a redeliverable pending state."""
    queue_table, extension_config, suffix = _events_extension_config(case, "nack")
    config = make_config(extension_config=extension_config, suffix=suffix)
    try:
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish("nack", {"retry": True})
        message = await _consume_async(channel, "nack")
        await channel.nack(message.event_id)
        row = await _status_row_async(config, queue_table, event_id, "status, attempts")
        assert row["status"] == "pending"
        assert row["attempts"] >= 1
    finally:
        await config.close_pool()
