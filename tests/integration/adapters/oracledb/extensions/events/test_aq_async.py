"""Async Oracle Advanced Queuing event channel parity tests.

Mirrors the asyncpg listen/notify parity bar for the async aq backend
(OracleAsyncAQEventBackend), which had no live-queue coverage before this suite.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractContextManager
from typing import Any

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleAsyncConfig
from sqlspec.extensions.events import AsyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")

_SUBSCRIBE_WAIT = 2.0
_POLL_INTERVAL = 0.05
_MAX_POLL_ATTEMPTS = 200


async def _wait_for_message(received: "list[Any]", count: int = 1) -> None:
    for _ in range(_MAX_POLL_ATTEMPTS):
        if len(received) >= count:
            return
        await asyncio.sleep(_POLL_INTERVAL)


def _async_config(oracle_service: OracleService, **events: Any) -> OracleAsyncConfig:
    events_config: dict[str, Any] = {"backend": "aq", **events}
    return OracleAsyncConfig(
        connection_config={
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
            "user": oracle_service.user,
            "password": oracle_service.password,
            "min": 1,
            "max": 5,
        },
        extension_config={"events": events_config},
    )


@pytest.fixture
async def oracle_aq_async_config(
    provision_classic_aq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Async Oracle config backed by a live aq queue."""

    config = _async_config(oracle_23ai_service)
    with provision_classic_aq():
        try:
            yield config
        finally:
            if config.connection_instance is not None:
                await config.close_pool()


async def test_oracle_aq_async_publish_and_ack(oracle_aq_async_config: OracleAsyncConfig) -> None:
    """Async AQ backend publishes and acknowledges a JSON event."""

    spec = SQLSpec()
    spec.add_config(oracle_aq_async_config)
    channel = spec.event_channel(oracle_aq_async_config)

    assert isinstance(channel, AsyncEventChannel)
    assert channel._backend_name == "aq"  # pyright: ignore[reportPrivateUsage]

    event_id = await channel.publish("alerts", {"action": "test"})
    assert len(event_id) == 32

    await channel.ack(event_id)
    await channel.shutdown()


async def test_oracle_aq_async_listen_delivery(oracle_aq_async_config: OracleAsyncConfig) -> None:
    """Async AQ backend delivers events to a channel.listen handler."""

    spec = SQLSpec()
    spec.add_config(oracle_aq_async_config)
    channel = spec.event_channel(oracle_aq_async_config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("notifications", _handler, poll_interval=0.2)
    await asyncio.sleep(_SUBSCRIBE_WAIT)

    event_id = await channel.publish("notifications", {"action": "async_delivery"})
    await _wait_for_message(received)
    if not received:
        event_id = await channel.publish("notifications", {"action": "async_delivery"})
        await _wait_for_message(received)

    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]
    assert message.event_id == event_id
    assert message.payload["action"] == "async_delivery"

    await channel.shutdown()


async def test_oracle_aq_async_metadata(oracle_aq_async_config: OracleAsyncConfig) -> None:
    """Async AQ backend preserves event metadata through the queue."""

    spec = SQLSpec()
    spec.add_config(oracle_aq_async_config)
    channel = spec.event_channel(oracle_aq_async_config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("meta_channel", _handler, poll_interval=0.2)
    await asyncio.sleep(_SUBSCRIBE_WAIT)

    metadata = {"source": "scheduler", "priority": 5}
    await channel.publish("meta_channel", {"action": "with_meta"}, metadata)
    await _wait_for_message(received)
    if not received:
        await channel.publish("meta_channel", {"action": "with_meta"}, metadata)
        await _wait_for_message(received)

    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]
    assert message.metadata is not None
    assert message.metadata["source"] == "scheduler"
    assert message.metadata["priority"] == 5

    await channel.shutdown()


async def test_oracle_aq_async_concurrent_multi_channel(
    provision_classic_aq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> None:
    """Concurrent listeners on distinct per-channel queues stay isolated and race-free.

    The aq backend routes a channel to its own physical queue via the
    ``{channel}`` template, so each listener must see only its own channel's events even
    while both drain the shared hub connection under an asyncio lock.
    """

    config = _async_config(oracle_23ai_service, aq_queue="SQLSPEC_EVT_{channel}")

    with (
        provision_classic_aq(queue_table="SQLSPEC_EVT_CHANA_T", queue_name="SQLSPEC_EVT_CHANA"),
        provision_classic_aq(queue_table="SQLSPEC_EVT_CHANB_T", queue_name="SQLSPEC_EVT_CHANB"),
    ):
        spec = SQLSpec()
        spec.add_config(config)
        channel = spec.event_channel(config)

        received_a: list[Any] = []
        received_b: list[Any] = []

        async def _handler_a(message: Any) -> None:
            received_a.append(message)

        async def _handler_b(message: Any) -> None:
            received_b.append(message)

        listener_a = channel.listen("chana", _handler_a, poll_interval=0.05)
        listener_b = channel.listen("chanb", _handler_b, poll_interval=0.05)
        await asyncio.sleep(0.5)

        for index in range(5):
            await channel.publish("chana", {"chan": "a", "i": index})
            await channel.publish("chanb", {"chan": "b", "i": index})
            await asyncio.sleep(0.05)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10.0
        while loop.time() < deadline:
            if len(received_a) >= 5 and len(received_b) >= 5:
                break
            if listener_a.task.done() or listener_b.task.done():
                break
            await asyncio.sleep(0.05)

        try:
            if listener_a.task.done():
                assert listener_a.task.exception() is None, "listener_a crashed under concurrency"
            if listener_b.task.done():
                assert listener_b.task.exception() is None, "listener_b crashed under concurrency"
            assert len(received_a) >= 5, f"chana delivery degraded: {len(received_a)}/5"
            assert len(received_b) >= 5, f"chanb delivery degraded: {len(received_b)}/5"
            assert all(m.payload["chan"] == "a" for m in received_a), "chana leaked chanb events"
            assert all(m.payload["chan"] == "b" for m in received_b), "chanb leaked chana events"
        finally:
            await channel.stop_listener(listener_a.id)
            await channel.stop_listener(listener_b.id)
            await channel.shutdown()
            if config.connection_instance is not None:
                await config.close_pool()
