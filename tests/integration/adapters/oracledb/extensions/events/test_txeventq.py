"""Oracle Transactional Event Queues (TxEventQ) event channel integration tests.

Mirrors the advanced_queue parity bar for the transactional_event_queue backend, which
shares the AQ client path and differs only in provisioning (CREATE_TRANSACTIONAL_EVENT_QUEUE).
"""

import asyncio
import time
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import AbstractContextManager
from typing import Any

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.extensions.events import AsyncEventChannel, SyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")

_QUEUE_NAME = "SQLSPEC_TXEVENTQ"
_SUBSCRIBE_WAIT = 2.0
_POLL_INTERVAL = 0.05
_MAX_POLL_ATTEMPTS = 200


def _sync_wait_for_message(received: "list[Any]", count: int = 1) -> None:
    for _ in range(_MAX_POLL_ATTEMPTS):
        if len(received) >= count:
            return
        time.sleep(_POLL_INTERVAL)


async def _async_wait_for_message(received: "list[Any]", count: int = 1) -> None:
    for _ in range(_MAX_POLL_ATTEMPTS):
        if len(received) >= count:
            return
        await asyncio.sleep(_POLL_INTERVAL)


def _sync_config(oracle_service: OracleService, **events: Any) -> OracleSyncConfig:
    return OracleSyncConfig(
        connection_config={
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
            "user": oracle_service.user,
            "password": oracle_service.password,
        },
        extension_config={"events": {"backend": "transactional_event_queue", "aq_queue": _QUEUE_NAME, **events}},
    )


def _async_config(oracle_service: OracleService, **events: Any) -> OracleAsyncConfig:
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
        extension_config={"events": {"backend": "transactional_event_queue", "aq_queue": _QUEUE_NAME, **events}},
    )


@pytest.fixture
def oracle_txeventq_config(
    provision_txeventq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> Generator[OracleSyncConfig, None, None]:
    """Sync Oracle config backed by a live Transactional Event Queue."""

    config = _sync_config(oracle_23ai_service)
    with provision_txeventq():
        try:
            yield config
        finally:
            config.close_pool()


@pytest.fixture
async def oracle_txeventq_async_config(
    provision_txeventq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Async Oracle config backed by a live Transactional Event Queue."""

    config = _async_config(oracle_23ai_service)
    with provision_txeventq():
        try:
            yield config
        finally:
            if config.connection_instance is not None:
                await config.close_pool()


def test_txeventq_publish_receive(oracle_txeventq_config: OracleSyncConfig) -> None:
    """TxEventQ backend publishes and receives a JSON payload via EventChannel."""

    spec = SQLSpec()
    spec.add_config(oracle_txeventq_config)
    channel = spec.event_channel(oracle_txeventq_config)

    assert isinstance(channel, SyncEventChannel)
    assert channel._backend_name == "transactional_event_queue"  # pyright: ignore[reportPrivateUsage]

    event_id = channel.publish("alerts", {"action": "refresh"})
    message = next(channel.iter_events("alerts", poll_interval=1.0))

    assert message.event_id == event_id
    assert message.payload["action"] == "refresh"

    channel.ack(message.event_id)


def test_txeventq_listen_delivery(oracle_txeventq_config: OracleSyncConfig) -> None:
    """TxEventQ backend delivers events to a sync channel.listen handler thread."""

    spec = SQLSpec()
    spec.add_config(oracle_txeventq_config)
    channel = spec.event_channel(oracle_txeventq_config)

    received: list[Any] = []

    def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("notifications", _handler, poll_interval=0.2)
    time.sleep(_SUBSCRIBE_WAIT)

    event_id = channel.publish("notifications", {"action": "sync_delivery"})
    _sync_wait_for_message(received)
    if not received:
        event_id = channel.publish("notifications", {"action": "sync_delivery"})
        _sync_wait_for_message(received)

    channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    assert received[0].event_id == event_id
    assert received[0].payload["action"] == "sync_delivery"

    channel.shutdown()


def test_txeventq_metadata(oracle_txeventq_config: OracleSyncConfig) -> None:
    """TxEventQ backend preserves event metadata through the queue."""

    spec = SQLSpec()
    spec.add_config(oracle_txeventq_config)
    channel = spec.event_channel(oracle_txeventq_config)

    received: list[Any] = []

    def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("meta_channel", _handler, poll_interval=0.2)
    time.sleep(_SUBSCRIBE_WAIT)

    metadata = {"source": "scheduler", "priority": 5}
    channel.publish("meta_channel", {"action": "with_meta"}, metadata)
    _sync_wait_for_message(received)
    if not received:
        channel.publish("meta_channel", {"action": "with_meta"}, metadata)
        _sync_wait_for_message(received)

    channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    assert received[0].metadata is not None
    assert received[0].metadata["source"] == "scheduler"
    assert received[0].metadata["priority"] == 5

    channel.shutdown()


async def test_txeventq_async_publish_and_ack(oracle_txeventq_async_config: OracleAsyncConfig) -> None:
    """Async TxEventQ backend publishes and acknowledges a JSON event."""

    spec = SQLSpec()
    spec.add_config(oracle_txeventq_async_config)
    channel = spec.event_channel(oracle_txeventq_async_config)

    assert isinstance(channel, AsyncEventChannel)
    assert channel._backend_name == "transactional_event_queue"  # pyright: ignore[reportPrivateUsage]

    event_id = await channel.publish("alerts", {"action": "test"})
    assert len(event_id) == 32

    await channel.ack(event_id)
    await channel.shutdown()


async def test_txeventq_async_listen_delivery(oracle_txeventq_async_config: OracleAsyncConfig) -> None:
    """Async TxEventQ backend delivers events to a channel.listen handler."""

    spec = SQLSpec()
    spec.add_config(oracle_txeventq_async_config)
    channel = spec.event_channel(oracle_txeventq_async_config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("notifications", _handler, poll_interval=0.2)
    await asyncio.sleep(_SUBSCRIBE_WAIT)

    event_id = await channel.publish("notifications", {"action": "async_delivery"})
    await _async_wait_for_message(received)
    if not received:
        event_id = await channel.publish("notifications", {"action": "async_delivery"})
        await _async_wait_for_message(received)

    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    assert received[0].event_id == event_id
    assert received[0].payload["action"] == "async_delivery"

    await channel.shutdown()


async def test_txeventq_async_metadata(oracle_txeventq_async_config: OracleAsyncConfig) -> None:
    """Async TxEventQ backend preserves event metadata through the queue."""

    spec = SQLSpec()
    spec.add_config(oracle_txeventq_async_config)
    channel = spec.event_channel(oracle_txeventq_async_config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("meta_channel", _handler, poll_interval=0.2)
    await asyncio.sleep(_SUBSCRIBE_WAIT)

    metadata = {"source": "scheduler", "priority": 5}
    await channel.publish("meta_channel", {"action": "with_meta"}, metadata)
    await _async_wait_for_message(received)
    if not received:
        await channel.publish("meta_channel", {"action": "with_meta"}, metadata)
        await _async_wait_for_message(received)

    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    assert received[0].metadata is not None
    assert received[0].metadata["source"] == "scheduler"
    assert received[0].metadata["priority"] == 5

    await channel.shutdown()


async def test_txeventq_async_concurrent_multi_channel(
    provision_txeventq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> None:
    """Concurrent listeners on distinct per-channel TxEventQ queues stay isolated and race-free."""

    config = _async_config(oracle_23ai_service, aq_queue="SQLSPEC_TXQ_{channel}")

    with provision_txeventq(queue_name="SQLSPEC_TXQ_CHANA"), provision_txeventq(queue_name="SQLSPEC_TXQ_CHANB"):
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
