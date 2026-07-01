"""Oracle Advanced Queuing event channel integration tests."""

import time
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager
from typing import Any

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.extensions.events import SyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")

_SUBSCRIBE_WAIT = 2.0
_POLL_INTERVAL = 0.05
_MAX_POLL_ATTEMPTS = 200


def _wait_for_message(received: "list[Any]", count: int = 1) -> None:
    for _ in range(_MAX_POLL_ATTEMPTS):
        if len(received) >= count:
            return
        time.sleep(_POLL_INTERVAL)


@pytest.fixture
def oracle_aq_config(
    provision_classic_aq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> Generator[OracleSyncConfig, None, None]:
    """Provision Oracle config with a live advanced_queue queue for tests."""

    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        },
        extension_config={"events": {"backend": "advanced_queue"}},
    )

    with provision_classic_aq():
        try:
            yield config
        finally:
            config.close_pool()


def test_oracle_aq_publish_receive(oracle_aq_config: OracleSyncConfig) -> None:
    """AQ backend publishes and receives a JSON payload via EventChannel."""

    spec = SQLSpec()
    spec.add_config(oracle_aq_config)
    channel = spec.event_channel(oracle_aq_config)

    assert isinstance(channel, SyncEventChannel)

    assert channel._backend_name == "advanced_queue"  # pyright: ignore[reportPrivateUsage]

    event_id = channel.publish("alerts", {"action": "refresh"})
    iterator = channel.iter_events("alerts", poll_interval=1.0)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.payload["action"] == "refresh"

    channel.ack(message.event_id)


def test_oracle_aq_listen_delivery(oracle_aq_config: OracleSyncConfig) -> None:
    """AQ backend delivers events to a sync channel.listen handler thread."""

    spec = SQLSpec()
    spec.add_config(oracle_aq_config)
    channel = spec.event_channel(oracle_aq_config)

    received: list[Any] = []

    def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("notifications", _handler, poll_interval=0.2)
    time.sleep(_SUBSCRIBE_WAIT)

    event_id = channel.publish("notifications", {"action": "sync_delivery"})
    _wait_for_message(received)
    if not received:
        event_id = channel.publish("notifications", {"action": "sync_delivery"})
        _wait_for_message(received)

    channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]
    assert message.event_id == event_id
    assert message.payload["action"] == "sync_delivery"

    channel.shutdown()


def test_oracle_aq_metadata(oracle_aq_config: OracleSyncConfig) -> None:
    """AQ backend preserves event metadata through the queue."""

    spec = SQLSpec()
    spec.add_config(oracle_aq_config)
    channel = spec.event_channel(oracle_aq_config)

    received: list[Any] = []

    def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("meta_channel", _handler, poll_interval=0.2)
    time.sleep(_SUBSCRIBE_WAIT)

    metadata = {"source": "scheduler", "priority": 5}
    channel.publish("meta_channel", {"action": "with_meta"}, metadata)
    _wait_for_message(received)
    if not received:
        channel.publish("meta_channel", {"action": "with_meta"}, metadata)
        _wait_for_message(received)

    channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]
    assert message.metadata is not None
    assert message.metadata["source"] == "scheduler"
    assert message.metadata["priority"] == 5

    channel.shutdown()
