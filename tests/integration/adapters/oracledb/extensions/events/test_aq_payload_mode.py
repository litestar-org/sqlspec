"""Thin-mode JSON payload verification for the Oracle advanced_queue events backend.

Confirms that classic Advanced Queuing carries a structured JSON payload through a full
enqueue/dequeue cycle while the adapter is in its default thin mode (no Instant Client).
"""

from collections.abc import Callable, Generator
from contextlib import AbstractContextManager

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.extensions.events import SyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture
def oracle_aq_json_config(
    provision_classic_aq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> Generator[OracleSyncConfig, None, None]:
    """Provision a JSON-payload AQ queue for the advanced_queue backend."""

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

    with provision_classic_aq(payload_type="JSON"):
        try:
            yield config
        finally:
            config.close_pool()


def test_advanced_queue_json_round_trips_in_thin_mode(oracle_aq_json_config: OracleSyncConfig) -> None:
    """A nested JSON payload survives enqueue/dequeue unchanged with a thin-mode connection."""

    with oracle_aq_json_config.provide_session() as driver:
        assert driver.connection.thin is True

    spec = SQLSpec()
    spec.add_config(oracle_aq_json_config)
    channel = spec.event_channel(oracle_aq_json_config)
    assert isinstance(channel, SyncEventChannel)

    payload = {"action": "refresh", "nested": {"count": 3, "tags": ["a", "b"]}, "flag": True}
    event_id = channel.publish("alerts", payload)
    message = next(channel.iter_events("alerts", poll_interval=1.0))

    assert message.event_id == event_id
    assert message.payload["action"] == "refresh"
    assert message.payload["nested"] == {"count": 3, "tags": ["a", "b"]}
    assert message.payload["flag"] is True

    channel.ack(message.event_id)
