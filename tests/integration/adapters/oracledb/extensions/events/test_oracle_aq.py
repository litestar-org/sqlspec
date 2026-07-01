"""Oracle Advanced Queuing event channel integration tests."""

from collections.abc import Callable, Generator
from contextlib import AbstractContextManager

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.extensions.events import SyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")


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

    event_id = channel.publish("alerts", {"action": "refresh"})
    iterator = channel.iter_events("alerts", poll_interval=1.0)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.payload["action"] == "refresh"

    channel.ack(message.event_id)
