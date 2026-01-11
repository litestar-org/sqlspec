# pyright: reportPrivateUsage=false
"""AsyncPG integration tests for the EventChannel native backend."""

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asyncpg_native_event_channel(postgres_service: "PostgresService") -> None:
    """AsyncPG configs surface native LISTEN/NOTIFY events."""

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        }
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    assert channel._backend_name == "listen_notify"

    event_id = await channel.publish("notifications", {"action": "native"})
    await channel.ack(event_id)

    if config.connection_instance:
        await config.close_pool()
