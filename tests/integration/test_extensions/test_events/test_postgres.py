"""Integration tests for native PostgreSQL event channels."""

import asyncio

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asyncpg_native_event_channel(postgres_service: PostgresService) -> None:
    """AsyncPG configs surface native LISTEN/NOTIFY events."""

    config = AsyncpgConfig(
        pool_config={
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

    event_id = await channel.publish_async("notifications", {"action": "native"})

    iterator = channel.iter_events_async("notifications", poll_interval=0.2)
    try:
        message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
    finally:
        await iterator.aclose()

    await channel.ack_async(message.event_id)

    assert message.event_id == event_id
    assert message.payload["action"] == "native"

    if config.pool_instance:
        await config.close_pool()
