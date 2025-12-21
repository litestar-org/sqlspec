# pyright: reportAttributeAccessIssue=false
"""Psqlpy integration tests for the EventChannel queue backend."""

import asyncio

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_psqlpy_event_channel_queue_fallback(tmp_path, postgres_service: PostgresService) -> None:
    """Psqlpy adapters consume events via the queue backend."""

    migrations_dir = tmp_path / "psqlpy_events"
    migrations_dir.mkdir()

    dsn = (
        f"postgres://{postgres_service.user}:{postgres_service.password}@"
        f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )
    config = PsqlpyConfig(
        connection_config={"dsn": dsn, "max_db_pool_size": 4},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        driver_features={"events_backend": "table_queue", "enable_events": True},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async("notifications", {"action": "psqlpy"})
    iterator = channel.iter_events_async("notifications", poll_interval=0.1)
    try:
        message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
    finally:
        await iterator.aclose()
    await channel.ack_async(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"

    await config.close_pool()
