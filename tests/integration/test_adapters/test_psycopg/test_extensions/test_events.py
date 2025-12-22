# pyright: reportAttributeAccessIssue=false, reportArgumentType=false
"""Psycopg integration tests for the EventChannel queue backend."""

import asyncio

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


def _build_conninfo(service: PostgresService) -> str:
    return f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


@pytest.mark.postgres
def test_psycopg_sync_event_channel_queue_fallback(tmp_path, postgres_service: PostgresService) -> None:
    """Psycopg sync configs use the queue fallback successfully."""

    migrations_dir = tmp_path / "psycopg_events"
    migrations_dir.mkdir()

    config = PsycopgSyncConfig(
        connection_config={"conninfo": _build_conninfo(postgres_service)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = SyncMigrationCommands(config)
    commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    try:
        event_id = channel.publish("notifications", {"action": "queue"})
        iterator = channel.iter_events("notifications", poll_interval=0.1)
        try:
            message = next(iterator)
        finally:
            iterator.close()
        channel.ack(message.event_id)

        with config.provide_session() as driver:
            row = driver.select_one(
                "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
            )

        assert row["status"] == "acked"
    finally:
        config.close_pool()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_psycopg_async_event_channel_queue_fallback(tmp_path, postgres_service: PostgresService) -> None:
    """Psycopg async configs use the queue backend."""

    migrations_dir = tmp_path / "psycopg_async_events"
    migrations_dir.mkdir()

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _build_conninfo(postgres_service)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    try:
        event_id = await channel.publish("notifications", {"action": "async_queue"})
        iterator = channel.iter_events("notifications", poll_interval=0.1)
        try:
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
        finally:
            await iterator.aclose()
        await channel.ack(message.event_id)

        async with config.provide_session() as driver:
            row = await driver.select_one(
                "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
            )

        assert row["status"] == "acked"
    finally:
        await config.close_pool()
