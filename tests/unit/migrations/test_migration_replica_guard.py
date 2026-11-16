"""Replica migration refusal and durable queue primary isolation."""

from collections.abc import Generator
from pathlib import Path
from typing import cast

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.extensions.events import EventMessage, SyncEventChannel
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.routing import RoutingContext


def test_replica_migrations_refuse_even_with_direct_connection(tmp_path: Path) -> None:
    primary = SqliteConfig(
        connection_config={"database": str(tmp_path / "writer.db")},
        replicas=[{"database": str(tmp_path / "reader.db")}],
    )
    reader = primary.replica_groups["default"].configs[0]
    with pytest.raises(MigrationError, match="read replica"):
        SyncMigrationCommands(reader)
    primary.close_pool()


def test_migrations_and_durable_events_keep_primary_in_replica_scope(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    primary = SqliteConfig(
        connection_config={"database": str(tmp_path / "writer.db")},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        replicas=[{"database": str(tmp_path / "reader.db")}],
    )
    with RoutingContext.replica():
        SyncMigrationCommands(primary).upgrade()
        channel = SyncEventChannel(primary)
        identity = channel.publish("events", {"value": 7})
        iterator = channel.iter_events("events", poll_interval=0.01)
        event = next(iterator)
        cast("Generator[EventMessage, None, None]", iterator).close()
        assert event.event_id == identity
        assert event.payload == {"value": 7}
        channel.ack(identity)
    with primary.provide_session() as session:
        assert session.select_value("SELECT COUNT(*) FROM sqlspec_event_queue") == 1
    reader = primary.replica_groups["default"].configs[0]
    with reader.provide_session() as session:
        assert session.select_value("SELECT COUNT(*) FROM sqlite_master WHERE name='sqlspec_event_queue'") == 0
    primary.close_pool()
