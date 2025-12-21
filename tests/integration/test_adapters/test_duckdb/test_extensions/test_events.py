"""DuckDB integration tests for EventChannel queue fallback."""

from typing import TYPE_CHECKING, cast

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.migrations.commands import SyncMigrationCommands

if TYPE_CHECKING:
    from sqlspec.extensions.events import SyncEventChannel


@pytest.mark.integration
@pytest.mark.duckdb
def test_duckdb_event_channel_queue_fallback(tmp_path) -> None:
    """DuckDB configs publish, consume, and ack events via the queue backend."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "duck_events.db"

    config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = SyncMigrationCommands(config)
    commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = cast("SyncEventChannel", spec.event_channel(config))

    event_id = channel.publish("notifications", {"action": "duck"})
    iterator = channel.iter_events("notifications", poll_interval=0.05)
    message = next(iterator)
    channel.ack(message.event_id)

    with config.provide_session() as driver:
        row = driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert message.payload["action"] == "duck"
    assert row["status"] == "acked"
