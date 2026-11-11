"""Live Teradata migration ledger and built-in extension reconciliation."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

from sqlspec.adapters.teradata import TeradataConfig
from sqlspec.config import ExtensionConfigs
from sqlspec.exceptions import NotFoundError

pytestmark = [pytest.mark.teradata, pytest.mark.xdist_group("teradata")]


def test_migrations_and_all_extension_schemas_reconcile_idempotently(
    teradata_connection_params: dict[str, Any],
) -> None:
    suffix = uuid4().hex[:8]
    names = {
        key: f"sqlspec_td_{key}_{suffix}"
        for key in ("ls", "ev", "sess", "events", "app", "user", "meta", "memory", "ledger")
    }
    extensions: ExtensionConfigs = {
        "litestar": {"session_table": names["ls"], "migration_mode": "reconcile"},
        "events": {"queue_table": names["ev"], "migration_mode": "reconcile"},
        "adk": {
            "session_table": names["sess"],
            "events_table": names["events"],
            "app_state_table": names["app"],
            "user_state_table": names["user"],
            "metadata_table": names["meta"],
            "memory_table": names["memory"],
            "enable_memory": True,
            "migration_mode": "reconcile",
        },
    }
    config = TeradataConfig(
        connection_config=teradata_connection_params,
        migration_config={"version_table_name": names["ledger"]},
        extension_config=extensions,
    )
    try:
        config.migrate_up()
        config.migrate_up()
        with config.provide_session() as session:
            tables = {table["table_name"].strip().lower() for table in session.data_dictionary.get_tables(session)}
            assert set(names.values()) <= tables
        from sqlspec import SQLSpec
        from sqlspec.adapters.teradata.adk import TeradataADKMemoryStore, TeradataADKStore
        from sqlspec.adapters.teradata.events import TeradataEventQueueStore
        from sqlspec.extensions.adk._types import StoredEvent
        from sqlspec.extensions.adk.memory._types import StoredMemory

        queue = TeradataEventQueueStore(config)
        with config.provide_session() as session:
            queue.reconcile_schema_sync(session)
            session.commit()
        spec = SQLSpec()
        spec.add_config(config)
        channel = spec.event_channel(config)
        event_id = channel.publish("orders", {"order_id": 101})
        iterator = channel.iter_events("orders", poll_interval=0.05)
        try:
            message = next(iterator)
            assert message.payload == {"order_id": 101}
            assert message.event_id == event_id
            channel.ack(message.event_id)
        finally:
            cast("Any", iterator).close()
        sessions = TeradataADKStore(config)
        memory = TeradataADKMemoryStore(config)
        sessions.create_tables()
        memory.create_tables()
        sessions.reconcile_schema()
        memory.reconcile_schema()
        created = sessions.create_session("session-1", "app", "user", {"count": 0})
        assert created["state"] == {"count": 0}
        now = datetime.now(UTC)
        event: StoredEvent = {
            "id": "event-1",
            "app_name": "app",
            "user_id": "user",
            "session_id": "session-1",
            "invocation_id": "inv-1",
            "timestamp": now,
            "event_data": {"author": "assistant"},
        }
        updated = sessions.append_event_and_update_state(event, "app", "user", "session-1", {"count": 1})
        assert updated["state"] == {"count": 1}
        entry: StoredMemory = {
            "id": "memory-1",
            "session_id": "session-1",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": "event-1",
            "author": "assistant",
            "timestamp": now,
            "content_json": {"text": "Teradata acceptance"},
            "content_text": "Teradata acceptance",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        }
        assert memory.insert_memory_entries([entry]) == 1
        assert [item["id"] for item in memory.search_entries("Teradata", "app", "user")] == ["memory-1"]
        sessions.delete_session("app", "user", "session-1")
        assert sessions.get_session("app", "user", "session-1") is None
        assert sessions.get_events("app", "user", "session-1") == []
        assert memory.delete_entries_by_session("session-1") == 1
    finally:
        with config.provide_session() as session:
            for key in ("memory", "meta", "user", "app", "events", "sess", "ev", "ls", "ledger"):
                try:
                    session.execute_script(f"DROP TABLE {names[key]}")
                    session.commit()
                except NotFoundError:
                    session.rollback()
        config.close_pool()


async def test_litestar_store_crud_and_schema_reconcile(teradata_connection_params: dict[str, Any]) -> None:
    from sqlspec.adapters.teradata.litestar import TeradataStore

    table = f"sqlspec_td_ls_{uuid4().hex[:8]}"
    config = TeradataConfig(
        connection_config=teradata_connection_params, extension_config={"litestar": {"session_table": table}}
    )
    try:
        store = TeradataStore(config)
        await store.create_table()
        await store.reconcile_schema()
        await store.set("key", b"payload-v2", expires_in=60)
        assert await store.get("key") == b"payload-v2"
        assert await store.exists("key") is True
        await store.delete("key")
        assert await store.get("key") is None
    finally:
        with config.provide_session() as session:
            try:
                session.execute_script(f"DROP TABLE {table}")
                session.commit()
            except NotFoundError:
                session.rollback()
        config.close_pool()


def test_migration_tracker_upgrade_idempotency_and_downgrade(
    teradata_connection_params: dict[str, Any], tmp_path: Path
) -> None:
    from sqlspec.migrations.commands import SyncMigrationCommands

    suffix = uuid4().hex[:8]
    table = f"sqlspec_td_mig_{suffix}"
    ledger = f"sqlspec_td_ledger_{suffix}"
    (tmp_path / "0001_create_items.sql").write_text(
        f"-- name: migrate-0001-up\nCREATE MULTISET TABLE {table} (id INTEGER);\n\n-- name: migrate-0001-down\nDROP TABLE {table};\n"
    )
    config = TeradataConfig(
        connection_config=teradata_connection_params,
        migration_config={"script_location": str(tmp_path), "version_table_name": ledger},
    )
    try:
        commands = SyncMigrationCommands(config)
        commands.upgrade()
        commands.upgrade()
        assert commands.current() == "0001"
        commands.downgrade("base")
        assert commands.current() is None
        with config.provide_session() as session:
            assert table not in {
                row["table_name"].strip().lower() for row in session.data_dictionary.get_tables(session)
            }
    finally:
        with config.provide_session() as session:
            for name in (table, ledger):
                try:
                    session.execute_script(f"DROP TABLE {name}")
                    session.commit()
                except NotFoundError:
                    session.rollback()
        config.close_pool()
