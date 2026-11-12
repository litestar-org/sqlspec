"""Native Snowflake adapter acceptance against Embucket and gated live Snowflake."""

import contextlib
import sys
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlspec.adapters.snowflake import SnowflakeConfig, SnowflakeDriver

pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.snowflake_connector,
    pytest.mark.integration,
    pytest.mark.xdist_group("snowflake"),
]


@pytest.fixture
def snowflake_items(snowflake_session: "SnowflakeDriver") -> "Generator[str, None, None]":
    """Create a scratch table and drop it even when an assertion fails."""
    table = "sqlspec_sf_" + uuid4().hex
    snowflake_session.execute(f"CREATE TABLE {table} (id INTEGER, name VARCHAR, amount INTEGER)")
    try:
        yield table
    finally:
        with contextlib.suppress(Exception):
            snowflake_session.rollback()
        snowflake_session.execute(f"DROP TABLE IF EXISTS {table}")


def test_snowflake_pooled_crud_and_parameter_binds(
    snowflake_config: "SnowflakeConfig", snowflake_session: "SnowflakeDriver", snowflake_items: str
) -> None:
    """Named and qmark binds survive pool release and reacquisition."""
    snowflake_session.execute(f"INSERT INTO {snowflake_items} VALUES (?, ?, ?)", (1, "alpha", 10))
    snowflake_session.execute_many(
        f"INSERT INTO {snowflake_items} VALUES (?, ?, ?)", [(2, "beta", 20), (3, "gamma", 30)]
    )
    snowflake_session.commit()
    with snowflake_config.provide_session() as other:
        assert other.select(f"SELECT id, name, amount FROM {snowflake_items} WHERE name = :name", {"name": "beta"}) == [
            {"id": 2, "name": "beta", "amount": 20}
        ]
        with pytest.raises(NotImplementedError):
            other.create_savepoint("sp1")


def test_snowflake_arrow_zero_row_and_stream_without_pandas(
    snowflake_session: "SnowflakeDriver", snowflake_items: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """JSON fallback and empty descriptions preserve Arrow columns without pandas."""
    monkeypatch.setitem(sys.modules, "pandas", None)
    snowflake_session.execute_many(
        f"INSERT INTO {snowflake_items} VALUES (?, ?, ?)", [(i, f"item_{i}", i * 10) for i in range(5)]
    )
    empty = snowflake_session.select_to_arrow(f"SELECT id, name FROM {snowflake_items} WHERE 1 = 0")
    assert empty.num_rows == 0
    assert empty.column_names == ["id", "name"]
    populated = snowflake_session.select_to_arrow(f"SELECT id, name FROM {snowflake_items} ORDER BY id")
    assert populated.num_rows == 5
    with snowflake_session.select_stream(f"SELECT id, name FROM {snowflake_items} ORDER BY id", chunk_size=2) as rows:
        assert list(rows) == [{"id": i, "name": f"item_{i}"} for i in range(5)]


def test_snowflake_execute_script(snowflake_session: "SnowflakeDriver", snowflake_items: str) -> None:
    """Execute both statements through the connector's native script parser."""
    result = snowflake_session.execute_script(
        f"INSERT INTO {snowflake_items} VALUES (1, 'alpha', 10); INSERT INTO {snowflake_items} VALUES (2, 'beta', 12);"
    )
    assert result.total_statements == 2
    assert snowflake_session.select_value(f"SELECT amount FROM {snowflake_items} WHERE id = 2") == 12


def test_snowflake_catalog_wire_query(snowflake_session: "SnowflakeDriver", snowflake_items: str) -> None:
    """Read the emulator's minimal catalog through native bound queries."""
    dictionary = snowflake_session.data_dictionary
    assert dictionary.get_optimal_type(snowflake_session, "json") == "VARIANT"
    tables = snowflake_session.select(
        "SELECT table_name FROM information_schema.tables WHERE table_name = :table_name",
        {"table_name": snowflake_items},
    )
    assert tables == [{"table_name": snowflake_items}]


async def test_snowflake_live_extension_stores_round_trip(snowflake_live_connection_config: dict[str, Any]) -> None:
    """Litestar and ADK stores persist session payloads through native Snowflake sessions."""
    from sqlspec.adapters.snowflake import SnowflakeConfig
    from sqlspec.adapters.snowflake.adk import SnowflakeADKMemoryStore, SnowflakeADKStore
    from sqlspec.adapters.snowflake.events import SnowflakeEventQueueStore
    from sqlspec.adapters.snowflake.litestar import SnowflakeStore
    from sqlspec.extensions.adk import StoredEvent, StoredMemory
    from sqlspec.extensions.events import SyncEventChannel

    snowflake_driver_config = SnowflakeConfig(
        connection_config={**snowflake_live_connection_config, "timezone": "America/Los_Angeles"},
        driver_features={"enable_lowercase_column_names": False},
    )
    token = uuid4().hex
    snowflake_driver_config.extension_config["litestar"] = {"session_table": "sqlspec_sf_sessions_" + token}
    snowflake_driver_config.extension_config["adk"] = {
        "session_table": "sqlspec_sf_adk_sessions_" + token,
        "events_table": "sqlspec_sf_adk_events_" + token,
        "app_state_table": "sqlspec_sf_adk_apps_" + token,
        "user_state_table": "sqlspec_sf_adk_users_" + token,
        "metadata_table": "sqlspec_sf_adk_metadata_" + token,
        "memory_table": "sqlspec_sf_adk_memory_" + token,
        "enable_memory": True,
    }
    snowflake_driver_config.extension_config["events"] = {
        "queue_table": "sqlspec_sf_queue_" + token,
        "backend": "poll_queue",
    }
    store = SnowflakeStore(snowflake_driver_config)
    adk = SnowflakeADKStore(snowflake_driver_config)
    memory = SnowflakeADKMemoryStore(snowflake_driver_config)
    queue = SnowflakeEventQueueStore(snowflake_driver_config)
    try:
        await store.create_table()
        await store.set("s1", b"payload")
        assert await store.get("s1") == b"payload"
        await store.set("ttl", b"expiring", expires_in=120)
        ttl = await store.expires_in("ttl")
        assert ttl is not None and 0 < ttl <= 120
        adk.create_tables()
        created = adk.create_session("s1", "sqlspec", "user1", {"step": 1})
        assert created["state"] == {"step": 1}
        assert adk.get_session("sqlspec", "user1", "s1") == created
        memory.create_tables()
        now = datetime.now(UTC)
        for app_name, user_id in (("app-a", "user-a"), ("app-a", "user-b"), ("app-b", "user-a")):
            adk.create_session("shared", app_name, user_id, {})
            adk.append_event(
                StoredEvent(
                    id=f"{app_name}-{user_id}",
                    app_name=app_name,
                    user_id=user_id,
                    session_id="shared",
                    invocation_id="invocation",
                    timestamp=now,
                    event_data={"text": "scoped event"},
                )
            )
        with snowflake_driver_config.provide_session() as session:
            session.execute(
                "UPDATE sqlspec_sf_adk_sessions_" + token + " SET update_time = ? "
                "WHERE app_name = ? AND user_id = ? AND id = ?",
                (now - timedelta(days=2), "app-a", "user-a", "shared"),
            )
            session.commit()
        assert adk.delete_idle_sessions(now - timedelta(days=1), app_name="app-a") == 1
        assert [event["id"] for event in adk.get_events("app-a", "user-b", "shared")] == ["app-a-user-b"]
        assert [event["id"] for event in adk.get_events("app-b", "user-a", "shared")] == ["app-b-user-a"]
        entry = StoredMemory(
            id="m1",
            session_id="s1",
            app_name="sqlspec",
            user_id="user1",
            scope="user",
            event_id="e1",
            author="user",
            timestamp=now,
            content_json={"text": "Snowflake warehouse"},
            content_text="Snowflake warehouse",
            metadata_json={"source": "test"},
            inserted_at=now,
            embedding=None,
        )
        assert memory.insert_memory_entries([entry]) == 1
        assert [item["id"] for item in memory.search_entries("warehouse", "sqlspec", "user1")] == ["m1"]
        with snowflake_driver_config.provide_session() as session:
            for statement in queue.create_statements():
                session.execute(statement)
        channel = SyncEventChannel(snowflake_driver_config)
        event_id = channel.publish("jobs", {"task": "sync"})
        events = channel.iter_events("jobs", poll_interval=0.01)
        try:
            event = next(events)
            assert event.event_id == event_id
            assert event.payload == {"task": "sync"}
        finally:
            if close := getattr(events, "close", None):
                close()
        event_ids = channel.publish_many([
            ("batch_jobs", {"job": 1}, {"source": "test"}),
            ("batch_jobs", {"job": 2}, {"source": "test"}),
        ])
        batch_events = channel.iter_events("batch_jobs", poll_interval=0.01)
        try:
            received = [next(batch_events), next(batch_events)]
            assert {event.event_id for event in received} == set(event_ids)
            assert sorted(event.payload["job"] for event in received) == [1, 2]
            assert [event.metadata for event in received] == [{"source": "test"}, {"source": "test"}]
        finally:
            if close := getattr(batch_events, "close", None):
                close()
    finally:
        try:
            with contextlib.suppress(Exception):
                await store.delete_all()
            with contextlib.suppress(Exception):
                memory.drop_tables()
            with contextlib.suppress(Exception):
                adk.drop_tables()
            with snowflake_driver_config.provide_session() as session:
                session.execute("DROP TABLE IF EXISTS sqlspec_sf_sessions_" + token)
                for statement in queue.drop_statements():
                    session.execute(statement)
        finally:
            snowflake_driver_config.close_pool()


def test_snowflake_live_auth_and_imported_keys(snowflake_live_session: "SnowflakeDriver") -> None:
    """Live Snowflake reports named foreign keys from SHOW IMPORTED KEYS."""
    suffix = uuid4().hex
    parent = "sqlspec_sf_parent_" + suffix
    child = "sqlspec_sf_child_" + suffix
    snowflake_live_session.execute(f"CREATE TABLE {parent} (id INTEGER PRIMARY KEY)")
    try:
        snowflake_live_session.execute(f"CREATE TABLE {child} (id INTEGER, parent_id INTEGER REFERENCES {parent}(id))")
        keys = snowflake_live_session.data_dictionary.get_foreign_keys(snowflake_live_session, child)
        assert any(key.referenced_table.lower() == parent for key in keys)
        tables = snowflake_live_session.data_dictionary.get_tables(snowflake_live_session)
        assert child in {table["table_name"].lower() for table in tables}
        columns = snowflake_live_session.data_dictionary.get_columns(snowflake_live_session, child)
        assert [column["column_name"].lower() for column in columns] == ["id", "parent_id"]
    finally:
        snowflake_live_session.execute(f"DROP TABLE IF EXISTS {child}")
        snowflake_live_session.execute(f"DROP TABLE IF EXISTS {parent}")


def test_snowflake_live_update_and_migration_removal(snowflake_live_session: "SnowflakeDriver") -> None:
    """Verify warehouse mutations unavailable in the Embucket emulator."""
    from sqlspec.adapters.snowflake import SnowflakeSyncMigrationTracker

    suffix = uuid4().hex
    table = "sqlspec_sf_mutations_" + suffix
    ledger = "sqlspec_sf_ledger_" + suffix
    tracker = SnowflakeSyncMigrationTracker(version_table_name=ledger)
    snowflake_live_session.execute(f"CREATE TABLE {table} (id INTEGER, amount INTEGER)")
    try:
        result = snowflake_live_session.execute_script(
            f"INSERT INTO {table} VALUES (1, 10); UPDATE {table} SET amount = 12 WHERE id = 1;"
        )
        assert result.total_statements == 2
        assert snowflake_live_session.select_value(f"SELECT amount FROM {table} WHERE id = 1") == 12
        tracker.ensure_tracking_table(snowflake_live_session)
        tracker.record_migration(snowflake_live_session, "0001", "init", 12, "abc123")
        assert tracker.get_current_version(snowflake_live_session) == "0001"
        tracker.remove_migration(snowflake_live_session, "0001")
        assert tracker.get_current_version(snowflake_live_session) is None
    finally:
        snowflake_live_session.execute(f"DROP TABLE IF EXISTS {ledger}")
        snowflake_live_session.execute(f"DROP TABLE IF EXISTS {table}")


def test_snowflake_live_stage_parquet_put_and_copy_into(snowflake_live_session: "SnowflakeDriver") -> None:
    """The enabled stage path loads Parquet into a live Snowflake scratch table."""
    import pyarrow as pa

    table = "sqlspec_sf_load_" + uuid4().hex
    snowflake_live_session.execute(f"CREATE TABLE {table} (id INTEGER, name VARCHAR)")
    try:
        snowflake_live_session.load_from_arrow(table, pa.table({"id": [1, 2], "name": ["alpha", "beta"]}))
        assert snowflake_live_session.select(f"SELECT id, name FROM {table} ORDER BY id") == [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    finally:
        snowflake_live_session.execute(f"DROP TABLE IF EXISTS {table}")
