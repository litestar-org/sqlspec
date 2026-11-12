"""Snowflake ADK session, state, event, and memory store behavior."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.snowflake.adk import SnowflakeADKMemoryStore, SnowflakeADKStore
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk._types import StoredEvent
from sqlspec.extensions.adk.memory._types import StoredMemory
from sqlspec.extensions.adk.migrations.reconcile import downgrade, reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.utils.adapters import resolve_extension_store_class
from tests.unit.adapters.test_snowflake._store_helpers import store_config


def _event(session_id: str = "s1") -> StoredEvent:
    return StoredEvent(
        id="e1",
        app_name="app",
        user_id="u1",
        session_id=session_id,
        invocation_id="turn1",
        timestamp=datetime(2026, 11, 11, tzinfo=UTC),
        event_data={"text": "budget review"},
    )


def _memory(event_id: str = "e1") -> StoredMemory:
    now = datetime(2026, 11, 11, tzinfo=UTC)
    return StoredMemory(
        id="m1",
        session_id="s1",
        app_name="app",
        user_id="u1",
        scope="user",
        event_id=event_id,
        author="agent",
        timestamp=now,
        content_json={"text": "budget"},
        content_text="budget",
        metadata_json={"source": "conversation"},
        inserted_at=now,
        embedding=None,
    )


def _sql(call: Any) -> str:
    statement = call.args[0]
    return str(statement.sql if hasattr(statement, "sql") else statement)


def test_adk_store_resolves_and_reconciles_all_six_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch, {"adk": {"enable_memory": True}})
    assert (
        resolve_extension_store_class(config, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore")
        is SnowflakeADKStore
    )
    assert (
        resolve_extension_store_class(
            config, extension="adk", store_attr="adk_memory_store_class", store_suffix="ADKMemoryStore"
        )
        is SnowflakeADKMemoryStore
    )
    driver.data_dictionary.get_tables.return_value = []
    context = MigrationContext(config=config, dialect="snowflake", driver=driver)
    reconcile(context)
    statements = [call.args[0] for call in driver.execute_script.call_args_list]
    assert len(statements) == 6
    assert all("CREATE TABLE IF NOT EXISTS" in sql for sql in statements)
    assert sum("VARIANT" in sql for sql in statements) == 5
    driver.commit.assert_not_called()
    driver.execute_script.reset_mock()
    downgrade(context)
    assert [call.args[0] for call in driver.execute_script.call_args_list] == [
        "DROP TABLE IF EXISTS adk_memory",
        "DROP TABLE IF EXISTS adk_internal_metadata",
        "DROP TABLE IF EXISTS adk_user_state",
        "DROP TABLE IF EXISTS adk_app_state",
        "DROP TABLE IF EXISTS adk_event",
        "DROP TABLE IF EXISTS adk_session",
    ]


def test_adk_session_creation_and_update_use_parse_json_select(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch, {"adk": {"owner_id_column": "owner_id VARCHAR(64)"}})
    store = SnowflakeADKStore(config)
    session = store.create_session("s1", "app", "u1", {"k": 1}, owner_id="owner1")
    sql, parameters = driver.execute.call_args.args
    assert (
        "(id, app_name, user_id, owner_id, state, create_time, update_time) SELECT ?, ?, ?, ?, PARSE_JSON(?), ?, ?"
        in sql
    )
    assert parameters[:5] == ["s1", "app", "u1", "owner1", '{"k":1}']
    assert session["state"] == {"k": 1}
    store.update_session_state("app", "u1", "s1", {"k": 2})
    assert "SET state = PARSE_JSON(?), update_time = ?" in driver.execute.call_args.args[0]
    assert driver.execute.call_args.args[1][0] == '{"k":2}'


@pytest.mark.parametrize("state", ['{"k":1}', {"k": 1}])
def test_adk_get_session_decodes_variant_and_renews(monkeypatch: pytest.MonkeyPatch, state: Any) -> None:
    config, driver = store_config(monkeypatch)
    now = datetime(2026, 11, 11, tzinfo=UTC)
    driver.select_one_or_none.return_value = {
        "ID": "s1",
        "APP_NAME": "app",
        "USER_ID": "u1",
        "STATE": state,
        "CREATE_TIME": now.isoformat(),
        "UPDATE_TIME": now,
    }
    session = SnowflakeADKStore(config).get_session("app", "u1", "s1", renew_for=60)
    renewed_at = driver.execute.call_args.args[1][0]
    assert session == {
        "id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "state": {"k": 1},
        "create_time": now,
        "update_time": renewed_at,
    }
    assert renewed_at > now
    assert "SET update_time = ?" in driver.execute.call_args.args[0]


def test_adk_get_session_rejects_non_object_state(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    now = datetime(2026, 11, 11, tzinfo=UTC)
    driver.select_one_or_none.return_value = {
        "id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "state": "[]",
        "create_time": now,
        "update_time": now,
    }
    with pytest.raises(TypeError, match="Expected a JSON object"):
        SnowflakeADKStore(config).get_session("app", "u1", "s1")


def test_adk_list_validates_pagination_and_orders_ties(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKStore(config)
    assert store.list_sessions("app", limit=0) == []
    driver.select.assert_not_called()
    with pytest.raises(ValueError, match="offset requires a limit"):
        store.list_sessions("app", offset=1)
    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=5, offset=2)
    sql, parameters = driver.select.call_args.args
    assert "ORDER BY create_time ASC, id ASC LIMIT ? OFFSET ?" in sql
    assert parameters == ["app", "u1", 5, 2]


def test_adk_append_and_state_update_are_atomic(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    now = datetime(2026, 11, 11, tzinfo=UTC)
    driver.select_one_or_none.return_value = {"create_time": now}
    session = SnowflakeADKStore(config).append_event_and_update_state(
        _event(), "app", "u1", "s1", {"n": 1}, app_state={"a": 1}, user_state={"u": 1}
    )
    assert session["create_time"] == now
    assert session["state"] == {"n": 1}
    statements = [_sql(call) for call in driver.execute.call_args_list]
    assert "UPDATE adk_session SET state = PARSE_JSON(?)" in statements[0]
    assert "INSERT INTO adk_event" in statements[1]
    assert "SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)" in statements[1]
    assert "MERGE INTO adk_app_state" in statements[2]
    assert "MERGE INTO adk_user_state" in statements[3]
    driver.transaction.return_value.__enter__.assert_called_once_with()
    driver.commit.assert_not_called()


def test_adk_missing_session_aborts_before_event_write(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    with pytest.raises(ValueError, match="Session missing-id not found"):
        SnowflakeADKStore(config).append_event_and_update_state(_event("missing-id"), "app", "u1", "missing-id", {})
    driver.execute.assert_not_called()
    assert driver.transaction.return_value.__exit__.call_args.args[0] is ValueError


@pytest.mark.parametrize("idle", [False, True])
def test_adk_deletes_child_events_before_sessions(monkeypatch: pytest.MonkeyPatch, idle: bool) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKStore(config)
    if idle:
        assert store.delete_idle_sessions(datetime(2026, 11, 11, tzinfo=UTC), "app") == 1
    else:
        store.delete_session("app", "u1", "s1")
    statements = [_sql(call) for call in driver.execute.call_args_list]
    assert len(statements) == 2
    assert statements[0].startswith("DELETE FROM adk_event WHERE session_id")
    assert statements[1].startswith("DELETE FROM adk_session WHERE")
    driver.transaction.return_value.__enter__.assert_called_once_with()


def test_adk_state_and_metadata_upserts_use_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKStore(config)
    store.upsert_app_state("app", {"a": 1})
    store.upsert_user_state("app", "u1", {"u": 2})
    store.set_metadata("schema", "1")
    statements = [_sql(call) for call in driver.execute.call_args_list]
    assert statements[0].startswith("MERGE INTO adk_app_state")
    assert statements[1].startswith("MERGE INTO adk_user_state")
    assert statements[2].startswith("MERGE INTO adk_internal_metadata")
    driver.select_one_or_none.return_value = {"state": '{"a":1}'}
    assert store.get_app_state("app") == {"a": 1}
    driver.select_one_or_none.return_value = {"STATE": {"u": 2}}
    assert store.get_user_state("app", "u1") == {"u": 2}
    driver.select_one_or_none.return_value = {"VALUE": "1"}
    assert store.get_metadata("schema") == "1"


def test_adk_missing_tables_are_empty_but_other_errors_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKStore(config)
    driver.select_one_or_none.side_effect = SQLSpecError("Table does not exist")
    driver.select.side_effect = SQLSpecError("Table does not exist")
    driver.execute.side_effect = SQLSpecError("Table does not exist")
    assert store.get_session("app", "u1", "s1") is None
    assert store.get_app_state("app") is None
    assert store.get_metadata("schema") is None
    assert store.list_sessions("app") == []
    assert store.get_events("app", "u1", "s1") == []
    assert store.delete_expired_events(datetime.now(UTC)) == 0
    assert store.delete_idle_user_states(datetime.now(UTC)) == 0
    assert store.delete_idle_sessions(datetime.now(UTC)) == 0
    store.delete_session("app", "u1", "s1")
    driver.select_one_or_none.side_effect = SQLSpecError("permission denied")
    with pytest.raises(SQLSpecError, match="permission denied"):
        store.get_session("app", "u1", "s1")


def test_adk_create_tables_append_event_and_event_hydration(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKStore(config)
    store.create_tables()
    assert driver.execute.call_count == 5
    assert all("CREATE TABLE IF NOT EXISTS" in _sql(call) for call in driver.execute.call_args_list)
    store.append_event(_event())
    assert driver.execute.call_args.args[1][-1] == '{"text":"budget review"}'
    row: dict[str, Any] = dict(_event())
    row["event_data"] = '{"text":"budget review"}'
    row["timestamp"] = row["timestamp"].isoformat()
    driver.select.return_value = [row]
    assert store.get_events("app", "u1", "s1", datetime(2026, 11, 10, tzinfo=UTC), 5) == [_event()]
    sql, parameters = driver.select.call_args.args
    assert "timestamp > ? ORDER BY timestamp ASC, id ASC LIMIT ?" in sql
    assert parameters[-1] == 5
    assert store.get_events("app", "u1", "s1", limit=0) == []
    with pytest.raises(ValueError, match="non-negative"):
        store.get_events("app", "u1", "s1", limit=-1)


def test_adk_missing_rows_and_unbounded_listing(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKStore(config)
    assert store.get_session("app", "u1", "s1") is None
    assert store.get_user_state("app", "u1") is None
    assert store.get_metadata("key") is None
    now = datetime(2026, 11, 11, tzinfo=UTC)
    row = {"id": "s1", "app_name": "app", "user_id": "u1", "state": None, "create_time": now, "update_time": now}
    driver.select.return_value = [row]
    assert store.list_sessions("app") == [{**row, "state": {}}]
    assert driver.select.call_args.args[1] == ["app"]


def test_adk_schema_accepts_three_part_table_names(monkeypatch: pytest.MonkeyPatch) -> None:
    config, _ = store_config(
        monkeypatch, {"adk": {"session_table": "db.schema.sessions", "memory_table": "db.schema.memories"}}
    )
    assert SnowflakeADKStore(config).session_table == "db.schema.sessions"
    assert SnowflakeADKMemoryStore(config).memory_table == "db.schema.memories"


def test_memory_merges_by_event_id_and_counts_new_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    first = MagicMock()
    first.get_affected_count.return_value = 1
    duplicate = MagicMock()
    duplicate.get_affected_count.return_value = 0
    driver.execute.side_effect = [first, duplicate]
    store = SnowflakeADKMemoryStore(config)
    assert store.insert_memory_entries([_memory(), _memory()]) == 1
    sql, parameters = driver.execute.call_args.args
    assert "ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT" in sql
    assert "PARSE_JSON(?) AS content_json" in sql
    assert '{"text":"budget"}' in parameters
    assert store.insert_memory_entries([]) == 0


@pytest.mark.parametrize(
    "scope, expected",
    [
        ("user", "AND user_id = ? AND scope = 'user'"),
        ("app", "AND scope = 'app'"),
        ("all", "AND ((scope = 'user' AND user_id = ?) OR scope = 'app')"),
    ],
)
def test_memory_search_scopes_and_variant_hydration(monkeypatch: pytest.MonkeyPatch, scope: Any, expected: str) -> None:
    config, driver = store_config(monkeypatch)
    row: dict[str, Any] = dict(_memory())
    row["content_json"] = '{"text":"budget"}'
    driver.select.return_value = [row]
    results = SnowflakeADKMemoryStore(config).search_entries(" budget ", "app", "u1", limit=5, scope_filter=scope)
    assert results[0]["content_json"] == {"text": "budget"}
    assert results[0]["metadata_json"] == {"source": "conversation"}
    sql, parameters = driver.select.call_args.args
    assert expected in sql
    assert "content_text ILIKE ?" in sql
    assert parameters[-2:] == ["%budget%", 5]


def test_memory_guards_and_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKMemoryStore(config)
    assert store.search_entries("   ", "app", "u1") == []
    assert store.search_entries("budget", "app", "u1", limit=0) == []
    driver.select.assert_not_called()
    entry = _memory()
    entry["embedding"] = [0.1]
    with pytest.raises(NotImplementedError, match="vector embeddings"):
        store.insert_memory_entries([entry])
    with pytest.raises(NotImplementedError, match="vector embeddings"):
        store.search_entries("budget", "app", "u1", embedding=[0.1])
    assert store.delete_entries_by_session("s1") == 1
    assert driver.execute.call_args.args == ("DELETE FROM adk_memory WHERE session_id = ?", ["s1"])
    assert store.delete_entries_older_than(7, "app", "user") == 1
    assert "timestamp < ? AND app_name = ? AND scope = ?" in driver.execute.call_args.args[0]
    disabled_config, disabled_driver = store_config(monkeypatch, {"adk": {"enable_memory": False}})
    disabled = SnowflakeADKMemoryStore(disabled_config)
    disabled.create_tables()
    disabled_driver.execute.assert_not_called()
    with pytest.raises(RuntimeError, match="disabled"):
        disabled.insert_memory_entries([])
    with pytest.raises(RuntimeError, match="disabled"):
        disabled.search_entries("budget", "app", "u1")


def test_memory_creation_owner_column_and_null_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(
        monkeypatch, {"adk": {"owner_id_column": "owner VARCHAR(64)", "memory_use_fts": True}}
    )
    store = SnowflakeADKMemoryStore(config)
    store.create_tables()
    assert "content_json VARIANT NOT NULL" in driver.execute.call_args.args[0]
    entry = _memory()
    entry["metadata_json"] = None
    assert store.insert_memory_entries([entry], owner_id="owner1") == 1
    sql, parameters = driver.execute.call_args.args
    assert "? AS owner" in sql
    assert parameters[7] == "owner1"
    row: dict[str, Any] = dict(entry)
    driver.select.return_value = [row]
    assert store.search_entries("budget", "app", "u1")[0]["metadata_json"] is None
    assert driver.select.call_args.args[1][-1] == 20
    with pytest.raises(ValueError, match="non-negative"):
        store.search_entries("budget", "app", "u1", limit=-1)


def test_memory_missing_tables_and_other_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeADKMemoryStore(config)
    driver.select.side_effect = SQLSpecError("Table does not exist")
    driver.execute.side_effect = SQLSpecError("Table does not exist")
    assert store.search_entries("budget", "app", "u1") == []
    assert store.delete_entries_by_session("s1") == 0
    assert store.delete_entries_older_than(7) == 0
    driver.select.side_effect = SQLSpecError("permission denied")
    with pytest.raises(SQLSpecError, match="permission denied"):
        store.search_entries("budget", "app", "u1")
