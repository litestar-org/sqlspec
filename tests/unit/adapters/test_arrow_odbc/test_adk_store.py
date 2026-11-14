"""Unit tests for arrow-odbc ADK store data-dictionary existence checks."""

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.adk import ArrowOdbcADKMemoryStore, ArrowOdbcADKStore
from sqlspec.adapters.arrow_odbc.adk.store import _Db2AdkSql, _TsqlAdkSql
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError
from sqlspec.extensions.adk.migrations.reconcile import reconcile
from sqlspec.migrations.context import MigrationContext


def _mock_config(adk_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _store_with_driver() -> "tuple[ArrowOdbcADKStore, MagicMock, MagicMock]":
    config = _mock_config()
    store = ArrowOdbcADKStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    return store, driver, config


def _all_tables(store: ArrowOdbcADKStore) -> "list[dict[str, Any]]":
    names = [
        store._session_table,
        store._events_table,
        store._app_state_table,
        store._user_state_table,
        store._metadata_table,
    ]
    return [{"schema_name": "dbo", "table_name": name} for name in names]


def _all_indexes(store: ArrowOdbcADKStore) -> "list[dict[str, Any]]":
    session_indexes = [
        {
            "schema_name": "dbo",
            "table_name": store._session_table,
            "index_name": f"idx_{store._session_table}_app_user",
        },
        {
            "schema_name": "dbo",
            "table_name": store._session_table,
            "index_name": f"idx_{store._session_table}_update_time",
        },
    ]
    event_indexes = [
        {"schema_name": "dbo", "table_name": store._events_table, "index_name": f"idx_{store._events_table}_scope"},
        {"schema_name": "dbo", "table_name": store._events_table, "index_name": f"idx_{store._events_table}_session"},
        {
            "schema_name": "dbo",
            "table_name": store._events_table,
            "index_name": f"idx_{store._events_table}_invocation",
        },
        {"schema_name": "dbo", "table_name": store._events_table, "index_name": f"idx_{store._events_table}_timestamp"},
        {
            "schema_name": "dbo",
            "table_name": store._events_table,
            "index_name": f"idx_{store._events_table}_app_timestamp",
        },
    ]
    return [*session_indexes, *event_indexes]


def test_mssql_adk_table_existence_uses_data_dictionary() -> None:
    """create_tables consults data_dictionary.get_tables and skips present tables."""

    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = _all_indexes(store)

    store.create_tables()

    driver.data_dictionary.get_tables.assert_called_once()
    issued = " ".join(str(call.args[0]) for call in driver.execute.call_args_list)
    assert "CREATE TABLE" not in issued.upper()
    assert "CREATE INDEX" not in issued.upper()


def test_mssql_adk_create_issues_ddl_for_absent_objects() -> None:
    """create_tables issues CREATE TABLE/INDEX when the dictionary reports nothing."""

    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = []
    driver.data_dictionary.get_indexes.return_value = []

    store.create_tables()

    issued = " ".join(str(call.args[0]) for call in driver.execute.call_args_list).upper()
    assert "CREATE TABLE" in issued
    assert "CREATE INDEX" in issued
    assert "SYS.TABLES" not in issued
    assert "SYS.INDEXES" not in issued


def test_existence_checks_bounded_query_count() -> None:
    """N existence checks in one create pass trigger a single domain load each."""

    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = _all_indexes(store)

    store.create_tables()

    assert driver.data_dictionary.get_tables.call_count == 1
    assert driver.data_dictionary.get_indexes.call_count == 1


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_capture(
    monkeypatch: "pytest.MonkeyPatch",
) -> "tuple[ArrowOdbcADKStore, list[tuple[str, tuple[Any, ...]]]]":
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: Any, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    config = _mock_config()
    driver = config.provide_session.return_value.__enter__.return_value
    driver.select.side_effect = lambda sql, params=(): capture(None, sql, params)
    return ArrowOdbcADKStore(config), calls


def test_arrow_odbc_list_sessions_binds_order_and_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """SQL Server row limiting inlines the validated integer offset and row count."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY"
    )
    assert params == ("app", "u1")


def test_arrow_odbc_list_sessions_defaults_to_recent_first_without_a_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """The default listing keeps recent-first ordering and emits no row limiting."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app")

    sql, params = calls[0]
    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_arrow_odbc_list_sessions_pages_an_unfiltered_listing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Row limiting composes with an app-only listing."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", limit=5)

    sql, params = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY")
    assert params == ("app",)


def test_arrow_odbc_list_sessions_zero_limit_never_queries(monkeypatch: "pytest.MonkeyPatch") -> None:
    """A zero limit short-circuits before any database work."""
    store, calls = _session_store_with_capture(monkeypatch)

    assert store.list_sessions("app", limit=0) == []
    assert calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_arrow_odbc_list_sessions_rejects_invalid_options(
    monkeypatch: "pytest.MonkeyPatch", options: "dict[str, Any]"
) -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, calls = _session_store_with_capture(monkeypatch)

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []


def test_arrow_odbc_adk_store_requires_mssql_or_db2() -> None:
    """ArrowOdbcADKStore requires dialect mssql or db2."""
    config = MagicMock()
    config.statement_config.dialect = "sqlite"
    config.extension_config = {"adk": {}}

    with pytest.raises(ImproperConfigurationError, match="ArrowOdbcADKStore requires dialect"):
        ArrowOdbcADKStore(config)


def test_arrow_odbc_adk_memory_store_rejects_embedding() -> None:
    """ArrowOdbcADKMemoryStore rejects embeddings with NotImplementedError."""
    config = _mock_config({"enable_memory": True})
    config.statement_config.dialect = "mssql"
    mem_store = ArrowOdbcADKMemoryStore(config)

    with pytest.raises(NotImplementedError, match="Vector embeddings are not supported"):
        mem_store.search_entries("hello", "app", "user", embedding=[0.1, 0.2])


def test_arrow_odbc_create_tables_creates_index_owned_by_other_table() -> None:
    """Arrow ODBC ADK creates index when identical index name exists on another table."""
    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "dbo", "table_name": "other_table", "index_name": f"idx_{store._session_table}_app_user"}
    ]

    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert any(f"CREATE INDEX [idx_{store._session_table}_app_user]" in stmt for stmt in executed)


def test_arrow_odbc_create_tables_skips_index_on_exact_target() -> None:
    """Arrow ODBC ADK skips index on exact target case-insensitively."""
    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = [
        {
            "schema_name": "DBO",
            "table_name": store._session_table.upper(),
            "index_name": f"IDX_{store._session_table.upper()}_APP_USER",
        }
    ]

    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any(f"idx_{store._session_table}_app_user" in stmt for stmt in executed)
    assert any(f"idx_{store._session_table}_update_time" in stmt for stmt in executed)


def test_arrow_odbc_create_tables_uses_queried_schema_for_rows_without_schema() -> None:
    """Arrow ODBC ADK defaults schema to queried schema for catalog rows without schema_name."""
    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = [
        {"table_name": store._session_table, "index_name": f"idx_{store._session_table}_app_user"}
    ]

    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any(f"idx_{store._session_table}_app_user" in stmt for stmt in executed)


def test_arrow_odbc_create_tables_keeps_configured_schemas_distinct() -> None:
    """Arrow ODBC ADK keeps distinct schemas isolated in catalog checks."""
    config = _mock_config({"session_table": "app.sessions", "events_table": "audit.events"})
    store = ArrowOdbcADKStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    driver.data_dictionary.get_tables.return_value = [
        {"schema_name": "app", "table_name": "sessions"},
        {"schema_name": "audit", "table_name": "events"},
    ]

    def mock_get_indexes(_driver: Any, schema: str | None = None) -> list[dict[str, Any]]:
        if schema == "audit":
            return [{"schema_name": "audit", "table_name": "sessions", "index_name": "idx_sessions_app_user"}]
        return []

    driver.data_dictionary.get_indexes.side_effect = mock_get_indexes

    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert any("CREATE INDEX [idx_sessions_app_user] ON [app].[sessions]" in stmt for stmt in executed)


def test_arrow_odbc_bracketed_identifiers_normalize_in_helpers_and_catalog_rows() -> None:
    """Arrow ODBC ADK normalizes bracketed identifiers from catalog rows."""
    from sqlspec.adapters.arrow_odbc.adk.store import _split_tsql_table

    assert _split_tsql_table("[app].[sessions]") == ("app", "sessions")

    config = _mock_config({"session_table": "app.sessions"})
    store = ArrowOdbcADKStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    driver.data_dictionary.get_tables.return_value = [
        {"schema_name": "[app]", "table_name": "[sessions]"},
        {"schema_name": "dbo", "table_name": "adk_events"},
        {"schema_name": "dbo", "table_name": "adk_app_state"},
        {"schema_name": "dbo", "table_name": "adk_user_state"},
        {"schema_name": "dbo", "table_name": "adk_metadata"},
    ]
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "[app]", "table_name": "[sessions]", "index_name": "[idx_sessions_app_user]"}
    ]

    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any("idx_sessions_app_user" in stmt for stmt in executed)


def test_arrow_odbc_memory_create_tables_creates_index_owned_by_other_table() -> None:
    """Arrow ODBC memory store creates index when identical index name exists on another table."""
    config = _mock_config({"enable_memory": True})
    config.statement_config.dialect = "mssql"
    store = ArrowOdbcADKMemoryStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    driver.data_dictionary.get_tables.return_value = [{"schema_name": "dbo", "table_name": store._memory_table}]
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "dbo", "table_name": "other_table", "index_name": f"idx_{store._memory_table}_session"}
    ]

    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert any(f"CREATE INDEX [idx_{store._memory_table}_session]" in stmt for stmt in executed)


def test_arrow_odbc_companions_create_only_missing_index() -> None:
    """Arrow ODBC ADK companion reconciliation creates only absent indexes without committing."""
    store, driver, _ = _store_with_driver()
    missing_name = f"idx_{store._session_table}_update_time"
    driver.data_dictionary.get_indexes.return_value = [
        row for row in _all_indexes(store) if row["index_name"] != missing_name
    ]

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert executed == [f"CREATE INDEX [{missing_name}] ON [dbo].[{store._session_table}] (update_time DESC)"]
    assert driver.commit.call_count == 0


def test_arrow_odbc_companions_repeat_is_noop() -> None:
    """Arrow ODBC ADK companion reconciliation issues zero CREATE INDEX statements when complete."""
    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_indexes.return_value = _all_indexes(store)

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any("CREATE INDEX" in stmt for stmt in executed)
    assert executed == []
    assert driver.commit.call_count == 0


def test_arrow_odbc_companions_create_index_owned_by_other_table() -> None:
    """Arrow ODBC ADK companion reconciliation creates an index when another table owns the same name."""
    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "dbo", "table_name": "other_table", "index_name": f"idx_{store._session_table}_app_user"}
    ]

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert (
        f"CREATE INDEX [idx_{store._session_table}_app_user] ON [dbo].[{store._session_table}] (app_name, user_id)"
        in executed
    )
    assert driver.commit.call_count == 0


def test_arrow_odbc_memory_companions_target_two_part_table() -> None:
    """Arrow ODBC memory companion reconciliation creates all 4 indexes on a two-part table and skips when disabled."""
    config = _mock_config({"enable_memory": True, "memory_table": "app.mem"})
    config.statement_config.dialect = "mssql"
    store = ArrowOdbcADKMemoryStore(config)
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = []

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert executed == [
        "CREATE INDEX [idx_mem_app_scope_user_time] ON [app].[mem] (app_name, scope, user_id, timestamp DESC)",
        "CREATE INDEX [idx_mem_scope] ON [app].[mem] (app_name, scope)",
        "CREATE INDEX [idx_mem_session] ON [app].[mem] (session_id)",
        "CREATE INDEX [idx_mem_timestamp] ON [app].[mem] (timestamp DESC)",
    ]
    driver.data_dictionary.get_indexes.assert_called_once_with(driver, schema="app")
    assert driver.commit.call_count == 0

    disabled_config = _mock_config({"enable_memory": False})
    disabled_config.statement_config.dialect = "mssql"
    disabled_store = ArrowOdbcADKMemoryStore(disabled_config)
    disabled_driver = MagicMock()

    disabled_store.reconcile_companion_objects_sync(disabled_driver)

    disabled_driver.data_dictionary.get_indexes.assert_not_called()
    disabled_driver.execute.assert_not_called()


def test_arrow_odbc_reconcile_refuses_dialect_without_hooks() -> None:
    """Arrow ODBC ADK reconcile refuses dialects without store hooks before executing DDL."""
    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver=x"},
        statement_config=StatementConfig(dialect="postgres"),
        extension_config={"adk": {"migration_mode": "reconcile"}},
    )
    driver = MagicMock()

    with pytest.raises(MigrationError):
        reconcile(MigrationContext(config=config, driver=driver, dialect="postgres"))

    assert driver.execute.call_count == 0


@pytest.mark.parametrize(
    ("sql_builder", "events_ref", "session_ref", "memory_ref", "expected_constraint"),
    [
        pytest.param(
            _TsqlAdkSql(),
            "[dbo].[adk_event]",
            "[dbo].[adk_session]",
            "[dbo].[adk_memory]",
            "CONSTRAINT [uq_adk_memory_event_id] UNIQUE (app_name, user_id, event_id)",
            id="tsql",
        ),
        pytest.param(
            _Db2AdkSql(),
            "adk_event",
            "adk_session",
            "adk_memory",
            "CONSTRAINT uq_adk_memory_event UNIQUE (app_name, user_id, event_id)",
            id="db2",
        ),
    ],
)
def test_arrow_odbc_sql_builders_scope_events_and_memory(
    sql_builder: _TsqlAdkSql | _Db2AdkSql, events_ref: str, session_ref: str, memory_ref: str, expected_constraint: str
) -> None:
    """_TsqlAdkSql and _Db2AdkSql join sessions in events_query and use three-column memory deduplication."""
    cutoff = datetime(2026, 2, 1, 12, 0, tzinfo=UTC)
    events_sql, events_params = sql_builder.events_query("adk_event", "app", "u1", "s1", cutoff, 5, "adk_session")
    normalized_events = _normalized(events_sql)
    assert f"FROM {events_ref} e JOIN {session_ref} s" in normalized_events
    assert "ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id" in normalized_events
    assert "WHERE s.app_name = ? AND s.user_id = ? AND s.id = ? AND e.timestamp > ?" in normalized_events
    assert events_params[:3] == ("app", "u1", "s1")

    dup_sql = _normalized(sql_builder.memory_duplicate_sql("adk_memory"))
    assert f"FROM {memory_ref} WHERE app_name = ? AND user_id = ? AND event_id = ?" in dup_sql

    ddl = _normalized(sql_builder.memory_table_ddl("adk_memory", None))
    assert expected_constraint in ddl


@pytest.mark.parametrize(
    ("renew_for", "expected_updates", "expected_commits"),
    [
        pytest.param(None, 0, 0, id="none"),
        pytest.param(0, 0, 0, id="zero"),
        pytest.param(-1, 0, 0, id="negative"),
        pytest.param(timedelta(0), 0, 0, id="zero-timedelta"),
        pytest.param(30, 1, 1, id="positive"),
        pytest.param(timedelta(milliseconds=250), 1, 1, id="subsecond-positive"),
    ],
)
def test_arrow_odbc_get_session_gates_update_and_commit_on_should_renew(
    renew_for: int | timedelta | None, expected_updates: int, expected_commits: int
) -> None:
    """ArrowOdbcADKStore.get_session runs UPDATE and commit() only when _should_renew holds."""
    store, driver, _ = _store_with_driver()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    driver.select_one_or_none.return_value = {
        "id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "state": "{}",
        "create_time": now,
        "update_time": now,
    }

    result = store.get_session("app", "u1", "s1", renew_for=renew_for)
    assert result is not None
    assert driver.execute.call_count == expected_updates
    assert driver.commit.call_count == expected_commits


@pytest.mark.parametrize("session_exists", [True, False])
def test_arrow_odbc_append_event_and_scoped_memory_delete(session_exists: bool) -> None:
    """ArrowOdbcADKStore verifies session ownership in append_event and scopes memory deletes."""
    store, driver, config = _store_with_driver()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    driver.select_one_or_none.return_value = (
        {"id": "s1", "app_name": "app", "user_id": "u1", "state": "{}", "create_time": now, "update_time": now}
        if session_exists
        else None
    )
    event: Any = {
        "id": "e1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv1",
        "timestamp": now,
        "event_data": {"x": 1},
    }

    if session_exists:
        store.append_event(event)
        executed_sql, executed_params = driver.execute.call_args.args
        assert "FROM [dbo].[adk_session] WHERE id = ? AND app_name = ? AND user_id = ?" in _normalized(executed_sql)
        assert executed_params[-3:] == ("s1", "app", "u1")
    else:
        with pytest.raises(ValueError, match="Session s1 not found"):
            store.append_event(event)
        driver.execute.assert_not_called()

    config.provide_session.reset_mock()
    with pytest.raises(ValueError, match="does not match target"):
        store.append_event_and_update_state(event, "other_app", "u1", "s1", {})
    config.provide_session.assert_not_called()

    mem_store = ArrowOdbcADKMemoryStore(config)
    driver.select_value.return_value = 2
    driver.execute.reset_mock()
    deleted = mem_store.delete_entries_by_session("s1", app_name="app", user_id="u1")
    assert deleted == 2
    delete_sql, delete_params = driver.execute.call_args.args
    assert (
        _normalized(delete_sql)
        == "DELETE FROM [dbo].[adk_memory] WHERE session_id = ? AND app_name = ? AND user_id = ?"
    )
    assert delete_params == ("s1", "app", "u1")
