"""Unit tests for the Db2 ADK session and memory stores."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.db2.adk import Db2SyncADKMemoryStore, Db2SyncADKStore
from sqlspec.adapters.db2.adk.store import _datetime_value, _is_table_missing
from sqlspec.adapters.db2.core import INDEX_EXISTS_SQL, TABLE_EXISTS_SQL, create_mapped_exception
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.extensions.adk import StoredEvent
from sqlspec.extensions.adk.memory import StoredMemory
from tests.unit.adapters.test_db2._fakes import FakeDb2Cursor, FakeDb2SessionConfig, db2_description, db2_error

if TYPE_CHECKING:
    from sqlspec.adapters.db2.config import Db2SyncConfig

SESSION_COLUMNS = ("id", "app_name", "user_id", "state", "create_time", "update_time")
UNDEFINED_NAME_TEXT = '"DB2INST1.ADK_SESSION" is an undefined name.'


def _config(*cursors: FakeDb2Cursor) -> FakeDb2SessionConfig:
    return FakeDb2SessionConfig(cursors, extension_config={"adk": {}})


def _session_store(*cursors: FakeDb2Cursor) -> "tuple[Db2SyncADKStore, FakeDb2SessionConfig]":
    config = _config(*cursors)
    return Db2SyncADKStore(cast("Db2SyncConfig", config)), config


def _memory_store(*cursors: FakeDb2Cursor) -> "tuple[Db2SyncADKMemoryStore, FakeDb2SessionConfig]":
    config = _config(*cursors)
    return Db2SyncADKMemoryStore(cast("Db2SyncConfig", config)), config


def _session_row() -> FakeDb2Cursor:
    stored = datetime(2026, 9, 24, 12, 0, 0)
    return FakeDb2Cursor(
        rows=[("s1", "app", "u1", '{"step": 1}', stored, stored)], description=db2_description(*SESSION_COLUMNS)
    )


def _utc_naive_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _assert_naive_utc_near(value: Any, expected: datetime) -> None:
    assert isinstance(value, datetime)
    assert value.tzinfo is None
    assert abs((value - expected).total_seconds()) < 1


def _assert_no_server_clock(statements: "list[tuple[str, Any]]") -> None:
    assert all("CURRENT TIMESTAMP" not in sql.upper() for sql, _ in statements)


def test_create_session_binds_utc_times() -> None:
    """Session creation binds naive-UTC create and update times."""
    store, config = _session_store(FakeDb2Cursor(), _session_row())

    session = store.create_session("s1", "app", "u1", {"step": 1})

    insert_sql, insert_params = config.executed[0]
    _assert_no_server_clock(config.executed)
    assert insert_sql.split()[:3] == ["INSERT", "INTO", "adk_session"]
    assert insert_params[:4] == ("s1", "app", "u1", '{"step":1}')
    _assert_naive_utc_near(insert_params[4], _utc_naive_now())
    assert insert_params[5] == insert_params[4]
    assert session["create_time"] == datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
    assert session["state"] == {"step": 1}


def test_get_session_renewal_binds_utc_time() -> None:
    """Renewing a session on read binds a naive-UTC update time."""
    store, config = _session_store(FakeDb2Cursor(), _session_row())

    session = store.get_session("app", "u1", "s1", renew_for=60)

    touch_sql, touch_params = config.executed[0]
    _assert_no_server_clock(config.executed)
    assert touch_sql.split()[:2] == ["UPDATE", "adk_session"]
    _assert_naive_utc_near(touch_params[0], _utc_naive_now())
    assert touch_params[1:] == ("app", "u1", "s1")
    assert session is not None
    assert session["id"] == "s1"


def test_update_session_state_binds_utc() -> None:
    """State updates bind the new state and a naive-UTC update time."""
    store, config = _session_store()

    store.update_session_state("app", "u1", "s1", {"step": 2})

    [(sql, params)] = config.executed
    _assert_no_server_clock(config.executed)
    assert params[0] == '{"step":2}'
    _assert_naive_utc_near(params[1], _utc_naive_now())
    assert params[2:] == ("app", "u1", "s1")
    assert config.connection.commits == 1
    assert sql.split()[:2] == ["UPDATE", "adk_session"]


def test_append_event_and_update_state_binds_utc() -> None:
    """Appending an event binds naive-UTC state, event and scoped-state times."""
    store, config = _session_store(FakeDb2Cursor(), _session_row())
    event_time = datetime(2026, 9, 24, 14, 30, tzinfo=timezone(timedelta(hours=2)))
    event = StoredEvent(
        id="e1",
        app_name="app",
        user_id="u1",
        session_id="s1",
        invocation_id="i1",
        timestamp=event_time,
        event_data={"text": "hi"},
    )

    store.append_event_and_update_state(event, "app", "u1", "s1", {"step": 3}, app_state={"a": 1}, user_state={"u": 1})

    update, _, insert, app_merge, user_merge = config.executed
    _assert_no_server_clock(config.executed)
    _assert_naive_utc_near(update[1][1], _utc_naive_now())
    assert insert[1][5] == datetime(2026, 9, 24, 12, 30)
    assert app_merge[1][:2] == ("app", '{"a":1}')
    _assert_naive_utc_near(app_merge[1][2], _utc_naive_now())
    assert user_merge[1][:3] == ("app", "u1", '{"u":1}')
    _assert_naive_utc_near(user_merge[1][3], _utc_naive_now())
    assert config.connection.commits == 1


def test_upsert_app_state_binds_utc() -> None:
    """App-state upserts bind a naive-UTC update time and match on unquoted columns."""
    store, config = _session_store()

    store.upsert_app_state("app", {"a": 1})

    [(sql, params)] = config.executed
    _assert_no_server_clock(config.executed)
    assert '"' not in sql
    assert params[:2] == ("app", '{"a":1}')
    _assert_naive_utc_near(params[2], _utc_naive_now())


def test_metadata_upsert_quotes_only_key_column() -> None:
    """Metadata statements quote the reserved KEY column in its folded spelling only."""
    store, config = _session_store()

    store.set_metadata("schema_version", "2")

    [(sql, params)] = config.executed
    assert '"KEY"' in sql
    assert sql.replace('"KEY"', "").count('"') == 0
    assert params == ("schema_version", "2")


@pytest.mark.parametrize("value", [123, object(), None], ids=["int", "object", "none"])
def test_datetime_value_rejects_unknown_types(value: object) -> None:
    """Values that are neither datetimes nor ISO text raise instead of becoming the current time."""
    with pytest.raises(TypeError, match="Unsupported Db2 timestamp value"):
        _datetime_value(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2026, 1, 1, 12), datetime(2026, 1, 1, 12, tzinfo=timezone.utc)),
        (datetime(2026, 1, 1, 14, tzinfo=timezone(timedelta(hours=2))), datetime(2026, 1, 1, 12, tzinfo=timezone.utc)),
        ("2026-01-01T12:00:00", datetime(2026, 1, 1, 12, tzinfo=timezone.utc)),
        (b"2026-01-01T12:00:00Z", datetime(2026, 1, 1, 12, tzinfo=timezone.utc)),
    ],
    ids=["naive", "aware", "text", "bytes"],
)
def test_datetime_value_returns_aware_utc(value: object, expected: datetime) -> None:
    """Datetimes and ISO text decode to aware UTC datetimes."""
    assert _datetime_value(value) == expected


def test_tables_and_indexes_use_unquoted_identifiers() -> None:
    """Table and index names render unquoted; only the metadata KEY column is quoted."""
    store, _ = _session_store()

    table_ddls = [
        store._sessions_table_ddl(),
        store._events_table_ddl(),
        store._app_states_table_ddl(),
        store._user_states_table_ddl(),
    ]

    assert "CREATE TABLE adk_session (" in table_ddls[0]
    assert "REFERENCES adk_session(id)" in table_ddls[1]
    assert all('"' not in ddl for ddl in table_ddls)
    assert '"KEY" VARCHAR(128) NOT NULL' in store._metadata_table_ddl()
    assert 'PRIMARY KEY ("KEY")' in store._metadata_table_ddl()


def test_create_tables_probes_catalog_per_object() -> None:
    """Each table and index is probed by its upper-folded name; existing objects are skipped."""
    present = FakeDb2Cursor(rows=[(1,)], description=db2_description("1"))
    store, config = _session_store(present)

    store.create_tables()

    table_probes = [params for sql, params in config.executed if sql == TABLE_EXISTS_SQL]
    index_probes = [params for sql, params in config.executed if sql == INDEX_EXISTS_SQL]
    created = [sql.split("(")[0].split() for sql, _ in config.executed if sql.lstrip().upper().startswith("CREATE")]
    assert table_probes == [
        (None, "ADK_SESSION"),
        (None, "ADK_EVENT"),
        (None, "ADK_APP_STATE"),
        (None, "ADK_USER_STATE"),
        (None, "ADK_INTERNAL_METADATA"),
    ]
    assert index_probes[:2] == [(None, "IDX_ADK_SESSION_APP_USER"), (None, "IDX_ADK_SESSION_UPDATE_TIME")]
    assert len(index_probes) == 7
    assert ["CREATE", "TABLE", "adk_session"] not in created
    assert ["CREATE", "TABLE", "adk_event"] in created
    assert ["CREATE", "INDEX", "idx_adk_session_app_user", "ON", "adk_session"] in created
    assert config.connection.commits == 1


def test_memory_create_tables_probes_catalog() -> None:
    """The memory store probes its table and indexes by upper-folded name."""
    store, config = _memory_store()

    store.create_tables()

    probes = [params for sql, params in config.executed if sql in {TABLE_EXISTS_SQL, INDEX_EXISTS_SQL}]
    assert probes[0] == (None, store.memory_table.upper())
    assert all(name == name.upper() for _, name in probes)
    assert sum(1 for sql, _ in config.executed if sql.lstrip().upper().startswith("CREATE")) == 4


def test_missing_table_detected_by_sqlstate() -> None:
    """Only SQLSTATE 42704 marks a table as missing."""
    missing = create_mapped_exception(db2_error(-204, "42704", UNDEFINED_NAME_TEXT))
    syntax = create_mapped_exception(db2_error(-104, "42601", 'An unexpected token "undefined name" was found.'))

    assert isinstance(missing, SQLParsingError)
    assert isinstance(syntax, SQLParsingError)
    assert _is_table_missing(missing) is True
    assert _is_table_missing(syntax) is False


def test_get_session_returns_none_when_table_missing() -> None:
    """Reads against an absent session table return None; other errors propagate."""
    store, _ = _session_store(FakeDb2Cursor(error=db2_error(-204, "42704", UNDEFINED_NAME_TEXT)))
    assert store.get_session("app", "u1", "s1") is None

    failing, _ = _session_store(FakeDb2Cursor(error=db2_error(-104, "42601", "An unexpected token was found.")))
    with pytest.raises(SQLSpecError):
        failing.get_session("app", "u1", "s1")


def test_delete_entries_older_than_binds_cutoff() -> None:
    """Memory retention binds a naive-UTC cutoff computed from the day count."""
    count = FakeDb2Cursor(rows=[(2,)], description=db2_description("row_count"))
    store, config = _memory_store(count, FakeDb2Cursor(rowcount=2))

    assert store.delete_entries_older_than(30, app_name="app") == 2

    (count_sql, count_params), (delete_sql, delete_params) = config.executed
    _assert_naive_utc_near(count_params[0], _utc_naive_now() - timedelta(days=30))
    assert count_params[1] == "app"
    assert delete_params == count_params
    assert delete_sql.startswith(f"DELETE FROM {store.memory_table} WHERE inserted_at < ?")
    assert "COUNT(*)" in count_sql


def test_memory_store_search_query() -> None:
    """Memory search matches case-insensitively with POSSTR and caps rows with FETCH FIRST."""
    store, config = _memory_store()

    assert store.search_entries(query="hello", app_name="test_app", user_id="user_1") == []

    [(sql, params)] = config.executed
    assert "POSSTR(LOWER(content_text), LOWER(?)) > 0" in sql
    assert "FETCH FIRST 20 ROWS ONLY" in sql
    assert params == ("test_app", "user_1", "hello")


@pytest.mark.parametrize(
    ("method", "table_attr", "column"),
    [
        ("delete_expired_events", "events_table", "timestamp"),
        ("delete_idle_sessions", "session_table", "update_time"),
        ("delete_idle_user_states", "user_state_table", "update_time"),
    ],
)
def test_retention_deletes_bind_naive_utc_cutoff(method: str, table_attr: str, column: str) -> None:
    """Retention deletes bind the aware cutoff as naive UTC and report the counted rows."""
    count = FakeDb2Cursor(rows=[(4,)], description=db2_description("row_count"))
    store, config = _session_store(count)
    cutoff = datetime(2026, 9, 24, 14, tzinfo=timezone(timedelta(hours=2)))

    assert getattr(store, method)(cutoff, app_name="app") == 4

    (_, count_params), (delete_sql, delete_params) = config.executed
    table = getattr(store, table_attr)
    assert delete_sql == f"DELETE FROM {table} WHERE {column} < ? AND app_name = ?"
    assert count_params == delete_params == (datetime(2026, 9, 24, 12), "app")


@pytest.mark.parametrize(
    "method", ["delete_expired_events", "delete_idle_sessions", "delete_idle_user_states"], ids=str
)
def test_retention_deletes_on_missing_table_return_zero(method: str) -> None:
    """Retention deletes against an absent table report zero rows."""
    store, _ = _session_store(FakeDb2Cursor(error=db2_error(-204, "42704", UNDEFINED_NAME_TEXT)))

    assert getattr(store, method)(datetime(2026, 9, 24, tzinfo=timezone.utc)) == 0


def test_scoped_state_and_metadata_reads() -> None:
    """App state, user state and metadata reads decode their single-column rows."""
    store, config = _session_store(
        FakeDb2Cursor(rows=[('{"a": 1}',)], description=db2_description("state")),
        FakeDb2Cursor(rows=[('{"u": 2}',)], description=db2_description("state")),
        FakeDb2Cursor(rows=[("2",)], description=db2_description("value")),
    )

    assert store.get_app_state("app") == {"a": 1}
    assert store.get_user_state("app", "u1") == {"u": 2}
    assert store.get_metadata("schema_version") == "2"
    assert store.get_metadata("absent") is None
    assert [params for _, params in config.executed] == [("app",), ("app", "u1"), ("schema_version",), ("absent",)]
    assert config.executed[2][0].startswith(f'SELECT value FROM {store.metadata_table} WHERE "KEY" = ?')


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (lambda store: store.get_app_state("app"), None),
        (lambda store: store.get_user_state("app", "u1"), None),
        (lambda store: store.get_metadata("k"), None),
        (lambda store: store.list_sessions("app"), []),
        (lambda store: store.get_events("app", "u1", "s1"), []),
    ],
    ids=["app-state", "user-state", "metadata", "list-sessions", "events"],
)
def test_reads_on_missing_table_return_empty(call: Any, expected: Any) -> None:
    """Reads against an absent table return their empty result."""
    store, _ = _session_store(FakeDb2Cursor(error=db2_error(-204, "42704", UNDEFINED_NAME_TEXT)))

    assert call(store) == expected


def test_get_events_binds_naive_utc_after_timestamp() -> None:
    """Event reads bind the lower time bound as naive UTC and decode event rows."""
    stored = datetime(2026, 9, 24, 12, 0, 0)
    rows = FakeDb2Cursor(
        rows=[("e1", "app", "u1", "s1", "i1", stored, '{"text": "hi"}')],
        description=db2_description(
            "id", "app_name", "user_id", "session_id", "invocation_id", "timestamp", "event_data"
        ),
    )
    store, config = _session_store(rows)
    after = datetime(2026, 9, 24, 13, tzinfo=timezone(timedelta(hours=2)))

    [event] = store.get_events("app", "u1", "s1", after_timestamp=after, limit=5)

    [(sql, params)] = config.executed
    assert params == ("app", "u1", "s1", datetime(2026, 9, 24, 11))
    assert "FETCH FIRST 5 ROWS ONLY" in sql
    assert event["timestamp"] == datetime(2026, 9, 24, 12, tzinfo=timezone.utc)
    assert event["event_data"] == {"text": "hi"}


def test_session_list_delete_and_drop_use_unquoted_tables() -> None:
    """Listing, deleting and dropping sessions address the unquoted table names."""
    store, config = _session_store(_session_row())

    [session] = store.list_sessions("app", "u1", limit=10)
    store.delete_session("app", "u1", "s1")

    (list_sql, list_params), (delete_sql, delete_params) = config.executed
    assert f"FROM {store.session_table}\n" in list_sql
    assert list_params == ("app", "u1", 0, 10)
    assert delete_sql == f"DELETE FROM {store.session_table} WHERE app_name = ? AND user_id = ? AND id = ?"
    assert delete_params == ("app", "u1", "s1")
    assert session["update_time"] == datetime(2026, 9, 24, 12, tzinfo=timezone.utc)
    assert store._drop_tables_sql() == [
        f"DROP TABLE {store.metadata_table}",
        f"DROP TABLE {store.user_state_table}",
        f"DROP TABLE {store.app_state_table}",
        f"DROP TABLE {store.events_table}",
        f"DROP TABLE {store.session_table}",
    ]


def test_create_session_with_owner_column_binds_owner_unquoted() -> None:
    """A configured owner column is inserted unquoted, before the state and times."""
    config = FakeDb2SessionConfig(
        [FakeDb2Cursor(), _session_row()], extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NOT NULL"}}
    )
    store = Db2SyncADKStore(cast("Db2SyncConfig", config))

    store.create_session("s1", "app", "u1", {}, owner_id=7)

    insert_sql, insert_params = config.executed[0]
    assert "id, app_name, user_id, tenant_id, state, create_time, update_time" in insert_sql
    assert insert_params[:5] == ("s1", "app", "u1", 7, "{}")
    _assert_naive_utc_near(insert_params[5], _utc_naive_now())


def test_insert_memory_entries_binds_naive_utc_and_skips_duplicates() -> None:
    """Memory inserts bind naive-UTC times, include the owner column, and skip known event ids."""
    config = FakeDb2SessionConfig(
        [FakeDb2Cursor(rows=[("m0",)], description=db2_description("id"))],
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER"}},
    )
    store = Db2SyncADKMemoryStore(cast("Db2SyncConfig", config))
    stamp = datetime(2026, 9, 24, 14, tzinfo=timezone(timedelta(hours=2)))
    entries = [
        StoredMemory(
            id=f"m{index}",
            session_id="s1",
            app_name="app",
            user_id="u1",
            scope="user",
            event_id=f"e{index}",
            author="agent",
            timestamp=stamp,
            content_json={"text": "hi"},
            content_text="hi",
            metadata_json=None,
            inserted_at=stamp,
            embedding=None,
        )
        for index in range(2)
    ]

    assert store.insert_memory_entries(entries, owner_id=7) == 1

    insert_sql, insert_params = config.executed[2]
    assert f"INSERT INTO {store.memory_table} (" in insert_sql
    assert "inserted_at, tenant_id" in insert_sql
    assert insert_params[7] == datetime(2026, 9, 24, 12)
    assert insert_params[11] == datetime(2026, 9, 24, 12)
    assert insert_params[12] == 7


def test_memory_search_decodes_rows_and_session_delete_counts() -> None:
    """Search rows decode to UTC memory records; session deletes return the counted rows."""
    stored = datetime(2026, 9, 24, 12)
    row = ("m1", "s1", "app", "u1", "user", "e1", None, stored, '{"text": "hi"}', "hi", None, stored)
    columns = (
        "id",
        "session_id",
        "app_name",
        "user_id",
        "scope",
        "event_id",
        "author",
        "timestamp",
        "content_json",
        "content_text",
        "metadata_json",
        "inserted_at",
    )
    store, config = _memory_store(
        FakeDb2Cursor(rows=[row], description=db2_description(*columns)),
        FakeDb2Cursor(rows=[(1,)], description=db2_description("row_count")),
    )

    [memory] = store.search_entries(query="hi", app_name="app", user_id="u1", scope_filter="user")
    deleted = store.delete_entries_by_session("s1")

    assert memory["inserted_at"] == datetime(2026, 9, 24, 12, tzinfo=timezone.utc)
    assert memory["author"] is None
    assert memory["metadata_json"] is None
    assert deleted == 1
    assert config.executed[-1] == (f"DELETE FROM {store.memory_table} WHERE session_id = ?", ("s1",))
    assert store._drop_memory_table_sql() == [f"DROP TABLE {store.memory_table}"]
