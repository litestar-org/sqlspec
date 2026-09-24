"""Unit tests for the Db2 variants of the arrow-odbc extension stores."""

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest
import sqlglot

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.adk.store import ArrowOdbcADKStore
from sqlspec.adapters.arrow_odbc.core import split_db2_name
from sqlspec.adapters.arrow_odbc.events.store import ArrowOdbcEventQueueStore
from sqlspec.adapters.arrow_odbc.litestar import ArrowOdbcStore
from sqlspec.migrations.schema import SchemaTarget
from tests.unit.adapters.test_arrow_odbc._db2_fakes import (
    DB2_CONNECTION_STRING,
    DB2_TIMESTAMP,
    EMPTY_RESULT,
    FakeArrowOdbcConnection,
    FakeOdbcError,
    ScriptedResponder,
    as_connection,
    db2_error_message,
    normalized_calls,
)

_FUTURE = datetime(2999, 1, 1)
_ONE_ROW = pa.table({"1": [1]})
_SESSION_COLUMNS = pa.table({
    name: pa.array([], pa.string()) for name in ("SESSION_ID", "DATA", "EXPIRES_AT", "CREATED_AT", "UPDATED_AT")
})
_SERVER_CLOCK_MARKERS = ("SYSUTCDATETIME", "CURRENT TIMESTAMP", "CURRENT_TIMESTAMP")


def _db2_config(connection: FakeArrowOdbcConnection) -> ArrowOdbcConfig:
    return ArrowOdbcConfig(
        connection_config={"connection_string": DB2_CONNECTION_STRING},
        connection_instance=as_connection(connection),
        extension_config={"litestar": {"session_table": "sess"}},
    )


def _session_store(connection: FakeArrowOdbcConnection) -> ArrowOdbcStore:
    return ArrowOdbcStore(_db2_config(connection))


@pytest.mark.parametrize(
    ("name", "expected"),
    [("sess", (None, "SESS")), ("app.sess", ("APP", "SESS")), ('"App"."MixedSess"', ("App", "MixedSess"))],
)
def test_db2_catalog_names_fold_unquoted_parts(name: str, expected: "tuple[str | None, str]") -> None:
    """Catalog probes use upper-folded names for unquoted parts and exact names for quoted parts."""
    assert split_db2_name(name) == expected


async def test_db2_litestar_create_table_probes_then_creates() -> None:
    """Only catalog objects that are missing are created, each by its own statement."""

    def responder(sql: str, parameters: "list[str | None] | None") -> "pa.Table | None":
        if "SYSCAT.TABLES" in sql and parameters == [None, "SESS"]:
            return _ONE_ROW
        if "WHERE 1=0" in sql:
            return _SESSION_COLUMNS
        return None

    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT, responder=responder)

    await _session_store(connection).create_table()

    calls = normalized_calls(connection)[:5]
    assert [calls[index][1] for index in (0, 1, 3)] == [[None, "SESS"], [None, "IX_SESS_EXP"], [None, "SESS_CHUNKS"]]
    assert "SYSCAT.TABLES" in calls[0][0] and "SYSCAT.INDEXES" in calls[1][0]
    assert calls[2] == ("CREATE INDEX IX_SESS_EXP ON sess(expires_at)", None)
    assert calls[3][0].startswith("SELECT 1 FROM SYSCAT.TABLES")
    assert calls[4][0].startswith("CREATE TABLE sess_chunks (")
    assert not any(sql.startswith(("ALTER", "CREATE TABLE sess (")) for sql, _ in normalized_calls(connection))
    assert connection.commit_calls == 1


async def test_db2_litestar_set_get_chunked_roundtrip() -> None:
    """A payload larger than one text chunk is split on write and reassembled on read."""
    payload = bytes(range(256)) * 20
    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT)
    store = _session_store(connection)

    await store.set("k", payload, 3600)

    chunk_inserts = [parameters for sql, parameters in connection.calls if "INSERT INTO sess_chunks" in sql]
    assert [chunk[:2] for chunk in chunk_inserts if chunk] == [["k", "0"], ["k", "1"]]
    connection.responder = ScriptedResponder(
        (
            "SELECT data, expires_at FROM sess",
            pa.table({"DATA": pa.array([None], pa.string()), "EXPIRES_AT": [_FUTURE]}),
        ),
        ("FROM sess_chunks", pa.table({"DATA": [chunk[2] for chunk in chunk_inserts if chunk]})),
    )

    assert await store.get("k") == payload


async def test_db2_litestar_binds_utc_times() -> None:
    """Every data statement binds naive-UTC timestamps instead of reading a server clock."""
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder(
            ("SELECT data, expires_at FROM sess", pa.table({"DATA": ["dg=="], "EXPIRES_AT": [_FUTURE]})),
            ("expired_count", pa.table({"EXPIRED_COUNT": [0]})),
        ),
    )
    store = _session_store(connection)

    await store.set("k", b"v", 60)
    await store.get("k", renew_for=60)
    await store.exists("k")
    await store.delete_expired()

    calls = normalized_calls(connection)
    assert not [sql for sql, _ in calls if any(marker in sql.upper() for marker in _SERVER_CLOCK_MARKERS)]
    assert calls[1] == (
        "INSERT INTO sess (session_id, data, expires_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        ["k", "dg==", DB2_TIMESTAMP, DB2_TIMESTAMP, DB2_TIMESTAMP],
    )
    assert calls[4] == (
        "UPDATE sess SET expires_at = ?, updated_at = ? WHERE session_id = ?",
        [DB2_TIMESTAMP, DB2_TIMESTAMP, "k"],
    )
    assert calls[5] == (
        "SELECT 1 AS exists_flag FROM sess WHERE session_id = ? AND (expires_at IS NULL OR expires_at > ?)",
        ["k", DB2_TIMESTAMP],
    )
    assert calls[6] == (
        "SELECT COUNT(*) AS expired_count FROM sess WHERE expires_at IS NOT NULL AND expires_at < ?",
        [DB2_TIMESTAMP],
    )


async def test_db2_litestar_update_binds_utc_times() -> None:
    """Updating an existing session binds the update time."""
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder(("SELECT session_id FROM sess", pa.table({"SESSION_ID": ["k"]}))),
    )

    await _session_store(connection).set("k", b"v")

    assert normalized_calls(connection)[1] == (
        "UPDATE sess SET data = ?, expires_at = ?, updated_at = ? WHERE session_id = ?",
        ["dg==", None, DB2_TIMESTAMP, "k"],
    )


def test_db2_litestar_ddl_parses_with_db2_dialect() -> None:
    """The Db2 DDL parses with the db2 dialect and yields the session table columns."""
    store = _session_store(FakeArrowOdbcConnection())
    ddl = store._table_ddl()  # pyright: ignore[reportPrivateUsage]

    statements = [statement for statement in sqlglot.parse(ddl, read="db2") if statement is not None]
    target = SchemaTarget.from_ddl("sess", ddl, dialect="db2")

    assert len(statements) == 3
    assert [column.name for column in target.create_table.columns] == [
        "session_id",
        "data",
        "expires_at",
        "created_at",
        "updated_at",
    ]
    assert "IF NOT EXISTS" not in ddl.upper()
    assert store._drop_table_sql() == ["DROP TABLE sess_chunks", "DROP TABLE sess"]  # pyright: ignore[reportPrivateUsage]


def _event_store(queue_table: str = "app_events") -> ArrowOdbcEventQueueStore:
    config = ArrowOdbcConfig(
        connection_config={"connection_string": DB2_CONNECTION_STRING},
        extension_config={"events": {"queue_table": queue_table}},
    )
    return ArrowOdbcEventQueueStore(config)


def test_db2_event_store_create_statements() -> None:
    """The Db2 queue DDL is plain CREATE TABLE/INDEX text that parses with the db2 dialect."""
    store = _event_store()

    statements = store.create_statements()

    assert statements == [
        (
            "CREATE TABLE app_events (event_id VARCHAR(64) NOT NULL PRIMARY KEY, channel VARCHAR(128) NOT NULL, "
            "payload_json CLOB NOT NULL, metadata_json CLOB, status VARCHAR(32) NOT NULL DEFAULT 'pending', "
            "available_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, lease_expires_at TIMESTAMP, "
            "attempts INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, "
            "acknowledged_at TIMESTAMP)"
        ),
        "CREATE INDEX idx_app_events_channel_status ON app_events(channel, status, available_at)",
    ]
    assert [sqlglot.parse_one(statement, read="db2").key for statement in statements] == ["create", "create"]
    assert store.drop_statements() == ["DROP TABLE app_events"]


@pytest.mark.parametrize(
    ("queue_table", "expected"), [("app_events", (None, "APP_EVENTS")), ("queue.app_events", ("QUEUE", "APP_EVENTS"))]
)
def test_db2_event_store_index_existence_target(queue_table: str, expected: "tuple[str | None, str]") -> None:
    """The index check targets the upper-folded catalog names of the unquoted queue table."""
    assert _event_store(queue_table)._index_existence_target() == expected  # pyright: ignore[reportPrivateUsage]


_EVENT_TIME = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
_DB2_SESSION_ROW = pa.table({
    "ID": ["s1"],
    "APP_NAME": ["app"],
    "USER_ID": ["u"],
    "STATE": ['{"a": 1}'],
    "CREATE_TIME": [_EVENT_TIME.replace(tzinfo=None)],
    "UPDATE_TIME": [_EVENT_TIME.replace(tzinfo=None)],
})
_DB2_EVENT_ROW = pa.table({
    "ID": ["e1"],
    "APP_NAME": ["app"],
    "USER_ID": ["u"],
    "SESSION_ID": ["s1"],
    "INVOCATION_ID": ["i1"],
    "TIMESTAMP": [_EVENT_TIME.replace(tzinfo=None)],
    "EVENT_DATA": ['{"x": 1}'],
})
_ADK_TABLES = ("ADK_SESSION", "ADK_EVENT", "ADK_APP_STATE", "ADK_USER_STATE", "ADK_INTERNAL_METADATA")


def _adk_store(connection: FakeArrowOdbcConnection, **adk_settings: Any) -> ArrowOdbcADKStore:
    config = ArrowOdbcConfig(
        connection_config={"connection_string": DB2_CONNECTION_STRING},
        connection_instance=as_connection(connection),
        extension_config={"adk": adk_settings},
    )
    return ArrowOdbcADKStore(config)


def _event_record() -> "Any":
    return {
        "id": "e1",
        "app_name": "app",
        "user_id": "u",
        "session_id": "s1",
        "invocation_id": "i1",
        "timestamp": _EVENT_TIME,
        "event_data": {"x": 1},
    }


def test_db2_adk_create_tables_probes_and_creates_missing() -> None:
    """Each ADK table and index is probed in SYSCAT and only missing objects are created."""

    def responder(sql: str, parameters: "list[str | None] | None") -> "pa.Table | None":
        if "SYSCAT.TABLES" in sql and parameters == [None, "ADK_SESSION"]:
            return _ONE_ROW
        if "SYSCAT.INDEXES" in sql and parameters == [None, "IDX_ADK_SESSION_APP_USER"]:
            return _ONE_ROW
        return None

    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT, responder=responder)

    _adk_store(connection).create_tables()

    calls = normalized_calls(connection)
    table_probes = [parameters for sql, parameters in calls if "SYSCAT.TABLES" in sql]
    index_probes = [parameters for sql, parameters in calls if "SYSCAT.INDEXES" in sql]
    created = [sql.split(" (", 1)[0] for sql, _ in calls if sql.startswith("CREATE")]
    assert table_probes == [[None, table] for table in _ADK_TABLES]
    assert len(index_probes) == 7
    assert created[:4] == [
        "CREATE TABLE adk_event",
        "CREATE TABLE adk_app_state",
        "CREATE TABLE adk_user_state",
        "CREATE TABLE adk_internal_metadata",
    ]
    assert "CREATE INDEX idx_adk_session_app_user ON adk_session" not in created
    assert len(created) == 4 + 6
    assert connection.commit_calls == 1


def test_db2_adk_session_roundtrip() -> None:
    """Sessions are created, read, updated and paged with Db2 statements and uppercase result keys."""
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT, responder=ScriptedResponder(("SELECT id, app_name, user_id, state", _DB2_SESSION_ROW))
    )
    store = _adk_store(connection, owner_id_column="tenant_id INTEGER")

    created = store.create_session("s1", "app", "u", {"a": 1}, owner_id=7)
    fetched = store.get_session("app", "u", "s1")
    store.update_session_state("app", "u", "s1", {"a": 2})
    listed = store.list_sessions("app", "u", limit=5, offset=10)

    assert created["id"] == "s1" and created["state"] == {"a": 1}
    assert fetched is not None and fetched["update_time"] == _EVENT_TIME
    assert [session["id"] for session in listed] == ["s1"]
    calls = normalized_calls(connection)
    assert calls[0] == (
        (
            "INSERT INTO adk_session ( id, app_name, user_id, tenant_id, state, create_time, update_time ) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ),
        ["s1", "app", "u", "7", '{"a":1}', DB2_TIMESTAMP, DB2_TIMESTAMP],
    )
    assert calls[1] == (
        (
            "SELECT id, app_name, user_id, state, create_time, update_time FROM adk_session "
            "WHERE app_name = ? AND user_id = ? AND id = ? FETCH FIRST 1 ROWS ONLY"
        ),
        ["app", "u", "s1"],
    )
    assert calls[3] == (
        "UPDATE adk_session SET state = ?, update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?",
        ['{"a":2}', DB2_TIMESTAMP, "app", "u", "s1"],
    )
    assert calls[4][0].endswith("ORDER BY update_time DESC, id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY")
    assert calls[4][1] == ["app", "u", "10", "5"]


def test_db2_adk_state_upsert_uses_merge_without_holdlock() -> None:
    """App, user and metadata upserts use a CAST-typed SYSDUMMY1 MERGE source with a bound UTC time."""
    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT)
    store = _adk_store(connection)

    store.upsert_app_state("app", {"g": 1})
    store.upsert_user_state("app", "u", {"h": 2})
    store.set_metadata("k", "v")

    calls = normalized_calls(connection)
    assert calls[0] == (
        (
            "MERGE INTO adk_app_state AS target USING (SELECT CAST(? AS VARCHAR(128)) AS app_name, "
            "CAST(? AS CLOB(1M)) AS state, CAST(? AS TIMESTAMP) AS now_utc FROM SYSIBM.SYSDUMMY1) AS source "
            "ON (target.app_name = source.app_name) WHEN MATCHED THEN UPDATE SET state = source.state, "
            "update_time = source.now_utc WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) "
            "VALUES (source.app_name, source.state, source.now_utc)"
        ),
        ["app", '{"g":1}', DB2_TIMESTAMP],
    )
    assert calls[1][1] == ["app", "u", '{"h":2}', DB2_TIMESTAMP]
    assert "ON (target.app_name = source.app_name AND target.user_id = source.user_id)" in calls[1][0]
    assert calls[2] == (
        (
            'MERGE INTO adk_internal_metadata AS target USING (SELECT CAST(? AS VARCHAR(128)) AS "KEY", '
            "CAST(? AS VARCHAR(512)) AS value FROM SYSIBM.SYSDUMMY1) AS source "
            'ON (target."KEY" = source."KEY") WHEN MATCHED THEN UPDATE SET value = source.value '
            'WHEN NOT MATCHED THEN INSERT ("KEY", value) VALUES (source."KEY", source.value)'
        ),
        ["k", "v"],
    )
    assert not [sql for sql, _ in calls if "HOLDLOCK" in sql]


def test_db2_adk_statements_use_fetch_first_not_top() -> None:
    """Single-row and limited reads use FETCH FIRST instead of TOP."""
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder(
            ("SELECT state FROM", pa.table({"STATE": ['{"g": 1}']})),
            ("SELECT value FROM", pa.table({"VALUE": ["v"]})),
            ("FROM adk_event", _DB2_EVENT_ROW),
        ),
    )
    store = _adk_store(connection)

    assert store.get_app_state("app") == {"g": 1}
    assert store.get_user_state("app", "u") == {"g": 1}
    assert store.get_metadata("k") == "v"
    events = store.get_events("app", "u", "s1", after_timestamp=_EVENT_TIME, limit=4)

    assert events[0]["event_data"] == {"x": 1}
    assert normalized_calls(connection) == [
        ("SELECT state FROM adk_app_state WHERE app_name = ? FETCH FIRST 1 ROWS ONLY", ["app"]),
        ("SELECT state FROM adk_user_state WHERE app_name = ? AND user_id = ? FETCH FIRST 1 ROWS ONLY", ["app", "u"]),
        ('SELECT value FROM adk_internal_metadata WHERE "KEY" = ? FETCH FIRST 1 ROWS ONLY', ["k"]),
        (
            (
                "SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data FROM adk_event "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND timestamp > ? "
                "ORDER BY timestamp ASC FETCH FIRST 4 ROWS ONLY"
            ),
            ["app", "u", "s1", "2026-01-02 03:04:05.678901"],
        ),
    ]


def test_db2_adk_binds_utc_times() -> None:
    """Session touches, event inserts and retention deletes bind naive-UTC Db2 timestamps."""
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder(
            ("row_count", pa.table({"ROW_COUNT": [2]})), ("SELECT id, app_name, user_id, state", _DB2_SESSION_ROW)
        ),
    )
    store = _adk_store(connection)

    store.get_session("app", "u", "s1", renew_for=60)
    store.append_event_and_update_state(_event_record(), "app", "u", "s1", {"a": 3}, app_state={"g": 1})
    deleted = store.delete_expired_events(_EVENT_TIME, app_name="app")

    calls = normalized_calls(connection)
    assert deleted == 2
    assert not [sql for sql, _ in calls if any(marker in sql.upper() for marker in _SERVER_CLOCK_MARKERS)]
    assert calls[0] == (
        "UPDATE adk_session SET update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?",
        [DB2_TIMESTAMP, "app", "u", "s1"],
    )
    assert calls[4] == (
        (
            "INSERT INTO adk_event ( id, app_name, user_id, session_id, invocation_id, timestamp, event_data ) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ),
        ["e1", "app", "u", "s1", "i1", "2026-01-02 03:04:05.678901", '{"x":1}'],
    )
    assert calls[-1] == (
        "DELETE FROM adk_event WHERE timestamp < ? AND app_name = ?",
        ["2026-01-02 03:04:05.678901", "app"],
    )


@pytest.mark.parametrize("sqlstate", ["42S02", "42704"])
def test_db2_adk_missing_table_detection_by_native_error(monkeypatch: pytest.MonkeyPatch, sqlstate: str) -> None:
    """An undefined-object error (native -204) reads as an empty store, whatever SQLSTATE the CLI reports."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    error = FakeOdbcError(db2_error_message(sqlstate, -204, 'SQL0204N "DB2INST1.ADK_SESSION" is an undefined name.'))
    store = _adk_store(FakeArrowOdbcConnection(error=error))

    assert store.get_session("app", "u", "s1") is None
    assert store.list_sessions("app") == []
    assert store.get_events("app", "u", "s1") == []
    assert store.get_app_state("app") is None
    assert store.get_metadata("k") is None
    assert store.delete_idle_sessions(_EVENT_TIME) == 0


def test_db2_adk_other_errors_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Errors other than an undefined object are raised."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    error = FakeOdbcError(
        db2_error_message("42501", -551, "SQL0551N The authorization ID does not have the privilege.")
    )
    store = _adk_store(FakeArrowOdbcConnection(error=error))

    with pytest.raises(Exception, match="SQL0551N"):
        store.get_session("app", "u", "s1")


def test_db2_adk_ddl_parses_with_db2_dialect() -> None:
    """Every ADK session-store DDL statement parses with the db2 dialect and declares NOT NULL keys."""
    store = _adk_store(FakeArrowOdbcConnection(), owner_id_column="tenant_id INTEGER")
    ddls = [
        store._sessions_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._events_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._app_states_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._user_states_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._metadata_table_ddl(),  # pyright: ignore[reportPrivateUsage]
    ]

    parsed = [sqlglot.parse_one(ddl, read="db2") for ddl in ddls]
    session_target = SchemaTarget.from_ddl("adk_session", ddls[0], dialect="db2")

    assert [statement.key for statement in parsed] == ["create"] * 5
    assert [column.name for column in session_target.create_table.columns] == [
        "id",
        "app_name",
        "user_id",
        "tenant_id",
        "state",
        "create_time",
        "update_time",
    ]
    assert all("NVARCHAR" not in ddl and "SYSUTCDATETIME" not in ddl for ddl in ddls)
    assert store._drop_tables_sql() == [  # pyright: ignore[reportPrivateUsage]
        "DROP TABLE adk_internal_metadata",
        "DROP TABLE adk_user_state",
        "DROP TABLE adk_app_state",
        "DROP TABLE adk_event",
        "DROP TABLE adk_session",
    ]
