"""Unit tests for the Db2 variants of the arrow-odbc extension stores."""

from datetime import datetime

import pyarrow as pa
import pytest
import sqlglot

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.core import split_db2_name
from sqlspec.adapters.arrow_odbc.events.store import ArrowOdbcEventQueueStore
from sqlspec.adapters.arrow_odbc.litestar import ArrowOdbcStore
from sqlspec.migrations.schema import SchemaTarget
from tests.unit.adapters.test_arrow_odbc._db2_fakes import (
    DB2_CONNECTION_STRING,
    DB2_TIMESTAMP,
    EMPTY_RESULT,
    FakeArrowOdbcConnection,
    ScriptedResponder,
    as_connection,
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
