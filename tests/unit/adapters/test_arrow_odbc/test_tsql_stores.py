"""Characterization tests for the SQL Server statements of the arrow-odbc extension stores."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.litestar import ArrowOdbcStore
from tests.unit.adapters.test_arrow_odbc._db2_fakes import (
    EMPTY_RESULT,
    ISO_TIMESTAMP,
    MSSQL_CONNECTION_STRING,
    FakeArrowOdbcConnection,
    ScriptedResponder,
    as_connection,
    normalized_calls,
)

_SESSION_COLUMNS = pa.table({
    name: pa.array([], pa.string()) for name in ("session_id", "data", "expires_at", "created_at", "updated_at")
})
_TSQL_SESSION_DDL = (
    "IF NOT EXISTS ( SELECT 1 FROM sys.tables WHERE name = N'sess' AND schema_id = SCHEMA_ID(N'dbo') ) BEGIN "
    "CREATE TABLE sess ( session_id NVARCHAR(255) PRIMARY KEY, data NVARCHAR(MAX) NULL, expires_at DATETIME2(6) NULL, "
    "created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(), updated_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME() ); "
    "CREATE INDEX IX_sess_expires_at ON sess(expires_at) WHERE expires_at IS NOT NULL; END; "
    "IF NOT EXISTS ( SELECT 1 FROM sys.tables WHERE name = N'sess_chunks' AND schema_id = SCHEMA_ID(N'dbo') ) BEGIN "
    "CREATE TABLE sess_chunks ( session_id NVARCHAR(255) NOT NULL, chunk_index INT NOT NULL, data NVARCHAR(3500) NOT NULL, "
    "CONSTRAINT PK_sess_chunks PRIMARY KEY (session_id, chunk_index), CONSTRAINT FK_sess_chunks_session FOREIGN KEY (session_id) "
    "REFERENCES sess(session_id) ON DELETE CASCADE ); END;"
)
_FUTURE = datetime(2999, 1, 1)


def _session_store(connection: FakeArrowOdbcConnection) -> ArrowOdbcStore:
    config = ArrowOdbcConfig(
        connection_config={"connection_string": MSSQL_CONNECTION_STRING},
        connection_instance=as_connection(connection),
        extension_config={"litestar": {"session_table": "sess"}},
    )
    return ArrowOdbcStore(config)


async def test_tsql_session_create_table() -> None:
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT, responder=ScriptedResponder(("WHERE 1=0", _SESSION_COLUMNS))
    )

    await _session_store(connection).create_table()

    assert normalized_calls(connection)[0] == (_TSQL_SESSION_DDL, None)
    assert connection.commit_calls == 1


async def test_tsql_session_drop_statements() -> None:
    store = _session_store(FakeArrowOdbcConnection())

    assert store._drop_table_sql() == [  # pyright: ignore[reportPrivateUsage]
        "IF OBJECT_ID(N'dbo.sess_chunks', N'U') IS NOT NULL DROP TABLE dbo.sess_chunks;",
        "IF OBJECT_ID(N'dbo.sess', N'U') IS NOT NULL DROP TABLE dbo.sess;",
    ]


async def test_tsql_session_set_inline_inserts() -> None:
    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT)

    await _session_store(connection).set("k", b"v", 60)

    assert normalized_calls(connection) == [
        ("SELECT session_id FROM sess WHERE session_id = ?", ["k"]),
        ("INSERT INTO sess (session_id, data, expires_at) VALUES (?, ?, ?)", ["k", "dg==", ISO_TIMESTAMP]),
        ("DELETE FROM sess_chunks WHERE session_id = ?", ["k"]),
    ]
    assert connection.commit_calls == 1


async def test_tsql_session_set_chunked_updates_existing() -> None:
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder(("SELECT session_id FROM sess", pa.table({"session_id": ["k"]}))),
    )

    await _session_store(connection).set("k", b"x" * 3000)

    calls = normalized_calls(connection)
    assert [sql for sql, _ in calls] == [
        "SELECT session_id FROM sess WHERE session_id = ?",
        "UPDATE sess SET data = ?, expires_at = ?, updated_at = SYSUTCDATETIME() WHERE session_id = ?",
        "DELETE FROM sess_chunks WHERE session_id = ?",
        "INSERT INTO sess_chunks (session_id, chunk_index, data) VALUES (?, ?, ?)",
        "INSERT INTO sess_chunks (session_id, chunk_index, data) VALUES (?, ?, ?)",
    ]
    assert calls[1][1] == [None, None, "k"]
    assert [parameters[:2] for _, parameters in calls[3:] if parameters] == [["k", "0"], ["k", "1"]]
    assert [len(parameters[2] or "") for _, parameters in calls[3:] if parameters] == [3500, 500]


async def test_tsql_session_get_renews_expiry() -> None:
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder((
            "SELECT data, expires_at FROM sess",
            pa.table({"data": ["dg=="], "expires_at": [_FUTURE]}),
        )),
    )

    assert await _session_store(connection).get("k", renew_for=60) == b"v"

    assert normalized_calls(connection) == [
        ("SELECT data, expires_at FROM sess WHERE session_id = ?", ["k"]),
        ("UPDATE sess SET expires_at = ?, updated_at = SYSUTCDATETIME() WHERE session_id = ?", [ISO_TIMESTAMP, "k"]),
    ]


async def test_tsql_session_get_reassembles_chunks() -> None:
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder(
            (
                "SELECT data, expires_at FROM sess",
                pa.table({"data": pa.array([None], pa.string()), "expires_at": [_FUTURE]}),
            ),
            ("FROM sess_chunks", pa.table({"data": ["dg", "=="]})),
        ),
    )

    assert await _session_store(connection).get("k") == b"v"

    assert normalized_calls(connection)[1] == (
        "SELECT data FROM sess_chunks WHERE session_id = ? ORDER BY chunk_index",
        ["k"],
    )


async def test_tsql_session_delete_and_delete_all() -> None:
    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT)
    store = _session_store(connection)

    await store.delete("k")
    await store.delete_all()

    assert normalized_calls(connection) == [
        ("DELETE FROM sess WHERE session_id = ?", ["k"]),
        ("DELETE FROM sess", None),
    ]
    assert connection.commit_calls == 2


async def test_tsql_session_exists_and_expires_in() -> None:
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT,
        responder=ScriptedResponder(
            ("exists_flag", pa.table({"exists_flag": [1]})),
            (
                "SELECT expires_at FROM sess",
                pa.table({"expires_at": [datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1)]}),
            ),
        ),
    )
    store = _session_store(connection)

    assert await store.exists("k") is True
    remaining = await store.expires_in("k")

    assert remaining is not None and 3500 < remaining <= 3600
    assert normalized_calls(connection) == [
        (
            (
                "SELECT 1 AS exists_flag FROM sess WHERE session_id = ? "
                "AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())"
            ),
            ["k"],
        ),
        ("SELECT expires_at FROM sess WHERE session_id = ?", ["k"]),
    ]


async def test_tsql_session_delete_expired() -> None:
    connection = FakeArrowOdbcConnection(
        result=EMPTY_RESULT, responder=ScriptedResponder(("expired_count", pa.table({"expired_count": [2]})))
    )

    assert await _session_store(connection).delete_expired() == 2

    assert normalized_calls(connection) == [
        (
            "SELECT COUNT(*) AS expired_count FROM sess WHERE expires_at IS NOT NULL AND expires_at < SYSUTCDATETIME()",
            None,
        ),
        ("DELETE FROM sess WHERE expires_at IS NOT NULL AND expires_at < SYSUTCDATETIME()", None),
    ]
