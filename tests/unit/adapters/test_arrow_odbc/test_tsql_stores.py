"""Characterization tests for the SQL Server statements of the arrow-odbc extension stores."""

from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.adk.store import ArrowOdbcADKMemoryStore, ArrowOdbcADKStore
from sqlspec.adapters.arrow_odbc.events.store import ArrowOdbcEventQueueStore
from sqlspec.adapters.arrow_odbc.litestar import ArrowOdbcStore
from tests.unit.adapters.test_arrow_odbc._db2_fakes import (
    EMPTY_RESULT,
    ISO_TIMESTAMP,
    MSSQL_CONNECTION_STRING,
    FakeArrowOdbcConnection,
    FakeOdbcError,
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


def test_tsql_event_store_create_and_drop_statements() -> None:
    config = ArrowOdbcConfig(
        connection_config={"connection_string": MSSQL_CONNECTION_STRING},
        extension_config={"events": {"queue_table": "app_events"}},
    )
    store = ArrowOdbcEventQueueStore(config)

    assert store.create_statements() == [
        (
            "IF OBJECT_ID(N'[dbo].[app_events]', N'U') IS NULL BEGIN CREATE TABLE app_events (event_id NVARCHAR(64) "
            "PRIMARY KEY, channel NVARCHAR(128) NOT NULL, payload_json NVARCHAR(MAX) NOT NULL, metadata_json NVARCHAR(MAX), "
            "status NVARCHAR(32) NOT NULL DEFAULT 'pending', available_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(), "
            "lease_expires_at DATETIME2(6), attempts INT NOT NULL DEFAULT 0, created_at DATETIME2(6) NOT NULL DEFAULT "
            "SYSUTCDATETIME(), acknowledged_at DATETIME2(6)); END"
        ),
        (
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_app_events_channel_status' AND object_id = "
            "OBJECT_ID(N'[dbo].[app_events(channel,]')) BEGIN CREATE INDEX idx_app_events_channel_status ON "
            "app_events(channel, status, available_at); END"
        ),
    ]
    assert store.drop_statements() == ["IF OBJECT_ID(N'[dbo].[app_events]', N'U') IS NOT NULL DROP TABLE app_events;"]
    assert store._index_existence_target() is None  # pyright: ignore[reportPrivateUsage]


_ADK_TIME = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
_ADK_SESSION_ROW = pa.table({
    "id": ["s1"],
    "app_name": ["app"],
    "user_id": ["u"],
    "state": ['{"a": 1}'],
    "create_time": [_ADK_TIME.replace(tzinfo=None)],
    "update_time": [_ADK_TIME.replace(tzinfo=None)],
})
_ADK_RESPONDER = ScriptedResponder(
    ("row_count", pa.table({"row_count": [3]})),
    ("SELECT TOP 1 state", pa.table({"state": ['{"b": 2}']})),
    ("SELECT TOP 1 value", pa.table({"value": ["v"]})),
    ("SELECT TOP 1 id,", _ADK_SESSION_ROW),
    ("SELECT id, app_name, user_id, state", _ADK_SESSION_ROW),
)
_ADK_EVENT: "Any" = {
    "id": "e1",
    "app_name": "app",
    "user_id": "u",
    "session_id": "s1",
    "invocation_id": "i1",
    "timestamp": _ADK_TIME,
    "event_data": {"x": 1},
}
_TSQL_ADK_SESSION_CALLS = [
    (
        "INSERT INTO [dbo].[adk_session] ( id, app_name, user_id, [tenant_id], state, create_time, update_time ) VALUES (?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())",
        ["s1", "app", "u", "7", '{"a":1}'],
    ),
    (
        "SELECT TOP 1 id, app_name, user_id, state, create_time, update_time FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? AND id = ?",
        ["app", "u", "s1"],
    ),
    (
        "UPDATE [dbo].[adk_session] SET update_time = SYSUTCDATETIME() WHERE app_name = ? AND user_id = ? AND id = ?",
        ["app", "u", "s1"],
    ),
    (
        "SELECT TOP 1 id, app_name, user_id, state, create_time, update_time FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? AND id = ?",
        ["app", "u", "s1"],
    ),
    (
        "UPDATE [dbo].[adk_session] SET state = ?, update_time = SYSUTCDATETIME() WHERE app_name = ? AND user_id = ? AND id = ?",
        ['{"a":2}', "app", "u", "s1"],
    ),
    (
        "SELECT id, app_name, user_id, state, create_time, update_time FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? ORDER BY update_time DESC, id DESC OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
        ["app", "u"],
    ),
    ("DELETE FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? AND id = ?", ["app", "u", "s1"]),
    (
        "INSERT INTO [dbo].[adk_event] ( id, app_name, user_id, session_id, invocation_id, timestamp, event_data ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ["e1", "app", "u", "s1", "i1", "2026-01-02T03:04:05.678901", '{"x":1}'],
    ),
    (
        "UPDATE [dbo].[adk_session] SET state = ?, update_time = SYSUTCDATETIME() WHERE app_name = ? AND user_id = ? AND id = ?",
        ['{"a":3}', "app", "u", "s1"],
    ),
    (
        "SELECT TOP 1 id, app_name, user_id, state, create_time, update_time FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? AND id = ?",
        ["app", "u", "s1"],
    ),
    (
        "INSERT INTO [dbo].[adk_event] ( id, app_name, user_id, session_id, invocation_id, timestamp, event_data ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ["e1", "app", "u", "s1", "i1", "2026-01-02T03:04:05.678901", '{"x":1}'],
    ),
    (
        "MERGE INTO [dbo].[adk_app_state] WITH (HOLDLOCK) AS target USING (SELECT ? AS [app_name], ? AS state) AS source ON (target.[app_name] = source.[app_name]) WHEN MATCHED THEN UPDATE SET state = source.state, update_time = SYSUTCDATETIME() WHEN NOT MATCHED THEN INSERT ([app_name], [state], [update_time]) VALUES (source.[app_name], source.[state], SYSUTCDATETIME());",
        ["app", '{"g":1}'],
    ),
    (
        "MERGE INTO [dbo].[adk_user_state] WITH (HOLDLOCK) AS target USING (SELECT ? AS [app_name], ? AS [user_id], ? AS state) AS source ON (target.[app_name] = source.[app_name] AND target.[user_id] = source.[user_id]) WHEN MATCHED THEN UPDATE SET state = source.state, update_time = SYSUTCDATETIME() WHEN NOT MATCHED THEN INSERT ([app_name], [user_id], [state], [update_time]) VALUES (source.[app_name], source.[user_id], source.[state], SYSUTCDATETIME());",
        ["app", "u", '{"h":2}'],
    ),
    (
        "SELECT TOP 4 id, app_name, user_id, session_id, invocation_id, timestamp, event_data FROM [dbo].[adk_event] WHERE app_name = ? AND user_id = ? AND session_id = ? AND timestamp > ? ORDER BY timestamp ASC",
        ["app", "u", "s1", "2026-01-02T03:04:05.678901"],
    ),
    (
        "SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data FROM [dbo].[adk_event] WHERE app_name = ? AND user_id = ? AND session_id = ? ORDER BY timestamp ASC",
        ["app", "u", "s1"],
    ),
    (
        "SELECT COUNT(*) AS row_count FROM [dbo].[adk_event] WHERE timestamp < ? AND app_name = ?",
        ["2026-01-02T03:04:05.678901", "app"],
    ),
    ("DELETE FROM [dbo].[adk_event] WHERE timestamp < ? AND app_name = ?", ["2026-01-02T03:04:05.678901", "app"]),
    ("SELECT COUNT(*) AS row_count FROM [dbo].[adk_session] WHERE update_time < ?", ["2026-01-02T03:04:05.678901"]),
    ("DELETE FROM [dbo].[adk_session] WHERE update_time < ?", ["2026-01-02T03:04:05.678901"]),
    (
        "SELECT COUNT(*) AS row_count FROM [dbo].[adk_user_state] WHERE update_time < ? AND app_name = ?",
        ["2026-01-02T03:04:05.678901", "app"],
    ),
    (
        "DELETE FROM [dbo].[adk_user_state] WHERE update_time < ? AND app_name = ?",
        ["2026-01-02T03:04:05.678901", "app"],
    ),
    ("SELECT TOP 1 state FROM [dbo].[adk_app_state] WHERE app_name = ?", ["app"]),
    ("SELECT TOP 1 state FROM [dbo].[adk_user_state] WHERE app_name = ? AND user_id = ?", ["app", "u"]),
    (
        "MERGE INTO [dbo].[adk_app_state] WITH (HOLDLOCK) AS target USING (SELECT ? AS [app_name], ? AS state) AS source ON (target.[app_name] = source.[app_name]) WHEN MATCHED THEN UPDATE SET state = source.state, update_time = SYSUTCDATETIME() WHEN NOT MATCHED THEN INSERT ([app_name], [state], [update_time]) VALUES (source.[app_name], source.[state], SYSUTCDATETIME());",
        ["app", '{"g":2}'],
    ),
    (
        "MERGE INTO [dbo].[adk_user_state] WITH (HOLDLOCK) AS target USING (SELECT ? AS [app_name], ? AS [user_id], ? AS state) AS source ON (target.[app_name] = source.[app_name] AND target.[user_id] = source.[user_id]) WHEN MATCHED THEN UPDATE SET state = source.state, update_time = SYSUTCDATETIME() WHEN NOT MATCHED THEN INSERT ([app_name], [user_id], [state], [update_time]) VALUES (source.[app_name], source.[user_id], source.[state], SYSUTCDATETIME());",
        ["app", "u", '{"h":3}'],
    ),
    ("SELECT TOP 1 value FROM [dbo].[adk_internal_metadata] WHERE [key] = ?", ["k"]),
    (
        "MERGE INTO [dbo].[adk_internal_metadata] WITH (HOLDLOCK) AS target USING (SELECT ? AS [key], ? AS value) AS source ON (target.[key] = source.[key]) WHEN MATCHED THEN UPDATE SET value = source.value WHEN NOT MATCHED THEN INSERT ([key], value) VALUES (source.[key], source.value);",
        ["k", "v"],
    ),
]
_TSQL_ADK_DDL = [
    "CREATE TABLE [dbo].[adk_session] ( row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT [df_adk_session_row_id] DEFAULT NEWSEQUENTIALID(), id NVARCHAR(128) NOT NULL, app_name NVARCHAR(128) NOT NULL, user_id NVARCHAR(128) NOT NULL, tenant_id INTEGER, state NVARCHAR(MAX) NOT NULL, create_time DATETIME2(6) NOT NULL CONSTRAINT [df_adk_session_create_time] DEFAULT SYSUTCDATETIME(), update_time DATETIME2(6) NOT NULL CONSTRAINT [df_adk_session_update_time] DEFAULT SYSUTCDATETIME(), CONSTRAINT [pk_adk_session_row_id] PRIMARY KEY (row_id), CONSTRAINT [uq_adk_session_id] UNIQUE (id) )",
    "CREATE TABLE [dbo].[adk_event] ( row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT [df_adk_event_row_id] DEFAULT NEWSEQUENTIALID(), id NVARCHAR(128) NOT NULL, app_name NVARCHAR(128) NOT NULL, user_id NVARCHAR(128) NOT NULL, session_id NVARCHAR(128) NOT NULL, invocation_id NVARCHAR(256) NOT NULL, timestamp DATETIME2(6) NOT NULL, event_data NVARCHAR(MAX) NOT NULL, CONSTRAINT [pk_adk_event_row_id] PRIMARY KEY (row_id), CONSTRAINT [uq_adk_event_id] UNIQUE (id), CONSTRAINT [fk_adk_event_session] FOREIGN KEY (session_id) REFERENCES [dbo].[adk_session](id) ON DELETE CASCADE )",
    "CREATE TABLE [dbo].[adk_app_state] ( app_name NVARCHAR(128) NOT NULL, state NVARCHAR(MAX) NOT NULL, update_time DATETIME2(6) NOT NULL CONSTRAINT [df_adk_app_state_update_time] DEFAULT SYSUTCDATETIME(), CONSTRAINT [pk_adk_app_state_app_name] PRIMARY KEY (app_name) )",
    "CREATE TABLE [dbo].[adk_user_state] ( app_name NVARCHAR(128) NOT NULL, user_id NVARCHAR(128) NOT NULL, state NVARCHAR(MAX) NOT NULL, update_time DATETIME2(6) NOT NULL CONSTRAINT [df_adk_user_state_update_time] DEFAULT SYSUTCDATETIME(), CONSTRAINT [pk_adk_user_state_app_user] PRIMARY KEY (app_name, user_id) )",
    "CREATE TABLE [dbo].[adk_internal_metadata] ( [key] NVARCHAR(128) NOT NULL, value NVARCHAR(512) NOT NULL, CONSTRAINT [pk_adk_internal_metadata_key] PRIMARY KEY ([key]) )",
]
_TSQL_ADK_DROPS = [
    "DROP TABLE IF EXISTS [dbo].[adk_internal_metadata]",
    "DROP TABLE IF EXISTS [dbo].[adk_user_state]",
    "DROP TABLE IF EXISTS [dbo].[adk_app_state]",
    "DROP TABLE IF EXISTS [dbo].[adk_event]",
    "DROP TABLE IF EXISTS [dbo].[adk_session]",
]


def _tsql_adk_config(connection: FakeArrowOdbcConnection) -> ArrowOdbcConfig:
    return ArrowOdbcConfig(
        connection_config={"connection_string": MSSQL_CONNECTION_STRING},
        connection_instance=as_connection(connection),
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER"}},
    )


def test_tsql_adk_session_store_statements() -> None:
    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT, responder=_ADK_RESPONDER)
    store = ArrowOdbcADKStore(_tsql_adk_config(connection))

    store.create_session("s1", "app", "u", {"a": 1}, owner_id=7)
    store.get_session("app", "u", "s1", renew_for=60)
    store.update_session_state("app", "u", "s1", {"a": 2})
    store.list_sessions("app", "u", limit=5, offset=10)
    store.delete_session("app", "u", "s1")
    store.append_event(_ADK_EVENT)
    store.append_event_and_update_state(_ADK_EVENT, "app", "u", "s1", {"a": 3}, app_state={"g": 1}, user_state={"h": 2})
    store.get_events("app", "u", "s1", after_timestamp=_ADK_TIME, limit=4)
    store.get_events("app", "u", "s1")
    store.delete_expired_events(_ADK_TIME, app_name="app")
    store.delete_idle_sessions(_ADK_TIME)
    store.delete_idle_user_states(_ADK_TIME, app_name="app")
    store.get_app_state("app")
    store.get_user_state("app", "u")
    store.upsert_app_state("app", {"g": 2})
    store.upsert_user_state("app", "u", {"h": 3})
    store.get_metadata("k")
    store.set_metadata("k", "v")

    assert normalized_calls(connection) == _TSQL_ADK_SESSION_CALLS


def test_tsql_adk_session_store_ddl() -> None:
    store = ArrowOdbcADKStore(_tsql_adk_config(FakeArrowOdbcConnection()))

    ddls = [
        store._sessions_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._events_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._app_states_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._user_states_table_ddl(),  # pyright: ignore[reportPrivateUsage]
        store._metadata_table_ddl(),  # pyright: ignore[reportPrivateUsage]
    ]

    assert [" ".join(ddl.split()) for ddl in ddls] == _TSQL_ADK_DDL
    assert store._drop_tables_sql() == _TSQL_ADK_DROPS  # pyright: ignore[reportPrivateUsage]


def test_tsql_adk_missing_table_reads_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    error = FakeOdbcError(
        "State: 42S02, Native error: 208, Message: [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
        "Invalid object name 'dbo.adk_app_state'."
    )
    store = ArrowOdbcADKStore(_tsql_adk_config(FakeArrowOdbcConnection(error=error)))

    assert store.get_app_state("app") is None


_TSQL_ADK_MEMORY_CALLS = [
    ("SELECT TOP 1 id FROM [dbo].[adk_memory] WHERE event_id = ?", ["e1"]),
    (
        "INSERT INTO [dbo].[adk_memory] ( id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_json, content_text, metadata_json, inserted_at, [tenant_id] ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            "m1",
            "s1",
            "app",
            "u",
            "user",
            "e1",
            "a",
            "2026-01-02T03:04:05.678901",
            '{"t":1}',
            "hello",
            None,
            "2026-01-02T03:04:05.678901",
            "7",
        ],
    ),
    (
        "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_json, content_text, metadata_json, inserted_at FROM [dbo].[adk_memory] WHERE app_name = ? AND ((scope = 'user' AND user_id = ?) OR scope = 'app') AND content_text LIKE ? ORDER BY timestamp DESC OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY",
        ["app", "u", "%hello%"],
    ),
    (
        "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_json, content_text, metadata_json, inserted_at FROM [dbo].[adk_memory] WHERE app_name = ? AND scope = 'app' AND content_text LIKE ? ORDER BY timestamp DESC OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY",
        ["app", "%hello%"],
    ),
    ("SELECT COUNT(*) AS row_count FROM [dbo].[adk_memory] WHERE session_id = ?", ["s1"]),
    ("DELETE FROM [dbo].[adk_memory] WHERE session_id = ?", ["s1"]),
    (
        "SELECT COUNT(*) AS row_count FROM [dbo].[adk_memory] WHERE inserted_at < ? AND app_name = ? AND scope = ?",
        [ISO_TIMESTAMP, "app", "user"],
    ),
    (
        "DELETE FROM [dbo].[adk_memory] WHERE inserted_at < ? AND app_name = ? AND scope = ?",
        [ISO_TIMESTAMP, "app", "user"],
    ),
]
_TSQL_ADK_MEMORY_DDL = "CREATE TABLE [dbo].[adk_memory] ( id NVARCHAR(128) NOT NULL, session_id NVARCHAR(128) NOT NULL, app_name NVARCHAR(128) NOT NULL, user_id NVARCHAR(128) NOT NULL, scope NVARCHAR(16) NOT NULL DEFAULT 'user', event_id NVARCHAR(128) NOT NULL, author NVARCHAR(256) NULL, timestamp DATETIME2(6) NOT NULL, content_json NVARCHAR(MAX) NOT NULL, content_text NVARCHAR(MAX) NOT NULL, metadata_json NVARCHAR(MAX) NULL, inserted_at DATETIME2(6) NOT NULL, tenant_id INTEGER, CONSTRAINT [pk_adk_memory_id] PRIMARY KEY (id), CONSTRAINT [uq_adk_memory_event_id] UNIQUE (event_id) )"
_TSQL_ADK_MEMORY_DROPS = ["DROP TABLE IF EXISTS [dbo].[adk_memory]"]

_ADK_MEMORY_ENTRY: "Any" = {
    "id": "m1",
    "session_id": "s1",
    "app_name": "app",
    "user_id": "u",
    "scope": "user",
    "event_id": "e1",
    "author": "a",
    "timestamp": _ADK_TIME,
    "content_json": {"t": 1},
    "content_text": "hello",
    "metadata_json": None,
    "inserted_at": _ADK_TIME,
    "embedding": None,
}


def test_tsql_adk_memory_store_statements() -> None:
    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT, responder=_ADK_RESPONDER)
    store = ArrowOdbcADKMemoryStore(_tsql_adk_config(connection))

    store.insert_memory_entries([_ADK_MEMORY_ENTRY], owner_id=7)
    store.search_entries("hello", "app", "u", limit=3)
    store.search_entries("hello", "app", "u", scope_filter="app")
    store.delete_entries_by_session("s1")
    store.delete_entries_older_than(30, app_name="app", scope="user")

    assert normalized_calls(connection) == _TSQL_ADK_MEMORY_CALLS
    assert " ".join(store._memory_table_ddl().split()) == _TSQL_ADK_MEMORY_DDL  # pyright: ignore[reportPrivateUsage]
    assert store._drop_memory_table_sql() == _TSQL_ADK_MEMORY_DROPS  # pyright: ignore[reportPrivateUsage]


def test_tsql_adk_memory_create_tables_and_insert_without_owner() -> None:
    connection = FakeArrowOdbcConnection(result=EMPTY_RESULT)
    config = ArrowOdbcConfig(
        connection_config={"connection_string": MSSQL_CONNECTION_STRING}, connection_instance=as_connection(connection)
    )
    store = ArrowOdbcADKMemoryStore(config)

    store.create_tables()
    store.insert_memory_entries([_ADK_MEMORY_ENTRY])

    statements = [sql for sql, _ in normalized_calls(connection)]
    assert [sql.split(" (", 1)[0] for sql in statements if sql.startswith("CREATE")] == [
        "CREATE TABLE [dbo].[adk_memory]",
        "CREATE INDEX [idx_adk_memory_app_scope_user_time] ON [dbo].[adk_memory]",
        "CREATE INDEX [idx_adk_memory_scope] ON [dbo].[adk_memory]",
        "CREATE INDEX [idx_adk_memory_session] ON [dbo].[adk_memory]",
    ]
    assert statements[-1] == (
        "INSERT INTO [dbo].[adk_memory] ( id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
        "content_json, content_text, metadata_json, inserted_at ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
