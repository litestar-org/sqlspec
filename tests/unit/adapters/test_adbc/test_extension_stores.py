"""Unit tests for ADBC Litestar and Events extension stores across enterprise dialects."""

from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.adbc.adk.store import AdbcADKMemoryStore, AdbcADKStore, _adbc_atomic_connection
from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore
from sqlspec.adapters.adbc.litestar.store import AdbcStore
from sqlspec.core import StatementConfig
from sqlspec.exceptions import IntegrityError

__all__ = ()


@pytest.mark.parametrize(
    ("dialect", "expected_placeholder", "expected_ts_expr", "expected_ddl_fragment"),
    [
        ("oracle", ":1", "SYSTIMESTAMP AT TIME ZONE 'UTC'", "VARCHAR2(255) PRIMARY KEY"),
        ("db2", "?", "CURRENT TIMESTAMP", "VARCHAR(255) NOT NULL PRIMARY KEY"),
        ("mssql", "?", "SYSUTCDATETIME()", "NVARCHAR(255) PRIMARY KEY"),
        ("tsql", "?", "SYSUTCDATETIME()", "NVARCHAR(255) PRIMARY KEY"),
    ],
)
def test_adbc_litestar_store_enterprise_dialect_primitives(
    dialect: str, expected_placeholder: str, expected_ts_expr: str, expected_ddl_fragment: str
) -> None:
    store = AdbcStore(_make_adbc_config("sqlite:///:memory:"))
    store._dialect = "mssql" if dialect == "tsql" else dialect

    assert store._get_param_placeholder(1) == expected_placeholder
    assert store._get_current_timestamp_expr() == expected_ts_expr
    assert expected_ddl_fragment in store._table_ddl()


def test_adbc_litestar_store_normalize_tsql_dialect() -> None:
    driver_stub = SimpleNamespace(dialect="tsql")
    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(driver_stub))
    store = AdbcStore(cast("Any", config_stub))
    assert store._get_dialect() == "mssql"


@pytest.mark.parametrize(
    ("dialect", "expected_drop_fragment"),
    [("oracle", "SQLCODE != -942"), ("db2", "SYSCAT.TABLES"), ("mssql", "IF OBJECT_ID")],
)
def test_adbc_litestar_store_enterprise_drop_sql(dialect: str, expected_drop_fragment: str) -> None:
    store = AdbcStore(_make_adbc_config("sqlite:///:memory:"))
    store._dialect = dialect
    drop_sql = "\n".join(store._drop_table_sql())
    assert expected_drop_fragment in drop_sql


@pytest.mark.parametrize("dialect", ["oracle", "db2", "mssql", "teradata"])
def test_adbc_litestar_store_enterprise_set_uses_check_then_insert_or_update(dialect: str) -> None:
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _DriverStub:
        def select_one(self, sql: str, *params: Any) -> dict[str, Any]:
            executed.append((sql.strip(), params))
            return {"count": 0, "row_count": 0}

        def execute(self, sql: str, *params: Any) -> None:
            executed.append((sql.strip(), params))

        def commit(self) -> None:
            pass

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = dialect
    store._set("sess_1", b"payload", expires_in=None)

    assert len(executed) == 2
    assert f"SELECT COUNT(*) AS {'row_count' if dialect == 'teradata' else 'count'}" in executed[0][0]
    assert "INSERT INTO" in executed[1][0]
    assert "ON CONFLICT" not in executed[1][0]


@pytest.mark.parametrize(
    ("uri", "expected_dialect", "expected_ddl_fragment", "expected_drop_fragment"),
    [
        ("oracle://localhost:1521/FREEPDB1", "oracle", "SQLCODE != -955", "SQLCODE != -942"),
        ("db2://db2inst1:secret@localhost:50000/testdb", "db2", "SQLSTATE '42710'", "SQLSTATE '42704'"),
        ("sqlserver://sa:pass@localhost:1433?database=app", "mssql", "IF OBJECT_ID", "IF OBJECT_ID"),
    ],
)
def test_adbc_event_queue_store_enterprise_dialects(
    uri: str, expected_dialect: str, expected_ddl_fragment: str, expected_drop_fragment: str
) -> None:
    config = _make_adbc_config(uri)
    store = AdbcEventQueueStore(config)

    assert store.dialect == expected_dialect
    create_sql = "\n".join(store.create_statements())
    drop_sql = "\n".join(store.drop_statements())
    assert expected_ddl_fragment in create_sql
    assert expected_drop_fragment in drop_sql


@pytest.mark.parametrize("label", ["row_count", "ROW_COUNT"])
def test_adbc_litestar_store_delete_expired_reads_count_alias(label: str) -> None:
    executed: list[str] = []

    class _DriverStub:
        def select_one(self, sql: str, *params: Any) -> dict[str, Any]:
            executed.append(sql)
            return {label: 2}

        def execute(self, sql: str, *params: Any) -> None:
            executed.append(sql)

        def commit(self) -> None:
            pass

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = "teradata"
    assert store._delete_expired() == 2
    assert "COUNT(*) AS row_count" in executed[0]
    assert executed[1].startswith("DELETE FROM")


def test_adbc_litestar_store_concurrent_set_fallback() -> None:
    """AdbcStore._set catches IntegrityError on INSERT race condition and falls back to UPDATE."""
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _DriverStub:
        def select_one(self, sql: str, *params: Any) -> dict[str, Any]:
            executed.append((sql.strip(), params))
            return {"count": 0, "row_count": 0}

        def execute(self, sql: str, *params: Any) -> None:
            executed.append((sql.strip(), params))
            if "INSERT INTO" in sql:
                raise IntegrityError("duplicate key value violates unique constraint")

        def commit(self) -> None:
            pass

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = "oracle"
    store._set("sess_1", b"payload", expires_in=None)

    assert any("UPDATE" in item[0] for item in executed)


def test_adbc_litestar_store_rollback_on_failure() -> None:
    """Mutating operations on AdbcStore invoke rollback on execution error."""
    rolled_back = False

    class _DriverStub:
        def execute(self, _sql: str, *_params: Any) -> None:
            raise RuntimeError("disk failure")

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            nonlocal rolled_back
            rolled_back = True

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = "oracle"

    with pytest.raises(RuntimeError, match="disk failure"):
        store._delete("sess_1")

    assert rolled_back is True


def _make_adbc_config(dialect_uri: str) -> AdbcConfig:
    return AdbcConfig(connection_config={"uri": dialect_uri})


@pytest.mark.parametrize(
    "connection_config",
    [{"uri": "teradata://user:pass@host/database"}, {"uri": "grpc+tls://host:443", "adbc_dialect": "teradata"}],
)
def test_adbc_teradata_extension_targets(connection_config: dict[str, str]) -> None:
    config = AdbcConfig(connection_config=connection_config)
    store = AdbcStore(config)
    queue = AdbcEventQueueStore(config)
    session_target = store.get_schema_targets()[0]
    queue_target = queue.get_schema_targets()[0]
    assert str(session_target.create_statement).startswith("CREATE MULTISET TABLE")
    assert str(queue_target.create_statement).startswith("CREATE MULTISET TABLE")
    assert (
        store.get_companion_indexes()[0].ddl
        == "CREATE INDEX idx_litestar_session_expires_at (expires_at) ON litestar_session"
    )
    assert (
        queue.create_statements()[1]
        == "CREATE INDEX idx_sqlspec_event_queue_channel_status (channel, status, available_at) ON sqlspec_event_queue"
    )
    assert queue.drop_statements() == ["DROP TABLE sqlspec_event_queue"]


@pytest.mark.parametrize("stmt_dialect", ["teradata", "vantage"])
def test_adbc_event_queue_store_flightsql_statement_dialect_detection(stmt_dialect: str) -> None:
    """AdbcEventQueueStore resolves FlightSQL and mutated statement_config Teradata dialects to teradata."""
    flight_cfg = AdbcConfig(
        connection_config={"driver": "flightsql", "uri": "grpc+tls://host:443"},
        statement_config=StatementConfig(dialect=stmt_dialect),
    )
    assert AdbcEventQueueStore(flight_cfg).dialect == "teradata"

    fallback_stub = SimpleNamespace(
        connection_config={"driver": "flightsql", "uri": "grpc+tls://host:443"},
        statement_config=StatementConfig(dialect=stmt_dialect),
        extension_config={"events": {}},
    )
    assert AdbcEventQueueStore(cast("Any", fallback_stub)).dialect == "teradata"

    bare_stub = SimpleNamespace(
        connection_config={}, statement_config=StatementConfig(dialect=stmt_dialect), extension_config={"events": {}}
    )
    assert AdbcEventQueueStore(cast("Any", bare_stub)).dialect == "teradata"


def test_adbc_teradata_schema_qualified_extension_targets() -> None:
    """Teradata Litestar and Events stores split and normalize database-qualified table names."""
    config = AdbcConfig(
        connection_config={"uri": "teradata://user:pass@host/database"},
        extension_config={
            "litestar": {"session_table": "my_db.my_sessions"},
            "events": {"queue_table": "my_db.my_queue"},
        },
    )
    store = AdbcStore(config)
    queue = AdbcEventQueueStore(config)

    session_target = store.get_schema_targets()[0]
    assert session_target.schema == "MY_DB"
    assert session_target.table_name == "MY_SESSIONS"

    companion = store.get_companion_indexes()[0]
    assert companion.schema == "MY_DB"
    assert companion.table == "MY_SESSIONS"
    assert companion.ddl == "CREATE INDEX idx_my_db_my_sessions_expires_at (expires_at) ON my_db.my_sessions"

    queue_target = queue.get_schema_targets()[0]
    assert queue_target.schema == "MY_DB"
    assert queue_target.table_name == "MY_QUEUE"
    assert queue._index_existence_target() == ("MY_DB", "MY_QUEUE")


def test_adbc_litestar_store_teradata_create_table_commits_after_companion_reconcile() -> None:
    """AdbcStore._create_table commits companion index creation and rolls back on failure for Teradata."""
    events: list[str] = []

    class _DataDictionaryStub:
        def get_indexes(self, _driver: Any, *, table: str, schema: str | None = None) -> list[dict[str, Any]]:
            events.append(f"get_indexes:{schema}:{table}")
            return []

    class _DriverStub:
        dialect = "teradata"
        data_dictionary = _DataDictionaryStub()

        def select_one_or_none(self, _sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
            events.append(f"probe:{params[0]}:{params[1]}")
            return None

        def execute(self, sql: str) -> None:
            events.append(f"execute:{sql.split()[3]}")

        def execute_script(self, sql: str) -> None:
            events.append(f"execute_script:{sql.split()[2]}")

        def commit(self) -> None:
            events.append("commit")

        def rollback(self) -> None:
            events.append("rollback")

    driver_stub = _DriverStub()
    config_stub = SimpleNamespace(
        connection_config={"uri": "teradata://user:pass@host/database"},
        extension_config={"litestar": {"session_table": "my_db.my_sessions"}},
        provide_session=lambda: nullcontext(driver_stub),
    )
    store = AdbcStore(cast("Any", config_stub))
    store._create_table()

    assert events == [
        "probe:MY_DB:MY_SESSIONS",
        "execute:my_db.my_sessions",
        "commit",
        "get_indexes:MY_DB:MY_SESSIONS",
        "execute_script:idx_my_db_my_sessions_expires_at",
        "commit",
    ]


class _TeradataCatalogDriver:
    """Fake session driver whose DROP TABLE fails for absent tables, like Teradata error 3807."""

    dialect = "vantage"

    def __init__(self, tables: "set[str]") -> None:
        self.tables = tables
        self.dropped: list[str] = []

    def select_one_or_none(self, _sql: str, parameters: "tuple[str | None, str]") -> "dict[str, int] | None":
        return {"found": 1} if parameters[1] in self.tables else None

    def execute_script(self, statement: str) -> None:
        name = statement.removeprefix("DROP TABLE ").upper()
        if name not in self.tables:
            msg = f"[Error 3807] Object '{name}' does not exist."
            raise RuntimeError(msg)
        self.tables.discard(name)
        self.dropped.append(name)

    def commit(self) -> None:
        return None


def test_adbc_teradata_extension_downgrades_skip_tables_already_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.adbc.adk.store import AdbcADKMemoryStore, AdbcADKStore
    from sqlspec.extensions.adk.migrations.reconcile import _downgrade_sync as adk_downgrade
    from sqlspec.extensions.events.migrations.reconcile import _downgrade_sync as events_downgrade
    from sqlspec.extensions.litestar.migrations.reconcile import _downgrade_sync as litestar_downgrade

    config = AdbcConfig(
        connection_config={"uri": "teradata://user:pass@host/database"},
        extension_config={"adk": {"enable_memory": True}},
    )
    driver = _TeradataCatalogDriver({"ADK_SESSION", "ADK_EVENT", "ADK_MEMORY", "LITESTAR_SESSION"})
    monkeypatch.setattr(AdbcConfig, "provide_session", lambda _: nullcontext(driver))

    adk_stores = [AdbcADKStore(config), AdbcADKMemoryStore(config)]
    adk_downgrade(driver, adk_stores)
    adk_downgrade(driver, adk_stores)
    litestar_store = AdbcStore(config)
    assert litestar_store._get_dialect() == "teradata"
    litestar_downgrade(driver, litestar_store)
    litestar_downgrade(driver, litestar_store)
    events_downgrade(driver, AdbcEventQueueStore(config), True)
    assert driver.dropped == ["ADK_MEMORY", "ADK_EVENT", "ADK_SESSION", "LITESTAR_SESSION"]

    driver.tables.update({"ADK_SESSION", "ADK_MEMORY"})
    adk_stores[0].drop_tables()
    adk_stores[1].drop_tables()
    assert driver.dropped[-2:] == ["ADK_SESSION", "ADK_MEMORY"]
    assert driver.tables == set()


@pytest.mark.parametrize("uri", ["postgresql://user:pass@host/db", "sqlite:///:memory:", "duckdb:///:memory:"])
def test_adbc_litestar_store_rejects_qualified_session_table_outside_teradata(uri: str) -> None:
    config = AdbcConfig(
        connection_config={"uri": uri}, extension_config={"litestar": {"session_table": "app.sessions"}}
    )
    with pytest.raises(ValueError, match=r"app\.sessions"):
        AdbcStore(config)


@pytest.mark.parametrize("expires_in", [None, 60])
def test_adbc_litestar_store_snowflake_set_uses_atomic_hex_merge(expires_in: int | None) -> None:
    """Snowflake AdbcStore._set executes a single atomic MERGE with TO_BINARY(..., 'HEX')."""
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _DriverStub:
        def execute(self, sql: str, *params: Any) -> None:
            executed.append((" ".join(sql.split()), params))

        def commit(self) -> None:
            executed.append(("COMMIT", ()))

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = "snowflake"
    store._set("sess_sf", b"\xde\xad", expires_in=expires_in)

    assert len(executed) == 2
    merge_sql, merge_params = executed[0]
    assert merge_sql.startswith(
        "MERGE INTO litestar_session t USING (SELECT ? AS session_id, TO_BINARY(?, 'HEX') AS data"
    )
    assert merge_params[:2] == ("sess_sf", "dead")
    if expires_in is None:
        assert "NULL AS expires_at" in merge_sql
        assert len(merge_params) == 2
    else:
        assert "? AS expires_at" in merge_sql
        assert len(merge_params) == 3
    assert executed[1] == ("COMMIT", ())


@pytest.mark.parametrize(
    ("expires_in", "expect_future"),
    [
        pytest.param(timedelta(milliseconds=250), True, id="subsecond-timedelta"),
        pytest.param(0, False, id="zero-expires-immediately"),
        pytest.param(-3, False, id="negative-expires-immediately"),
    ],
)
def test_adbc_litestar_store_set_ttl_precision_and_immediate_expiry(
    expires_in: int | timedelta, expect_future: bool
) -> None:
    """AdbcStore._set preserves sub-second TTLs and writes past timestamps for non-positive expires_in."""
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _DriverStub:
        def execute(self, sql: str, *params: Any) -> None:
            executed.append((" ".join(sql.split()), params))

        def commit(self) -> None:
            pass

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = "postgresql"

    before = datetime.now(UTC)
    store._set("sess_1", b"payload", expires_in=expires_in)
    after = datetime.now(UTC)

    assert len(executed) == 1
    bound_expires_at = executed[0][1][2]
    assert isinstance(bound_expires_at, datetime)
    if expect_future:
        assert before + timedelta(milliseconds=200) <= bound_expires_at <= after + timedelta(milliseconds=300)
    else:
        assert before - timedelta(microseconds=1) <= bound_expires_at <= after - timedelta(microseconds=1)


@pytest.mark.parametrize(
    ("renew_for", "expected_executes", "expected_commits"),
    [
        pytest.param(None, 0, 0, id="none"),
        pytest.param(0, 0, 0, id="zero"),
        pytest.param(-1, 0, 0, id="negative"),
        pytest.param(timedelta(0), 0, 0, id="zero-timedelta"),
        pytest.param(30, 1, 1, id="positive"),
        pytest.param(timedelta(milliseconds=200), 1, 1, id="subsecond-positive"),
    ],
)
def test_adbc_litestar_store_get_renews_only_for_positive_duration(
    renew_for: int | timedelta | None, expected_executes: int, expected_commits: int
) -> None:
    """AdbcStore._get updates expires_at only when _should_renew(renew_for) holds."""
    executed: list[tuple[str, tuple[Any, ...]]] = []
    commits = 0
    future_time = datetime.now(UTC) + timedelta(seconds=120)

    class _DriverStub:
        def select_one_or_none(self, _sql: str, *_params: Any) -> dict[str, Any]:
            return {"data": b"payload", "expires_at": future_time}

        def execute(self, sql: str, *params: Any) -> None:
            executed.append((" ".join(sql.split()), params))

        def commit(self) -> None:
            nonlocal commits
            commits += 1

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = "postgresql"

    assert store._should_renew(renew_for) is (expected_executes == 1)
    assert store._calculate_expires_at(None) is None
    assert store._get("sess_1", renew_for=renew_for) == b"payload"
    assert len(executed) == expected_executes
    assert commits == expected_commits


@pytest.mark.parametrize(
    ("uri", "dialect", "expect_returning"),
    [
        ("postgresql://u:p@h/db", "postgresql", True),
        ("sqlite:///:memory:", "sqlite", True),
        ("duckdb:///:memory:", "duckdb", True),
        ("snowflake://u:p@acct/db/schema", "snowflake", False),
        ("oracle://localhost:1521/FREEPDB1", "oracle", False),
        ("db2://u:p@localhost:50000/testdb", "db2", False),
        ("sqlserver://sa:pass@localhost:1433?database=app", "mssql", False),
        ("teradata://u:p@host/db", "teradata", False),
    ],
)
def test_adbc_adk_generated_sql_scoping_across_dialects(
    monkeypatch: pytest.MonkeyPatch, uri: str, dialect: str, expect_returning: bool
) -> None:
    """ADBC ADK stores generate three-column session joins, ownership-checked event inserts, and composite memory DDL."""
    executed: list[tuple[str, tuple[Any, ...]]] = []
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = []
    cursor.rowcount = 1

    def _exec(sql: str, params: Any = ()) -> None:
        executed.append((" ".join(sql.split()), tuple(params)))

    cursor.execute.side_effect = _exec
    native = SimpleNamespace(
        get_option=lambda _: "false", set_autocommit=lambda _: None, commit=MagicMock(), rollback=MagicMock()
    )
    conn = SimpleNamespace(
        adbc_connection=native, _autocommit=False, cursor=lambda: cursor, commit=MagicMock(), rollback=MagicMock()
    )
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    cfg = AdbcConfig(connection_config={"uri": uri}, extension_config={"adk": {"enable_memory": True}})
    store = AdbcADKStore(cfg)
    mem_store = AdbcADKMemoryStore(cfg)
    assert store.dialect == dialect

    now = datetime(2026, 6, 1, tzinfo=UTC)
    event = {
        "id": "e1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv1",
        "timestamp": now,
        "event_data": {"x": 1},
    }
    store.append_event(cast("Any", event))
    insert_sql, insert_params = executed[-1]
    assert "FROM adk_session WHERE id =" in insert_sql
    assert ("RETURNING 1" in insert_sql) is expect_returning
    assert insert_params[-3:] == ("s1", "app", "u1")

    cursor.fetchone.return_value = None
    cursor.rowcount = 0
    with pytest.raises(ValueError, match="Session s1 not found"):
        store.append_event(cast("Any", event))

    executed.clear()
    with pytest.raises(ValueError, match="does not match target"):
        store.append_event_and_update_state(cast("Any", event), "other_app", "u1", "s1", {})
    assert executed == []

    executed.clear()
    store.get_events("app", "u1", "s1")
    events_sql, events_params = executed[-1]
    assert (
        "JOIN adk_session s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id" in events_sql
    )
    assert "s.app_name =" in events_sql and "s.user_id =" in events_sql and "s.id =" in events_sql
    assert events_params == ("app", "u1", "s1")

    mem_ddl = mem_store._memory_base_table_ddl()
    assert "UNIQUE (app_name, user_id, event_id)" in mem_ddl
    assert "event_id VARCHAR(128) NOT NULL UNIQUE" not in mem_ddl

    executed.clear()
    mem_store.delete_entries_by_session("s1", app_name="app", user_id="u1")
    del_sql, del_params = executed[-1]
    assert "DELETE FROM adk_memory WHERE session_id =" in del_sql
    assert "app_name =" in del_sql and "user_id =" in del_sql
    assert del_params == ("s1", "app", "u1")


@pytest.mark.parametrize(
    ("dialect", "expect_toggle"),
    [("duckdb", True), ("teradata", True), ("postgresql", False), ("sqlite", False), ("snowflake", False)],
)
def test_adbc_atomic_connection_toggles_autocommit_for_duckdb_and_teradata(
    monkeypatch: pytest.MonkeyPatch, dialect: str, expect_toggle: bool
) -> None:
    """_adbc_atomic_connection disables and restores autocommit for DuckDB and Teradata only."""
    toggles: list[bool] = []
    native_commits: list[str] = []
    conn_commits: list[str] = []
    cursor = MagicMock()
    native = SimpleNamespace(
        get_option=lambda _: "true",
        set_autocommit=lambda flag: toggles.append(flag),
        commit=lambda: native_commits.append("native_commit"),
        rollback=MagicMock(),
    )
    conn = SimpleNamespace(
        adbc_connection=native,
        cursor=lambda: cursor,
        commit=lambda: conn_commits.append("conn_commit"),
        rollback=MagicMock(),
    )
    cfg = AdbcConfig(connection_config={"uri": "duckdb:///:memory:"})
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    with _adbc_atomic_connection(cfg, dialect) as active_conn:
        active_conn.commit()

    if expect_toggle:
        assert toggles == [False, True]
        assert native_commits == ["native_commit"]
        assert conn_commits == []
    else:
        assert toggles == []
        assert conn_commits == ["conn_commit"]

    if dialect == "duckdb":
        native_commits.clear()
        toggles.clear()
        store = AdbcADKStore(cfg)
        store.delete_session("app", "u1", "s1")
        assert toggles == [False, True]
        assert native_commits == ["native_commit"]
