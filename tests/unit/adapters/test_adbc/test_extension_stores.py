"""Unit tests for ADBC Litestar and Events extension stores across enterprise dialects."""

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

import pytest

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


@pytest.mark.parametrize("dialect", ["oracle", "db2", "mssql"])
def test_adbc_litestar_store_enterprise_set_uses_check_then_insert_or_update(dialect: str) -> None:
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _DriverStub:
        def select_one(self, sql: str, *params: Any) -> dict[str, Any]:
            executed.append((sql.strip(), params))
            return {"count": 0}

        def execute(self, sql: str, *params: Any) -> None:
            executed.append((sql.strip(), params))

        def commit(self) -> None:
            pass

    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(_DriverStub()))
    store = AdbcStore(cast("Any", config_stub))
    store._dialect = dialect
    store._set("sess_1", b"payload", expires_in=None)

    assert len(executed) == 2
    assert "SELECT COUNT(*)" in executed[0][0]
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


def test_adbc_litestar_store_concurrent_set_fallback() -> None:
    """AdbcStore._set catches IntegrityError on INSERT race condition and falls back to UPDATE."""
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _DriverStub:
        def select_one(self, sql: str, *params: Any) -> dict[str, Any]:
            executed.append((sql.strip(), params))
            return {"count": 0}

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
