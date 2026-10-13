"""Unit tests for ADBC Litestar and Events extension stores across enterprise dialects."""

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore
from sqlspec.adapters.adbc.litestar.store import ADBCStore

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
    store = ADBCStore(_make_adbc_config("sqlite:///:memory:"))
    store._dialect = "mssql" if dialect == "tsql" else dialect

    assert store._get_param_placeholder(1) == expected_placeholder
    assert store._get_current_timestamp_expr() == expected_ts_expr
    assert expected_ddl_fragment in store._table_ddl()


def test_adbc_litestar_store_normalize_tsql_dialect() -> None:
    driver_stub = SimpleNamespace(dialect="tsql")
    config_stub = SimpleNamespace(extension_config={"litestar": {}}, provide_session=lambda: nullcontext(driver_stub))
    store = ADBCStore(cast("Any", config_stub))
    assert store._get_dialect() == "mssql"


@pytest.mark.parametrize(
    ("dialect", "expected_drop_fragment"),
    [("oracle", "SQLCODE != -942"), ("db2", "SYSCAT.TABLES"), ("mssql", "IF OBJECT_ID")],
)
def test_adbc_litestar_store_enterprise_drop_sql(dialect: str, expected_drop_fragment: str) -> None:
    store = ADBCStore(_make_adbc_config("sqlite:///:memory:"))
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
    store = ADBCStore(cast("Any", config_stub))
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


def _make_adbc_config(dialect_uri: str) -> AdbcConfig:
    return AdbcConfig(connection_config={"uri": dialect_uri})
