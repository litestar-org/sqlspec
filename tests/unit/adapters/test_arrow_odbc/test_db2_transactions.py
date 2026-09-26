"""Unit tests for arrow-odbc Db2 transactions, savepoints, and bulk-load identifiers."""

from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver
from sqlspec.core import LimitOffsetFilter
from sqlspec.exceptions import ImproperConfigurationError
from tests.unit.adapters.test_arrow_odbc._db2_fakes import DB2_CONNECTION_STRING, FakeArrowOdbcConnection, as_connection

_CAPABILITIES: "dict[str, Any]" = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "partition_strategies": [],
}


def _db2_config(connection: FakeArrowOdbcConnection, **connection_config: Any) -> ArrowOdbcConfig:
    return ArrowOdbcConfig(
        connection_config={"connection_string": DB2_CONNECTION_STRING, **connection_config},
        connection_instance=as_connection(connection),
        driver_features={"storage_capabilities": _CAPABILITIES},
    )


def test_db2_begin_executes_no_sql_and_requires_autocommit_off() -> None:
    """Db2 begin() only marks the transaction and needs an autocommit-off connection."""
    connection = FakeArrowOdbcConnection()
    with _db2_config(connection, autocommit=False).provide_session() as driver:
        driver.begin()
        driver.execute("INSERT INTO orders (id) VALUES (?)", 1)
        driver.commit()

    assert connection.statements == ["INSERT INTO orders (id) VALUES (?)"]
    assert connection.commit_calls == 1

    autocommit_connection = FakeArrowOdbcConnection()
    with _db2_config(autocommit_connection).provide_session() as driver:
        with pytest.raises(ImproperConfigurationError, match="autocommit"):
            driver.begin()

    assert autocommit_connection.statements == []


def test_db2_savepoint_syntax() -> None:
    """Db2 savepoints retain cursors on rollback; release and rollback-to keep the standard form."""
    connection = FakeArrowOdbcConnection()
    with _db2_config(connection, autocommit=False).provide_session() as driver:
        driver.create_savepoint("sp1")
        driver.release_savepoint("sp1")
        driver.rollback_to_savepoint("sp1")

    assert connection.statements == [
        "SAVEPOINT sp1 ON ROLLBACK RETAIN CURSORS",
        "RELEASE SAVEPOINT sp1",
        "ROLLBACK TO SAVEPOINT sp1",
    ]


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("orders", "DELETE FROM orders"),
        ("sales.orders", "DELETE FROM sales.orders"),
        ('"MixedCase"', 'DELETE FROM "MixedCase"'),
        ('sales."order-lines"', 'DELETE FROM sales."order-lines"'),
    ],
)
def test_db2_load_from_arrow_overwrite_uses_folded_identifier(table: str, expected: str) -> None:
    """Db2 overwrite deletes fold unquoted names like the bulk insert target does."""
    connection = FakeArrowOdbcConnection()
    with _db2_config(connection).provide_session() as driver:
        driver.load_from_arrow(table, pa.table({"id": [1]}), overwrite=True)

    assert connection.statements == [expected]
    assert connection.inserts == [("from_table_to_db", table, 1)]


def test_mssql_load_from_arrow_overwrite_still_quotes() -> None:
    """SQL Server overwrite deletes keep quoting every identifier part."""
    connection = FakeArrowOdbcConnection()
    driver = ArrowOdbcDriver(
        as_connection(connection),
        driver_features={"dbms_name": "Microsoft SQL Server", "storage_capabilities": _CAPABILITIES},
    )

    driver.load_from_arrow("orders", pa.table({"id": [1]}), overwrite=True)

    assert connection.statements == ['DELETE FROM "orders"']


def test_db2_pagination_placeholders_are_not_inlined() -> None:
    """Db2 keeps OFFSET/FETCH placeholders bound instead of inlining them like SQL Server."""
    connection = FakeArrowOdbcConnection()
    with _db2_config(connection).provide_session() as driver:
        driver.execute("SELECT id, name FROM users", LimitOffsetFilter(limit=10, offset=5))

    sql, parameters = connection.calls[0]
    assert "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY" in sql
    assert parameters == ["5", "10"]
