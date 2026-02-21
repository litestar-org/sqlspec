import sqlite3
from unittest.mock import Mock

from sqlspec.adapters.sqlite.core import resolve_rowcount
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core.result import DMLResult


def test_resolve_rowcount_fast_path() -> None:
    # Cursor with rowcount
    cursor = Mock()
    cursor.rowcount = 10

    # Should get 10
    assert resolve_rowcount(cursor) == 10


def test_resolve_rowcount_missing_attr() -> None:
    # Cursor without rowcount
    cursor = Mock(spec=[])  # No attributes

    # Should not crash, return 0
    assert resolve_rowcount(cursor) == 0


def test_resolve_rowcount_none_value() -> None:
    cursor = Mock()
    cursor.rowcount = None
    assert resolve_rowcount(cursor) == 0


def test_resolve_rowcount_negative() -> None:
    cursor = Mock()
    cursor.rowcount = -1
    assert resolve_rowcount(cursor) == 0


def test_sqlite_execute_many_thin_path_skips_dispatch() -> None:
    connection = sqlite3.connect(":memory:")
    driver = SqliteDriver(connection=connection)
    driver.execute("CREATE TABLE test_thin_path (value TEXT)")

    result = driver.execute_many("INSERT INTO test_thin_path (value) VALUES (?)", [("a",), ("b",)])

    assert isinstance(result, DMLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected >= 0
    assert connection.execute("SELECT COUNT(*) FROM test_thin_path").fetchone()[0] == 2
    connection.close()


def test_sqlite_execute_many_falls_back_when_coercion_required() -> None:
    connection = sqlite3.connect(":memory:")
    driver = SqliteDriver(connection=connection)
    driver.execute("CREATE TABLE test_fallback_path (flag INTEGER)")

    result = driver.execute_many("INSERT INTO test_fallback_path (flag) VALUES (?)", [(True,), (False,)])

    assert not isinstance(result, DMLResult)
    assert result.operation_type == "INSERT"
    assert connection.execute("SELECT COUNT(*) FROM test_fallback_path").fetchone()[0] == 2
    connection.close()
