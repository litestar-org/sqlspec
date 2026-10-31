import sqlite3
from unittest.mock import Mock

from sqlspec.adapters.sqlite.core import resolve_rowcount
from sqlspec.adapters.sqlite.driver import SqliteDriver


def test_resolve_rowcount_fast_path() -> None:
    cursor = Mock()
    cursor.rowcount = 10
    assert resolve_rowcount(cursor) == 10


def test_resolve_rowcount_missing_attr() -> None:
    cursor = Mock(spec=[])
    assert resolve_rowcount(cursor) == 0


def test_resolve_rowcount_none_value() -> None:
    cursor = Mock()
    cursor.rowcount = None
    assert resolve_rowcount(cursor) == 0


def test_resolve_rowcount_negative() -> None:
    cursor = Mock()
    cursor.rowcount = -1
    assert resolve_rowcount(cursor) == 0


def test_sqlite_execute_many_inserts_records() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        driver = SqliteDriver(connection=connection)
        driver.execute("CREATE TABLE test_many (value TEXT)")

        result = driver.execute_many("INSERT INTO test_many (value) VALUES (?)", [("a",), ("b",)])

        assert result.operation_type == "INSERT"
        assert result.rows_affected >= 0
        assert connection.execute("SELECT COUNT(*) FROM test_many").fetchone()[0] == 2
    finally:
        connection.close()


def test_sqlite_execute_many_with_coercion() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        driver = SqliteDriver(connection=connection)
        driver.execute("CREATE TABLE test_coercion (flag INTEGER)")

        result = driver.execute_many("INSERT INTO test_coercion (flag) VALUES (?)", [(True,), (False,)])

        assert result.operation_type == "INSERT"
        assert connection.execute("SELECT COUNT(*) FROM test_coercion").fetchone()[0] == 2
    finally:
        connection.close()


def test_sqlite_execute_many_with_subclass_coercion() -> None:
    import json
    from collections import defaultdict

    connection = sqlite3.connect(":memory:")
    try:
        driver = SqliteDriver(connection=connection)
        driver.execute("CREATE TABLE test_subclass_coercion (data TEXT)")

        result = driver.execute_many(
            "INSERT INTO test_subclass_coercion (data) VALUES (?)", [(defaultdict(int, a=1),), (defaultdict(int, b=2),)]
        )

        assert result.operation_type == "INSERT"
        rows = connection.execute("SELECT data FROM test_subclass_coercion ORDER BY data").fetchall()
        assert [json.loads(row[0]) for row in rows] == [{"a": 1}, {"b": 2}]
    finally:
        connection.close()
