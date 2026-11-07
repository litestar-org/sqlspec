"""Unit coverage for DuckDB migration schema hooks."""

from typing import Any

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.duckdb.driver import DuckDBDriver


class FakeDuckDBConnection:
    def __init__(self, schema_exists: bool = True, search_path: str = "") -> None:
        self.schema_exists = schema_exists
        self.search_path = search_path
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> "FakeDuckDBConnection":
        self.executed.append((sql, parameters))
        return self

    def fetchone(self) -> tuple[Any] | None:
        if self.executed[-1][0] == "SELECT current_setting('search_path')":
            return (self.search_path,)
        return (1,) if self.schema_exists else None


CAPTURE = ("SELECT current_setting('search_path')", None)


def test_duckdb_migration_schema_hooks() -> None:
    connection = FakeDuckDBConnection()
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert connection.executed == [
        CAPTURE,
        ('SET search_path = "tenant"', None),
        ("SELECT 1 FROM information_schema.schemata WHERE schema_name = ?", ["tenant"]),
    ]
    assert DuckDBConfig.supports_migration_schemas is True


def test_duckdb_has_schema_returns_false_for_missing_schema() -> None:
    connection = FakeDuckDBConnection(schema_exists=False)
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    assert driver.has_schema("missing") is False


def test_duckdb_migration_schema_escapes_quoted_identifier() -> None:
    connection = FakeDuckDBConnection()
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    driver.set_migration_session_schema('tenant"s')

    assert connection.executed == [CAPTURE, ('SET search_path = "tenant""s"', None)]


@pytest.mark.parametrize(
    ("search_path", "expected_statement"),
    [("", "SET search_path = ''"), ("t,u", "SET search_path = 't,u'"), ("o'r", "SET search_path = 'o''r'")],
    ids=["empty", "list", "quote"],
)
def test_duckdb_reset_restores_captured_search_path(search_path: str, expected_statement: str) -> None:
    connection = FakeDuckDBConnection(search_path=search_path)
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    driver.reset_migration_session_schema()

    assert connection.executed[-1] == (expected_statement, None)


def test_duckdb_second_switch_keeps_first_capture() -> None:
    connection = FakeDuckDBConnection(search_path="a")
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    driver.set_migration_session_schema("b")
    driver.set_migration_session_schema("c")
    driver.reset_migration_session_schema()

    assert connection.executed.count(CAPTURE) == 1
    assert connection.executed[-1] == ("SET search_path = 'a'", None)


def test_duckdb_reset_without_switch_is_noop() -> None:
    connection = FakeDuckDBConnection()
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    driver.reset_migration_session_schema()
    assert connection.executed == []

    driver.set_migration_session_schema("tenant")
    driver.reset_migration_session_schema()
    statements_after_first_reset = list(connection.executed)
    driver.reset_migration_session_schema()

    assert connection.executed == statements_after_first_reset


def test_duckdb_failed_reset_keeps_capture_for_retry() -> None:
    connection = FakeDuckDBConnection(search_path="a")
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]
    driver.set_migration_session_schema("tenant")

    def fail_execute(_sql: str, _parameters: Any | None = None) -> FakeDuckDBConnection:
        msg = "connection lost"
        raise RuntimeError(msg)

    healthy_execute = connection.execute
    connection.execute = fail_execute  # type: ignore[method-assign]
    with pytest.raises(Exception, match="connection lost"):
        driver.reset_migration_session_schema()

    connection.execute = healthy_execute  # type: ignore[method-assign]
    driver.reset_migration_session_schema()

    assert connection.executed[-1] == ("SET search_path = 'a'", None)
