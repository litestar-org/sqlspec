"""Unit coverage for mssql-python migration schema hooks."""

from typing import Any, cast
from unittest.mock import Mock

import pytest

from sqlspec.adapters.mssql_python.config import MssqlPythonConfig
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.adapters.pymssql.driver import PymssqlDriver


class FakeCursor:
    def __init__(self, current_schema: str = "dbo", schema_exists: bool = True) -> None:
        self.user_name = "sqlspec_migrator"
        self.current_schema = current_schema
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    def fetchone(self) -> tuple[Any, ...] | None:
        if not self.executed:
            return None
        last_sql = self.executed[-1][0]
        if "SCHEMA_NAME()" in last_sql:
            return (self.user_name, self.current_schema)
        if "FROM sys.schemas" in last_sql:
            return (1,) if self.schema_exists else None
        return None

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.commits = 0

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


def test_mssql_python_migration_schema_hooks() -> None:
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = MssqlPythonDriver(cast("Any", connection))

    driver.set_migration_session_schema("tenant")
    assert connection.commits == 0
    assert driver.has_schema("tenant") is True
    driver.reset_migration_session_schema()
    assert connection.commits == 1

    assert cursor.executed == [
        ("SELECT USER_NAME() AS user_name, SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant];", None),
        ("SELECT 1 FROM sys.schemas WHERE name = ?", ("tenant",)),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [dbo];", None),
    ]
    assert MssqlPythonConfig.supports_migration_schemas is True


def test_mssql_python_has_schema_returns_false_for_missing_schema() -> None:
    cursor = FakeCursor(schema_exists=False)
    driver = MssqlPythonDriver(cast("Any", FakeConnection(cursor)))

    assert driver.has_schema("missing") is False
    assert cursor.executed == [("SELECT 1 FROM sys.schemas WHERE name = ?", ("missing",))]


def test_mssql_python_migration_schema_escapes_bracket_identifier() -> None:
    cursor = FakeCursor()
    driver = MssqlPythonDriver(cast("Any", FakeConnection(cursor)))

    driver.set_migration_session_schema("tenant]s")
    assert cursor.executed == [
        ("SELECT USER_NAME() AS user_name, SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant]]s];", None),
    ]


def test_mssql_python_second_switch_reuses_captured_user_and_schema() -> None:
    cursor = FakeCursor(current_schema="sales")
    driver = MssqlPythonDriver(cast("Any", FakeConnection(cursor)))

    driver.set_migration_session_schema("tenant_a")
    driver.set_migration_session_schema("tenant_b")
    driver.reset_migration_session_schema()

    assert cursor.executed == [
        ("SELECT USER_NAME() AS user_name, SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant_a];", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant_b];", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [sales];", None),
    ]


def test_mssql_python_reset_without_set_is_noop() -> None:
    cursor = FakeCursor()
    driver = MssqlPythonDriver(cast("Any", FakeConnection(cursor)))

    driver.reset_migration_session_schema()
    assert cursor.executed == []


@pytest.mark.parametrize("driver_type", [MssqlPythonDriver, PymssqlDriver])
@pytest.mark.parametrize("failure_point", ["execute", "commit"])
def test_schema_restore_retries_original_schema_after_failure(
    driver_type: "type[MssqlPythonDriver | PymssqlDriver]", failure_point: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    cursor = FakeCursor(current_schema="sales")
    connection = FakeConnection(cursor)
    driver = driver_type(cast("Any", connection))
    driver.set_migration_session_schema("tenant")
    failure = RuntimeError("restore failed")
    target = cursor if failure_point == "execute" else connection
    original = getattr(target, failure_point)
    failing = Mock(side_effect=failure)
    monkeypatch.setattr(target, failure_point, failing)
    with pytest.raises(RuntimeError, match="restore failed") as caught:
        driver.reset_migration_session_schema()
    assert caught.value is failure
    monkeypatch.setattr(target, failure_point, original)
    cursor.user_name = "different_user"
    cursor.current_schema = "tenant"
    driver.reset_migration_session_schema()
    assert cursor.executed[-1] == ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [sales];", None)
    assert connection.commits == 1
    completed = list(cursor.executed)
    driver.reset_migration_session_schema()
    assert cursor.executed == completed
    assert connection.commits == 1
