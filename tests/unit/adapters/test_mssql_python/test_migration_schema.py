"""Unit coverage for mssql-python migration schema hooks."""

from typing import Any, cast

from sqlspec.adapters.mssql_python.config import MssqlPythonConfig
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver


class FakeCursor:
    def __init__(self, current_schema: str = "dbo", schema_exists: bool = True) -> None:
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
            return (self.current_schema,)
        if "FROM sys.schemas" in last_sql:
            return (1,) if self.schema_exists else None
        return None

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor


def test_mssql_python_migration_schema_hooks() -> None:
    cursor = FakeCursor()
    driver = MssqlPythonDriver(cast("Any", FakeConnection(cursor)))

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True
    driver.reset_migration_session_schema()

    assert cursor.executed == [
        ("SELECT SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER CURRENT_USER WITH DEFAULT_SCHEMA = [tenant];", None),
        ("SELECT 1 FROM sys.schemas WHERE name = ?", ("tenant",)),
        ("ALTER USER CURRENT_USER WITH DEFAULT_SCHEMA = [dbo];", None),
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
        ("SELECT SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER CURRENT_USER WITH DEFAULT_SCHEMA = [tenant]]s];", None),
    ]


def test_mssql_python_reset_without_set_is_noop() -> None:
    cursor = FakeCursor()
    driver = MssqlPythonDriver(cast("Any", FakeConnection(cursor)))

    driver.reset_migration_session_schema()
    assert cursor.executed == []
