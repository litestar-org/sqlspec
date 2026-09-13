"""Unit coverage for pymssql migration schema hooks."""

from typing import Any, cast

from sqlspec.adapters.pymssql.config import PymssqlConfig
from sqlspec.adapters.pymssql.driver import PymssqlDriver


class FakeCursor:
    def __init__(self, current_schema: str = "dbo", schema_exists: bool = True, as_dict: bool = False) -> None:
        self.user_name = "sqlspec_migrator"
        self.current_schema = current_schema
        self.schema_exists = schema_exists
        self.as_dict = as_dict
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    def fetchone(self) -> Any:
        if not self.executed:
            return None
        last_sql = self.executed[-1][0]
        if "SCHEMA_NAME()" in last_sql:
            if self.as_dict:
                return {"user_name": self.user_name, "schema_name": self.current_schema}
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


def test_pymssql_migration_schema_hooks() -> None:
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = PymssqlDriver(cast("Any", connection))

    driver.set_migration_session_schema("tenant")
    assert connection.commits == 0
    assert driver.has_schema("tenant") is True
    driver.reset_migration_session_schema()
    assert connection.commits == 1

    assert cursor.executed == [
        ("SELECT USER_NAME() AS user_name, SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant];", None),
        ("SELECT 1 FROM sys.schemas WHERE name = %s", ("tenant",)),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [dbo];", None),
    ]
    assert PymssqlConfig.supports_migration_schemas is True


def test_pymssql_has_schema_returns_false_for_missing_schema() -> None:
    cursor = FakeCursor(schema_exists=False)
    driver = PymssqlDriver(cast("Any", FakeConnection(cursor)))

    assert driver.has_schema("missing") is False
    assert cursor.executed == [("SELECT 1 FROM sys.schemas WHERE name = %s", ("missing",))]


def test_pymssql_migration_schema_escapes_bracket_identifier() -> None:
    cursor = FakeCursor()
    driver = PymssqlDriver(cast("Any", FakeConnection(cursor)))

    driver.set_migration_session_schema("tenant]s")
    assert cursor.executed == [
        ("SELECT USER_NAME() AS user_name, SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant]]s];", None),
    ]


def test_pymssql_migration_schema_restores_from_dict_rows() -> None:
    cursor = FakeCursor(current_schema="sales", as_dict=True)
    driver = PymssqlDriver(cast("Any", FakeConnection(cursor)))

    driver.set_migration_session_schema("tenant")
    driver.reset_migration_session_schema()

    assert cursor.executed[-1] == ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [sales];", None)


def test_pymssql_second_switch_reuses_captured_user_and_schema() -> None:
    cursor = FakeCursor(current_schema="sales")
    driver = PymssqlDriver(cast("Any", FakeConnection(cursor)))

    driver.set_migration_session_schema("tenant_a")
    driver.set_migration_session_schema("tenant_b")
    driver.reset_migration_session_schema()

    assert cursor.executed == [
        ("SELECT USER_NAME() AS user_name, SCHEMA_NAME() AS schema_name;", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant_a];", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [tenant_b];", None),
        ("ALTER USER [sqlspec_migrator] WITH DEFAULT_SCHEMA = [sales];", None),
    ]


def test_pymssql_reset_without_set_is_noop() -> None:
    cursor = FakeCursor()
    driver = PymssqlDriver(cast("Any", FakeConnection(cursor)))

    driver.reset_migration_session_schema()
    assert cursor.executed == []
