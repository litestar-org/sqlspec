"""Unit coverage for DuckDB migration schema hooks."""

from typing import Any

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.duckdb.driver import DuckDBDriver


class FakeDuckDBConnection:
    def __init__(self, schema_exists: bool = True) -> None:
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> "FakeDuckDBConnection":
        self.executed.append((sql, parameters))
        return self

    def fetchone(self) -> tuple[int] | None:
        return (1,) if self.schema_exists else None


def test_duckdb_migration_schema_hooks() -> None:
    connection = FakeDuckDBConnection()
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert connection.executed == [
        ("SET search_path = 'tenant'", None),
        ("SELECT 1 FROM information_schema.schemata WHERE schema_name = ?", ["tenant"]),
    ]
    assert DuckDBConfig.supports_migration_schemas is True


def test_duckdb_has_schema_returns_false_for_missing_schema() -> None:
    connection = FakeDuckDBConnection(schema_exists=False)
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    assert driver.has_schema("missing") is False


def test_duckdb_migration_schema_escapes_search_path_literal() -> None:
    connection = FakeDuckDBConnection()
    driver = DuckDBDriver(connection)  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant's")

    assert connection.executed == [("SET search_path = 'tenant''s'", None)]
