"""Unit coverage for Oracle migration schema hooks."""

from typing import Any

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver


class FakeSyncCursor:
    def __init__(self, schema_exists: bool = True) -> None:
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    def fetchone(self) -> tuple[int] | None:
        return (1,) if self.schema_exists else None

    def close(self) -> None:
        return None


class FakeSyncConnection:
    def __init__(self, cursor: FakeSyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeSyncCursor:
        return self._cursor


class FakeAsyncCursor:
    def __init__(self, schema_exists: bool = True) -> None:
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, Any]] = []

    async def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    async def fetchone(self) -> tuple[int] | None:
        return (1,) if self.schema_exists else None

    def close(self) -> None:
        return None


class FakeAsyncConnection:
    def __init__(self, cursor: FakeAsyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeAsyncCursor:
        return self._cursor


def test_oracle_sync_migration_schema_hooks() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert cursor.executed == [
        ('ALTER SESSION SET CURRENT_SCHEMA = "TENANT"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "TENANT"}),
    ]
    assert OracleSyncConfig.supports_migration_schemas is True


def test_oracle_sync_preserves_quoted_identifier_case() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema('"mixedCase"')
    assert driver.has_schema('"mixedCase"') is True

    assert cursor.executed == [
        ('ALTER SESSION SET CURRENT_SCHEMA = "mixedCase"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "mixedCase"}),
    ]


@pytest.mark.anyio
async def test_oracle_async_migration_schema_hooks() -> None:
    cursor = FakeAsyncCursor()
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    await driver.set_migration_session_schema("tenant")
    assert await driver.has_schema("tenant") is True

    assert cursor.executed == [
        ('ALTER SESSION SET CURRENT_SCHEMA = "TENANT"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "TENANT"}),
    ]
    assert OracleAsyncConfig.supports_migration_schemas is True


@pytest.mark.anyio
async def test_oracle_async_has_schema_returns_false_for_missing_user() -> None:
    cursor = FakeAsyncCursor(schema_exists=False)
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    assert await driver.has_schema("missing") is False
