# pyright: reportPrivateUsage=false
"""Unit coverage for PostgreSQL migration schema hooks."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.migrations.commands import SyncMigrationCommands


class FakeAsyncpgConnection:
    def __init__(self, schema_exists: bool = True) -> None:
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, *parameters: Any) -> str:
        self.executed.append((sql, parameters))
        return "OK"

    async def fetchval(self, sql: str, *parameters: Any) -> int | None:
        self.executed.append((sql, parameters))
        return 1 if self.schema_exists else None


class FakePsqlpyConnection:
    def __init__(self, schema_exists: bool = True) -> None:
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, Any]] = []

    async def execute(self, sql: str, parameters: Any | None = None) -> str:
        self.executed.append((sql, parameters))
        return "OK"

    async def fetch(self, sql: str, parameters: Any | None = None) -> list[tuple[int]]:
        self.executed.append((sql, parameters))
        return [(1,)] if self.schema_exists else []


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


class FakePsycopgSyncConnection:
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

    async def close(self) -> None:
        return None


class FakePsycopgAsyncConnection:
    def __init__(self, cursor: FakeAsyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeAsyncCursor:
        return self._cursor


class FakeAdbcConnection:
    def __init__(self, cursor: FakeSyncCursor, *, dialect: str = "postgresql") -> None:
        self._cursor = cursor
        self._dialect = dialect

    def adbc_get_info(self) -> dict[str, str]:
        return {"vendor_name": self._dialect, "driver_name": self._dialect}

    def cursor(self) -> FakeSyncCursor:
        return self._cursor


@pytest.mark.anyio
async def test_asyncpg_migration_schema_hooks() -> None:
    connection = FakeAsyncpgConnection()
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    await driver.set_migration_session_schema('tenant_"one"')
    assert await driver.has_schema("tenant") is True

    assert connection.executed == [
        ('SET LOCAL search_path TO "tenant_""one""", "$user", public', ()),
        ("SELECT 1 FROM information_schema.schemata WHERE schema_name = $1", ("tenant",)),
    ]
    assert AsyncpgConfig.supports_migration_schemas is True


@pytest.mark.anyio
async def test_asyncpg_non_transactional_migration_schema_hooks() -> None:
    connection = FakeAsyncpgConnection()
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    await driver.set_migration_non_transactional_schema("tenant")
    await driver.reset_migration_session_schema()

    assert connection.executed == [('SET search_path TO "tenant", "$user", public', ()), ("RESET search_path", ())]


@pytest.mark.anyio
async def test_psqlpy_migration_schema_hooks() -> None:
    connection = FakePsqlpyConnection()
    driver = PsqlpyDriver(connection)  # type: ignore[arg-type]

    await driver.set_migration_session_schema("tenant")
    assert await driver.has_schema("missing") is True

    assert connection.executed == [
        ('SET LOCAL search_path TO "tenant", "$user", public', None),
        ("SELECT 1 FROM information_schema.schemata WHERE schema_name = $1", ["missing"]),
    ]
    assert PsqlpyConfig.supports_migration_schemas is True


@pytest.mark.anyio
async def test_psqlpy_non_transactional_migration_schema_hooks() -> None:
    connection = FakePsqlpyConnection()
    driver = PsqlpyDriver(connection)  # type: ignore[arg-type]

    await driver.set_migration_non_transactional_schema("tenant")
    await driver.reset_migration_session_schema()

    assert connection.executed == [('SET search_path TO "tenant", "$user", public', None), ("RESET search_path", None)]


@pytest.mark.anyio
async def test_psqlpy_has_schema_returns_false_for_empty_result() -> None:
    connection = FakePsqlpyConnection(schema_exists=False)
    driver = PsqlpyDriver(connection)  # type: ignore[arg-type]

    assert await driver.has_schema("missing") is False


def test_psycopg_sync_migration_schema_hooks() -> None:
    cursor = FakeSyncCursor()
    driver = PsycopgSyncDriver(FakePsycopgSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert cursor.executed == [
        ('SET LOCAL search_path TO "tenant", "$user", public', None),
        ("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", ("tenant",)),
    ]
    assert PsycopgSyncConfig.supports_migration_schemas is True


def test_psycopg_sync_non_transactional_migration_schema_hooks() -> None:
    cursor = FakeSyncCursor()
    driver = PsycopgSyncDriver(FakePsycopgSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_non_transactional_schema("tenant")
    driver.reset_migration_session_schema()

    assert cursor.executed == [('SET search_path TO "tenant", "$user", public', None), ("RESET search_path", None)]


@pytest.mark.anyio
async def test_psycopg_async_migration_schema_hooks() -> None:
    cursor = FakeAsyncCursor()
    driver = PsycopgAsyncDriver(FakePsycopgAsyncConnection(cursor))  # type: ignore[arg-type]

    await driver.set_migration_session_schema("tenant")
    assert await driver.has_schema("missing") is True

    assert cursor.executed == [
        ('SET LOCAL search_path TO "tenant", "$user", public', None),
        ("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", ("missing",)),
    ]
    assert PsycopgAsyncConfig.supports_migration_schemas is True


@pytest.mark.anyio
async def test_psycopg_async_non_transactional_migration_schema_hooks() -> None:
    cursor = FakeAsyncCursor()
    driver = PsycopgAsyncDriver(FakePsycopgAsyncConnection(cursor))  # type: ignore[arg-type]

    await driver.set_migration_non_transactional_schema("tenant")
    await driver.reset_migration_session_schema()

    assert cursor.executed == [('SET search_path TO "tenant", "$user", public', None), ("RESET search_path", None)]


def test_adbc_postgres_migration_schema_hooks() -> None:
    cursor = FakeSyncCursor()
    driver = AdbcDriver(FakeAdbcConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert cursor.executed == [
        ('SET search_path TO "tenant", "$user", public', None),
        ("SELECT 1 FROM information_schema.schemata WHERE schema_name = $1", ["tenant"]),
    ]


def test_adbc_postgres_non_transactional_migration_schema_hooks() -> None:
    cursor = FakeSyncCursor()
    driver = AdbcDriver(FakeAdbcConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_non_transactional_schema("tenant")
    driver.reset_migration_session_schema()

    assert cursor.executed == [('SET search_path TO "tenant", "$user", public', None), ("RESET search_path", None)]


def test_adbc_non_postgres_migration_schema_hooks_are_noops() -> None:
    cursor = FakeSyncCursor()
    driver = AdbcDriver(FakeAdbcConnection(cursor, dialect="sqlite"))  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert cursor.executed == []


def test_adbc_config_supports_migration_schemas_for_postgres_only() -> None:
    pg_config = AdbcConfig(connection_config={"uri": "postgresql://example.invalid/db"})
    sqlite_config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})

    assert pg_config.supports_migration_schemas is True
    assert sqlite_config.supports_migration_schemas is False


def test_adbc_postgres_cached_migration_commands_resolve_tracker_schema(tmp_path: Path) -> None:
    pg_config = AdbcConfig(
        connection_config={"uri": "postgresql://example.invalid/db"},
        migration_config={
            "script_location": str(tmp_path),
            "default_schema": "app_schema",
            "version_table_schema": "history_schema",
        },
    )

    commands = pg_config.get_migration_commands()

    assert isinstance(commands, SyncMigrationCommands)
    assert commands.tracker.version_table_schema == "history_schema"
    assert commands.tracker.version_table == "history_schema.ddl_migrations"
