"""Unit coverage for adapters that accept migration schemas as no-ops."""

import pytest

from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.adapters.asyncmy.driver import AsyncmyDriver
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core import StatementConfig


class FakeConnection:
    def adbc_get_info(self) -> dict[str, str]:
        return {"driver_name": "sql server"}


def test_sqlite_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    driver = SqliteDriver(object())  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")

    assert driver.has_schema("tenant") is True
    assert "SQLite driver does not support default schemas" in caplog.text


@pytest.mark.anyio
async def test_aiosqlite_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    driver = AiosqliteDriver(object())  # type: ignore[arg-type]

    await driver.set_migration_session_schema("tenant")

    assert await driver.has_schema("tenant") is True
    assert "aiosqlite driver does not support default schemas" in caplog.text


@pytest.mark.anyio
async def test_asyncmy_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    driver = AsyncmyDriver(object())  # type: ignore[arg-type]

    await driver.set_migration_session_schema("tenant")

    assert await driver.has_schema("tenant") is True
    assert "asyncmy driver does not support default schemas" in caplog.text


def test_adbc_sql_server_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    driver = AdbcDriver(
        FakeConnection(),  # type: ignore[arg-type]
        statement_config=StatementConfig(dialect="tsql"),
        driver_features={},
    )

    driver.set_migration_session_schema("tenant")

    assert driver.has_schema("tenant") is True
    assert "SQL Server schema support not yet implemented for ADBC" in caplog.text


def test_adbc_non_postgres_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    driver = AdbcDriver(
        FakeConnection(),  # type: ignore[arg-type]
        statement_config=StatementConfig(dialect="sqlite"),
        driver_features={},
    )

    driver.set_migration_session_schema("tenant")

    assert driver.has_schema("tenant") is True
    assert "ADBC driver does not support default schemas" in caplog.text
