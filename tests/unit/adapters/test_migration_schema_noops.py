"""Unit coverage for adapters that accept migration schemas as no-ops."""

import logging

import pytest

from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.adapters.asyncmy.driver import AsyncmyDriver
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core import StatementConfig


class FakeConnection:
    def adbc_get_info(self) -> dict[str, str]:
        return {"driver_name": "sql server"}


def _records(caplog: pytest.LogCaptureFixture, event: str) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.getMessage() == event]


def _extra(record: logging.LogRecord) -> dict[str, object]:
    return dict(getattr(record, "extra_fields", {}))


def test_sqlite_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    driver = SqliteDriver(object())  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    set_records = _records(caplog, "migration.schema.noop")
    validation_records = _records(caplog, "migration.schema.validation.noop")
    assert set_records, "expected migration.schema.noop event"
    assert validation_records, "expected migration.schema.validation.noop event"
    assert _extra(set_records[0]) == {"driver": "SqliteDriver", "schema": "tenant"}
    assert _extra(validation_records[0]) == {"driver": "SqliteDriver", "schema": "tenant"}


@pytest.mark.anyio
async def test_aiosqlite_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    driver = AiosqliteDriver(object())  # type: ignore[arg-type]

    await driver.set_migration_session_schema("tenant")
    assert await driver.has_schema("tenant") is True

    set_records = _records(caplog, "migration.schema.noop")
    validation_records = _records(caplog, "migration.schema.validation.noop")
    assert set_records, "expected migration.schema.noop event"
    assert validation_records, "expected migration.schema.validation.noop event"
    assert _extra(set_records[0]) == {"driver": "AiosqliteDriver", "schema": "tenant"}
    assert _extra(validation_records[0]) == {"driver": "AiosqliteDriver", "schema": "tenant"}


@pytest.mark.anyio
async def test_asyncmy_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    driver = AsyncmyDriver(object())  # type: ignore[arg-type]

    await driver.set_migration_session_schema("tenant")
    assert await driver.has_schema("tenant") is True

    set_records = _records(caplog, "migration.schema.noop")
    validation_records = _records(caplog, "migration.schema.validation.noop")
    assert set_records, "expected migration.schema.noop event"
    assert validation_records, "expected migration.schema.validation.noop event"
    assert _extra(set_records[0]) == {"driver": "AsyncmyDriver", "schema": "tenant"}
    assert _extra(validation_records[0]) == {"driver": "AsyncmyDriver", "schema": "tenant"}


def test_adbc_sql_server_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    driver = AdbcDriver(
        FakeConnection(),  # type: ignore[arg-type]
        statement_config=StatementConfig(dialect="tsql"),
        driver_features={},
    )

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    set_records = _records(caplog, "migration.schema.noop")
    validation_records = _records(caplog, "migration.schema.validation.noop")
    assert set_records, "expected migration.schema.noop event"
    assert validation_records, "expected migration.schema.validation.noop event"
    assert _extra(set_records[0]) == {"driver": "AdbcDriver", "schema": "tenant"}
    assert _extra(validation_records[0]) == {"driver": "AdbcDriver", "schema": "tenant"}


def test_adbc_non_postgres_migration_schema_noop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    driver = AdbcDriver(
        FakeConnection(),  # type: ignore[arg-type]
        statement_config=StatementConfig(dialect="sqlite"),
        driver_features={},
    )

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    set_records = _records(caplog, "migration.schema.noop")
    validation_records = _records(caplog, "migration.schema.validation.noop")
    assert set_records, "expected migration.schema.noop event"
    assert validation_records, "expected migration.schema.validation.noop event"
    assert _extra(set_records[0]) == {"driver": "AdbcDriver", "schema": "tenant"}
    assert _extra(validation_records[0]) == {"driver": "AdbcDriver", "schema": "tenant"}
