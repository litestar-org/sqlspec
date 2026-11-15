"""Lease lock for backends without a native session lock."""

import logging
import sqlite3
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgSyncDriver
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker

GOOD_MIGRATION = (
    "-- name: migrate-0001-up\nCREATE TABLE items (id INTEGER);\n-- name: migrate-0001-down\nDROP TABLE items;\n"
)
FAILING_MIGRATION = "-- name: migrate-0002-up\nINSERT INTO nope VALUES (1);\n"


def _driver_with_dialect(dialect: str) -> Any:
    return type(f"_Fake{dialect.title()}Driver", (), {"dialect": dialect})()


def _duckdb_config(db_path: Path, migrations_dir: Path | None = None, **migration_config: Any) -> DuckDBConfig:
    config = {"enabled": True, **migration_config}
    if migrations_dir is not None:
        config["script_location"] = str(migrations_dir)
    return DuckDBConfig(connection_config={"database": str(db_path)}, migration_config=config)


def _lock_rows(db_path: Path, table: str = "ddl_migrations_lock") -> list[tuple[Any, ...]]:
    connection = duckdb.connect(str(db_path))
    try:
        return connection.execute(f"SELECT lock_name, owner_token, expires_at FROM {table}").fetchall()
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("oracle", "lease"),
        ("duckdb", "lease"),
        ("spanner", "lease"),
        ("db2", "lease"),
        ("sybase", "lease"),
        ("ase", "lease"),
        ("sqlanywhere", "lease"),
        ("teradata", "lease"),
        ("vantage", "lease"),
        ("bigquery", "unsupported"),
        ("snowflake", "unsupported"),
        ("unknownsql", ""),
        ("postgres", "postgres"),
        ("mysql", "mysql"),
        ("tsql", "mssql"),
        ("sqlite", "sqlite"),
    ],
)
def test_lock_backend_kind_matrix(dialect: str, expected: str) -> None:
    assert SyncMigrationTracker._resolve_lock_backend(_driver_with_dialect(dialect)) == expected  # pyright: ignore[reportPrivateUsage]


def test_lock_backend_kind_cockroach_and_bridge_sqlite() -> None:
    cockroach = CockroachPsycopgSyncDriver(MagicMock())
    bridge_sqlite = AdbcDriver(MagicMock(), dialect="sqlite")

    assert SyncMigrationTracker._resolve_lock_backend(cockroach) == "lease"  # pyright: ignore[reportPrivateUsage]
    assert SyncMigrationTracker._resolve_lock_backend(bridge_sqlite) == "lease"  # pyright: ignore[reportPrivateUsage]


def test_lease_lock_blocks_second_migrator_duckdb(tmp_path: Path) -> None:
    db_path = tmp_path / "lease.duckdb"
    first_config, second_config = _duckdb_config(db_path), _duckdb_config(db_path)
    try:
        with first_config.provide_session() as first, second_config.provide_session() as second:
            first_tracker, second_tracker = SyncMigrationTracker(), SyncMigrationTracker()
            with first_tracker.acquire_lock(first):
                with pytest.raises(MigrationError, match="another migrator holds the lease"):
                    with second_tracker.acquire_lock(second, timeout_seconds=1):
                        pass
            with second_tracker.acquire_lock(second, timeout_seconds=1):
                pass
    finally:
        first_config.close_pool()
        second_config.close_pool()


def test_lease_lock_takes_over_expired_lease(tmp_path: Path) -> None:
    db_path = tmp_path / "stale.duckdb"
    config = _duckdb_config(db_path)
    tracker = SyncMigrationTracker()
    try:
        with config.provide_session() as driver:
            with tracker.acquire_lock(driver):
                pass
            driver.execute("INSERT INTO ddl_migrations_lock VALUES ('sqlspec_mig_ddl_migrations', 'stale', 0)")
            with tracker.acquire_lock(driver):
                held = driver.select("SELECT owner_token FROM ddl_migrations_lock")
                assert [row["owner_token"] for row in held] != ["stale"]
                assert len(held) == 1
    finally:
        config.close_pool()


def test_lease_released_after_command(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_items.sql").write_text(GOOD_MIGRATION, encoding="utf-8")
    db_path = tmp_path / "cmd.duckdb"
    config = _duckdb_config(db_path, migrations_dir)
    commands = SyncMigrationCommands(config)
    try:
        commands.upgrade(echo=False)
        assert _lock_rows_via(config) == []
        (migrations_dir / "0002_bad.sql").write_text(FAILING_MIGRATION, encoding="utf-8")
        with pytest.raises(MigrationError):
            commands.upgrade(echo=False)
        assert _lock_rows_via(config) == []
    finally:
        config.close_pool()


def _lock_rows_via(config: DuckDBConfig) -> list[Any]:
    with config.provide_session() as driver:
        return list(driver.select("SELECT lock_name FROM ddl_migrations_lock"))


def test_renew_lock_raises_after_takeover(tmp_path: Path) -> None:
    config = _duckdb_config(tmp_path / "renew.duckdb")
    tracker = SyncMigrationTracker()
    try:
        with config.provide_session() as driver:
            with tracker.acquire_lock(driver):
                tracker.renew_lock(driver)
                driver.execute("DELETE FROM ddl_migrations_lock")
                with pytest.raises(MigrationError, match="lease expired"):
                    tracker.renew_lock(driver)
    finally:
        config.close_pool()


def test_renew_lock_without_lease_is_noop(tmp_path: Path) -> None:
    config = _duckdb_config(tmp_path / "noop.duckdb")
    try:
        with config.provide_session() as driver:
            SyncMigrationTracker().renew_lock(driver)
    finally:
        config.close_pool()


def test_lock_lease_seconds_setting(tmp_path: Path) -> None:
    config = _duckdb_config(tmp_path / "lease_len.duckdb", lock_lease_seconds=5)
    commands = SyncMigrationCommands(config)
    try:
        with config.provide_session() as driver:
            before_ms = int(time.time() * 1000)
            with commands.tracker.acquire_lock(driver):
                rows = driver.select("SELECT expires_at FROM ddl_migrations_lock")
            expires_at = int(rows[0]["expires_at"])
        assert 4000 <= expires_at - before_ms <= 5500
    finally:
        config.close_pool()


def test_lock_lease_seconds_rejects_non_positive() -> None:
    with pytest.raises(MigrationError, match="lock_lease_seconds"):
        SyncMigrationTracker(lock_lease_seconds=0)


def test_lease_lock_adbc_sqlite(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_items.sql").write_text(GOOD_MIGRATION, encoding="utf-8")
    db_path = tmp_path / "adbc.db"
    config = AdbcConfig(
        connection_config={"uri": f"sqlite:///{db_path}"},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )
    try:
        SyncMigrationCommands(config).upgrade(echo=False)
    finally:
        config.close_pool()

    with sqlite3.connect(db_path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        leases = connection.execute("SELECT COUNT(*) FROM ddl_migrations_lock").fetchone()
    assert {"items", "ddl_migrations_lock"} <= tables
    assert leases == (0,)


def test_unsupported_backend_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    driver = _driver_with_dialect("snowflake")
    tracker = SyncMigrationTracker()

    with caplog.at_level(logging.WARNING):
        with tracker.acquire_lock(driver):
            pass
        with tracker.acquire_lock(driver):
            pass

    messages = [record for record in caplog.records if "migration.lock.unsupported" in record.getMessage()]
    assert len(messages) == 1


@pytest.mark.parametrize("dialect", ["oracle", "spanner", "teradata", "duckdb"])
def test_lock_table_ddl_renders_per_dialect(dialect: str) -> None:
    target = SyncMigrationTracker()._lock_schema_target()  # pyright: ignore[reportPrivateUsage]

    statement = target.create_table.build(dialect=dialect).sql.upper()

    assert "PRIMARY KEY" in statement
    assert "LOCK_NAME" in statement


class _LeaseAiosqliteDriver(AiosqliteDriver):
    dialect = "duckdb"


@pytest.fixture
async def async_lease_driver(tmp_path: Path) -> "AsyncIterator[_LeaseAiosqliteDriver]":
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "async_lease.db")})
    async with config.provide_connection() as connection:
        yield _LeaseAiosqliteDriver(
            connection, statement_config=config.statement_config, driver_features=config.driver_features
        )
    await config.close_pool()


@pytest.mark.anyio
async def test_async_lease_lock_acquires_renews_and_releases(async_lease_driver: _LeaseAiosqliteDriver) -> None:
    tracker = AsyncMigrationTracker()
    driver = async_lease_driver

    async with tracker.acquire_lock(driver):
        await tracker.renew_lock(driver)
        held = await driver.select("SELECT lock_name FROM ddl_migrations_lock")
        assert len(held) == 1
        with pytest.raises(MigrationError, match="another migrator holds the lease"):
            async with AsyncMigrationTracker().acquire_lock(driver, timeout_seconds=1):
                pass

    assert await driver.select("SELECT lock_name FROM ddl_migrations_lock") == []
