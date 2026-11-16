"""Lease lock for backends without a native session lock."""

import logging
import sqlite3
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import duckdb
import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgSyncDriver
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.exceptions import MigrationError, SerializationConflictError
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
            with pytest.raises(MigrationError, match="lease expired"):
                with tracker.acquire_lock(driver):
                    tracker.renew_lock(driver)
                    driver.execute("DELETE FROM ddl_migrations_lock")
                    tracker.renew_lock(driver)
    finally:
        config.close_pool()


class _NoRowcountDuckDBDriver(DuckDBDriver):
    """Reports zero affected rows for every UPDATE and DELETE, like drivers without rowcounts."""

    def execute(self, statement: Any, /, *parameters: Any, **kwargs: Any) -> Any:
        result = super().execute(statement, *parameters, **kwargs)
        if result.operation_type in {"UPDATE", "DELETE"}:
            result.rows_affected = 0
        return result


@contextmanager
def _no_rowcount_driver(db_path: Path) -> "Iterator[_NoRowcountDuckDBDriver]":
    config = _duckdb_config(db_path)
    try:
        with config.provide_connection() as connection:
            yield _NoRowcountDuckDBDriver(
                connection, statement_config=config.statement_config, driver_features=config.driver_features
            )
    finally:
        config.close_pool()


class _ConflictOnceDuckDBDriver(DuckDBDriver):
    """Fails the first lease insert with a serialization conflict, like a concurrent CockroachDB claim."""

    conflicts = 0

    def execute(self, statement: Any, /, *parameters: Any, **kwargs: Any) -> Any:
        if self.conflicts == 0 and "INSERT INTO" in str(getattr(statement, "sql", statement)).upper():
            type(self).conflicts += 1
            msg = "restart transaction"
            raise SerializationConflictError(msg)
        return super().execute(statement, *parameters, **kwargs)


def test_lease_claim_retries_after_serialization_conflict(tmp_path: Path) -> None:
    config = _duckdb_config(tmp_path / "conflict.duckdb")
    try:
        with config.provide_connection() as connection:
            driver = _ConflictOnceDuckDBDriver(
                connection, statement_config=config.statement_config, driver_features=config.driver_features
            )
            tracker = SyncMigrationTracker()
            with tracker.acquire_lock(driver, timeout_seconds=5):
                assert len(driver.select("SELECT lock_name FROM ddl_migrations_lock")) == 1
        assert _ConflictOnceDuckDBDriver.conflicts == 1
    finally:
        config.close_pool()


def test_lease_renews_and_releases_without_reported_rowcounts(tmp_path: Path) -> None:
    with _no_rowcount_driver(tmp_path / "no_rowcount.duckdb") as driver:
        tracker = SyncMigrationTracker()
        with tracker.acquire_lock(driver):
            tracker.renew_lock(driver)
            assert len(driver.select("SELECT lock_name FROM ddl_migrations_lock")) == 1

        assert driver.select("SELECT lock_name FROM ddl_migrations_lock") == []


def test_renew_detects_takeover_without_reported_rowcounts(tmp_path: Path) -> None:
    with _no_rowcount_driver(tmp_path / "no_rowcount_renew.duckdb") as driver:
        tracker = SyncMigrationTracker()
        with pytest.raises(MigrationError, match="lease expired"):
            with tracker.acquire_lock(driver):
                driver.execute("UPDATE ddl_migrations_lock SET owner_token = 'another-migrator'")
                tracker.renew_lock(driver)


def test_release_detects_takeover_without_reported_rowcounts(tmp_path: Path) -> None:
    with _no_rowcount_driver(tmp_path / "no_rowcount_release.duckdb") as driver:
        with pytest.raises(MigrationError, match="lease expired"):
            with SyncMigrationTracker().acquire_lock(driver):
                driver.execute("UPDATE ddl_migrations_lock SET owner_token = 'another-migrator'")


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


@asynccontextmanager
async def _held_lease(tmp_path: Path, is_async: bool, error: "BaseException | None") -> "AsyncIterator[None]":
    """Acquire the migration lease, delete it as another migrator would, then raise ``error`` if given."""
    if is_async:
        async_config = AiosqliteConfig(connection_config={"database": str(tmp_path / "lost.db")})
        try:
            async with async_config.provide_connection() as connection:
                driver = _LeaseAiosqliteDriver(
                    connection,
                    statement_config=async_config.statement_config,
                    driver_features=async_config.driver_features,
                )
                async with AsyncMigrationTracker().acquire_lock(driver):
                    await driver.execute("DELETE FROM ddl_migrations_lock")
                    if error is not None:
                        raise error
                    yield
        finally:
            await async_config.close_pool()
        return
    config = _duckdb_config(tmp_path / "lost.duckdb")
    try:
        with config.provide_session() as sync_driver, SyncMigrationTracker().acquire_lock(sync_driver):
            sync_driver.execute("DELETE FROM ddl_migrations_lock")
            if error is not None:
                raise error
            yield
    finally:
        config.close_pool()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_release_raises_when_lease_was_taken_over(tmp_path: Path, is_async: bool) -> None:
    with pytest.raises(MigrationError, match="lease expired"):
        async with _held_lease(tmp_path, is_async, None):
            pass


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_release_after_takeover_does_not_mask_original_error(tmp_path: Path, is_async: bool) -> None:
    with pytest.raises(ValueError, match="boom"):
        async with _held_lease(tmp_path, is_async, ValueError("boom")):
            pass


def test_lease_renewed_before_each_reconciler(tmp_path: Path) -> None:
    config = _duckdb_config(tmp_path / "recon.duckdb")
    commands = SyncMigrationCommands(config)
    events: list[str] = []
    first, second = MagicMock(), MagicMock()
    first.name, second.name = "first", "second"

    def renew_lock(driver: Any) -> None:
        events.append("renew")

    def execute_reconciliation(driver: Any, descriptor: Any) -> int:
        events.append(descriptor.name)
        return 1

    commands.tracker.renew_lock = renew_lock  # type: ignore[method-assign]
    commands.runner.execute_reconciliation = execute_reconciliation  # type: ignore[method-assign]
    commands.extension_reconcilers = cast("Any", (first, second))
    try:
        commands._run_reconcilers(MagicMock(), [], use_logger=False, echo=False, summary_only=True)  # pyright: ignore[reportPrivateUsage]
    finally:
        config.close_pool()

    assert events == ["renew", "first", "renew", "second"]
