"""Extension-store additive schema reconciliation tests."""

import inspect
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.litestar.store import AdbcStore
from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SqliteStore
from sqlspec.exceptions import MigrationError
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.schema import SchemaTarget, ensure_schema_sync


class AdditiveSQLiteStore(SqliteStore):
    """Session store target with one additive column."""

    def _table_ddl(self) -> str:
        return super()._table_ddl().replace("expires_at REAL\n", "expires_at REAL,\n            schema_tag TEXT\n")


class AdditiveSqliteADKStore(SqliteADKStore):
    """ADK target with one additive session column."""

    def _sessions_table_ddl(self) -> str:
        return (
            super()
            ._sessions_table_ddl()
            .replace("update_time REAL NOT NULL\n", "update_time REAL NOT NULL,\n            schema_tag TEXT\n")
        )


class AdditiveSqliteADKMemoryStore(SqliteADKMemoryStore):
    """ADK memory target with one additive column."""

    def _memory_table_ddl(self) -> str:
        return super()._memory_table_ddl().replace("embedding BLOB\n", "embedding BLOB,\n            schema_tag TEXT\n")


class AdditiveSqliteEventQueueStore(SqliteEventQueueStore):
    """Event queue target with one additive column."""

    def _table_ddl(self) -> str:
        return super()._table_ddl().replace("acknowledged_at TIMESTAMP", "acknowledged_at TIMESTAMP, schema_tag TEXT")


async def test_litestar_session_store_reconcile_uses_target_ddl(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "litestar.db")},
        extension_config={"litestar": {"session_table": "sessions"}},
    )
    await SqliteStore(config).create_table()

    store = AdditiveSQLiteStore(config)
    await store.reconcile_schema(assume_existing=True)

    with config.provide_session() as driver:
        columns = driver.data_dictionary.get_columns(driver, "sessions")
    assert "schema_tag" in {str(column["column_name"]).casefold() for column in columns}


def test_adk_additive_column_does_not_require_schema_version_seed(tmp_path: Path) -> None:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "adk.db")}, extension_config={"adk": {}})
    SqliteADKStore(config).create_tables()

    store = AdditiveSqliteADKStore(config)
    store.reconcile_schema(assume_existing=True)

    with config.provide_session() as driver:
        columns = driver.data_dictionary.get_columns(driver, store.session_table)
        seed_rows = driver.select(f"SELECT value FROM {store.metadata_table} WHERE key = ?", ("schema_version",))
    assert "schema_tag" in {str(column["column_name"]).casefold() for column in columns}
    assert seed_rows == []


def test_adk_memory_additive_column_uses_target_ddl(tmp_path: Path) -> None:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "adk-memory.db")}, extension_config={"adk": {}})
    SqliteADKMemoryStore(config).create_tables()

    store = AdditiveSqliteADKMemoryStore(config)
    store.reconcile_schema(assume_existing=True)

    with config.provide_session() as driver:
        columns = driver.data_dictionary.get_columns(driver, store.memory_table)
    assert "schema_tag" in {str(column["column_name"]).casefold() for column in columns}


def test_events_queue_store_reconcile_additive(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "events.db")},
        extension_config={"events": {"queue_table": "events_queue"}},
    )
    base_store = SqliteEventQueueStore(config)
    with config.provide_session() as driver:
        driver.execute_script(base_store.create_statements()[0])

    store = AdditiveSqliteEventQueueStore(config)
    with config.provide_session() as driver:
        store.reconcile_schema_sync(driver)
        columns = driver.data_dictionary.get_columns(driver, store.table_name)
    assert "schema_tag" in {str(column["column_name"]).casefold() for column in columns}


def test_packaged_extension_migration_and_additive_ensure_are_independent(tmp_path: Path) -> None:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "queues-consumer.db")})
    settings = {"manage_schema": True, "create_schema": True, "run_migrations": True}
    target = SchemaTarget.from_ddl(
        "queue_jobs", "CREATE TABLE IF NOT EXISTS queue_jobs (id TEXT PRIMARY KEY, payload BLOB)", dialect="sqlite"
    )
    applied_versions: list[str] = []

    with config.provide_session() as driver:

        def run_packaged_migration(migration_driver: object) -> None:
            migration_driver.execute_script("CREATE TABLE queue_jobs (id TEXT PRIMARY KEY)")  # type: ignore[attr-defined]
            applied_versions.append("ext_litestar_queues_0001")

        result = ensure_schema_sync(
            driver,
            [target],
            manage_schema=settings["manage_schema"],
            create_schema=settings["create_schema"],
            run_migrations=settings["run_migrations"],
            migration_runner=run_packaged_migration,
        )
        columns = driver.data_dictionary.get_columns(driver, "queue_jobs")

    assert result.migrations_run is True
    assert result.added_columns == {"queue_jobs": ["payload"]}
    assert applied_versions == ["ext_litestar_queues_0001"]
    assert {str(column["column_name"]).casefold() for column in columns} == {"id", "payload"}


def test_litestar_store_get_schema_targets_derives_table_and_columns() -> None:
    store = SqliteStore(SqliteConfig(connection_config={"database": ":memory:"}))

    targets = store.get_schema_targets()

    assert len(targets) == 1
    target = targets[0]
    assert target.table_name == "litestar_session"
    assert [col.name for col in target.create_table.columns] == ["session_id", "data", "expires_at"]
    assert target.constraint_columns == frozenset({"session_id"})


@pytest.mark.parametrize(("driver_dialect", "expected_cached_dialect"), [("sqlite", "sqlite"), ("tsql", "mssql")])
def test_adbc_litestar_store_prepare_schema_sync_populates_dialect_without_nested_session(
    driver_dialect: str, expected_cached_dialect: str
) -> None:
    def _forbid_nested_session() -> object:
        msg = "provide_session must not be called when driver is supplied to prepare_schema_sync"
        raise AssertionError(msg)

    fake_cfg = cast(
        "Any",
        SimpleNamespace(
            extension_config={"litestar": {"session_table": "sessions"}}, provide_session=_forbid_nested_session
        ),
    )
    store = AdbcStore(fake_cfg)
    fake_driver = SimpleNamespace(dialect=driver_dialect)

    store.prepare_schema_sync(fake_driver)
    targets = store.get_schema_targets()

    assert store._dialect == expected_cached_dialect
    assert len(targets) == 1
    assert targets[0].table_name == "sessions"
    assert [col.name for col in targets[0].create_table.columns] == ["session_id", "data", "expires_at"]


def test_litestar_reconcile_refuses_unresolvable_config() -> None:
    fake_driver = MagicMock()
    with pytest.raises(MigrationError):
        litestar_reconcile(MigrationContext(config=None, driver=fake_driver))
    fake_driver.execute.assert_not_called()
    fake_driver.execute_script.assert_not_called()


def test_litestar_reconcile_refuses_unresolvable_adapter_and_unclassified_flightsql() -> None:
    fake_driver = MagicMock()
    unresolvable_config = SimpleNamespace(extension_config={"litestar": {"migration_mode": "reconcile"}})

    with pytest.raises(MigrationError, match='migration_mode="versioned"'):
        litestar_reconcile(MigrationContext(config=unresolvable_config, driver=fake_driver))
    fake_driver.execute.assert_not_called()
    fake_driver.execute_script.assert_not_called()

    flightsql_config = AdbcConfig(
        connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://localhost:8815"},
        extension_config={"litestar": {"migration_mode": "reconcile"}},
    )
    with pytest.raises(MigrationError, match='migration_mode="versioned"'):
        litestar_reconcile(MigrationContext(config=flightsql_config, driver=fake_driver))
    fake_driver.execute.assert_not_called()
    fake_driver.execute_script.assert_not_called()


@pytest.mark.anyio
async def test_litestar_reconcile_sync_and_async_creates_and_adds_columns_without_commit(tmp_path: Path) -> None:
    sync_db = tmp_path / "sync_litestar.db"
    sync_cfg = SqliteConfig(
        connection_config={"database": str(sync_db)}, extension_config={"litestar": {"migration_mode": "reconcile"}}
    )
    async_db = tmp_path / "async_litestar.db"
    async_cfg = AiosqliteConfig(
        connection_config={"database": str(async_db)}, extension_config={"litestar": {"migration_mode": "reconcile"}}
    )
    try:
        with sync_cfg.provide_session() as sync_driver:
            sync_driver.execute_script(
                "CREATE TABLE litestar_session (session_id TEXT PRIMARY KEY, data BLOB NOT NULL); "
                "INSERT INTO litestar_session (session_id, data) VALUES ('s1', X'6162');"
            )
            sync_driver.commit()

        with sync_cfg.provide_session() as sync_driver:
            sync_driver.begin()
            outcome = litestar_reconcile(MigrationContext(config=sync_cfg, driver=sync_driver))
            assert outcome is None
            assert sync_driver.connection.in_transaction is True
            with sqlite3.connect(sync_db) as peer_conn:
                peer_cols = {str(r[1]).casefold() for r in peer_conn.execute("PRAGMA table_info(litestar_session)")}
            assert "expires_at" not in peer_cols
            sync_driver.commit()

        with sync_cfg.provide_session() as sync_driver:
            cols = {
                str(c["column_name"]).casefold()
                for c in sync_driver.data_dictionary.get_columns(sync_driver, "litestar_session")
            }
            rows = sync_driver.select("SELECT session_id, data FROM litestar_session")
        assert "expires_at" in cols
        assert len(rows) == 1
        assert rows[0]["session_id"] == "s1"

        async with async_cfg.provide_session() as async_driver:
            await async_driver.begin()
            coro = litestar_reconcile(MigrationContext(config=async_cfg, driver=async_driver))
            assert inspect.isawaitable(coro)
            await coro
            assert async_driver.connection.in_transaction is True
            with sqlite3.connect(async_db) as peer_conn:
                peer_tables = {
                    str(r[0]).casefold()
                    for r in peer_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
            assert "litestar_session" not in peer_tables
            await async_driver.commit()

        async with async_cfg.provide_session() as async_driver:
            tables = await async_driver.data_dictionary.get_tables(async_driver)
        assert "litestar_session" in {str(t["table_name"]).casefold() for t in tables}

        disabled_cfg = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"migration_mode": "reconcile", "manage_schema": False}},
        )
        noop_driver = MagicMock()
        assert litestar_reconcile(MigrationContext(config=disabled_cfg, driver=noop_driver)) is None
        noop_driver.execute.assert_not_called()
        noop_driver.execute_script.assert_not_called()
        noop_driver.data_dictionary.get_tables.assert_not_called()
    finally:
        sync_cfg.close_pool()
        await async_cfg.close_pool()


def test_litestar_reconcile_create_schema_false_refuses_missing_table_and_strict_refuses_drift(tmp_path: Path) -> None:
    no_create_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "missing.db")},
        extension_config={"litestar": {"migration_mode": "reconcile", "create_schema": False}},
    )
    type_drift_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "drift.db")},
        extension_config={"litestar": {"migration_mode": "reconcile"}},
    )
    missing_pk_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "missing_pk.db")},
        extension_config={"litestar": {"migration_mode": "reconcile"}},
    )
    try:
        with no_create_cfg.provide_session() as driver, pytest.raises(MigrationError, match="litestar_session"):
            litestar_reconcile(MigrationContext(config=no_create_cfg, driver=driver))

        with type_drift_cfg.provide_session() as driver:
            driver.execute_script(
                "CREATE TABLE litestar_session (session_id TEXT PRIMARY KEY, data BLOB NOT NULL, expires_at BLOB)"
            )
            driver.commit()
            with pytest.raises(MigrationError, match="expires_at"):
                litestar_reconcile(MigrationContext(config=type_drift_cfg, driver=driver))

        with missing_pk_cfg.provide_session() as driver:
            driver.execute_script("CREATE TABLE litestar_session (data BLOB NOT NULL, expires_at REAL)")
            driver.commit()
            with pytest.raises(MigrationError, match="session_id"):
                litestar_reconcile(MigrationContext(config=missing_pk_cfg, driver=driver))
    finally:
        no_create_cfg.close_pool()
        type_drift_cfg.close_pool()
        missing_pk_cfg.close_pool()


def test_litestar_reconcile_refuses_nullable_data_column(tmp_path: Path) -> None:
    cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "nullable_data.db")},
        extension_config={"litestar": {"migration_mode": "reconcile"}},
    )
    try:
        with cfg.provide_session() as driver:
            driver.execute_script("CREATE TABLE litestar_session (session_id TEXT PRIMARY KEY, data BLOB)")
            driver.commit()
            with pytest.raises(MigrationError, match=r"litestar_session.*data|data.*litestar_session"):
                litestar_reconcile(MigrationContext(config=cfg, driver=driver))
            cols = {
                str(c["column_name"]).casefold() for c in driver.data_dictionary.get_columns(driver, "litestar_session")
            }
        assert cols == {"session_id", "data"}
    finally:
        cfg.close_pool()
