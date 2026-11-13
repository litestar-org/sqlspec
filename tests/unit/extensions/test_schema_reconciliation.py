"""Extension-store additive schema reconciliation tests."""

import inspect
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.litestar.store import AdbcStore
from sqlspec.adapters.aiosqlite.adk.store import AiosqliteADKMemoryStore, AiosqliteADKStore
from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.adapters.aiosqlite.litestar.store import AiosqliteStore
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SqliteStore
from sqlspec.exceptions import MigrationError
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.extensions.litestar.store import CompanionIndex
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
        return (
            super()._memory_table_ddl().replace("embedding BLOB,\n", "embedding BLOB,\n            schema_tag TEXT,\n")
        )


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
            cast("Any", migration_driver).execute_script("CREATE TABLE queue_jobs (id TEXT PRIMARY KEY)")
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


@pytest.mark.anyio
async def test_events_reconcile_sync_and_async_end_to_end_partial_table_and_strict_refusal(tmp_path: Path) -> None:
    from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

    sync_migrations = tmp_path / "sync_events_migrations"
    sync_migrations.mkdir(parents=True, exist_ok=True)
    sync_db = tmp_path / "sync_events.db"
    sync_cfg = SqliteConfig(
        connection_config={"database": str(sync_db)},
        migration_config={"script_location": str(sync_migrations)},
        extension_config={"events": {"backend": "poll_queue", "migration_mode": "reconcile"}},
    )

    async_migrations = tmp_path / "async_events_migrations"
    async_migrations.mkdir(parents=True, exist_ok=True)
    async_db = tmp_path / "async_events.db"
    async_cfg = AiosqliteConfig(
        connection_config={"database": str(async_db)},
        migration_config={"script_location": str(async_migrations)},
        extension_config={"events": {"backend": "poll_queue", "migration_mode": "reconcile"}},
    )

    no_create_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "events_no_create.db")},
        extension_config={"events": {"backend": "poll_queue", "migration_mode": "reconcile", "create_schema": False}},
    )
    missing_payload_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "events_missing_payload.db")},
        extension_config={"events": {"backend": "poll_queue", "migration_mode": "reconcile"}},
    )

    partial_ddl = (
        "CREATE TABLE sqlspec_event_queue ("
        "event_id VARCHAR(64) PRIMARY KEY, "
        "channel VARCHAR(128) NOT NULL, "
        "payload_json TEXT NOT NULL, "
        "metadata_json TEXT, "
        "status VARCHAR(32) NOT NULL DEFAULT 'pending', "
        "available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "lease_expires_at TIMESTAMP, "
        "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
        "); "
        "INSERT INTO sqlspec_event_queue (event_id, channel, payload_json) VALUES ('e1', 'orders', '{}');"
    )

    try:
        with sync_cfg.provide_session() as session:
            session.execute_script(partial_ddl)
            session.commit()

        sync_commands = SyncMigrationCommands(sync_cfg)
        sync_commands.upgrade()
        sync_commands.upgrade()

        with sync_cfg.provide_session() as session:
            sync_cols = {
                str(c["column_name"]).casefold()
                for c in session.data_dictionary.get_columns(session, "sqlspec_event_queue")
            }
            sync_indexes = {
                str(idx["index_name"]).casefold()
                for idx in session.data_dictionary.get_indexes(session, table="sqlspec_event_queue")
            }
            sync_rows = session.select("SELECT event_id, attempts FROM sqlspec_event_queue")
            sync_ledger = session.select("SELECT version_num FROM ddl_migrations")

        assert {"acknowledged_at", "attempts"}.issubset(sync_cols)
        assert "idx_sqlspec_event_queue_channel_status" in sync_indexes
        assert len(sync_rows) == 1
        assert sync_rows[0]["event_id"] == "e1"
        assert sync_rows[0]["attempts"] == 0
        assert all(not str(r["version_num"]).startswith("ext_events_") for r in sync_ledger)

        sync_commands.downgrade()
        with sync_cfg.provide_session() as session:
            tables_after_down = {str(t["table_name"]).casefold() for t in session.data_dictionary.get_tables(session)}
        assert "sqlspec_event_queue" not in tables_after_down
        sync_commands.upgrade()
        with sync_cfg.provide_session() as session:
            tables_after_up = {str(t["table_name"]).casefold() for t in session.data_dictionary.get_tables(session)}
        assert "sqlspec_event_queue" in tables_after_up

        async with async_cfg.provide_session() as async_session:
            await async_session.execute_script(partial_ddl)
            await async_session.commit()

        async_commands = AsyncMigrationCommands(async_cfg)
        await async_commands.upgrade()
        await async_commands.upgrade()

        async with async_cfg.provide_session() as async_session:
            async_cols = {
                str(c["column_name"]).casefold()
                for c in await async_session.data_dictionary.get_columns(async_session, "sqlspec_event_queue")
            }
            async_indexes = {
                str(idx["index_name"]).casefold()
                for idx in await async_session.data_dictionary.get_indexes(async_session, table="sqlspec_event_queue")
            }
            async_rows = await async_session.select("SELECT event_id, attempts FROM sqlspec_event_queue")

        assert {"acknowledged_at", "attempts"}.issubset(async_cols)
        assert "idx_sqlspec_event_queue_channel_status" in async_indexes
        assert len(async_rows) == 1
        assert async_rows[0]["attempts"] == 0

        await async_commands.downgrade()
        async with async_cfg.provide_session() as async_session:
            async_tables_after_down = {
                str(t["table_name"]).casefold() for t in await async_session.data_dictionary.get_tables(async_session)
            }
        assert "sqlspec_event_queue" not in async_tables_after_down
        await async_commands.upgrade()
        async with async_cfg.provide_session() as async_session:
            async_tables_after_up = {
                str(t["table_name"]).casefold() for t in await async_session.data_dictionary.get_tables(async_session)
            }
        assert "sqlspec_event_queue" in async_tables_after_up

        with no_create_cfg.provide_session() as drv, pytest.raises(MigrationError, match="sqlspec_event_queue"):
            events_reconcile(MigrationContext(config=no_create_cfg, driver=drv))

        with missing_payload_cfg.provide_session() as drv:
            drv.execute_script(
                "CREATE TABLE sqlspec_event_queue (event_id VARCHAR(64) PRIMARY KEY, channel VARCHAR(128) NOT NULL)"
            )
            drv.commit()
            with pytest.raises(MigrationError, match="payload_json"):
                events_reconcile(MigrationContext(config=missing_payload_cfg, driver=drv))
    finally:
        sync_cfg.close_pool()
        await async_cfg.close_pool()
        no_create_cfg.close_pool()
        missing_payload_cfg.close_pool()


def _invoke_adk_reconcile(context: MigrationContext) -> Any:
    import importlib

    reconcile_mod = importlib.import_module("sqlspec.extensions.adk.migrations.reconcile")
    return reconcile_mod.reconcile(context)


def test_adk_session_targets_use_supplied_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})
    qualified_config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"adk": {"session_table": "analytics.adk_session"}}
    )

    class _TwoPartSqliteADKStore(SqliteADKStore):
        _max_table_name_parts = 2

    try:
        store = SqliteADKStore(config)
        qualified_store = _TwoPartSqliteADKStore(qualified_config)
        monkeypatch.setattr(SqliteConfig, "provide_session", MagicMock(side_effect=AssertionError("second session")))
        driver = MagicMock()

        targets = store.get_schema_targets_sync(driver)
        qualified_targets = qualified_store.get_schema_targets_sync(driver)
        store.prepare_reconcile_sync(driver)
        store.reconcile_companion_objects_sync(driver)
        store.finalize_reconcile_sync(driver)

        assert [t.identity for t in targets] == [
            "adk_session",
            "adk_event",
            "adk_app_state",
            "adk_user_state",
            "adk_internal_metadata",
        ]
        assert qualified_targets[0].identity == "analytics.adk_session"
        assert [c.args[0] for c in driver.execute.call_args_list] == store._companion_index_statements()
    finally:
        config.close_pool()
        qualified_config.close_pool()


@pytest.mark.anyio
async def test_adk_session_targets_async_use_supplied_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"adk": {"session_table": "custom_sessions"}}
    )
    try:
        store = AiosqliteADKStore(config)
        monkeypatch.setattr(AiosqliteConfig, "provide_session", MagicMock(side_effect=AssertionError("second session")))
        driver = MagicMock(execute=AsyncMock())

        targets = await store.get_schema_targets_async(driver)
        await store.prepare_reconcile_async(driver)
        await store.reconcile_companion_objects_async(driver)
        await store.finalize_reconcile_async(driver)

        assert len(targets) == 5
        assert targets[0].identity == "custom_sessions"
        assert [c.args[0] for c in driver.execute.await_args_list] == store._companion_index_statements()
    finally:
        await config.close_pool()


@pytest.mark.anyio
async def test_adk_memory_targets_follow_enabled_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    disabled_cfg = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"adk": {"enable_memory": False}}
    )
    enabled_cfg = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})
    async_enabled_cfg = AiosqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})

    class _ListDdlMemoryStore(BaseSyncADKMemoryStore[SqliteConfig]):
        def create_tables(self) -> None:
            return None

        def insert_memory_entries(self, entries: list[Any], owner_id: object | None = None) -> int:
            del entries, owner_id
            return 0

        def search_entries(
            self,
            query: str,
            app_name: str,
            user_id: str,
            limit: int | None = None,
            scope_filter: Any = "all",
            embedding: Any = None,
        ) -> list[Any]:
            del query, app_name, user_id, limit, scope_filter, embedding
            return []

        def delete_entries_by_session(
            self, session_id: str, app_name: str | None = None, user_id: str | None = None
        ) -> int:
            del session_id, app_name, user_id
            return 0

        def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
            del days, app_name, scope
            return 0

        def _memory_table_ddl(self) -> list[str]:
            return [
                "CREATE TABLE IF NOT EXISTS adk_memory (id TEXT PRIMARY KEY, app_name TEXT NOT NULL)",
                "CREATE INDEX IF NOT EXISTS idx_extra ON adk_memory(app_name)",
            ]

        def _drop_memory_table_sql(self) -> list[str]:
            return ["DROP TABLE IF EXISTS adk_memory"]

    try:
        disabled_store = SqliteADKMemoryStore(disabled_cfg)
        sqlite_enabled_store = SqliteADKMemoryStore(enabled_cfg)
        enabled_store = _ListDdlMemoryStore(enabled_cfg)
        async_store = AiosqliteADKMemoryStore(async_enabled_cfg)
        monkeypatch.setattr(SqliteConfig, "provide_session", MagicMock(side_effect=AssertionError("second session")))
        monkeypatch.setattr(AiosqliteConfig, "provide_session", MagicMock(side_effect=AssertionError("second session")))
        driver = MagicMock(execute=AsyncMock())

        assert disabled_store.get_schema_targets_sync(driver) == []
        sqlite_targets = sqlite_enabled_store.get_schema_targets_sync(driver)
        assert len(sqlite_targets) == 1
        assert sqlite_targets[0].identity == "adk_memory"
        sync_targets = enabled_store.get_schema_targets_sync(driver)
        assert len(sync_targets) == 1
        assert sync_targets[0].identity == "adk_memory"

        async_targets = await async_store.get_schema_targets_async(driver)
        assert len(async_targets) == 1
        assert async_targets[0].identity == "adk_memory"
        enabled_store.prepare_reconcile_sync(driver)
        enabled_store.reconcile_companion_objects_sync(driver)
        enabled_store.finalize_reconcile_sync(driver)
        await async_store.prepare_reconcile_async(driver)
        await async_store.reconcile_companion_objects_async(driver)
        await async_store.finalize_reconcile_async(driver)
        assert [c.args[0] for c in driver.execute.await_args_list] == async_store._memory_index_statements()
    finally:
        disabled_cfg.close_pool()
        enabled_cfg.close_pool()
        await async_enabled_cfg.close_pool()


def test_adk_reconcile_schema_still_adds_column(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "adk_runtime_reconcile.db")}, extension_config={"adk": {}}
    )
    try:
        SqliteADKStore(config).create_tables()
        store = AdditiveSqliteADKStore(config)
        store.reconcile_schema(assume_existing=True)

        with config.provide_session() as driver:
            columns = driver.data_dictionary.get_columns(driver, "adk_session")
        assert "schema_tag" in {str(col["column_name"]).casefold() for col in columns}
    finally:
        config.close_pool()


def test_adk_reconcile_creates_only_enabled_targets(tmp_path: Path) -> None:
    sessions_only_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "sessions_only.db")},
        extension_config={"adk": {"migration_mode": "reconcile", "enable_memory": False}},
    )
    memory_only_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "memory_only.db")},
        extension_config={"adk": {"migration_mode": "reconcile", "enable_sessions": False}},
    )
    try:
        with sessions_only_cfg.provide_session() as driver:
            assert (
                _invoke_adk_reconcile(MigrationContext(config=sessions_only_cfg, dialect="sqlite", driver=driver))
                is None
            )
            session_tables = {str(t["table_name"]).casefold() for t in driver.data_dictionary.get_tables(driver)}
        assert session_tables == {
            "adk_session",
            "adk_event",
            "adk_app_state",
            "adk_user_state",
            "adk_internal_metadata",
        }

        with memory_only_cfg.provide_session() as driver:
            assert (
                _invoke_adk_reconcile(MigrationContext(config=memory_only_cfg, dialect="sqlite", driver=driver)) is None
            )
            memory_tables = {str(t["table_name"]).casefold() for t in driver.data_dictionary.get_tables(driver)}
        assert memory_tables == {"adk_memory"}
    finally:
        sessions_only_cfg.close_pool()
        memory_only_cfg.close_pool()


def test_adk_reconcile_repeat_keeps_rows_and_names(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "adk_repeat.db")},
        extension_config={
            "adk": {"migration_mode": "reconcile", "session_table": "custom_sessions", "events_table": "custom_events"}
        },
    )
    try:
        with config.provide_session() as driver:
            _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))
            driver.execute(
                "INSERT INTO custom_sessions (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("s1", "app", "u1", "{}", 1.0, 1.0),
            )
            _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))
            count = driver.select_value("SELECT count(*) FROM custom_sessions")
            tables = {str(t["table_name"]).casefold() for t in driver.data_dictionary.get_tables(driver)}
        assert int(count) == 1
        assert "custom_sessions" in tables
        assert "custom_events" in tables
        assert "adk_session" not in tables
    finally:
        config.close_pool()


def test_adk_reconcile_uses_supplied_driver_without_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "adk_no_commit.db"
    config = SqliteConfig(
        connection_config={"database": str(db_path)}, extension_config={"adk": {"migration_mode": "reconcile"}}
    )
    try:
        with config.provide_session() as driver:
            driver.begin()
            commit_spy = MagicMock()
            with monkeypatch.context() as m:
                m.setattr(SqliteConfig, "provide_session", MagicMock(side_effect=AssertionError("second session")))
                m.setattr(SqliteDriver, "commit", commit_spy)
                _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))
            assert commit_spy.call_count == 0
            assert driver.connection.in_transaction is True
            with sqlite3.connect(db_path) as peer_conn:
                peer_tables = {
                    str(r[0]).casefold()
                    for r in peer_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
            assert "adk_session" not in peer_tables
            driver.commit()
    finally:
        config.close_pool()


def test_adk_reconcile_orders_phases(tmp_path: Path) -> None:
    call_log: list[str] = []

    class _RecordingSessionStore(SqliteADKStore):
        def prepare_schema_sync(self, driver: Any) -> None:
            del driver
            call_log.append("session.prepare")

        def prepare_reconcile_sync(self, driver: Any) -> None:
            del driver
            call_log.append("session.prepare_reconcile")

        def get_schema_targets_sync(self, driver: Any) -> list[SchemaTarget]:
            call_log.append("session.targets")
            return super().get_schema_targets_sync(driver)

        def reconcile_companion_objects_sync(self, driver: Any) -> None:
            del driver
            call_log.append("session.companions")

        def finalize_reconcile_sync(self, driver: Any) -> None:
            del driver
            call_log.append("session.finalize")

    class _RecordingMemoryStore(SqliteADKMemoryStore):
        def prepare_schema_sync(self, driver: Any) -> None:
            del driver
            call_log.append("memory.prepare")

        def prepare_reconcile_sync(self, driver: Any) -> None:
            del driver
            call_log.append("memory.prepare_reconcile")

        def get_schema_targets_sync(self, driver: Any) -> list[SchemaTarget]:
            call_log.append("memory.targets")
            return super().get_schema_targets_sync(driver)

        def reconcile_companion_objects_sync(self, driver: Any) -> None:
            del driver
            call_log.append("memory.companions")

        def finalize_reconcile_sync(self, driver: Any) -> None:
            del driver
            call_log.append("memory.finalize")

    class _RecordingConfig(SqliteConfig):
        adk_store_class = _RecordingSessionStore
        adk_memory_store_class = _RecordingMemoryStore

    config = _RecordingConfig(
        connection_config={"database": str(tmp_path / "adk_phases.db")},
        extension_config={"adk": {"migration_mode": "reconcile"}},
    )
    try:
        with config.provide_session() as driver:
            _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))
        assert call_log == [
            "session.prepare",
            "memory.prepare",
            "session.prepare_reconcile",
            "memory.prepare_reconcile",
            "session.targets",
            "memory.targets",
            "session.companions",
            "memory.companions",
            "session.finalize",
            "memory.finalize",
        ]
    finally:
        config.close_pool()


def test_adk_reconcile_refuses_unsafe_target_before_helper_ddl(tmp_path: Path) -> None:
    call_log: list[str] = []

    class _RecordingSessionStore(SqliteADKStore):
        def reconcile_companion_objects_sync(self, driver: Any) -> None:
            del driver
            call_log.append("session.companions")

        def finalize_reconcile_sync(self, driver: Any) -> None:
            del driver
            call_log.append("session.finalize")

    class _RecordingMemoryStore(SqliteADKMemoryStore):
        def reconcile_companion_objects_sync(self, driver: Any) -> None:
            del driver
            call_log.append("memory.companions")

        def finalize_reconcile_sync(self, driver: Any) -> None:
            del driver
            call_log.append("memory.finalize")

    class _RecordingConfig(SqliteConfig):
        adk_store_class = _RecordingSessionStore
        adk_memory_store_class = _RecordingMemoryStore

    config = _RecordingConfig(
        connection_config={"database": str(tmp_path / "adk_unsafe.db")},
        extension_config={"adk": {"migration_mode": "reconcile"}},
    )
    try:
        with config.provide_session() as driver:
            driver.execute_script("CREATE TABLE adk_session (id TEXT PRIMARY KEY)")
            driver.commit()
            with pytest.raises(MigrationError, match="adk_session"):
                _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))
            tables = {str(t["table_name"]).casefold() for t in driver.data_dictionary.get_tables(driver)}
        assert "adk_event" not in tables
        assert call_log == []
    finally:
        config.close_pool()


def test_adk_reconcile_accepts_application_subclass(tmp_path: Path) -> None:
    class AppConfig(SqliteConfig):
        pass

    AppConfig.__module__ = "myapp.db"
    config = AppConfig(
        connection_config={"database": str(tmp_path / "adk_app_subclass.db")},
        extension_config={"adk": {"migration_mode": "reconcile"}},
    )
    try:
        with config.provide_session() as driver:
            _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))
            tables = {str(t["table_name"]).casefold() for t in driver.data_dictionary.get_tables(driver)}
        assert "adk_session" in tables
        assert "adk_memory" in tables
    finally:
        config.close_pool()


def test_adk_reconcile_honors_manage_schema_false() -> None:
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"adk": {"migration_mode": "reconcile", "manage_schema": False}},
    )
    driver = MagicMock()
    try:
        assert _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver)) is None
        assert driver.mock_calls == []
    finally:
        config.close_pool()


def test_adk_reconcile_create_schema_false_refuses_missing_table(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "adk_no_create.db")},
        extension_config={"adk": {"migration_mode": "reconcile", "create_schema": False}},
    )
    try:
        with config.provide_session() as driver:
            with pytest.raises(MigrationError, match="adk_session"):
                _invoke_adk_reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))
            tables = {str(t["table_name"]).casefold() for t in driver.data_dictionary.get_tables(driver)}
        assert tables == set()
    finally:
        config.close_pool()


@pytest.mark.anyio
async def test_adk_reconcile_dispatches_on_driver_mode(tmp_path: Path) -> None:
    sync_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "adk_dispatch_sync.db")},
        extension_config={"adk": {"migration_mode": "reconcile"}},
    )
    async_cfg = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "adk_dispatch_async.db")},
        extension_config={"adk": {"migration_mode": "reconcile"}},
    )
    try:
        with sync_cfg.provide_session() as sync_driver:
            sync_outcome = _invoke_adk_reconcile(
                MigrationContext(config=sync_cfg, dialect="sqlite", driver=sync_driver)
            )
            assert sync_outcome is None
            sync_tables = {str(t["table_name"]).casefold() for t in sync_driver.data_dictionary.get_tables(sync_driver)}
        assert "adk_session" in sync_tables

        async with async_cfg.provide_session() as async_driver:
            async_outcome = _invoke_adk_reconcile(
                MigrationContext(config=async_cfg, dialect="sqlite", driver=async_driver)
            )
            assert inspect.isawaitable(async_outcome)
            await async_outcome
            async_tables = {
                str(t["table_name"]).casefold() for t in await async_driver.data_dictionary.get_tables(async_driver)
            }
        assert "adk_session" in async_tables
    finally:
        sync_cfg.close_pool()
        await async_cfg.close_pool()


def test_base_litestar_store_extracts_standalone_and_inline_mysql_companion_indexes() -> None:
    cfg = SqliteConfig(connection_config={"database": ":memory:"})

    class _PartialIndexStore(SqliteStore):
        def _table_ddl(self) -> str:
            return (
                "CREATE TABLE IF NOT EXISTS litestar_session ("
                "session_id TEXT PRIMARY KEY, data BLOB NOT NULL, expires_at TIMESTAMPTZ); "
                "CREATE INDEX IF NOT EXISTS idx_litestar_session_expires_at "
                "ON litestar_session (expires_at) WHERE expires_at IS NOT NULL"
            )

    class _InlineMysqlStore(SqliteStore):
        __slots__ = ("_index_options",)

        def __init__(self, config: SqliteConfig) -> None:
            super().__init__(config)
            self._index_options = " USING BTREE"

        def _table_ddl(self) -> str:
            return (
                "CREATE TABLE IF NOT EXISTS litestar_session ("
                "session_id VARCHAR(255) PRIMARY KEY, data LONGBLOB NOT NULL, expires_at DATETIME(6), "
                f"INDEX idx_litestar_session_expires_at (expires_at){self._index_options}"
                ") ENGINE=InnoDB"
            )

    class _IndexFreeStore(SqliteStore):
        def _table_ddl(self) -> str:
            return "CREATE TABLE IF NOT EXISTS litestar_session (session_id TEXT PRIMARY KEY, data BLOB NOT NULL)"

    class _ProceduralStore(SqliteStore):
        def _table_ddl(self) -> str:
            return (
                "IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'litestar_session') "
                "BEGIN CREATE TABLE litestar_session (session_id NVARCHAR(255) PRIMARY KEY); "
                "CREATE INDEX IX_litestar_session_expires_at ON litestar_session(expires_at); END;"
            )

    try:
        sqlite_indexes = SqliteStore(cfg).get_companion_indexes()
        assert len(sqlite_indexes) == 1
        assert sqlite_indexes[0].name == "idx_litestar_session_expires_at"
        assert sqlite_indexes[0].table == "litestar_session"
        assert sqlite_indexes[0].schema is None
        assert sqlite_indexes[0].ddl.startswith("CREATE INDEX IF NOT EXISTS idx_litestar_session_expires_at")
        assert sqlite_indexes[0].ddl.endswith(";")

        partial_indexes = _PartialIndexStore(cfg).get_companion_indexes()
        assert partial_indexes == [
            CompanionIndex(
                name="idx_litestar_session_expires_at",
                table="litestar_session",
                schema=None,
                ddl=(
                    "CREATE INDEX IF NOT EXISTS idx_litestar_session_expires_at "
                    "ON litestar_session (expires_at) WHERE expires_at IS NOT NULL;"
                ),
            )
        ]

        mysql_indexes = _InlineMysqlStore(cfg).get_companion_indexes()
        assert mysql_indexes == [
            CompanionIndex(
                name="idx_litestar_session_expires_at",
                table="litestar_session",
                schema=None,
                ddl="CREATE INDEX idx_litestar_session_expires_at ON litestar_session (expires_at) USING BTREE",
            )
        ]

        index_free_store = _IndexFreeStore(cfg)
        assert index_free_store.get_companion_indexes() == []
        fake_driver = MagicMock()
        index_free_store.reconcile_companion_objects_sync(fake_driver)
        fake_driver.data_dictionary.get_indexes.assert_not_called()

        assert _ProceduralStore(cfg).get_companion_indexes() == []
    finally:
        cfg.close_pool()


@pytest.mark.anyio
async def test_litestar_reconcile_restores_dropped_sqlite_expiry_index_sync_and_async(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync_db = tmp_path / "sync_drop_idx.db"
    sync_cfg = SqliteConfig(
        connection_config={"database": str(sync_db)}, extension_config={"litestar": {"migration_mode": "reconcile"}}
    )
    async_db = tmp_path / "async_drop_idx.db"
    async_cfg = AiosqliteConfig(
        connection_config={"database": str(async_db)}, extension_config={"litestar": {"migration_mode": "reconcile"}}
    )
    try:
        await SqliteStore(sync_cfg).create_table()
        with sync_cfg.provide_session() as sync_driver:
            sync_driver.execute(
                "INSERT INTO litestar_session (session_id, data, expires_at) VALUES (?, ?, ?)", ("s1", b"payload", 99.0)
            )
            sync_driver.execute_script("DROP INDEX idx_litestar_session_expires_at")
            sync_driver.commit()
            before_sync = {
                str(idx["index_name"]).casefold()
                for idx in sync_driver.data_dictionary.get_indexes(sync_driver, table="litestar_session")
            }
        assert "idx_litestar_session_expires_at" not in before_sync

        with sync_cfg.provide_session() as sync_driver:
            sync_driver.begin()
            sync_commit_spy = MagicMock()
            with monkeypatch.context() as m:
                m.setattr(SqliteDriver, "commit", sync_commit_spy)
                assert litestar_reconcile(MigrationContext(config=sync_cfg, driver=sync_driver)) is None
            assert sync_commit_spy.call_count == 0
            assert sync_driver.connection.in_transaction is True
            sync_driver.commit()

        with sync_cfg.provide_session() as sync_driver:
            after_sync = {
                str(idx["index_name"]).casefold()
                for idx in sync_driver.data_dictionary.get_indexes(sync_driver, table="litestar_session")
            }
            sync_rows = sync_driver.select("SELECT session_id, data FROM litestar_session")
        assert "idx_litestar_session_expires_at" in after_sync
        assert len(sync_rows) == 1
        assert sync_rows[0]["session_id"] == "s1"

        await AiosqliteStore(async_cfg).create_table()
        async with async_cfg.provide_session() as async_driver:
            await async_driver.execute(
                "INSERT INTO litestar_session (session_id, data, expires_at) VALUES (?, ?, ?)", ("s2", b"payload", 99.0)
            )
            await async_driver.execute_script("DROP INDEX idx_litestar_session_expires_at")
            await async_driver.commit()
            before_async = {
                str(idx["index_name"]).casefold()
                for idx in await async_driver.data_dictionary.get_indexes(async_driver, table="litestar_session")
            }
        assert "idx_litestar_session_expires_at" not in before_async

        async with async_cfg.provide_session() as async_driver:
            await async_driver.begin()
            async_commit_spy = AsyncMock()
            with monkeypatch.context() as m:
                m.setattr(AiosqliteDriver, "commit", async_commit_spy)
                coro = litestar_reconcile(MigrationContext(config=async_cfg, driver=async_driver))
                assert inspect.isawaitable(coro)
                await coro
            assert async_commit_spy.call_count == 0
            assert async_driver.connection.in_transaction is True
            await async_driver.commit()

        async with async_cfg.provide_session() as async_driver:
            after_async = {
                str(idx["index_name"]).casefold()
                for idx in await async_driver.data_dictionary.get_indexes(async_driver, table="litestar_session")
            }
            async_rows = await async_driver.select("SELECT session_id FROM litestar_session")
        assert "idx_litestar_session_expires_at" in after_async
        assert len(async_rows) == 1
        assert async_rows[0]["session_id"] == "s2"
    finally:
        sync_cfg.close_pool()
        await async_cfg.close_pool()


@pytest.mark.anyio
async def test_litestar_companion_indexes_skipped_when_present_case_insensitive_and_schema_scoped() -> None:
    cfg = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar": {"session_table": "app.litestar_session"}},
    )

    class _QualifiedStore(SqliteStore):
        @staticmethod
        def _ensure_table_name(table_name: str) -> None:
            del table_name

        def _table_ddl(self) -> str:
            return (
                "CREATE TABLE app.litestar_session (session_id TEXT PRIMARY KEY, data BLOB NOT NULL, expires_at REAL); "
                "CREATE INDEX idx_litestar_session_expires_at ON app.litestar_session (expires_at)"
            )

    try:
        store = _QualifiedStore(cfg)
        indexes = store.get_companion_indexes()
        assert indexes == [
            CompanionIndex(
                name="idx_litestar_session_expires_at",
                table="litestar_session",
                schema="app",
                ddl="CREATE INDEX idx_litestar_session_expires_at ON app.litestar_session (expires_at);",
            )
        ]

        matching_rows = [
            {"index_name": "IDX_LITESTAR_SESSION_EXPIRES_AT", "table_name": "LITESTAR_SESSION", "schema_name": "APP"}
        ]
        other_schema_rows = [
            {"index_name": "IDX_LITESTAR_SESSION_EXPIRES_AT", "table_name": "LITESTAR_SESSION", "schema_name": "OTHER"}
        ]

        sync_present = MagicMock()
        sync_present.data_dictionary.get_indexes.return_value = matching_rows
        store.reconcile_companion_objects_sync(sync_present)
        sync_present.data_dictionary.get_indexes.assert_called_once_with(
            sync_present, table="litestar_session", schema="app"
        )
        sync_present.execute_script.assert_not_called()
        sync_present.commit.assert_not_called()

        sync_missing = MagicMock()
        sync_missing.data_dictionary.get_indexes.return_value = other_schema_rows
        store.reconcile_companion_objects_sync(sync_missing)
        sync_missing.execute_script.assert_called_once_with(
            "CREATE INDEX idx_litestar_session_expires_at ON app.litestar_session (expires_at);"
        )
        sync_missing.commit.assert_not_called()

        async_present = SimpleNamespace(
            data_dictionary=SimpleNamespace(get_indexes=AsyncMock(return_value=matching_rows)),
            execute_script=AsyncMock(),
            commit=AsyncMock(),
        )
        await store.reconcile_companion_objects_async(async_present)
        async_present.execute_script.assert_not_awaited()
        async_present.commit.assert_not_awaited()

        async_missing = SimpleNamespace(
            data_dictionary=SimpleNamespace(get_indexes=AsyncMock(return_value=other_schema_rows)),
            execute_script=AsyncMock(),
            commit=AsyncMock(),
        )
        await store.reconcile_companion_objects_async(async_missing)
        async_missing.execute_script.assert_awaited_once_with(
            "CREATE INDEX idx_litestar_session_expires_at ON app.litestar_session (expires_at);"
        )
        async_missing.commit.assert_not_awaited()
    finally:
        cfg.close_pool()


@pytest.mark.anyio
async def test_builtin_extensions_default_to_reconcile_and_drop_at_base_downgrade(tmp_path: Path) -> None:
    """Built-in extensions (litestar, adk, events) default to reconcile mode and drop tables on base downgrade."""
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_app.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE app_items (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0001-down\nDROP TABLE app_items;\n",
        encoding="utf-8",
    )

    sync_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "builtin_sync.db")},
        migration_config={"script_location": str(migrations_dir)},
        extension_config={
            "litestar": {"session_table": "litestar_session"},
            "adk": {"enable_memory": True},
            "events": {"backend": "poll_queue"},
        },
    )
    async_cfg = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "builtin_async.db")},
        migration_config={"script_location": str(migrations_dir)},
        extension_config={
            "litestar": {"session_table": "litestar_session"},
            "adk": {"enable_memory": True},
            "events": {"backend": "poll_queue"},
        },
    )
    try:
        sync_cmds = SyncMigrationCommands(sync_cfg)
        assert [d.name for d in sync_cmds.extension_reconcilers] == ["litestar", "adk", "events"]
        assert sync_cmds.runner.extension_migrations == {}

        sync_cmds.upgrade()
        with sync_cfg.provide_session() as drv:
            tables = {str(t["table_name"]).casefold() for t in drv.data_dictionary.get_tables(drv)}
        assert {
            "app_items",
            "litestar_session",
            "adk_session",
            "adk_event",
            "adk_memory",
            "sqlspec_event_queue",
        }.issubset(tables)

        sync_cmds.downgrade()
        with sync_cfg.provide_session() as drv:
            tables_after = {str(t["table_name"]).casefold() for t in drv.data_dictionary.get_tables(drv)}
        assert not (
            {"app_items", "litestar_session", "adk_session", "adk_event", "adk_memory", "sqlspec_event_queue"}
            & tables_after
        )

        async_cmds = AsyncMigrationCommands(async_cfg)
        assert [d.name for d in async_cmds.extension_reconcilers] == ["litestar", "adk", "events"]
        await async_cmds.upgrade()
        async with async_cfg.provide_session() as drv:
            async_tables = {str(t["table_name"]).casefold() for t in await drv.data_dictionary.get_tables(drv)}
        assert {
            "app_items",
            "litestar_session",
            "adk_session",
            "adk_event",
            "adk_memory",
            "sqlspec_event_queue",
        }.issubset(async_tables)

        await async_cmds.downgrade()
        async with async_cfg.provide_session() as drv:
            async_tables_after = {str(t["table_name"]).casefold() for t in await drv.data_dictionary.get_tables(drv)}
        assert not (
            {"app_items", "litestar_session", "adk_session", "adk_event", "adk_memory", "sqlspec_event_queue"}
            & async_tables_after
        )
    finally:
        sync_cfg.close_pool()
        await async_cfg.close_pool()
