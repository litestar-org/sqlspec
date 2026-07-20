"""Extension-store additive schema reconciliation tests."""

from pathlib import Path

from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SQLiteStore
from sqlspec.migrations.schema import SchemaTarget, ensure_schema_sync


class AdditiveSQLiteStore(SQLiteStore):
    """Session store target with one additive column."""

    def _table_ddl(self) -> str:
        return super()._table_ddl().replace("expires_at REAL\n", "expires_at REAL,\n            schema_tag TEXT\n")


class AdditiveSqliteADKStore(SqliteADKStore):
    """ADK target with one additive session column."""

    def _sessions_table_ddl(self) -> str:
        return super()._sessions_table_ddl().replace(
            "update_time REAL NOT NULL\n", "update_time REAL NOT NULL,\n            schema_tag TEXT\n"
        )


class AdditiveSqliteADKMemoryStore(SqliteADKMemoryStore):
    """ADK memory target with one additive column."""

    def _memory_table_ddl(self) -> str:
        return super()._memory_table_ddl().replace(
            "inserted_at REAL NOT NULL\n", "inserted_at REAL NOT NULL,\n            schema_tag TEXT\n"
        )


class AdditiveSqliteEventQueueStore(SqliteEventQueueStore):
    """Event queue target with one additive column."""

    def _table_ddl(self) -> str:
        return super()._table_ddl().replace(
            "acknowledged_at TIMESTAMP", "acknowledged_at TIMESTAMP, schema_tag TEXT"
        )


async def test_litestar_session_store_reconcile_uses_target_ddl(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "litestar.db")},
        extension_config={"litestar": {"session_table": "sessions"}},
    )
    await SQLiteStore(config).create_table()

    store = AdditiveSQLiteStore(config)
    await store.reconcile_schema(assume_existing=True)

    with config.provide_session() as driver:
        columns = driver.data_dictionary.get_columns(driver, "sessions")
    assert "schema_tag" in {str(column["column_name"]).casefold() for column in columns}


def test_adk_additive_column_does_not_require_schema_version_seed(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "adk.db")},
        extension_config={"adk": {}},
    )
    SqliteADKStore(config).create_tables()

    store = AdditiveSqliteADKStore(config)
    store.reconcile_schema(assume_existing=True)

    with config.provide_session() as driver:
        columns = driver.data_dictionary.get_columns(driver, store.session_table)
        seed_rows = driver.select(
            f"SELECT value FROM {store.metadata_table} WHERE key = ?",
            ("schema_version",),
        )
    assert "schema_tag" in {str(column["column_name"]).casefold() for column in columns}
    assert seed_rows == []


def test_adk_memory_additive_column_uses_target_ddl(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "adk-memory.db")},
        extension_config={"adk": {}},
    )
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
        "queue_jobs",
        "CREATE TABLE IF NOT EXISTS queue_jobs (id TEXT PRIMARY KEY, payload BLOB)",
        dialect="sqlite",
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
