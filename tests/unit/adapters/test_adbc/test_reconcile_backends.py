"""Unit tests for ADBC multi-backend extension schema reconciliation."""

import contextlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("adbc_driver_manager")

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk.store import AdbcADKMemoryStore, AdbcADKStore
from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore
from sqlspec.adapters.adbc.litestar.store import AdbcStore
from sqlspec.core import StatementConfig
from sqlspec.extensions.adk.migrations.reconcile import reconcile as adk_reconcile
from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.schema import SchemaTarget


@pytest.mark.parametrize(
    ("connection_config", "expected_table", "expected_index"),
    [
        ({"uri": "postgresql://localhost:5432/app"}, "litestar_session", "idx_litestar_session_expires_at"),
        ({"driver_name": "sqlite", "uri": ":memory:"}, "litestar_session", "idx_litestar_session_expires_at"),
        ({"driver_name": "duckdb", "path": ":memory:"}, "litestar_session", "idx_litestar_session_expires_at"),
    ],
)
def test_adbc_litestar_standalone_index_dialects_extract_companion_index(
    connection_config: dict[str, Any], expected_table: str, expected_index: str
) -> None:
    """Standalone CREATE INDEX dialects extract one companion index from store DDL."""
    config = AdbcConfig(connection_config=connection_config)
    store = AdbcStore(config)

    targets = store.get_schema_targets()
    assert len(targets) == 1
    assert [col.name for col in targets[0].create_table.columns] == ["session_id", "data", "expires_at"]

    indexes = store.get_companion_indexes()
    assert len(indexes) == 1
    companion = indexes[0]
    assert companion.name == expected_index
    assert companion.table == expected_table
    assert companion.schema is None
    assert "CREATE INDEX IF NOT EXISTS" in companion.ddl

    missing_driver = MagicMock()
    missing_driver.data_dictionary.get_indexes.return_value = []
    store.reconcile_companion_objects_sync(missing_driver)
    missing_driver.execute_script.assert_called_once_with(companion.ddl)

    present_driver = MagicMock()
    present_driver.data_dictionary.get_indexes.return_value = [
        {"index_name": expected_index, "table_name": expected_table}
    ]
    store.reconcile_companion_objects_sync(present_driver)
    present_driver.execute_script.assert_not_called()


@pytest.mark.parametrize(
    ("connection_config", "expected_table", "expected_index", "ddl_marker"),
    [
        (
            {"uri": "oracle://localhost:1521/FREEPDB1"},
            "litestar_session",
            "idx_litestar_session_expires_at",
            "SQLCODE NOT IN (-955, -1408)",
        ),
        (
            {"uri": "db2://localhost:50000/testdb"},
            "LITESTAR_SESSION",
            "IDX_LITESTAR_SESSION_EXPIRES_AT",
            "CREATE INDEX idx_litestar_session_expires_at ON litestar_session(expires_at)",
        ),
        (
            {"driver_name": "mssql", "uri": "mssql://localhost:1433/app"},
            "litestar_session",
            "idx_litestar_session_expires_at",
            "SELECT 1 FROM sys.indexes",
        ),
        (
            {"uri": "teradata://localhost:1025/app"},
            "LITESTAR_SESSION",
            "idx_litestar_session_expires_at",
            "CREATE INDEX idx_litestar_session_expires_at (expires_at) ON litestar_session",
        ),
    ],
)
def test_adbc_litestar_procedural_and_catalog_dialects_probe_before_repair(
    connection_config: dict[str, Any], expected_table: str, expected_index: str, ddl_marker: str
) -> None:
    """Oracle, Db2, SQL Server, and Teradata return one catalog-probed companion index."""
    config = AdbcConfig(connection_config=connection_config)
    store = AdbcStore(config)

    targets = store.get_schema_targets()
    assert len(targets) == 1
    assert [col.name.lower() for col in targets[0].create_table.columns] == ["session_id", "data", "expires_at"]

    indexes = store.get_companion_indexes()
    assert len(indexes) == 1
    companion = indexes[0]
    assert companion.name == expected_index
    assert companion.table == expected_table
    assert companion.schema is None
    assert ddl_marker in companion.ddl

    missing_driver = MagicMock()
    missing_driver.data_dictionary.get_indexes.return_value = []
    store.reconcile_companion_objects_sync(missing_driver)
    missing_driver.data_dictionary.get_indexes.assert_called_once_with(
        missing_driver, table=expected_table, schema=None
    )
    missing_driver.execute_script.assert_called_once_with(companion.ddl)

    present_driver = MagicMock()
    present_driver.data_dictionary.get_indexes.return_value = [
        {"index_name": expected_index, "table_name": expected_table}
    ]
    store.reconcile_companion_objects_sync(present_driver)
    present_driver.execute_script.assert_not_called()


@pytest.mark.parametrize(
    "connection_config",
    [
        {"driver_name": "bigquery", "project_id": "demo-proj", "dataset_id": "demo_ds"},
        {"driver_name": "snowflake", "uri": "snowflake://acct/db/public"},
    ],
)
def test_adbc_litestar_no_index_dialects_return_empty_companions(connection_config: dict[str, Any]) -> None:
    """BigQuery and Snowflake session stores omit secondary companion indexes."""
    config = AdbcConfig(connection_config=connection_config)
    store = AdbcStore(config)

    targets = store.get_schema_targets()
    assert len(targets) == 1
    assert [col.name.lower() for col in targets[0].create_table.columns] == ["session_id", "data", "expires_at"]
    assert store.get_companion_indexes() == []

    driver = MagicMock()
    store.reconcile_companion_objects_sync(driver)
    driver.data_dictionary.get_indexes.assert_not_called()
    driver.execute_script.assert_not_called()


def test_adbc_litestar_prepare_schema_honors_flightsql_explicit_dialect_and_gizmosql_backend() -> None:
    """FlightSQL explicit statement dialect and GizmoSQL backend override transport fallback dialect."""
    flight_pg = AdbcConfig(
        connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://localhost:8815"},
        statement_config=StatementConfig(dialect="postgres"),
    )
    flight_store = AdbcStore(flight_pg)
    flight_store.prepare_schema_sync(SimpleNamespace(dialect="sqlite"))

    flight_targets = flight_store.get_schema_targets()
    assert "BYTEA" in str(flight_targets[0].create_statement)
    assert len(flight_store.get_companion_indexes()) == 1

    gizmo_duck = AdbcConfig(
        connection_config={
            "driver_name": "adbc_driver_flightsql",
            "uri": "grpc+tls://localhost:31337",
            "gizmosql_backend": "duckdb",
        }
    )
    gizmo_store = AdbcStore(gizmo_duck)
    gizmo_store.prepare_schema_sync(SimpleNamespace(dialect="sqlite"))

    gizmo_targets = gizmo_store.get_schema_targets()
    assert "VARCHAR PRIMARY KEY" in str(gizmo_targets[0].create_statement)
    assert len(gizmo_store.get_companion_indexes()) == 1


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
def test_adbc_local_backends_reconcile_and_repair_dropped_indexes_end_to_end(backend: str, tmp_path: Path) -> None:
    """SQLite and DuckDB ADBC configs create and repair Litestar and Events companion indexes."""
    if backend == "sqlite":
        pytest.importorskip("adbc_driver_sqlite")
        conn_cfg: dict[str, Any] = {"driver_name": "sqlite", "uri": str(tmp_path / "reconcile.sqlite")}
    else:
        pytest.importorskip("adbc_driver_duckdb")
        conn_cfg = {"driver_name": "duckdb", "path": str(tmp_path / "reconcile.duckdb")}

    config = AdbcConfig(
        connection_config=conn_cfg,
        extension_config={"litestar": {"migration_mode": "reconcile"}, "events": {"migration_mode": "reconcile"}},
    )

    with config.provide_session() as driver:
        litestar_reconcile(MigrationContext(config=config, driver=driver))
        events_reconcile(MigrationContext(config=config, driver=driver))
        driver.commit()

        session_indexes = {
            row["index_name"] for row in driver.data_dictionary.get_indexes(driver, table="litestar_session")
        }
        queue_indexes = {
            row["index_name"] for row in driver.data_dictionary.get_indexes(driver, table="sqlspec_event_queue")
        }
        assert "idx_litestar_session_expires_at" in session_indexes
        assert "idx_sqlspec_event_queue_channel_status" in queue_indexes

        driver.execute("DROP INDEX idx_litestar_session_expires_at")
        driver.execute("DROP INDEX idx_sqlspec_event_queue_channel_status")
        driver.commit()

        assert "idx_litestar_session_expires_at" not in {
            row["index_name"] for row in driver.data_dictionary.get_indexes(driver, table="litestar_session")
        }

        litestar_reconcile(MigrationContext(config=config, driver=driver))
        events_reconcile(MigrationContext(config=config, driver=driver))
        driver.commit()

        restored_session_indexes = {
            row["index_name"] for row in driver.data_dictionary.get_indexes(driver, table="litestar_session")
        }
        restored_queue_indexes = {
            row["index_name"] for row in driver.data_dictionary.get_indexes(driver, table="sqlspec_event_queue")
        }
        assert "idx_litestar_session_expires_at" in restored_session_indexes
        assert "idx_sqlspec_event_queue_channel_status" in restored_queue_indexes

    event_store = AdbcEventQueueStore(config)
    assert event_store.dialect == backend


@pytest.mark.parametrize(
    ("connection_config", "expected_store_dialect"),
    [
        ({"uri": "postgresql://localhost:5432/app"}, "postgresql"),
        ({"driver_name": "sqlite", "uri": ":memory:"}, "sqlite"),
        ({"driver_name": "duckdb", "path": ":memory:"}, "duckdb"),
        ({"driver_name": "bigquery", "project_id": "demo-proj", "dataset_id": "demo_ds"}, "bigquery"),
        ({"driver_name": "snowflake", "uri": "snowflake://acct/db/public"}, "snowflake"),
        ({"uri": "oracle://localhost:1521/FREEPDB1"}, "oracle"),
        ({"uri": "db2://localhost:50000/testdb"}, "db2"),
        ({"driver_name": "mssql", "uri": "mssql://localhost:1433/app"}, "mssql"),
        ({"uri": "teradata://localhost:1025/app"}, "teradata"),
    ],
)
def test_adbc_adk_targets_parse_for_every_classified_dialect(
    connection_config: dict[str, Any], expected_store_dialect: str
) -> None:
    """Session, event, state, metadata, and memory SchemaTargets parse on every classified ADBC dialect."""
    config = AdbcConfig(connection_config=connection_config, extension_config={"adk": {"enable_memory": True}})
    session_store = AdbcADKStore(config)
    memory_store = AdbcADKMemoryStore(config)

    assert session_store.dialect == expected_store_dialect
    assert memory_store.dialect == expected_store_dialect

    session_targets = session_store.get_schema_targets_sync(None)
    assert len(session_targets) == 5
    target_columns = {
        target.table_name.lower(): {col.name.lower() for col in target.create_table.columns}
        for target in session_targets
    }
    assert {"id", "app_name", "user_id", "state", "create_time", "update_time"} <= target_columns["adk_session"]
    assert {"id", "app_name", "user_id", "session_id", "invocation_id", "timestamp", "event_data"} <= target_columns[
        "adk_event"
    ]
    assert {"app_name", "state", "update_time"} <= target_columns["adk_app_state"]
    assert {"app_name", "user_id", "state", "update_time"} <= target_columns["adk_user_state"]
    assert {"key", "value"} <= target_columns["adk_internal_metadata"]

    memory_targets = memory_store.get_schema_targets_sync(None)
    assert len(memory_targets) == 1
    memory_cols = {col.name.lower() for col in memory_targets[0].create_table.columns}
    assert {
        "id",
        "session_id",
        "app_name",
        "user_id",
        "scope",
        "event_id",
        "timestamp",
        "content_json",
        "content_text",
        "inserted_at",
    } <= memory_cols


@pytest.mark.parametrize(
    ("connection_config", "expected_parse_dialect"),
    [
        ({"uri": "postgresql://localhost:5432/app"}, "postgres"),
        ({"driver_name": "mssql", "uri": "mssql://localhost:1433/app"}, "tsql"),
    ],
)
def test_adbc_adk_internal_dialect_names_forward_postgres_and_tsql_to_from_ddl(
    connection_config: dict[str, Any], expected_parse_dialect: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PostgreSQL and SQL Server ADK stores pass postgres and tsql to SchemaTarget.from_ddl."""
    observed_dialects: list[Any] = []
    original_from_ddl = SchemaTarget.from_ddl

    def spy_from_ddl(
        table_name: str, create_statement: str, *, schema: str | None = None, dialect: Any = None
    ) -> SchemaTarget:
        observed_dialects.append(dialect)
        return original_from_ddl(table_name, create_statement, schema=schema, dialect=dialect)

    monkeypatch.setattr(SchemaTarget, "from_ddl", spy_from_ddl)

    config = AdbcConfig(connection_config=connection_config, extension_config={"adk": {"enable_memory": True}})
    session_targets = AdbcADKStore(config).get_schema_targets_sync(None)
    memory_targets = AdbcADKMemoryStore(config).get_schema_targets_sync(None)

    assert len(session_targets) == 5
    assert len(memory_targets) == 1
    assert observed_dialects == [expected_parse_dialect] * 6


def test_adbc_adk_db2_targets_parse_with_default_and_preserve_raw_create_statement() -> None:
    """Db2 ADK targets parse WITH DEFAULT columns while keeping WITH DEFAULT in create_statement."""
    config = AdbcConfig(
        connection_config={"uri": "db2://localhost:50000/testdb"}, extension_config={"adk": {"enable_memory": True}}
    )
    session_targets = AdbcADKStore(config).get_schema_targets_sync(None)
    session_target = session_targets[0]
    session_col_names = {col.name.lower() for col in session_target.create_table.columns}

    assert session_target.table_name == "ADK_SESSION"
    assert "create_time" in session_col_names
    assert "update_time" in session_col_names
    assert isinstance(session_target.create_statement, str)
    assert "WITH DEFAULT CURRENT TIMESTAMP" in session_target.create_statement

    memory_target = AdbcADKMemoryStore(config).get_schema_targets_sync(None)[0]
    memory_col_names = {col.name.lower() for col in memory_target.create_table.columns}
    assert memory_target.table_name == "ADK_MEMORY"
    assert "scope" in memory_col_names
    assert "inserted_at" in memory_col_names
    assert isinstance(memory_target.create_statement, str)
    assert "WITH DEFAULT 'user'" in memory_target.create_statement
    assert "WITH DEFAULT CURRENT TIMESTAMP" in memory_target.create_statement


def test_adbc_adk_postgresql_full_text_index_emitted_when_absent_and_skipped_when_present() -> None:
    """PostgreSQL memory companion hook emits one GIN index statement when idx_adk_memory_fts is absent."""
    config = AdbcConfig(
        connection_config={"uri": "postgresql://localhost:5432/app"},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": True, "vector_index_type": "none"}},
    )
    store = AdbcADKMemoryStore(config)
    base_indexes = [
        {"index_name": "idx_adk_memory_app_scope_user_time", "table_name": "adk_memory"},
        {"index_name": "idx_adk_memory_scope", "table_name": "adk_memory"},
        {"index_name": "idx_adk_memory_session", "table_name": "adk_memory"},
    ]

    missing_fts_driver = MagicMock()
    missing_fts_driver.data_dictionary.get_indexes.return_value = list(base_indexes)
    store.reconcile_companion_objects_sync(missing_fts_driver)

    executed = [str(call.args[0]) for call in missing_fts_driver.execute.call_args_list]
    assert executed == [
        "CREATE INDEX IF NOT EXISTS idx_adk_memory_fts ON adk_memory USING GIN (to_tsvector('english', content_text))"
    ]
    missing_fts_driver.commit.assert_not_called()

    present_fts_driver = MagicMock()
    present_fts_driver.data_dictionary.get_indexes.return_value = [
        *base_indexes,
        {"index_name": "idx_adk_memory_fts", "table_name": "adk_memory"},
    ]
    store.reconcile_companion_objects_sync(present_fts_driver)
    present_fts_driver.execute.assert_not_called()


def test_adbc_adk_flightsql_explicit_dialect_and_gizmosql_backend_select_matching_ddl() -> None:
    """FlightSQL explicit statement dialect and GizmoSQL backend resolve matching ADK store DDL."""
    flight_pg = AdbcConfig(
        connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://localhost:8815"},
        statement_config=StatementConfig(dialect="postgres"),
        extension_config={"adk": {"enable_memory": True}},
    )
    pg_session_store = AdbcADKStore(flight_pg)
    pg_memory_store = AdbcADKMemoryStore(flight_pg)
    assert pg_session_store.dialect == "postgresql"
    assert pg_memory_store.dialect == "postgresql"
    assert "JSONB" in str(pg_session_store.get_schema_targets_sync(None)[0].create_statement)
    assert "JSONB" in str(pg_memory_store.get_schema_targets_sync(None)[0].create_statement)

    gizmo_duck = AdbcConfig(
        connection_config={
            "driver_name": "adbc_driver_flightsql",
            "uri": "grpc+tls://localhost:31337",
            "gizmosql_backend": "duckdb",
        },
        extension_config={"adk": {"enable_memory": True}},
    )
    duck_session_store = AdbcADKStore(gizmo_duck)
    duck_memory_store = AdbcADKMemoryStore(gizmo_duck)
    assert duck_session_store.dialect == "duckdb"
    assert duck_memory_store.dialect == "duckdb"
    assert "state JSON NOT NULL" in str(duck_session_store.get_schema_targets_sync(None)[0].create_statement)
    assert "content_json JSON NOT NULL" in str(duck_memory_store.get_schema_targets_sync(None)[0].create_statement)


@pytest.mark.parametrize(
    ("connection_config", "ddl_marker"),
    [
        ({"uri": "oracle://localhost:1521/FREEPDB1"}, "SQLCODE NOT IN (-955, -1408)"),
        ({"uri": "db2://localhost:50000/testdb"}, "CREATE INDEX idx_adk_session_app_user ON adk_session"),
        ({"driver_name": "mssql", "uri": "mssql://localhost:1433/app"}, "SELECT 1 FROM sys.indexes"),
        ({"driver_name": "duckdb", "path": ":memory:"}, "CREATE INDEX IF NOT EXISTS idx_adk_session_app_user"),
        (
            {"uri": "teradata://localhost:1025/app"},
            "CREATE INDEX idx_adk_session_app_user (app_name, user_id) ON adk_session",
        ),
    ],
)
def test_adbc_adk_companion_repair_probes_catalog_on_non_sqlite_indexed_backends(
    connection_config: dict[str, Any], ddl_marker: str
) -> None:
    """Non-SQLite indexed backends probe get_indexes before creating missing ADK companion indexes."""
    config = AdbcConfig(connection_config=connection_config, extension_config={"adk": {"enable_memory": True}})
    session_store = AdbcADKStore(config)
    memory_store = AdbcADKMemoryStore(config)

    missing_driver = MagicMock()
    missing_driver.data_dictionary.get_indexes.return_value = []
    session_store.reconcile_companion_objects_sync(missing_driver)
    memory_store.reconcile_companion_objects_sync(missing_driver)

    executed = [str(call.args[0]) for call in missing_driver.execute.call_args_list]
    assert len(executed) == 6
    assert any(ddl_marker in sql for sql in executed)
    missing_driver.commit.assert_not_called()

    present_driver = MagicMock()
    present_driver.data_dictionary.get_indexes.side_effect = [
        [{"index_name": "idx_adk_session_app_user"}, {"index_name": "idx_adk_session_update_time"}],
        [{"index_name": "idx_adk_event_session"}],
        [
            {"index_name": "idx_adk_memory_app_scope_user_time"},
            {"index_name": "idx_adk_memory_scope"},
            {"index_name": "idx_adk_memory_session"},
        ],
    ]
    session_store.reconcile_companion_objects_sync(present_driver)
    memory_store.reconcile_companion_objects_sync(present_driver)
    present_driver.execute.assert_not_called()


@pytest.mark.parametrize(
    "connection_config",
    [
        {"driver_name": "bigquery", "project_id": "demo-proj", "dataset_id": "demo_ds"},
        {"driver_name": "snowflake", "uri": "snowflake://acct/db/public"},
    ],
)
def test_adbc_adk_warehouse_backends_skip_companion_indexes(connection_config: dict[str, Any]) -> None:
    """BigQuery and Snowflake ADK stores skip index metadata queries and index creation."""
    config = AdbcConfig(connection_config=connection_config, extension_config={"adk": {"enable_memory": True}})
    driver = MagicMock()
    AdbcADKStore(config).reconcile_companion_objects_sync(driver)
    AdbcADKMemoryStore(config).reconcile_companion_objects_sync(driver)
    driver.data_dictionary.get_indexes.assert_not_called()
    driver.execute.assert_not_called()


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
def test_adbc_adk_local_backends_migrate_up_and_repair_dropped_indexes(backend: str, tmp_path: Path) -> None:
    """SQLite and DuckDB ADBC configs persist ADK tables and repair dropped companion indexes via migrate_up."""
    if backend == "sqlite":
        pytest.importorskip("adbc_driver_sqlite")
        conn_cfg: dict[str, Any] = {"driver_name": "sqlite", "uri": str(tmp_path / "adk_reconcile.sqlite")}
    else:
        pytest.importorskip("adbc_driver_duckdb")
        conn_cfg = {"driver_name": "duckdb", "path": str(tmp_path / "adk_reconcile.duckdb")}

    config = AdbcConfig(
        connection_config=conn_cfg,
        migration_config={"script_location": str(tmp_path / "migrations")},
        extension_config={"adk": {"enable_memory": True, "migration_mode": "reconcile"}},
    )
    config.migrate_up()

    with config.provide_session() as driver:
        tables = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
        assert {
            "adk_session",
            "adk_event",
            "adk_app_state",
            "adk_user_state",
            "adk_internal_metadata",
            "adk_memory",
        } <= tables
        driver.execute("DROP INDEX idx_adk_session_app_user")
        driver.execute("DROP INDEX idx_adk_memory_scope")
        driver.commit()

    config.migrate_up()

    with config.provide_session() as driver:
        adk_reconcile(MigrationContext(config=config, driver=driver))
        session_indexes = {row["index_name"] for row in driver.data_dictionary.get_indexes(driver, table="adk_session")}
        memory_indexes = {row["index_name"] for row in driver.data_dictionary.get_indexes(driver, table="adk_memory")}
        assert "idx_adk_session_app_user" in session_indexes
        assert "idx_adk_memory_scope" in memory_indexes


def test_adbc_litestar_store_snowflake_hex_binary_and_uppercase_rows() -> None:
    """Snowflake ADBC session store binds TO_BINARY hex strings and reads uppercase row keys."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_snowflake"})
    store = AdbcStore(config)
    fake_driver = MagicMock()
    fake_driver.dialect = "snowflake"
    fake_driver.select_one.side_effect = [{"COUNT": 0}, {"COUNT": 1}]
    fake_driver.select_one_or_none.side_effect = [
        {"DATA": b"hello".hex(), "EXPIRES_AT": None},
        {"DATA": "plain-text", "EXPIRES_AT": None},
        {"DATA": b"hello".hex(), "EXPIRES_AT": None},
    ]

    @contextlib.contextmanager
    def _fake_session(_self: Any) -> Any:
        yield fake_driver

    with patch.object(AdbcConfig, "provide_session", _fake_session):
        store._set("k1", b"hello")
        store._set("k1", b"world")
        fetched_hex = store._get("k1")
        fetched_plain = store._get("k1")
        ttl = store._expires_in("k1")

    insert_call = fake_driver.execute.call_args_list[0]
    update_call = fake_driver.execute.call_args_list[1]
    assert "TO_BINARY(?, 'HEX')" in str(insert_call.args[0])
    assert insert_call.args[2] == b"hello".hex()
    assert "TO_BINARY(?, 'HEX')" in str(update_call.args[0])
    assert update_call.args[1] == b"world".hex()
    assert fetched_hex == b"hello"
    assert fetched_plain == b"plain-text"
    assert ttl is None

    db2_config = AdbcConfig(connection_config={"uri": "db2://localhost:50000/testdb"})
    db2_store = AdbcStore(db2_config)
    db2_driver = MagicMock()
    db2_driver.dialect = "db2"
    db2_driver.select_one_or_none.return_value = {"DATA": b"persisted-payload".hex().upper(), "EXPIRES_AT": 0}

    @contextlib.contextmanager
    def _fake_db2_session(_self: Any) -> Any:
        yield db2_driver

    with patch.object(AdbcConfig, "provide_session", _fake_db2_session):
        assert db2_store._get("seed-session") == b"persisted-payload"

    select_call = db2_driver.select_one_or_none.call_args_list[0]
    assert (
        "SELECT HEX(CAST(data AS VARCHAR(16336) FOR BIT DATA)) AS data, "
        "CASE WHEN expires_at IS NULL THEN 0 ELSE 1 END AS expires_at"
    ) in str(select_call.args[0])


@pytest.mark.parametrize(
    ("stmt_dialect", "expected_dialect"),
    [
        ("postgres", "postgresql"),
        ("sqlite", "sqlite"),
        ("duckdb", "duckdb"),
        ("bigquery", "bigquery"),
        ("snowflake", "snowflake"),
        ("oracle", "oracle"),
        ("db2", "db2"),
        ("tsql", "mssql"),
        ("teradata", "teradata"),
        ("vantage", "teradata"),
    ],
)
def test_adbc_adk_store_mutated_statement_config_dialect_and_qualified_names(
    stmt_dialect: str, expected_dialect: str
) -> None:
    """ADK store resolves mutated statement_config dialects and schema-qualified table targets."""
    config = AdbcConfig(
        connection_config={},
        statement_config=StatementConfig(dialect=stmt_dialect),
        extension_config={"adk": {"session_table": "adk_sessions", "events_table": "adk_events"}},
    )
    config.connection_config = {}
    store = AdbcADKStore(config)
    assert store.dialect == expected_dialect
    targets = store.get_schema_targets_sync(None)
    assert len(targets) == 5

    unconfigured = AdbcConfig(
        connection_config={"driver_name": "unknown_custom_driver"},
        statement_config=StatementConfig(dialect="unknown_dialect"),
    )
    assert AdbcADKStore(unconfigured).dialect == "generic"
