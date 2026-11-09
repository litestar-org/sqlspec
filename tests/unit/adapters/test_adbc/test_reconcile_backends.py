"""Unit tests for ADBC multi-backend extension schema reconciliation."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

pytest.importorskip("adbc_driver_manager")

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore
from sqlspec.adapters.adbc.litestar.store import AdbcStore
from sqlspec.core import StatementConfig
from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.context import MigrationContext


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
    ],
)
def test_adbc_litestar_procedural_and_catalog_dialects_probe_before_repair(
    connection_config: dict[str, Any], expected_table: str, expected_index: str, ddl_marker: str
) -> None:
    """Oracle, Db2, and SQL Server return one catalog-probed companion index."""
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
