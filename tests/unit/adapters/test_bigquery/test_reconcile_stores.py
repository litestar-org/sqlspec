"""Unit tests for BigQuery Litestar and event queue store schema reconciliation."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.litestar.store import BigQueryStore
from sqlspec.config import ExtensionConfigs
from sqlspec.exceptions import MigrationError
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.utils import validate_builtin_reconcile_adapter


def _bigquery_litestar_config(
    litestar_settings: dict[str, Any] | None = None, *, dataset_id: str | None = None
) -> BigQueryConfig:
    ext: ExtensionConfigs = {
        "litestar": litestar_settings or {"session_table": "litestar_session", "migration_mode": "reconcile"}
    }
    conn: dict[str, Any] = {"project": "test-project"}
    if dataset_id is not None:
        conn["dataset_id"] = f"test-project.{dataset_id}" if "." not in dataset_id else dataset_id
    return BigQueryConfig(connection_config=conn, extension_config=ext)


def test_bigquery_reconcile_guard_accepts_bigquery() -> None:
    """validate_builtin_reconcile_adapter accepts BigQueryConfig."""
    config = _bigquery_litestar_config()
    assert validate_builtin_reconcile_adapter(config) == "bigquery"


def test_bigquery_litestar_reconcile_adds_nullable_column_and_has_no_companions() -> None:
    """Missing nullable column is added via ALTER TABLE ADD COLUMN and companion indexes are empty."""
    config = _bigquery_litestar_config({
        "session_table": "sess",
        "partitioning": True,
        "partition_expiration_days": 7,
        "require_partition_filter": True,
        "migration_mode": "reconcile",
    })
    store = BigQueryStore(config)
    assert store.get_companion_indexes() == []

    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "sess"}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "session_id", "data_type": "STRING", "is_nullable": "NO"},
        {"column_name": "data", "data_type": "BYTES", "is_nullable": "NO"},
        {"column_name": "created_at", "data_type": "TIMESTAMP", "is_nullable": "YES"},
    ]

    assert litestar_reconcile(MigrationContext(config=config, dialect="bigquery", driver=driver)) is None

    assert driver.execute.call_count == 1
    executed_stmt = driver.execute.call_args.args[0]
    sql = executed_stmt.sql if hasattr(executed_stmt, "sql") else str(executed_stmt)
    assert "ALTER TABLE" in sql
    assert "ADD COLUMN `expires_at` TIMESTAMP" in sql
    driver.commit.assert_not_called()

    qualified_cfg = _bigquery_litestar_config(
        {"session_table": "sess", "migration_mode": "reconcile"}, dataset_id="analytics"
    )
    qualified_target = BigQueryStore(qualified_cfg).get_schema_targets()[0]
    assert qualified_target.schema == "analytics"
    assert qualified_target.table_name == "sess"


def test_bigquery_litestar_reconcile_refuses_not_null_and_defaulted_columns() -> None:
    """Missing NOT NULL or DEFAULT columns fail strict reconciliation with the migration remedy."""
    config = _bigquery_litestar_config({"session_table": "sess", "migration_mode": "reconcile"})

    missing_data_driver = MagicMock()
    missing_data_driver.data_dictionary.get_tables.return_value = [{"table_name": "sess"}]
    missing_data_driver.data_dictionary.get_columns.return_value = [
        {"column_name": "session_id", "data_type": "STRING", "is_nullable": "NO"},
        {"column_name": "expires_at", "data_type": "TIMESTAMP", "is_nullable": "YES"},
        {"column_name": "created_at", "data_type": "TIMESTAMP", "is_nullable": "YES"},
    ]
    with pytest.raises(MigrationError, match="data") as data_exc:
        litestar_reconcile(MigrationContext(config=config, dialect="bigquery", driver=missing_data_driver))
    assert 'migration_mode="versioned"' in str(data_exc.value)

    missing_default_driver = MagicMock()
    missing_default_driver.data_dictionary.get_tables.return_value = [{"table_name": "sess"}]
    missing_default_driver.data_dictionary.get_columns.return_value = [
        {"column_name": "session_id", "data_type": "STRING", "is_nullable": "NO"},
        {"column_name": "data", "data_type": "BYTES", "is_nullable": "NO"},
        {"column_name": "expires_at", "data_type": "TIMESTAMP", "is_nullable": "YES"},
    ]
    with pytest.raises(MigrationError, match="created_at") as created_exc:
        litestar_reconcile(MigrationContext(config=config, dialect="bigquery", driver=missing_default_driver))
    assert 'migration_mode="versioned"' in str(created_exc.value)
