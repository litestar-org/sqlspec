# pyright: reportPrivateUsage=false
"""Unit tests for mysql-connector Litestar session store DDL configuration."""

from unittest.mock import MagicMock

from sqlspec.adapters.mysqlconnector.litestar import MysqlConnectorAsyncStore, MysqlConnectorSyncStore

DEFAULT_ENGINE_CLAUSE = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"


def _mock_config(litestar_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"litestar": litestar_config or {}}
    return config


def test_mysqlconnector_async_store_default_ddl_unchanged() -> None:
    """Without table_options, the emitted DDL keeps today's exact ENGINE clause."""
    store = MysqlConnectorAsyncStore(_mock_config())

    ddl = store._table_ddl()

    assert ddl.strip().endswith(DEFAULT_ENGINE_CLAUSE)


def test_mysqlconnector_async_store_honors_table_options() -> None:
    """A configured table_options value is interpolated into the CREATE TABLE DDL."""
    store = MysqlConnectorAsyncStore(_mock_config({"table_options": "COMMENT='litestar-session'"}))

    ddl = store._table_ddl()

    assert f"{DEFAULT_ENGINE_CLAUSE} COMMENT='litestar-session'" in ddl


def test_mysqlconnector_sync_store_default_ddl_unchanged() -> None:
    """Without table_options, the emitted DDL keeps today's exact ENGINE clause."""
    store = MysqlConnectorSyncStore(_mock_config())

    ddl = store._table_ddl()

    assert ddl.strip().endswith(DEFAULT_ENGINE_CLAUSE)


def test_mysqlconnector_sync_store_honors_table_options() -> None:
    """A configured table_options value is interpolated into the CREATE TABLE DDL."""
    store = MysqlConnectorSyncStore(_mock_config({"table_options": "COMMENT='litestar-session'"}))

    ddl = store._table_ddl()

    assert f"{DEFAULT_ENGINE_CLAUSE} COMMENT='litestar-session'" in ddl
