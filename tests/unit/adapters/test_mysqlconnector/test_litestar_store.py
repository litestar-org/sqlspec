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


def test_mysqlconnector_drop_table_sql_omits_drop_index() -> None:
    """_drop_table_sql must emit only DROP TABLE IF EXISTS without preceding DROP INDEX."""
    async_store = MysqlConnectorAsyncStore(_mock_config())
    sync_store = MysqlConnectorSyncStore(_mock_config())

    assert async_store._drop_table_sql() == ["DROP TABLE IF EXISTS litestar_session"]
    assert sync_store._drop_table_sql() == ["DROP TABLE IF EXISTS litestar_session"]


async def test_mysqlconnector_set_uses_mariadb_compatible_upsert() -> None:
    """set must use VALUES(col) in ON DUPLICATE KEY UPDATE for MySQL and MariaDB compatibility."""
    from unittest.mock import AsyncMock

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.close = AsyncMock()
    conn = MagicMock()
    conn.cursor = AsyncMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn

    store = MysqlConnectorAsyncStore(config)
    await store.set("session-1", b"payload", 300)

    assert cursor.execute.call_count == 1
    sql = cursor.execute.call_args_list[0].args[0]
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "data = VALUES(data)" in sql
    assert "expires_at = VALUES(expires_at)" in sql
    assert "AS new" not in sql


def test_mysqlconnector_sync_create_table_does_not_commit() -> None:
    """Sync _create_table does not call driver.commit() after execute_script."""
    driver = MagicMock()
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=driver)
    session.__exit__ = MagicMock(return_value=None)
    config = _mock_config()
    config.provide_session = lambda *_a, **_k: session

    store = MysqlConnectorSyncStore(config)
    store._create_table()

    assert driver.execute_script.called
    assert not driver.commit.called
