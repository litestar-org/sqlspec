# pyright: reportPrivateUsage=false
"""Unit tests for PyMySQL Litestar session store DDL configuration."""

from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.pymysql.litestar import PyMysqlStore
from sqlspec.exceptions import ImproperConfigurationError

DEFAULT_ENGINE_CLAUSE = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"


def _mock_config(litestar_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"litestar": litestar_config or {}}
    return config


def test_pymysql_litestar_store_default_ddl_unchanged() -> None:
    """Without table_options, the emitted DDL keeps today's exact ENGINE clause."""
    store = PyMysqlStore(_mock_config())

    ddl = store._table_ddl()

    assert ddl.strip().endswith(DEFAULT_ENGINE_CLAUSE)


def test_pymysql_litestar_store_honors_table_options() -> None:
    """A configured table_options value is interpolated into the CREATE TABLE DDL."""
    store = PyMysqlStore(_mock_config({"table_options": "COMMENT='litestar-session'"}))

    ddl = store._table_ddl()

    assert f"{DEFAULT_ENGINE_CLAUSE} COMMENT='litestar-session'" in ddl


def test_pymysql_litestar_store_honors_index_options() -> None:
    """Configured index options are interpolated into the inline expiry index."""
    store = PyMysqlStore(_mock_config({"index_options": "COMMENT 'session-expiry'"}))

    assert "(expires_at) COMMENT 'session-expiry'" in store._table_ddl()


def test_pymysql_litestar_store_rejects_non_string_options() -> None:
    """Explicit option values cannot silently degrade to an empty clause."""
    with pytest.raises(ImproperConfigurationError, match="table_options"):
        PyMysqlStore(_mock_config({"table_options": 42}))


def test_pymysql_litestar_store_drop_table_sql_omits_drop_index() -> None:
    """_drop_table_sql must emit only DROP TABLE IF EXISTS without preceding DROP INDEX."""
    store = PyMysqlStore(_mock_config())
    assert store._drop_table_sql() == ["DROP TABLE IF EXISTS litestar_session"]


def test_pymysql_litestar_store_set_uses_mariadb_compatible_upsert() -> None:
    """_set must use VALUES(col) in ON DUPLICATE KEY UPDATE for MySQL and MariaDB compatibility."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__.return_value = conn
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn

    store = PyMysqlStore(config)
    store._set("session-1", b"payload", 300)

    assert cursor.execute.call_count == 1
    sql = cursor.execute.call_args_list[0].args[0]
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "data = VALUES(data)" in sql
    assert "expires_at = VALUES(expires_at)" in sql
    assert "AS new" not in sql
