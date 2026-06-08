"""Unit tests for pool logging across adapters.

Note: Tests access private/protected attributes (_pool_id, _database_name, _ADAPTER_NAME)
intentionally to verify internal logging implementation details.
"""

import logging

import pytest

from sqlspec.utils.logging import POOL_LOGGER_NAME


def test_pool_logger_name_constant() -> None:
    """Test that POOL_LOGGER_NAME constant is correctly defined."""
    assert POOL_LOGGER_NAME == "sqlspec.pool"


def test_pool_logger_independent_configuration() -> None:
    """Test that sqlspec.pool can be configured independently from sqlspec root."""
    root_logger = logging.getLogger("sqlspec")
    pool_logger = logging.getLogger(POOL_LOGGER_NAME)
    original_root_level = root_logger.level
    original_pool_level = pool_logger.level
    try:
        root_logger.setLevel(logging.WARNING)
        pool_logger.setLevel(logging.DEBUG)
        assert pool_logger.isEnabledFor(logging.DEBUG)
        assert not root_logger.isEnabledFor(logging.INFO)
    finally:
        root_logger.setLevel(original_root_level)
        pool_logger.setLevel(original_pool_level)


def test_sqlite_pool_logging_sqlite_pool_uses_pool_logger() -> None:
    """Test that SQLite pool imports and uses POOL_LOGGER_NAME."""
    from sqlspec.adapters.sqlite.pool import _ADAPTER_NAME
    from sqlspec.adapters.sqlite.pool import POOL_LOGGER_NAME as sqlite_pool_logger_name

    assert sqlite_pool_logger_name == "sqlspec.pool"
    assert _ADAPTER_NAME == "sqlite"


def test_sqlite_pool_logging_sqlite_pool_has_pool_id() -> None:
    """Test that SQLite pool generates a pool_id."""
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    pool = SqliteConnectionPool({"database": ":memory:"})
    assert hasattr(pool, "_pool_id")
    assert len(pool._pool_id) == 8


def test_sqlite_pool_logging_sqlite_pool_database_name_property() -> None:
    """Test that SQLite pool has _database_name property for logging."""
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    pool = SqliteConnectionPool({"database": ":memory:"})
    assert pool._database_name == ":memory:"
    pool2 = SqliteConnectionPool({"database": "/tmp/test.db"})
    assert pool2._database_name == "/tmp/test.db"


def test_pymysql_pool_logging_pymysql_pool_uses_pool_logger() -> None:
    """Test that PyMySQL pool imports and uses POOL_LOGGER_NAME."""
    from sqlspec.adapters.pymysql.pool import _ADAPTER_NAME
    from sqlspec.adapters.pymysql.pool import POOL_LOGGER_NAME as pymysql_pool_logger_name

    assert pymysql_pool_logger_name == "sqlspec.pool"
    assert _ADAPTER_NAME == "pymysql"


def test_pymysql_pool_logging_pymysql_pool_has_pool_id() -> None:
    """Test that PyMySQL pool generates a pool_id."""
    from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool

    pool = PyMysqlConnectionPool({"database": "test_db"})
    assert hasattr(pool, "_pool_id")
    assert len(pool._pool_id) == 8


def test_pymysql_pool_logging_pymysql_pool_database_name_property() -> None:
    """Test that PyMySQL pool has _database_name property for logging."""
    from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool

    pool = PyMysqlConnectionPool({"database": "production_db"})
    assert pool._database_name == "production_db"
    pool2 = PyMysqlConnectionPool({})
    assert pool2._database_name == "unknown"


def test_duck_db_pool_logging_duckdb_pool_uses_pool_logger() -> None:
    """Test that DuckDB pool imports and uses POOL_LOGGER_NAME."""
    from sqlspec.adapters.duckdb.pool import _ADAPTER_NAME
    from sqlspec.adapters.duckdb.pool import POOL_LOGGER_NAME as duckdb_pool_logger_name

    assert duckdb_pool_logger_name == "sqlspec.pool"
    assert _ADAPTER_NAME == "duckdb"


def test_duck_db_pool_logging_duckdb_pool_has_pool_id() -> None:
    """Test that DuckDB pool generates a pool_id."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    pool = DuckDBConnectionPool({"database": ":memory:"})
    assert hasattr(pool, "_pool_id")
    assert len(pool._pool_id) == 8


def test_duck_db_pool_logging_duckdb_pool_database_name_property() -> None:
    """Test that DuckDB pool has _database_name property for logging."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    pool = DuckDBConnectionPool({})
    assert pool._database_name == ":memory:"
    pool2 = DuckDBConnectionPool({"database": ":memory:"})
    assert pool2._database_name == ":memory:"
    pool3 = DuckDBConnectionPool({"database": "/tmp/test.duckdb"})
    assert pool3._database_name == "/tmp/test.duckdb"


async def test_aiosqlite_connection_pool_logging_aiosqlite_pool_uses_pool_logger() -> None:
    """Test that aiosqlite pool imports and uses POOL_LOGGER_NAME."""
    from sqlspec.adapters.aiosqlite.pool import _ADAPTER_NAME
    from sqlspec.adapters.aiosqlite.pool import POOL_LOGGER_NAME as aiosqlite_pool_logger_name

    assert aiosqlite_pool_logger_name == "sqlspec.pool"
    assert _ADAPTER_NAME == "aiosqlite"


async def test_aiosqlite_connection_pool_logging_aiosqlite_pool_has_pool_id() -> None:
    """Test that aiosqlite pool generates a pool_id."""
    from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool

    pool = AiosqliteConnectionPool({"database": ":memory:"})
    assert hasattr(pool, "_pool_id")
    assert len(pool._pool_id) == 8


async def test_aiosqlite_connection_pool_logging_aiosqlite_pool_database_name_property() -> None:
    """Test that aiosqlite pool has _database_name property for logging."""
    from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool

    pool = AiosqliteConnectionPool({"database": ":memory:"})
    assert pool._database_name == ":memory:"
    pool2 = AiosqliteConnectionPool({"database": "/tmp/test.db"})
    assert pool2._database_name == "test.db"


def test_pool_logging_messages_pool_recycle_message_format(caplog: pytest.LogCaptureFixture) -> None:
    """Test that pool recycle logs use structured message format."""
    import time

    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    caplog.set_level(logging.DEBUG, logger="sqlspec.pool")
    pool = SqliteConnectionPool({"database": ":memory:"}, recycle_seconds=1)
    with pool.get_connection():
        pass
    pool._thread_local.created_at = time.time() - 2
    with pool.get_connection():
        pass
    recycle_records = [r for r in caplog.records if "pool.connection.recycle" in r.getMessage()]
    assert len(recycle_records) >= 1
    record = recycle_records[0]
    extra = record.__dict__.get("extra_fields", {})
    assert extra.get("adapter") == "sqlite"
    assert "pool_id" in extra
    assert "database" in extra
    assert extra.get("reason") == "exceeded_recycle_time"
