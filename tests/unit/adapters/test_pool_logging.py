"""Unit tests for pool logging across adapters.

Note: Tests access private/protected attributes (_pool_id, _database_name, _ADAPTER_NAME)
intentionally to verify internal logging implementation details.
"""
# pyright: reportPrivateUsage=false

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

    # Save original levels
    original_root_level = root_logger.level
    original_pool_level = pool_logger.level

    try:
        # Set sqlspec root to WARNING, but sqlspec.pool to DEBUG
        root_logger.setLevel(logging.WARNING)
        pool_logger.setLevel(logging.DEBUG)

        assert pool_logger.isEnabledFor(logging.DEBUG)

        # Root should not allow INFO
        assert not root_logger.isEnabledFor(logging.INFO)
    finally:
        # Restore original levels to avoid affecting other tests
        root_logger.setLevel(original_root_level)
        pool_logger.setLevel(original_pool_level)


class TestSqlitePoolLogging:
    """Tests for SQLite pool logging structure."""

    def test_sqlite_pool_uses_pool_logger(self) -> None:
        """Test that SQLite pool imports and uses POOL_LOGGER_NAME."""
        from sqlspec.adapters.sqlite.pool import _ADAPTER_NAME
        from sqlspec.adapters.sqlite.pool import POOL_LOGGER_NAME as sqlite_pool_logger_name

        assert sqlite_pool_logger_name == "sqlspec.pool"
        assert _ADAPTER_NAME == "sqlite"

    def test_sqlite_pool_has_pool_id(self) -> None:
        """Test that SQLite pool generates a pool_id."""
        from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

        pool = SqliteConnectionPool({"database": ":memory:"})
        assert hasattr(pool, "_pool_id")
        assert len(pool._pool_id) == 8  # UUID prefix

    def test_sqlite_pool_database_name_property(self) -> None:
        """Test that SQLite pool has _database_name property for logging."""
        from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

        # Memory database
        pool = SqliteConnectionPool({"database": ":memory:"})
        assert pool._database_name == ":memory:"

        # File database
        pool2 = SqliteConnectionPool({"database": "/tmp/test.db"})
        assert pool2._database_name == "/tmp/test.db"


class TestPymysqlPoolLogging:
    """Tests for PyMySQL pool logging structure."""

    def test_pymysql_pool_uses_pool_logger(self) -> None:
        """Test that PyMySQL pool imports and uses POOL_LOGGER_NAME."""
        from sqlspec.adapters.pymysql.pool import _ADAPTER_NAME
        from sqlspec.adapters.pymysql.pool import POOL_LOGGER_NAME as pymysql_pool_logger_name

        assert pymysql_pool_logger_name == "sqlspec.pool"
        assert _ADAPTER_NAME == "pymysql"

    def test_pymysql_pool_has_pool_id(self) -> None:
        """Test that PyMySQL pool generates a pool_id."""
        from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool

        pool = PyMysqlConnectionPool({"database": "test_db"})
        assert hasattr(pool, "_pool_id")
        assert len(pool._pool_id) == 8  # UUID prefix

    def test_pymysql_pool_database_name_property(self) -> None:
        """Test that PyMySQL pool has _database_name property for logging."""
        from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool

        pool = PyMysqlConnectionPool({"database": "production_db"})
        assert pool._database_name == "production_db"

        # Missing database returns "unknown"
        pool2 = PyMysqlConnectionPool({})
        assert pool2._database_name == "unknown"


class TestDuckDBPoolLogging:
    """Tests for DuckDB pool logging structure."""

    def test_duckdb_pool_uses_pool_logger(self) -> None:
        """Test that DuckDB pool imports and uses POOL_LOGGER_NAME."""
        from sqlspec.adapters.duckdb.pool import _ADAPTER_NAME
        from sqlspec.adapters.duckdb.pool import POOL_LOGGER_NAME as duckdb_pool_logger_name

        assert duckdb_pool_logger_name == "sqlspec.pool"
        assert _ADAPTER_NAME == "duckdb"

    def test_duckdb_pool_has_pool_id(self) -> None:
        """Test that DuckDB pool generates a pool_id."""
        from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

        pool = DuckDBConnectionPool({"database": ":memory:"})
        assert hasattr(pool, "_pool_id")
        assert len(pool._pool_id) == 8  # UUID prefix

    def test_duckdb_pool_database_name_property(self) -> None:
        """Test that DuckDB pool has _database_name property for logging."""
        from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

        # Memory database (empty string)
        pool = DuckDBConnectionPool({})
        assert pool._database_name == ":memory:"

        # Memory database (explicit)
        pool2 = DuckDBConnectionPool({"database": ":memory:"})
        assert pool2._database_name == ":memory:"

        # File database
        pool3 = DuckDBConnectionPool({"database": "/tmp/test.duckdb"})
        assert pool3._database_name == "/tmp/test.duckdb"


class TestAiosqliteConnectionPoolLogging:
    """Tests for aiosqlite pool logging structure."""

    @pytest.mark.asyncio
    async def test_aiosqlite_pool_uses_pool_logger(self) -> None:
        """Test that aiosqlite pool imports and uses POOL_LOGGER_NAME."""
        from sqlspec.adapters.aiosqlite.pool import _ADAPTER_NAME
        from sqlspec.adapters.aiosqlite.pool import POOL_LOGGER_NAME as aiosqlite_pool_logger_name

        assert aiosqlite_pool_logger_name == "sqlspec.pool"
        assert _ADAPTER_NAME == "aiosqlite"

    @pytest.mark.asyncio
    async def test_aiosqlite_pool_has_pool_id(self) -> None:
        """Test that aiosqlite pool generates a pool_id."""
        from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool

        pool = AiosqliteConnectionPool({"database": ":memory:"})
        assert hasattr(pool, "_pool_id")
        assert len(pool._pool_id) == 8  # UUID prefix

    @pytest.mark.asyncio
    async def test_aiosqlite_pool_database_name_property(self) -> None:
        """Test that aiosqlite pool has _database_name property for logging."""
        from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool

        # Memory database
        pool = AiosqliteConnectionPool({"database": ":memory:"})
        assert pool._database_name == ":memory:"

        # File database - aiosqlite extracts just the filename for privacy
        pool2 = AiosqliteConnectionPool({"database": "/tmp/test.db"})
        assert pool2._database_name == "test.db"


class TestPoolLoggingMessages:
    """Tests for pool logging message formats."""

    def test_pool_recycle_message_format(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that pool recycle logs use structured message format."""
        import time

        from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

        caplog.set_level(logging.DEBUG, logger="sqlspec.pool")

        # Create pool with very short recycle time (must be > 0 to trigger recycle)
        pool = SqliteConnectionPool({"database": ":memory:"}, recycle_seconds=1)

        # Get connection to trigger creation
        with pool.get_connection():
            pass

        # Manually set created_at to past to trigger recycle
        pool._thread_local.created_at = time.time() - 2

        # Get another connection to trigger recycle
        with pool.get_connection():
            pass

        # Find recycle log
        recycle_records = [r for r in caplog.records if "pool.connection.recycle" in r.getMessage()]
        assert len(recycle_records) >= 1

        record = recycle_records[0]
        # Check structured fields via extra_fields
        extra = record.__dict__.get("extra_fields", {})
        assert extra.get("adapter") == "sqlite"
        assert "pool_id" in extra
        assert "database" in extra
        assert extra.get("reason") == "exceeded_recycle_time"
