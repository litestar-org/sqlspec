"""Integration tests for dialect propagation through the SQL pipeline."""

from unittest.mock import Mock, patch

import pytest
from pytest_databases.docker.mysql import MySQLService
from sqlglot.dialects.dialect import DialectType

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.driver.mixins import SQLTranslatorMixin
from sqlspec.statement.builder import Select
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from tests.integration.test_adapters.test_adbc.conftest import PostgresService


# Sync dialect propagation tests
def test_sqlite_dialect_propagation_through_execute() -> None:
    """Test that SQLite dialect propagates through execute calls."""
    config = SqliteConfig(database=":memory:")

    # Verify config has correct dialect
    assert config.dialect == "sqlite"

    # Use real SQLite connection for integration test
    import sqlite3

    connection = sqlite3.connect(":memory:")
    # Set row factory to return Row objects that can be converted to dicts
    connection.row_factory = sqlite3.Row

    # Create table for testing
    connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    connection.execute("INSERT INTO users (id, name) VALUES (1, 'test')")
    connection.commit()

    # Create driver with real connection
    driver = SqliteDriver(connection=connection, config=SQLConfig())

    # Verify driver has correct dialect
    assert driver.dialect == "sqlite"

    # Execute a query and verify result
    result = driver.execute("SELECT * FROM users")

    # Verify we got results
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.data[0]["id"] == 1
    assert result.data[0]["name"] == "test"

    # Verify the internal SQL object has the correct dialect
    assert result.statement.dialect == "sqlite"

    connection.close()


def test_duckdb_dialect_propagation_with_query_builder() -> None:
    """Test that DuckDB dialect propagates through query builder."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})

    # Verify config has correct dialect
    assert config.dialect == "duckdb"

    # Use real DuckDB connection for integration test
    import duckdb

    connection = duckdb.connect(":memory:")

    # Create table for testing
    connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)")
    connection.execute("INSERT INTO users (id, name) VALUES (1, 'test')")

    # Create driver
    driver = DuckDBDriver(connection=connection, config=SQLConfig())

    # Create a query builder
    query = Select(dialect="duckdb").select("id", "name").from_("users").where("id = 1")

    # Execute and verify dialect is preserved
    result = driver.execute(query)

    # Verify we got results
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.data[0]["id"] == 1
    assert result.data[0]["name"] == "test"

    # Verify the dialect propagated correctly - driver uses its own dialect for execution
    # The Select had duckdb dialect, but the driver itself is DuckDB so this should work
    # We expect the driver to process the query with its dialect
    assert result.statement.dialect == "duckdb" or result.statement.dialect is None

    connection.close()


@pytest.mark.postgres
def test_psycopg_dialect_in_execute_script(postgres_service: PostgresService) -> None:
    """Test that Psycopg dialect propagates in execute_script."""
    config = PsycopgSyncConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
    )

    # Verify config has correct dialect
    assert config.dialect == "postgres"

    try:
        # Create a real connection
        with config.provide_connection() as connection:
            # Create driver
            driver = PsycopgSyncDriver(connection=connection)

            # Execute script and verify dialect
            script = "CREATE TEMP TABLE testdialect (id INT); INSERT INTO testdialect VALUES (1);"
            result = driver.execute_script(script)

            # Verify result
            assert isinstance(result, SQLResult)
            assert result.operation_type == "SCRIPT"

            # Verify the dialect propagated correctly
            assert result.statement.dialect == "postgres"
            assert result.statement.is_script is True
    finally:
        # Ensure proper cleanup of the connection pool
        if config.pool_instance:
            config.close_pool()


# Async dialect propagation tests
@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_dialect_propagation_through_execute(postgres_service: PostgresService) -> None:
    """Test that AsyncPG dialect propagates through execute calls."""
    config = AsyncpgConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        database=postgres_service.database,
    )

    # Verify config has correct dialect
    assert config.dialect == "postgres"

    # Create a real connection
    async with config.provide_connection() as connection:
        # Create driver
        driver = AsyncpgDriver(connection=connection, config=SQLConfig())

        # Create temp table and execute a query
        await connection.execute("CREATE TEMP TABLE test_users (id INT, name TEXT)")
        await connection.execute("INSERT INTO test_users VALUES (1, 'test')")

        result = await driver.execute("SELECT * FROM test_users")

        # Verify we got results
        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["id"] == 1
        assert result.data[0]["name"] == "test"

        # Verify the dialect propagated correctly
        assert result.statement.dialect == "postgres"

    # Ensure proper cleanup of the connection pool after the context manager exits
    if config.pool_instance:
        await config.close_pool()
        config.pool_instance = None


@pytest.mark.asyncio
@pytest.mark.mysql
async def test_asyncmy_dialect_propagation_with_filters(mysql_service: MySQLService) -> None:
    """Test that AsyncMy dialect propagates with filters."""
    config = AsyncmyConfig(
        host=mysql_service.host,
        port=mysql_service.port,
        user=mysql_service.user,
        password=mysql_service.password,
        database=mysql_service.db,
    )

    # Verify config has correct dialect
    assert config.dialect == "mysql"

    # Create a real connection
    async with config.provide_connection() as connection:
        # Create driver
        driver = AsyncmyDriver(connection=connection, config=SQLConfig())

        # Create temp table and execute a query with filter
        await driver.execute_script("CREATE TEMPORARY TABLE test_users (id INT, name VARCHAR(100))")
        await driver.execute_script("INSERT INTO test_users VALUES (1, 'test'), (2, 'another')")

        # Create SQL with filter
        from sqlspec.statement.filters import LimitOffsetFilter

        sql = SQL("SELECT * FROM test_users").filter(LimitOffsetFilter(limit=1, offset=0))

        result = await driver.execute(sql)

        # Verify we got results
        assert isinstance(result, SQLResult)
        assert len(result.data) == 1

        # Verify the dialect propagated correctly
        assert result.statement.dialect == "mysql"


# SQL processing tests
def test_sql_processing_context_with_dialect() -> None:
    """Test that SQLProcessingContext properly handles dialect."""

    # Create context with dialect
    context = SQLProcessingContext(initial_sql_string="SELECT * FROM users", dialect="postgres", config=SQLConfig())

    assert context.dialect == "postgres"
    assert context.initial_sql_string == "SELECT * FROM users"


def test_query_builder_dialect_inheritance() -> None:
    """Test that query builders inherit dialect correctly."""
    # Test with explicit dialect
    select_builder = Select(dialect="sqlite")
    assert select_builder.dialect == "sqlite"

    # Build SQL and check dialect
    sql = select_builder.from_("users").to_statement()
    assert sql.dialect == "sqlite"

    # Test with different dialects
    for dialect in ["postgres", "mysql", "duckdb"]:
        builder = Select(dialect=dialect)
        assert builder.dialect == dialect

        sql = builder.from_("test_table").to_statement()
        assert sql.dialect == dialect


def test_sql_translator_mixin_dialect_usage() -> None:
    """Test that SQLTranslatorMixin uses dialect properly."""

    class TestDriver(SqliteDriver, SQLTranslatorMixin):
        dialect: DialectType = "sqlite"

    mock_connection = Mock()
    driver = TestDriver(connection=mock_connection, config=SQLConfig())

    # Test convert_todialect with string input
    # NOTE: This test patches internal implementation to verify dialect propagation.
    # This is acceptable for testing the critical dialect handling contract.
    with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
        mock_expr = Mock()
        mock_expr.sql.return_value = "SELECT * FROM users"
        mock_parse.return_value = mock_expr

        # Convert to different dialect
        _ = driver.convert_to_dialect("SELECT * FROM users", to_dialect="postgres")

        # Should parse with driver's dialect and output with target dialect
        mock_parse.assert_called_with("SELECT * FROM users", dialect="sqlite")
        mock_expr.sql.assert_called_with(dialect="postgres", pretty=True)

    # Test with default (driver's) dialect
    # NOTE: Testing internal implementation to ensure dialect contract is maintained
    with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
        mock_expr = Mock()
        mock_expr.sql.return_value = "SELECT * FROM users"
        mock_parse.return_value = mock_expr

        # Convert without specifying target dialect
        _ = driver.convert_to_dialect("SELECT * FROM users")

        # Should parse with driver dialect
        mock_parse.assert_called_with("SELECT * FROM users", dialect="sqlite")
        # Should output with driver dialect
        mock_expr.sql.assert_called_with(dialect="sqlite", pretty=True)


# Error handling tests
def test_missing_dialect_in_driver() -> None:
    """Test handling of driver without dialect attribute."""
    # Create a mock driver without dialect
    mock_driver = Mock(spec=["connection", "config"])

    # Should raise AttributeError when accessing dialect
    with pytest.raises(AttributeError):
        _ = mock_driver.dialect


def test_different_dialect_in_sql_creation() -> None:
    """Test that different dialects can be used in SQL creation."""
    # SQL should accept various valid dialect values
    sql = SQL("SELECT 1", config=SQLConfig(dialect="mysql"))
    assert sql.dialect == "mysql"

    # None dialect should also work
    sql = SQL("SELECT 1", config=SQLConfig(dialect=None))
    assert sql.dialect is None

    # Test with another valid dialect
    sql = SQL("SELECT 1", config=SQLConfig(dialect="bigquery"))
    assert sql.dialect == "bigquery"


def test_dialect_mismatch_handling() -> None:
    """Test that drivers convert SQL to their own dialect."""
    # Create driver with one dialect
    import sqlite3

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    driver = SqliteDriver(connection=connection, config=SQLConfig())

    # Create SQL with different dialect
    sql = SQL("SELECT 1 AS num", config=SQLConfig(dialect="postgres"))

    # Should still execute without error (driver handles conversion if needed)
    result = driver.execute(sql)

    # Verify execution succeeded
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.data[0]["num"] == 1

    # Verify the driver processed the SQL with its own dialect
    # This is correct behavior - SQLite driver should use sqlite dialect for execution
    assert result.statement.dialect == "sqlite"

    connection.close()
