"""Integration tests for dialect propagation through the SQL pipeline."""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlglot.dialects.dialect import DialectType

# from sqlspec.adapters.asyncmy import AsyncmyDriver  # TODO: Fix import
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.driver.mixins import SQLTranslatorMixin
from sqlspec.statement.builder import SelectBuilder
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.sql import SQL, SQLConfig


# Sync dialect propagation tests
def test_sqlite_dialect_propagation_through_execute() -> None:
    """Test that SQLite dialect propagates through execute calls."""
    config = SqliteConfig(database=":memory:")

    # Verify config has correct dialect
    assert config.dialect == "sqlite"

    # Create a mock connection
    mock_connection = Mock()

    # Create driver
    driver = SqliteDriver(connection=mock_connection, config=SQLConfig())

    # Verify driver has correct dialect
    assert driver.dialect == "sqlite"

    # Execute a query
    with patch.object(driver, "_execute_statement") as mock_execute:
        mock_execute.return_value = {"data": [], "column_names": ["id", "name"], "rows_affected": 0}

        driver.execute("SELECT * FROM users")

        # Check that _build_statement was called and passed a SQL object with correct dialect
        mock_execute.assert_called_once()
        sql_statement = mock_execute.call_args.kwargs["statement"]
        assert isinstance(sql_statement, SQL)
        assert sql_statement._dialect == "sqlite"


def test_duckdb_dialect_propagation_with_query_builder() -> None:
    """Test that DuckDB dialect propagates through query builder."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})

    # Verify config has correct dialect
    assert config.dialect == "duckdb"

    # Create a mock connection
    mock_connection = Mock()

    # Create driver
    driver = DuckDBDriver(connection=mock_connection, config=SQLConfig())

    # Create a query builder
    query = SelectBuilder(dialect="duckdb").from_("users").where("id = 1")

    # Execute and verify dialect is preserved
    with patch.object(driver, "_execute_statement") as mock_execute:
        mock_execute.return_value = {"data": [], "column_names": ["id", "name"], "rows_affected": 0}
        driver.execute(query)

        # Get the SQL statement that was passed to _execute_statement
        call_args = mock_execute.call_args
        sql_statement = call_args.kwargs["statement"]
        assert isinstance(sql_statement, SQL)
        assert sql_statement._dialect == "duckdb"


def test_psycopg_dialect_in_execute_script() -> None:
    """Test that Psycopg dialect propagates in execute_script."""
    config = PsycopgSyncConfig(pool_config={"conninfo": "postgresql://test:test@localhost/test"})

    # Verify config has correct dialect
    assert config.dialect == "postgres"

    # Create a mock connection
    mock_connection = Mock()

    # Create driver
    driver = PsycopgSyncDriver(connection=mock_connection, config=SQLConfig())

    # Execute script and verify dialect
    with patch.object(driver, "_execute_statement") as mock_execute:
        mock_execute.return_value = {"statements_executed": 2, "status_message": "SCRIPT EXECUTED"}

        script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"
        driver.execute_script(script)

        # Get the SQL statement that was passed to _execute_statement
        call_args = mock_execute.call_args
        sql_statement = call_args.kwargs["statement"]
        assert isinstance(sql_statement, SQL)
        assert sql_statement._dialect == "postgres"
        assert sql_statement.is_script is True


# Async dialect propagation tests
@pytest.mark.asyncio
async def test_asyncpg_dialect_propagation_through_execute() -> None:
    """Test that AsyncPG dialect propagates through execute calls."""
    config = AsyncpgConfig(host="localhost", port=5432, database="test", user="test", password="test")

    # Verify config has correct dialect
    assert config.dialect == "postgres"

    # Create a mock connection
    mock_connection = AsyncMock()

    # Create driver
    driver = AsyncpgDriver(connection=mock_connection, config=SQLConfig())

    # Execute a query and verify dialect is passed through
    with patch.object(driver, "_execute_statement", new_callable=AsyncMock) as mock_execute:
        # Mock to return the appropriate result dict
        mock_execute.return_value = {"data": [], "column_names": ["id", "name"], "rows_affected": 0}

        await driver.execute("SELECT * FROM users")

        # Check that _execute_statement was called with SQL object with correct dialect
        mock_execute.assert_called_once()
        sql_statement = mock_execute.call_args.kwargs["statement"]
        assert isinstance(sql_statement, SQL)
        assert sql_statement._dialect == "postgres"


@pytest.mark.asyncio
async def test_asyncmy_dialect_propagation_with_filters() -> None:
    """Test that AsyncMy dialect propagates with filters."""
    # TODO: Implement this test when AsyncmyConfig is available
    pytest.skip("AsyncmyConfig import missing")


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
    select_builder = SelectBuilder(dialect="sqlite")
    assert select_builder.dialect == "sqlite"

    # Build SQL and check dialect
    sql = select_builder.from_("users").to_statement()
    assert sql._dialect == "sqlite"

    # Test with different dialects
    for dialect in ["postgres", "mysql", "duckdb"]:
        builder = SelectBuilder(dialect=dialect)
        assert builder.dialect == dialect

        sql = builder.from_("test_table").to_statement()
        assert sql._dialect == dialect


def test_sql_translator_mixin_dialect_usage() -> None:
    """Test that SQLTranslatorMixin uses dialect properly."""

    class TestDriver(SqliteDriver, SQLTranslatorMixin):
        dialect: DialectType = "sqlite"

    mock_connection = Mock()
    driver = TestDriver(connection=mock_connection, config=SQLConfig())

    # Test convert_to_dialect with string input
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
    sql = SQL("SELECT 1", _dialect="mysql")
    assert sql._dialect == "mysql"

    # None dialect should also work
    sql = SQL("SELECT 1", _dialect=None)
    assert sql._dialect is None

    # Test with another valid dialect
    sql = SQL("SELECT 1", _dialect="bigquery")
    assert sql._dialect == "bigquery"


def test_dialect_mismatch_handling() -> None:
    """Test that drivers convert SQL to their own dialect."""
    # Create driver with one dialect
    mock_connection = Mock()
    driver = SqliteDriver(connection=mock_connection, config=SQLConfig())

    # Create SQL with different dialect
    sql = SQL("SELECT 1", _dialect="postgres")

    # Should still execute without error (driver handles conversion if needed)
    with patch.object(driver, "_execute_statement") as mock_execute:
        mock_execute.return_value = {"data": [], "column_names": [], "rows_affected": 0}

        # This should work - driver can execute SQL with different dialect
        _ = driver.execute(sql)

        # Verify the SQL object was converted to driver's dialect
        call_args = mock_execute.call_args
        sql_statement = call_args.kwargs["statement"]
        assert sql_statement._dialect == "sqlite"  # Converted to driver's dialect
