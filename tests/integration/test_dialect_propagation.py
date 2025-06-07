"""Integration tests for dialect propagation through the SQL pipeline."""

from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlglot.dialects.dialect import DialectType

from sqlspec.adapters.asyncmy import AsyncMyConfig, AsyncmyDriver
from sqlspec.adapters.asyncpg import AsyncPGConfig, AsyncpgDriver
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.psycopg import PsycopgConfig, PsycopgSyncDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.statement.builder import SelectBuilder
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.sql import SQL, SQLConfig


class TestDialectPropagationSync:
    """Test dialect propagation in synchronous drivers."""

    def test_sqlite_dialect_propagation_through_execute(self) -> None:
        """Test that SQLite dialect propagates through execute calls."""
        config = SqliteConfig(connection_config={"database": ":memory:"})

        # Verify config has correct dialect
        assert config.dialect == "sqlite"

        # Create a mock connection
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = []

        # Create driver
        driver = SqliteDriver(
            connection=mock_connection,
            config=SQLConfig(),
        )

        # Verify driver has correct dialect
        assert driver.dialect == "sqlite"

        # Execute a query
        with patch.object(driver, "_execute_statement") as mock_execute:
            mock_execute.return_value = {"data": [], "column_names": ["id", "name"]}

            driver.execute("SELECT * FROM users")

            # Check that _build_statement was called and passed a SQL object with correct dialect
            mock_execute.assert_called_once()
            sql_statement = mock_execute.call_args.kwargs["statement"]
            assert isinstance(sql_statement, SQL)
            assert sql_statement._dialect == "sqlite"

    def test_duckdb_dialect_propagation_with_query_builder(self) -> None:
        """Test that DuckDB dialect propagates through query builder."""
        config = DuckDBConfig(connection_config={"database": ":memory:"})

        # Verify config has correct dialect
        assert config.dialect == "duckdb"

        # Create a mock connection
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = []

        # Create driver
        driver = DuckDBDriver(
            connection=mock_connection,
            config=SQLConfig(),
        )

        # Create a query builder
        query = SelectBuilder(dialect="duckdb").from_("users").where("id = 1")

        # Execute and verify dialect is preserved
        with patch.object(driver, "_execute_statement") as mock_execute:
            mock_execute.return_value = []
            driver.execute(query)

            # Get the SQL statement that was passed to _execute_statement
            call_args = mock_execute.call_args
            sql_statement = call_args.kwargs["statement"]
            assert isinstance(sql_statement, SQL)
            assert sql_statement._dialect == "duckdb"

    def test_psycopg_dialect_in_execute_script(self) -> None:
        """Test that Psycopg dialect propagates in execute_script."""
        config = PsycopgConfig(pool_config={"conninfo": "postgresql://test:test@localhost/test"})

        # Verify config has correct dialect
        assert config.dialect == "postgres"

        # Create a mock connection
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor

        # Create driver
        driver = PsycopgSyncDriver(
            connection=mock_connection,
            config=SQLConfig(),
        )

        # Execute script and verify dialect
        with patch.object(driver, "_execute_statement") as mock_execute:
            mock_execute.return_value = "SCRIPT EXECUTED"

            script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"
            driver.execute_script(script)

            # Get the SQL statement that was passed to _execute_statement
            call_args = mock_execute.call_args
            sql_statement = call_args.kwargs["statement"]
            assert isinstance(sql_statement, SQL)
            assert sql_statement._dialect == "postgres"
            assert sql_statement.is_script is True


class TestDialectPropagationAsync:
    """Test dialect propagation in asynchronous drivers."""

    @pytest.mark.asyncio
    async def test_asyncpg_dialect_propagation_through_execute(self) -> None:
        """Test that AsyncPG dialect propagates through execute calls."""
        config = AsyncPGConfig(
            pool_config={
                "host": "localhost",
                "port": 5432,
                "database": "test",
                "user": "test",
                "password": "test",
            }
        )

        # Verify config has correct dialect
        assert config.dialect == "postgres"

        # Create a mock connection
        from unittest.mock import AsyncMock

        mock_connection = AsyncMock()
        mock_connection.fetch.return_value = []

        # Create driver
        driver = AsyncpgDriver(
            connection=mock_connection,
            config=SQLConfig(),
        )

        # Execute a query and verify dialect is passed through
        with patch.object(driver, "_execute_statement") as mock_execute:
            # Mock to return an empty list like asyncpg would
            mock_execute.return_value = []

            await driver.execute("SELECT * FROM users")

            # Check that _execute_statement was called with SQL object with correct dialect
            mock_execute.assert_called_once()
            sql_statement = mock_execute.call_args.kwargs["statement"]
            assert isinstance(sql_statement, SQL)
            assert sql_statement._dialect == "postgres"

    @pytest.mark.asyncio
    async def test_asyncmy_dialect_propagation_with_filters(self) -> None:
        """Test that AsyncMy dialect propagates with filters."""
        config = AsyncMyConfig(
            pool_config={
                "host": "localhost",
                "port": 3306,
                "database": "test",
                "user": "test",
                "password": "test",
            }
        )

        # Verify config has correct dialect
        assert config.dialect == "mysql"

        # Create a mock connection
        from unittest.mock import AsyncMock

        mock_connection = AsyncMock()
        mock_cursor = AsyncMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.__aenter__.return_value = mock_cursor
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = []

        # Create driver
        driver = AsyncmyDriver(
            connection=mock_connection,
            config=SQLConfig(),
        )

        # Execute with filters
        from sqlspec.statement.filters import StatementFilter

        class TestFilter(StatementFilter):
            def append_to_statement(self, statement: SQL) -> SQL:
                return statement

        test_filter = TestFilter()

        with patch.object(driver, "_execute_statement") as mock_execute:
            # Mock to return the cursor as asyncmy would
            mock_execute.return_value = mock_cursor

            await driver.execute("SELECT * FROM users", test_filter)

            # Get the SQL statement that was passed to _execute_statement
            call_args = mock_execute.call_args
            sql_statement = call_args.kwargs["statement"]
            assert isinstance(sql_statement, SQL)
            assert sql_statement._dialect == "mysql"


class TestDialectInSQLProcessing:
    """Test dialect handling in SQL processing pipeline."""

    def test_sql_processing_context_with_dialect(self) -> None:
        """Test that SQLProcessingContext properly handles dialect."""

        # Create context with dialect
        context = SQLProcessingContext(
            initial_sql_string="SELECT * FROM users",
            dialect="postgres",
            config=SQLConfig(),
        )

        assert context.dialect == "postgres"
        assert context.initial_sql_string == "SELECT * FROM users"

    def test_query_builder_dialect_inheritance(self) -> None:
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

    def test_sql_translator_mixin_dialect_usage(self) -> None:
        """Test that SQLTranslatorMixin uses dialect properly."""
        from sqlspec.statement.mixins import SQLTranslatorMixin

        class TestDriver(SqliteDriver, SQLTranslatorMixin[Any]):
            dialect: DialectType = "sqlite"

        mock_connection = Mock()
        driver = TestDriver(
            connection=mock_connection,
            config=SQLConfig(),
        )

        # Test convert_to_dialect with string input
        with patch("sqlspec.statement.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "SELECT * FROM users"
            mock_parse.return_value = mock_expr

            # Convert to different dialect
            _ = driver.convert_to_dialect("SELECT * FROM users", to_dialect="postgres")

            # Should parse with driver's dialect and output with target dialect
            mock_parse.assert_called_with("SELECT * FROM users", dialect="sqlite")
            mock_expr.sql.assert_called_with(dialect="postgres", pretty=True)

        # Test with default (driver's) dialect
        with patch("sqlspec.statement.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "SELECT * FROM users"
            mock_parse.return_value = mock_expr

            # Convert without specifying target dialect
            _ = driver.convert_to_dialect("SELECT * FROM users")

            # Should parse with driver dialect
            mock_parse.assert_called_with("SELECT * FROM users", dialect="sqlite")
            # Should output with driver dialect
            mock_expr.sql.assert_called_with(dialect="sqlite", pretty=True)


class TestDialectErrorHandling:
    """Test error handling related to dialect."""

    def test_missing_dialect_in_driver(self) -> None:
        """Test handling of driver without dialect attribute."""
        # Create a mock driver without dialect
        mock_driver = Mock(spec=["connection", "config"])

        # Should raise AttributeError when accessing dialect
        with pytest.raises(AttributeError):
            _ = mock_driver.dialect

    def test_invalid_dialect_in_sql_creation(self) -> None:
        """Test that invalid dialects are handled gracefully."""
        # SQL should accept any dialect value without validation
        sql = SQL("SELECT 1", dialect="invalid_dialect")
        assert sql._dialect == "invalid_dialect"

        # None dialect should also work
        sql = SQL("SELECT 1", dialect=None)
        assert sql._dialect is None

    def test_dialect_mismatch_warning(self) -> None:
        """Test potential dialect mismatches are handled."""
        # Create driver with one dialect
        mock_connection = Mock()
        driver = SqliteDriver(
            connection=mock_connection,
            config=SQLConfig(),
        )

        # Create SQL with different dialect
        sql = SQL("SELECT 1", dialect="postgres")

        # Should still execute without error (driver handles conversion if needed)
        with patch.object(driver, "_execute_statement") as mock_execute:
            mock_execute.return_value = {"data": [], "column_names": []}

            # This should work - driver can execute SQL with different dialect
            _ = driver.execute(sql)

            # Verify the SQL object retained its original dialect
            call_args = mock_execute.call_args
            sql_statement = call_args.kwargs["statement"]
            assert sql_statement._dialect == "postgres"  # Original dialect preserved
