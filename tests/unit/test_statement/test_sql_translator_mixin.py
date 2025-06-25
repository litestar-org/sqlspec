"""Tests for SQLTranslatorMixin with config dialect support."""

from typing import Any, ClassVar
from unittest.mock import Mock, patch

import pytest
from sqlglot import exp

from sqlspec.config import NoPoolSyncConfig
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins._sql_translator import SQLTranslatorMixin
from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow


class MockConnection:
    """Mock database connection."""

    pass


class MockDriver(SyncDriverAdapterProtocol[MockConnection, DictRow], SQLTranslatorMixin):
    """Mock driver with SQLTranslatorMixin for testing."""

    dialect = "sqlite"  # Use a real dialect for testing
    parameter_style: "ParameterStyle" = ParameterStyle.QMARK

    def _execute_statement(self, statement: Any, connection: Any = None, **kwargs: Any) -> Any:
        return {"data": []}

    def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
        return result

    def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
        return result

    def _get_placeholder_style(self) -> "ParameterStyle":
        return ParameterStyle.QMARK


class MockConfig(NoPoolSyncConfig[MockConnection, MockDriver]):
    """Mock config for testing dialect property."""

    driver_class: ClassVar[type[MockDriver]] = MockDriver

    def __init__(self) -> None:
        self.instrumentation = Mock()
        self.statement_config = SQLConfig()
        self.connection_config = {"host": "localhost"}
        super().__init__()

    connection_type: "type[MockConnection]" = MockConnection
    driver_type: "type[MockDriver]" = MockDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return self.connection_config

    def create_connection(self) -> MockConnection:
        return MockConnection()


class TestSQLTranslatorMixinWithDialect:
    """Test SQLTranslatorMixin functionality with dialect support."""

    def test_translator_mixin_uses_driver_dialect(self) -> None:
        """Test that SQLTranslatorMixin uses the driver's dialect attribute."""
        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        assert driver.dialect == "sqlite"

        # Test convert_to_dialect with string SQL
        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "SELECT * FROM users"
            mock_parse.return_value = mock_expr

            driver.convert_to_dialect("SELECT * FROM users")

            # Should parse with driver's dialect
            mock_parse.assert_called_once_with("SELECT * FROM users", dialect="sqlite")
            # Should output with driver's dialect when to_dialect is None
            mock_expr.sql.assert_called_once_with(dialect="sqlite", pretty=True)

    def test_translator_mixin_with_sql_object(self) -> None:
        """Test convert_to_dialect with SQL object input."""
        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        # Create SQL object with expression
        sql = SQL("SELECT * FROM users WHERE id = ?", parameters=[1])

        # Force SQL to parse by accessing expression property
        _ = sql.expression

        # Mock the expression's sql method
        with patch.object(sql.expression, "sql") as mock_sql:
            mock_sql.return_value = "SELECT * FROM users WHERE id = ?"

            driver.convert_to_dialect(sql)

            # Should use driver's dialect for output
            mock_sql.assert_called_once_with(dialect="sqlite", pretty=True)

    def test_translator_mixin_with_target_dialect(self) -> None:
        """Test convert_to_dialect with explicit target dialect."""
        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        # Test conversion to different dialect
        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "SELECT * FROM users"
            mock_parse.return_value = mock_expr

            driver.convert_to_dialect("SELECT * FROM users", to_dialect="postgres")

            # Should parse with driver's dialect
            mock_parse.assert_called_once_with("SELECT * FROM users", dialect="sqlite")
            # Should output with target dialect
            mock_expr.sql.assert_called_once_with(dialect="postgres", pretty=True)

    def test_translator_mixin_with_expression_input(self) -> None:
        """Test convert_to_dialect with sqlglot Expression input."""
        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        # Create a sqlglot expression
        expr = exp.Select().select("*").from_("users")

        with patch.object(expr, "sql") as mock_sql:
            mock_sql.return_value = "SELECT * FROM users"

            driver.convert_to_dialect(expr)

            # Should output with driver's dialect
            mock_sql.assert_called_once_with(dialect="sqlite", pretty=True)

    def test_translator_mixin_error_handling(self) -> None:
        """Test error handling in convert_to_dialect."""
        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        # Test with SQL object without expression
        sql = Mock(spec=SQL)
        sql.expression = None

        with pytest.raises(SQLConversionError, match="Statement could not be parsed"):
            driver.convert_to_dialect(sql)

        # Test with parse error
        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_parse.side_effect = Exception("Parse error")

            with pytest.raises(SQLConversionError, match="Failed to parse SQL statement"):
                driver.convert_to_dialect("INVALID SQL")

        # Test with conversion error
        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.side_effect = Exception("Conversion error")
            mock_parse.return_value = mock_expr

            with pytest.raises(SQLConversionError, match="Failed to convert SQL expression"):
                driver.convert_to_dialect("SELECT * FROM users")

    def test_translator_mixin_with_different_driver_dialects(self) -> None:
        """Test SQLTranslatorMixin with various driver dialects."""
        dialects = ["sqlite", "postgres", "mysql", "duckdb", "bigquery"]

        for dialect in dialects:
            # Create driver with specific dialect
            class TestDriver(MockDriver):
                pass

            TestDriver.dialect = dialect

            driver = TestDriver(connection=MockConnection(), config=SQLConfig())

            assert driver.dialect == dialect

            # Test that it uses the correct dialect
            with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
                mock_expr = Mock()
                mock_expr.sql.return_value = f"SELECT * FROM users -- {dialect}"
                mock_parse.return_value = mock_expr

                driver.convert_to_dialect("SELECT * FROM users")

                # Should parse with driver's dialect
                mock_parse.assert_called_with("SELECT * FROM users", dialect=dialect)
                # Should output with driver's dialect
                mock_expr.sql.assert_called_with(dialect=dialect, pretty=True)

    def test_translator_mixin_with_config_dialect(self) -> None:
        """Test that driver gets dialect from config properly."""
        config = MockConfig()

        # Config should have dialect from driver_class
        assert config.dialect == "sqlite"

        # Create driver instance
        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        # Driver should have its own dialect
        assert driver.dialect == "sqlite"

        # Test translation works
        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "SELECT 1"
            mock_parse.return_value = mock_expr

            driver.convert_to_dialect("SELECT 1")

            mock_parse.assert_called_with("SELECT 1", dialect="sqlite")
            mock_expr.sql.assert_called_with(dialect="sqlite", pretty=True)

    def test_translator_mixin_pretty_formatting(self) -> None:
        """Test pretty formatting option in convert_to_dialect."""
        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "SELECT * FROM users"
            mock_parse.return_value = mock_expr

            # Test with pretty=True (default)
            driver.convert_to_dialect("SELECT * FROM users")
            mock_expr.sql.assert_called_with(dialect="sqlite", pretty=True)

            # Test with pretty=False
            driver.convert_to_dialect("SELECT * FROM users", pretty=False)
            mock_expr.sql.assert_called_with(dialect="sqlite", pretty=False)


class TestRealAdapterTranslatorMixin:
    """Test SQLTranslatorMixin with real adapter classes."""

    def test_sqlite_translator_mixin(self) -> None:
        """Test SQLite driver with SQLTranslatorMixin."""
        from sqlspec.adapters.sqlite import SqliteDriver

        mock_connection = Mock()
        driver = SqliteDriver(connection=mock_connection, config=SQLConfig())

        assert driver.dialect == "sqlite"

        # Test conversion
        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "SELECT * FROM users"
            mock_parse.return_value = mock_expr

            driver.convert_to_dialect("SELECT * FROM users", to_dialect="postgres")

            mock_parse.assert_called_with("SELECT * FROM users", dialect="sqlite")
            mock_expr.sql.assert_called_with(dialect="postgres", pretty=True)

    def test_postgres_translator_mixin(self) -> None:
        """Test PostgreSQL drivers with SQLTranslatorMixin."""
        from sqlspec.adapters.asyncpg import AsyncpgDriver
        from sqlspec.adapters.psycopg import PsycopgSyncDriver

        # Test AsyncPG
        mock_connection = Mock()
        asyncpg_driver = AsyncpgDriver(connection=mock_connection, config=SQLConfig())

        assert asyncpg_driver.dialect == "postgres"

        # Test Psycopg
        psycopg_driver = PsycopgSyncDriver(connection=mock_connection, config=SQLConfig())

        assert psycopg_driver.dialect == "postgres"

    def test_mysql_translator_mixin(self) -> None:
        """Test MySQL driver with SQLTranslatorMixin."""
        from sqlspec.adapters.asyncmy import AsyncmyDriver

        mock_connection = Mock()
        driver = AsyncmyDriver(connection=mock_connection, config=SQLConfig())

        assert driver.dialect == "mysql"

    def test_duckdb_translator_mixin(self) -> None:
        """Test DuckDB driver with SQLTranslatorMixin."""
        from sqlspec.adapters.duckdb import DuckDBDriver

        mock_connection = Mock()
        driver = DuckDBDriver(connection=mock_connection, config=SQLConfig())

        assert driver.dialect == "duckdb"
