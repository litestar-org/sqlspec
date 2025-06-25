"""Comprehensive tests for config dialect property implementation."""

from typing import Any, ClassVar, Optional
from unittest.mock import Mock, patch

import pytest
from sqlglot.dialects.dialect import Dialect

from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow


class MockConnection:
    """Mock database connection."""

    pass


class MockDriver(SyncDriverAdapterProtocol[MockConnection, DictRow]):
    """Mock driver for testing."""

    dialect = "sqlite"  # Use a real dialect for testing
    parameter_style = ParameterStyle.QMARK

    def _execute_statement(self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any) -> Any:
        return {"data": [], "column_names": []}

    def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
        return result

    def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
        return result

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK


class MockAsyncDriver(AsyncDriverAdapterProtocol[MockConnection, DictRow]):
    """Mock async driver for testing."""

    dialect = "postgres"  # Use a real dialect for testing
    parameter_style = ParameterStyle.NUMERIC

    async def _execute_statement(
        self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any
    ) -> Any:
        return {"data": [], "column_names": []}

    async def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
        return result

    async def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
        return result

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NUMERIC


class TestSyncConfigDialect:
    """Test sync config dialect implementation."""

    def test_no_pool_sync_config_dialect(self) -> None:
        """Test that NoPoolSyncConfig returns dialect from driver class."""

        class TestNoPoolConfig(NoPoolSyncConfig[MockConnection, MockDriver]):
            driver_type: ClassVar[type[MockDriver]] = MockDriver  # type: ignore[misc]

            def __init__(self, **kwargs: Any) -> None:
                self.statement_config = SQLConfig()
                self.host = "localhost"
                self.connection_type = MockConnection  # type: ignore[assignment]
                self.driver_type = MockDriver  # type: ignore[assignment,misc]
                super().__init__(**kwargs)

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return {"host": self.host}

            def create_connection(self) -> MockConnection:
                return MockConnection()

        config = TestNoPoolConfig()
        assert config.dialect == "sqlite"

    def test_no_pool_sync_config_dialect_with_missing_driver_type(self) -> None:
        """Test that config raises AttributeError when driver_type is not set and driver has no dialect."""

        # Create a driver without dialect attribute
        class DriverWithoutDialect(SyncDriverAdapterProtocol[MockConnection, DictRow]):
            # No dialect attribute
            parameter_style = ParameterStyle.QMARK

            def _execute_statement(
                self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any
            ) -> Any:
                return {"data": []}

            def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
                return result

            def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
                return result

            def _get_placeholder_style(self) -> ParameterStyle:
                return ParameterStyle.QMARK

        class BrokenNoPoolConfig(NoPoolSyncConfig[MockConnection, DriverWithoutDialect]):
            # Intentionally not setting driver_type

            def __init__(self, **kwargs: Any) -> None:
                self.statement_config = SQLConfig()
                self.host = "localhost"
                super().__init__(**kwargs)

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return {"host": self.host}

            def create_connection(self) -> MockConnection:
                return MockConnection()

        config = BrokenNoPoolConfig()
        with pytest.raises(AttributeError) as exc_info:
            _ = config.dialect

        assert "driver_type" in str(exc_info.value)

    def test_sync_database_config_dialect(self) -> None:
        """Test that SyncDatabaseConfig returns dialect from driver class."""

        class MockPool:
            pass

        class TestSyncDbConfig(SyncDatabaseConfig[MockConnection, MockPool, MockDriver]):
            driver_type: type[MockDriver] = MockDriver

            def __init__(self, **kwargs: Any) -> None:
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                self.pool_instance = None
                super().__init__(**kwargs)

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return self.connection_config

            def create_connection(self) -> MockConnection:
                return MockConnection()

            def _create_pool(self) -> MockPool:
                return MockPool()

            def _close_pool(self) -> None:
                pass

        config = TestSyncDbConfig()
        assert config.dialect == "sqlite"


class TestAsyncConfigDialect:
    """Test async config dialect implementation."""

    @pytest.mark.asyncio
    async def test_no_pool_async_config_dialect(self) -> None:
        """Test that NoPoolAsyncConfig returns dialect from driver class."""

        class TestNoPoolAsyncConfig(NoPoolAsyncConfig[MockConnection, MockAsyncDriver]):
            driver_type: type[MockAsyncDriver] = MockAsyncDriver
            connection_type: type[MockConnection] = MockConnection

            def __init__(self, **kwargs: Any) -> None:
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                super().__init__(**kwargs)

            @property
            def dialect(self) -> str:
                return "postgres"

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return self.connection_config

            async def create_connection(self) -> MockConnection:
                return MockConnection()

        config = TestNoPoolAsyncConfig()
        assert config.dialect == "postgres"

    @pytest.mark.asyncio
    async def test_async_database_config_dialect(self) -> None:
        """Test that AsyncDatabaseConfig returns dialect from driver class."""

        class MockAsyncPool:
            pass

        class TestAsyncDbConfig(AsyncDatabaseConfig[MockConnection, MockAsyncPool, MockAsyncDriver]):
            driver_type: type[MockAsyncDriver] = MockAsyncDriver

            def __init__(self, **kwargs: Any) -> None:
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                self.pool_instance = None
                super().__init__(**kwargs)

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return self.connection_config

            async def create_connection(self) -> MockConnection:
                return MockConnection()

            async def _create_pool(self) -> MockAsyncPool:
                return MockAsyncPool()

            async def _close_pool(self) -> None:
                pass

        config = TestAsyncDbConfig()
        assert config.dialect == "postgres"


class TestRealAdapterDialects:
    """Test that real adapter configs properly expose dialect."""

    def test_sqlite_config_dialect(self) -> None:
        """Test SQLite config dialect property."""
        from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver

        # SqliteConfig should have driver_type set
        assert hasattr(SqliteConfig, "driver_type")
        assert SqliteConfig.driver_type == SqliteDriver

        # Create instance and check dialect
        config = SqliteConfig(database=":memory:")
        assert config.dialect == "sqlite"

    def test_duckdb_config_dialect(self) -> None:
        """Test DuckDB config dialect property."""
        from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver

        # DuckDBConfig should have driver_type set
        assert hasattr(DuckDBConfig, "driver_type")
        assert DuckDBConfig.driver_type == DuckDBDriver

        # Create instance and check dialect
        config = DuckDBConfig(connection_config={"database": ":memory:"})
        assert config.dialect == "duckdb"

    @pytest.mark.asyncio
    async def test_asyncpg_config_dialect(self) -> None:
        """Test AsyncPG config dialect property."""
        from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver

        # AsyncpgConfig should have driver_type set
        assert hasattr(AsyncpgConfig, "driver_type")
        assert AsyncpgConfig.driver_type == AsyncpgDriver

        # Create instance and check dialect
        config = AsyncpgConfig(host="localhost", port=5432, database="test", user="test", password="test")
        assert config.dialect == "postgres"

    def test_psycopg_config_dialect(self) -> None:
        """Test Psycopg config dialect property."""
        from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver

        # PsycopgConfig should have driver_type set
        assert hasattr(PsycopgSyncConfig, "driver_type")
        assert PsycopgSyncConfig.driver_type == PsycopgSyncDriver

        # Create instance and check dialect
        config = PsycopgSyncConfig(conninfo="postgresql://test:test@localhost/test")
        assert config.dialect == "postgres"

    @pytest.mark.asyncio
    async def test_asyncmy_config_dialect(self) -> None:
        """Test AsyncMy config dialect property."""
        from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver

        # AsyncmyConfig should have driver_type set
        assert hasattr(AsyncmyConfig, "driver_type")
        assert AsyncmyConfig.driver_type == AsyncmyDriver

        # Create instance and check dialect
        config = AsyncmyConfig(
            pool_config={"host": "localhost", "port": 3306, "database": "test", "user": "test", "password": "test"}
        )
        assert config.dialect == "mysql"


class TestDialectPropagation:
    """Test that dialect properly propagates through the system."""

    def test_dialect_in_sql_build_statement(self) -> None:
        """Test that dialect is passed when building SQL statements."""
        from sqlspec.statement.sql import SQL

        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        # When driver builds a statement, it should pass its dialect
        statement = driver._build_statement("SELECT * FROM users")
        assert isinstance(statement, SQL)
        assert statement._dialect == "sqlite"

    def test_dialect_in_execute_script(self) -> None:
        """Test that dialect is passed in execute_script."""
        from sqlspec.statement.sql import SQL

        driver = MockDriver(connection=MockConnection(), config=SQLConfig())

        with patch.object(driver, "_execute_statement") as mock_execute:
            mock_execute.return_value = "SCRIPT EXECUTED"

            driver.execute_script("CREATE TABLE test (id INT);")

            # Check that SQL was created with correct dialect
            call_args = mock_execute.call_args
            sql_statement = call_args[1]["statement"]
            assert isinstance(sql_statement, SQL)
            assert sql_statement._dialect == "sqlite"

    def test_sql_translator_mixin_uses_driver_dialect(self) -> None:
        """Test that SQLTranslatorMixin uses the driver's dialect."""

        from sqlspec.driver.mixins import SQLTranslatorMixin

        class TestTranslatorDriver(MockDriver, SQLTranslatorMixin):
            dialect = "postgres"

        driver = TestTranslatorDriver(connection=MockConnection(), config=SQLConfig())

        # Test convert_to_dialect uses driver dialect by default
        test_sql = "SELECT * FROM users"
        with patch("sqlspec.driver.mixins._sql_translator.parse_one") as mock_parse:
            mock_expr = Mock()
            mock_expr.sql.return_value = "converted sql"
            mock_parse.return_value = mock_expr

            driver.convert_to_dialect(test_sql)

            # Should parse with driver dialect
            mock_parse.assert_called_once_with(test_sql, dialect="postgres")
            # Should convert to driver dialect when to_dialect is None
            mock_expr.sql.assert_called_once_with(dialect="postgres", pretty=True)


class TestDialectValidation:
    """Test dialect validation and error handling."""

    def test_invalid_dialect_type(self) -> None:
        """Test that invalid dialect types are handled."""

        # Test with various dialect types
        dialects = ["sqlite", Dialect.get_or_raise("postgres"), None]

        for dialect in dialects:
            sql = SQL("SELECT 1", _dialect=dialect)  # type: ignore[arg-type]
            # Should not raise during initialization
            assert sql._dialect == dialect

    def test_config_missing_driver_type_attribute_error(self) -> None:
        """Test proper error when accessing dialect on config without driver_type."""

        # Create a driver without dialect attribute
        class DriverWithoutDialect(SyncDriverAdapterProtocol[MockConnection, DictRow]):
            # No dialect attribute
            parameter_style = ParameterStyle.QMARK

            def _execute_statement(
                self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any
            ) -> Any:
                return {"data": []}

            def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
                return result

            def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
                return result

            def _get_placeholder_style(self) -> ParameterStyle:
                return ParameterStyle.QMARK

        class IncompleteConfig(NoPoolSyncConfig[MockConnection, DriverWithoutDialect]):
            # No driver_type attribute

            def __init__(self, **kwargs: Any) -> None:
                self.statement_config = SQLConfig()
                self.host = "localhost"
                super().__init__(**kwargs)

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return {"host": self.host}

            def create_connection(self) -> MockConnection:
                return MockConnection()

        config = IncompleteConfig()

        # Should raise AttributeError with helpful message
        with pytest.raises(AttributeError) as exc_info:
            _ = config.dialect

        assert "driver_type" in str(exc_info.value)
