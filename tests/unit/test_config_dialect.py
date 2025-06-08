"""Comprehensive tests for config dialect property implementation."""

from typing import Any, ClassVar, Optional
from unittest.mock import Mock, patch

import pytest

from sqlspec.config import (
    AsyncDatabaseConfig,
    InstrumentationConfig,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    SyncDatabaseConfig,
)
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow


class MockConnection:
    """Mock database connection."""

    pass


class MockDriver(SyncDriverAdapterProtocol[MockConnection, DictRow]):
    """Mock driver for testing."""

    dialect = "sqlite"  # Use a real dialect for testing
    parameter_style = "qmark"

    def _execute_statement(self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any) -> Any:
        return {"data": [], "column_names": []}

    def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
        return result

    def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
        return result

    def _get_placeholder_style(self) -> str:
        return "qmark"


class MockAsyncDriver(AsyncDriverAdapterProtocol[MockConnection, DictRow]):
    """Mock async driver for testing."""

    dialect = "postgres"  # Use a real dialect for testing
    parameter_style = "numeric"

    async def _execute_statement(
        self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any
    ) -> Any:
        return {"data": [], "column_names": []}

    async def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
        return result

    async def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
        return result

    def _get_placeholder_style(self) -> str:
        return "numeric"


class TestSyncConfigDialect:
    """Test sync config dialect implementation."""

    def test_nopoolsync_config_dialect(self) -> None:
        """Test that NoPoolSyncConfig returns dialect from driver class."""

        class TestNoPoolConfig(NoPoolSyncConfig[MockConnection, MockDriver]):
            driver_class: ClassVar[type[MockDriver]] = MockDriver

            def __init__(self, **kwargs: Any) -> None:
                self.instrumentation = InstrumentationConfig()
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                super().__init__(**kwargs)

            @property
            def connection_type(self) -> type[MockConnection]:
                return MockConnection

            @property
            def driver_type(self) -> type[MockDriver]:
                return MockDriver

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return self.connection_config

            def create_connection(self) -> MockConnection:
                return MockConnection()

        config = TestNoPoolConfig()
        assert config.dialect == "sqlite"

    def test_nopoolsync_config_dialect_with_missing_driver_class(self) -> None:
        """Test that config raises AttributeError when driver_class is not set and driver has no dialect."""

        # Create a driver without dialect attribute
        class DriverWithoutDialect(SyncDriverAdapterProtocol[MockConnection, DictRow]):
            # No dialect attribute
            parameter_style = "qmark"

            def _execute_statement(
                self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any
            ) -> Any:
                return {"data": []}

            def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
                return result

            def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
                return result

            def _get_placeholder_style(self) -> str:
                return "qmark"

        class BrokenNoPoolConfig(NoPoolSyncConfig[MockConnection, DriverWithoutDialect]):
            # Intentionally not setting driver_class

            def __init__(self, **kwargs: Any) -> None:
                self.instrumentation = InstrumentationConfig()
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                super().__init__(**kwargs)

            @property
            def connection_type(self) -> type[MockConnection]:
                return MockConnection

            @property
            def driver_type(self) -> type[DriverWithoutDialect]:
                return DriverWithoutDialect

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return self.connection_config

            def create_connection(self) -> MockConnection:
                return MockConnection()

        config = BrokenNoPoolConfig()
        with pytest.raises(AttributeError) as exc_info:
            _ = config.dialect

        assert "driver_class" in str(exc_info.value)

    def test_sync_database_config_dialect(self) -> None:
        """Test that SyncDatabaseConfig returns dialect from driver class."""

        class MockPool:
            pass

        class TestSyncDbConfig(SyncDatabaseConfig[MockConnection, MockPool, MockDriver]):
            driver_class: ClassVar[type[MockDriver]] = MockDriver

            def __init__(self, **kwargs: Any) -> None:
                self.instrumentation = InstrumentationConfig()
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                self.pool_instance = None
                super().__init__(**kwargs)

            @property
            def connection_type(self) -> type[MockConnection]:
                return MockConnection

            @property
            def driver_type(self) -> type[MockDriver]:
                return MockDriver

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
    async def test_nopoolasync_config_dialect(self) -> None:
        """Test that NoPoolAsyncConfig returns dialect from driver class."""

        class TestNoPoolAsyncConfig(NoPoolAsyncConfig[MockConnection, MockAsyncDriver]):
            driver_class: ClassVar[type[MockAsyncDriver]] = MockAsyncDriver

            def __init__(self, **kwargs: Any) -> None:
                self.instrumentation = InstrumentationConfig()
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                super().__init__(**kwargs)

            @property
            def connection_type(self) -> type[MockConnection]:
                return MockConnection

            @property
            def driver_type(self) -> type[MockAsyncDriver]:
                return MockAsyncDriver

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
            driver_class: ClassVar[type[MockAsyncDriver]] = MockAsyncDriver

            def __init__(self, **kwargs: Any) -> None:
                self.instrumentation = InstrumentationConfig()
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                self.pool_instance = None
                super().__init__(**kwargs)

            @property
            def connection_type(self) -> type[MockConnection]:
                return MockConnection

            @property
            def driver_type(self) -> type[MockAsyncDriver]:
                return MockAsyncDriver

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

        # SqliteConfig should have driver_class set
        assert hasattr(SqliteConfig, "driver_class")
        assert SqliteConfig.driver_class == SqliteDriver

        # Create instance and check dialect
        config = SqliteConfig(connection_config={"database": ":memory:"})
        assert config.dialect == "sqlite"

    def test_duckdb_config_dialect(self) -> None:
        """Test DuckDB config dialect property."""
        from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver

        # DuckDBConfig should have driver_class set
        assert hasattr(DuckDBConfig, "driver_class")
        assert DuckDBConfig.driver_class == DuckDBDriver

        # Create instance and check dialect
        config = DuckDBConfig(connection_config={"database": ":memory:"})
        assert config.dialect == "duckdb"

    @pytest.mark.asyncio
    async def test_asyncpg_config_dialect(self) -> None:
        """Test AsyncPG config dialect property."""
        from sqlspec.adapters.asyncpg import AsyncPGConfig, AsyncpgDriver

        # AsyncPGConfig should have driver_class set
        assert hasattr(AsyncPGConfig, "driver_class")
        assert AsyncPGConfig.driver_class == AsyncpgDriver

        # Create instance and check dialect
        config = AsyncPGConfig(
            pool_config={
                "host": "localhost",
                "port": 5432,
                "database": "test",
                "user": "test",
                "password": "test",
            }
        )
        assert config.dialect == "postgres"

    def test_psycopg_config_dialect(self) -> None:
        """Test Psycopg config dialect property."""
        from sqlspec.adapters.psycopg import PsycopgConfig, PsycopgSyncDriver

        # PsycopgConfig should have driver_class set
        assert hasattr(PsycopgConfig, "driver_class")
        assert PsycopgConfig.driver_class == PsycopgSyncDriver

        # Create instance and check dialect
        config = PsycopgConfig(pool_config={"conninfo": "postgresql://test:test@localhost/test"})
        assert config.dialect == "postgres"

    @pytest.mark.asyncio
    async def test_asyncmy_config_dialect(self) -> None:
        """Test AsyncMy config dialect property."""
        from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver

        # AsyncmyConfig should have driver_class set
        assert hasattr(AsyncmyConfig, "driver_class")
        assert AsyncmyConfig.driver_class == AsyncmyDriver

        # Create instance and check dialect
        config = AsyncmyConfig(
            pool_config={
                "host": "localhost",
                "port": 3306,
                "database": "test",
                "user": "test",
                "password": "test",
            }
        )
        assert config.dialect == "mysql"


class TestDialectPropagation:
    """Test that dialect properly propagates through the system."""

    def test_dialect_in_sql_build_statement(self) -> None:
        """Test that dialect is passed when building SQL statements."""
        from sqlspec.statement.sql import SQL

        driver = MockDriver(
            connection=MockConnection(),
            config=SQLConfig(),
        )

        # When driver builds a statement, it should pass its dialect
        statement = driver._build_statement("SELECT * FROM users")
        assert isinstance(statement, SQL)
        assert statement._dialect == "sqlite"

    def test_dialect_in_execute_script(self) -> None:
        """Test that dialect is passed in execute_script."""
        from sqlspec.statement.sql import SQL

        driver = MockDriver(
            connection=MockConnection(),
            config=SQLConfig(),
        )

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

        from sqlspec.statement.mixins import SQLTranslatorMixin

        class TestTranslatorDriver(MockDriver, SQLTranslatorMixin[MockConnection]):
            dialect = "postgres"

        driver = TestTranslatorDriver(
            connection=MockConnection(),
            config=SQLConfig(),
        )

        # Test convert_to_dialect uses driver dialect by default
        test_sql = "SELECT * FROM users"
        with patch("sqlspec.statement.mixins._sql_translator.parse_one") as mock_parse:
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
        from sqlglot.dialects.dialect import Dialect

        from sqlspec.statement.sql import SQL

        # Test with various dialect types
        dialects = ["sqlite", Dialect.get_or_raise("postgres"), None]

        for dialect in dialects:
            sql = SQL("SELECT 1", dialect=dialect)
            # Should not raise during initialization
            assert sql._dialect == dialect

    def test_config_missing_driver_class_attribute_error(self) -> None:
        """Test proper error when accessing dialect on config without driver_class."""

        # Create a driver without dialect attribute
        class DriverWithoutDialect(SyncDriverAdapterProtocol[MockConnection, DictRow]):
            # No dialect attribute
            parameter_style = "qmark"

            def _execute_statement(
                self, statement: Any, connection: Optional[MockConnection] = None, **kwargs: Any
            ) -> Any:
                return {"data": []}

            def _wrap_select_result(self, statement: Any, result: Any, schema_type: Any = None, **kwargs: Any) -> Any:
                return result

            def _wrap_execute_result(self, statement: Any, result: Any, **kwargs: Any) -> Any:
                return result

            def _get_placeholder_style(self) -> str:
                return "qmark"

        class IncompleteConfig(NoPoolSyncConfig[MockConnection, DriverWithoutDialect]):
            # No driver_class attribute

            def __init__(self, **kwargs: Any) -> None:
                self.instrumentation = InstrumentationConfig()
                self.statement_config = SQLConfig()
                self.connection_config = {"host": "localhost"}
                super().__init__(**kwargs)

            @property
            def connection_type(self) -> type[MockConnection]:
                return MockConnection

            @property
            def driver_type(self) -> type[DriverWithoutDialect]:
                return DriverWithoutDialect

            @property
            def connection_config_dict(self) -> dict[str, Any]:
                return self.connection_config

            def create_connection(self) -> MockConnection:
                return MockConnection()

        config = IncompleteConfig()

        # Should raise AttributeError with helpful message
        with pytest.raises(AttributeError) as exc_info:
            _ = config.dialect

        assert "driver_class" in str(exc_info.value)
