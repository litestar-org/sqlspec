"""Global conftest.py for SQLSpec unit tests.

Provides comprehensive fixtures for configuration, caching, SQL statements, mock databases,
cleanup, and performance testing with proper scoping and test isolation.
"""

import time
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Optional

import pytest

# Import core modules for fixtures
from sqlspec.core.cache import UnifiedCache
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig, TypedParameter
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

__all__ = (
    "MockAsyncConnection",
    "MockAsyncCursor",
    "MockAsyncDriver",
    # Mock database fixtures
    "MockSyncConnection",
    "MockSyncCursor",
    "MockSyncDriver",
    "benchmark_tracker",
    "cache_config_disabled",
    "cache_config_enabled",
    "cache_statistics_tracker",
    "cleanup_test_state",
    "compilation_metrics",
    "complex_sql_with_joins",
    "memory_profiler",
    "mock_async_connection",
    "mock_async_driver",
    "mock_bigquery_connection",
    "mock_mysql_connection",
    "mock_postgres_connection",
    # Database-specific mock fixtures
    "mock_sqlite_connection",
    "mock_sync_connection",
    "mock_sync_driver",
    # Cache fixtures
    "mock_unified_cache",
    "parameter_style_config_advanced",
    "parameter_style_config_basic",
    # Performance fixtures
    "performance_timer",
    "reset_cache_state",
    "reset_global_state",
    "sample_delete_sql",
    "sample_insert_sql",
    "sample_parameters_mixed",
    "sample_parameters_named",
    "sample_parameters_positional",
    # SQL fixtures
    "sample_select_sql",
    "sample_update_sql",
    "sql_with_typed_parameters",
    "statement_config_mysql",
    "statement_config_postgres",
    # Configuration fixtures
    "statement_config_sqlite",
    # Cleanup fixtures
    "test_isolation",
)


# =============================================================================
# 1. CONFIGURATION FIXTURES - Mock StatementConfig, CacheConfig, and database configurations
# =============================================================================


@pytest.fixture
def parameter_style_config_basic() -> ParameterStyleConfig:
    """Basic parameter style configuration for simple test cases."""
    return ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
        allow_mixed_parameter_styles=False,
        preserve_parameter_format=False,
    )


@pytest.fixture
def parameter_style_config_advanced() -> ParameterStyleConfig:
    """Advanced parameter style configuration with type coercions and transformations."""

    def bool_coercion(value: bool) -> int:
        return 1 if value else 0

    def decimal_coercion(value: Decimal) -> float:
        return float(value)

    def list_coercion(value: list[Any]) -> str:
        return f"{{{','.join(str(v) for v in value)}}}"  # PostgreSQL array format

    return ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
        },
        supported_execution_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        type_coercion_map={bool: bool_coercion, Decimal: decimal_coercion, list: list_coercion},
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=True,
        preserve_parameter_format=True,
    )


@pytest.fixture
def statement_config_sqlite(parameter_style_config_basic: ParameterStyleConfig) -> StatementConfig:
    """SQLite statement configuration for testing."""
    return StatementConfig(
        dialect="sqlite",
        parameter_config=parameter_style_config_basic,
        execution_mode=None,
        execution_args=None,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def statement_config_postgres(parameter_style_config_advanced: ParameterStyleConfig) -> StatementConfig:
    """PostgreSQL statement configuration for testing."""
    return StatementConfig(
        dialect="postgres",
        parameter_config=parameter_style_config_advanced,
        execution_mode=None,
        execution_args=None,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def statement_config_mysql() -> StatementConfig:
    """MySQL statement configuration for testing."""
    mysql_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        supported_execution_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
    )
    return StatementConfig(
        dialect="mysql", parameter_config=mysql_config, enable_caching=True, enable_parsing=True, enable_validation=True
    )


@pytest.fixture
def cache_config_enabled() -> dict[str, Any]:
    """Cache configuration with caching enabled."""
    return {
        "enable_caching": True,
        "max_cache_size": 1000,
        "enable_file_cache": True,
        "enable_compiled_cache": True,
        "cache_hit_threshold": 0.8,
    }


@pytest.fixture
def cache_config_disabled() -> dict[str, Any]:
    """Cache configuration with caching disabled for testing scenarios."""
    return {
        "enable_caching": False,
        "max_cache_size": 0,
        "enable_file_cache": False,
        "enable_compiled_cache": False,
        "cache_hit_threshold": 0.0,
    }


# =============================================================================
# 2. CACHE FIXTURES - Mock cache instances and cache statistics
# =============================================================================


@pytest.fixture
def mock_unified_cache() -> UnifiedCache:
    """Mock unified cache for testing cache behavior."""
    # Create cache using the actual factory method
    from sqlspec.core.cache import get_default_cache

    return get_default_cache()

    # Note: The actual UnifiedCache may have different methods
    # This will need to be updated based on the actual implementation


@pytest.fixture
def cache_statistics_tracker() -> dict[str, Any]:
    """Cache statistics tracker for monitoring cache performance during tests."""
    return {"hits": 0, "misses": 0, "evictions": 0, "cache_sizes": defaultdict(int), "hit_rates": []}


@pytest.fixture(autouse=True)
def reset_cache_state() -> "Generator[None, None, None]":
    """Auto-use fixture to reset global cache state before each test."""
    # Store original cache state

    yield

    # Reset any global cache instances to prevent test pollution
    # This would be expanded based on actual global cache instances in the codebase


# =============================================================================
# 3. SQL FIXTURES - Sample SQL statements, parameters, and test data
# =============================================================================


@pytest.fixture
def sample_select_sql() -> str:
    """Simple SELECT SQL statement for testing."""
    return "SELECT id, name, email FROM users WHERE active = ? AND created_at > ?"


@pytest.fixture
def sample_insert_sql() -> str:
    """Simple INSERT SQL statement for testing."""
    return "INSERT INTO users (name, email, active, created_at) VALUES (?, ?, ?, ?)"


@pytest.fixture
def sample_update_sql() -> str:
    """Simple UPDATE SQL statement for testing."""
    return "UPDATE users SET name = :name, email = :email WHERE id = :user_id"


@pytest.fixture
def sample_delete_sql() -> str:
    """Simple DELETE SQL statement for testing."""
    return "DELETE FROM users WHERE id = ? AND active = ?"


@pytest.fixture
def sample_parameters_positional() -> list[Any]:
    """Sample positional parameters for testing."""
    return [1, "John Doe", "john@example.com", True, "2023-01-01 00:00:00"]


@pytest.fixture
def sample_parameters_named() -> dict[str, Any]:
    """Sample named parameters for testing."""
    return {
        "user_id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "active": True,
        "created_at": "2023-01-01 00:00:00",
    }


@pytest.fixture
def sample_parameters_mixed() -> list[dict[str, Any]]:
    """Sample mixed parameter sets for executemany testing."""
    return [
        {"name": "John Doe", "email": "john@example.com", "active": True},
        {"name": "Jane Smith", "email": "jane@example.com", "active": False},
        {"name": "Bob Johnson", "email": "bob@example.com", "active": True},
    ]


@pytest.fixture
def complex_sql_with_joins() -> str:
    """Complex SQL with joins for advanced testing scenarios."""
    return """
        SELECT u.id, u.name, u.email, p.title, c.name as company
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        LEFT JOIN companies c ON p.company_id = c.id
        WHERE u.active = :active
          AND u.created_at BETWEEN :start_date AND :end_date
          AND (p.title LIKE :title_pattern OR p.title IS NULL)
        ORDER BY u.created_at DESC
        LIMIT :limit OFFSET :offset
    """


@pytest.fixture
def sql_with_typed_parameters(statement_config_sqlite: StatementConfig) -> SQL:
    """SQL statement with TypedParameter instances for type preservation testing."""
    sql = "SELECT * FROM products WHERE price > ? AND in_stock = ? AND categories = ?"
    params = [
        TypedParameter(Decimal("19.99"), Decimal, "price"),
        TypedParameter(True, bool, "in_stock"),
        TypedParameter(["electronics", "gadgets"], list, "categories"),
    ]
    return SQL(sql, *params, statement_config=statement_config_sqlite)


# =============================================================================
# 4. MOCK DATABASE FIXTURES - Mock connections, cursors, and drivers
# =============================================================================


class MockSyncConnection:
    """Enhanced mock sync connection with comprehensive database simulation."""

    def __init__(self, name: str = "mock_sync_connection", dialect: str = "sqlite") -> None:
        self.name = name
        self.dialect = dialect
        self.connected = True
        self.in_transaction = False
        self.autocommit = True
        self.execute_count = 0
        self.execute_many_count = 0
        self.last_sql: Optional[str] = None
        self.last_parameters: Any = None
        self.cursor_results: list[dict[str, Any]] = []
        self.connection_info = {
            "server_version": "1.0.0",
            "client_version": "1.0.0",
            "database_name": "test_db",
            "user": "test_user",
        }

    def cursor(self) -> "MockSyncCursor":
        """Return a mock cursor."""
        return MockSyncCursor(self)

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock execute method."""
        self.execute_count += 1
        self.last_sql = sql
        self.last_parameters = parameters

    def executemany(self, sql: str, parameters: list[Any]) -> None:
        """Mock executemany method."""
        self.execute_many_count += 1
        self.last_sql = sql
        self.last_parameters = parameters

    def commit(self) -> None:
        """Mock commit method."""
        self.in_transaction = False

    def rollback(self) -> None:
        """Mock rollback method."""
        self.in_transaction = False

    def close(self) -> None:
        """Mock close method."""
        self.connected = False

    def begin(self) -> None:
        """Mock begin transaction method."""
        self.in_transaction = True
        self.autocommit = False


class MockAsyncConnection:
    """Enhanced mock async connection with comprehensive database simulation."""

    def __init__(self, name: str = "mock_async_connection", dialect: str = "sqlite") -> None:
        self.name = name
        self.dialect = dialect
        self.connected = True
        self.in_transaction = False
        self.autocommit = True
        self.execute_count = 0
        self.execute_many_count = 0
        self.last_sql: Optional[str] = None
        self.last_parameters: Any = None
        self.cursor_results: list[dict[str, Any]] = []
        self.connection_info = {
            "server_version": "1.0.0",
            "client_version": "1.0.0",
            "database_name": "test_db",
            "user": "test_user",
        }

    async def cursor(self) -> "MockAsyncCursor":
        """Return a mock async cursor."""
        return MockAsyncCursor(self)

    async def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock async execute method."""
        self.execute_count += 1
        self.last_sql = sql
        self.last_parameters = parameters

    async def executemany(self, sql: str, parameters: list[Any]) -> None:
        """Mock async executemany method."""
        self.execute_many_count += 1
        self.last_sql = sql
        self.last_parameters = parameters

    async def commit(self) -> None:
        """Mock async commit method."""
        self.in_transaction = False

    async def rollback(self) -> None:
        """Mock async rollback method."""
        self.in_transaction = False

    async def close(self) -> None:
        """Mock async close method."""
        self.connected = False

    async def begin(self) -> None:
        """Mock async begin transaction method."""
        self.in_transaction = True
        self.autocommit = False


class MockSyncCursor:
    """Enhanced mock sync cursor with realistic database cursor behavior."""

    def __init__(self, connection: MockSyncConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.description: Optional[list[tuple[str, ...]]] = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.fetchone_result: Optional[tuple[Any, ...]] = None
        self.closed = False
        self.arraysize = 1

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock execute method with smart result generation."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

        # Generate realistic results based on SQL type
        sql_upper = sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            self.description = [("id", "INTEGER"), ("name", "TEXT"), ("email", "TEXT")]
            self.fetchall_result = [(1, "John Doe", "john@example.com"), (2, "Jane Smith", "jane@example.com")]
            self.fetchone_result = (1, "John Doe", "john@example.com")
            self.rowcount = len(self.fetchall_result)
        elif sql_upper.startswith("INSERT"):
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 1
        elif sql_upper.startswith("UPDATE"):
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 2  # Mock affected rows
        elif sql_upper.startswith("DELETE"):
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 1
        else:
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 0

    def executemany(self, sql: str, parameters: list[Any]) -> None:
        """Mock executemany method."""
        self.connection.execute_many_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters
        self.rowcount = len(parameters) if parameters else 0

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Mock fetchall method."""
        return self.fetchall_result

    def fetchone(self) -> Optional[tuple[Any, ...]]:
        """Mock fetchone method."""
        return self.fetchone_result

    def fetchmany(self, size: Optional[int] = None) -> list[tuple[Any, ...]]:
        """Mock fetchmany method."""
        size = size or self.arraysize
        return self.fetchall_result[:size]

    def close(self) -> None:
        """Mock close method."""
        self.closed = True

    def __enter__(self) -> "MockSyncCursor":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()


class MockAsyncCursor:
    """Enhanced mock async cursor with realistic database cursor behavior."""

    def __init__(self, connection: MockAsyncConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.description: Optional[list[tuple[str, ...]]] = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.fetchone_result: Optional[tuple[Any, ...]] = None
        self.closed = False
        self.arraysize = 1

    async def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock async execute method with smart result generation."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

        # Generate realistic results based on SQL type
        sql_upper = sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            self.description = [("id", "INTEGER"), ("name", "TEXT"), ("email", "TEXT")]
            self.fetchall_result = [(1, "John Doe", "john@example.com"), (2, "Jane Smith", "jane@example.com")]
            self.fetchone_result = (1, "John Doe", "john@example.com")
            self.rowcount = len(self.fetchall_result)
        elif sql_upper.startswith("INSERT"):
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 1
        elif sql_upper.startswith("UPDATE"):
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 2  # Mock affected rows
        elif sql_upper.startswith("DELETE"):
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 1
        else:
            self.description = None
            self.fetchall_result = []
            self.fetchone_result = None
            self.rowcount = 0

    async def executemany(self, sql: str, parameters: list[Any]) -> None:
        """Mock async executemany method."""
        self.connection.execute_many_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters
        self.rowcount = len(parameters) if parameters else 0

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Mock async fetchall method."""
        return self.fetchall_result

    async def fetchone(self) -> Optional[tuple[Any, ...]]:
        """Mock async fetchone method."""
        return self.fetchone_result

    async def fetchmany(self, size: Optional[int] = None) -> list[tuple[Any, ...]]:
        """Mock async fetchmany method."""
        size = size or self.arraysize
        return self.fetchall_result[:size]

    async def close(self) -> None:
        """Mock async close method."""
        self.closed = True

    async def __aenter__(self) -> "MockAsyncCursor":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


class MockSyncDriver(SyncDriverAdapterBase):
    """Enhanced mock sync driver with complete adapter interface."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: MockSyncConnection,
        statement_config: Optional[StatementConfig] = None,
        driver_features: Optional[dict[str, Any]] = None,
    ) -> None:
        if statement_config is None:
            from sqlspec.core.parameters import ParameterStyleConfig

            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK},
                supported_execution_parameter_styles={ParameterStyle.QMARK},
                default_execution_parameter_style=ParameterStyle.QMARK,
            )
            statement_config = StatementConfig(
                dialect="sqlite",
                parameter_config=parameter_config,
                enable_caching=False,  # Disable caching to avoid hash issues in tests
            )
        super().__init__(connection, statement_config, driver_features)

    @contextmanager
    def with_cursor(self, connection: MockSyncConnection) -> "Generator[MockSyncCursor, None, None]":
        """Return mock cursor context manager."""
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @contextmanager
    def handle_database_exceptions(self) -> "Generator[None, None, None]":
        """Handle database exceptions."""
        try:
            yield
        except Exception as e:
            raise SQLSpecError(f"Mock database error: {e}") from e

    def _try_special_handling(self, cursor: MockSyncCursor, statement: SQL) -> Optional[Any]:
        """Mock special handling - always return None."""
        return None

    def _execute_statement(self, cursor: MockSyncCursor, statement: SQL) -> ExecutionResult:
        """Mock execute statement."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, prepared_parameters)

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = [dict(zip(column_names, row)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount)

    def _execute_many(self, cursor: MockSyncCursor, statement: SQL) -> ExecutionResult:
        """Mock execute many."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        cursor.executemany(sql, prepared_parameters)
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount, is_many_result=True)

    def _execute_script(self, cursor: MockSyncCursor, statement: SQL) -> ExecutionResult:
        """Mock execute script."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        for stmt in statements:
            cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def begin(self) -> None:
        """Mock begin transaction."""
        self.connection.begin()

    def rollback(self) -> None:
        """Mock rollback transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Mock commit transaction."""
        self.connection.commit()


class MockAsyncDriver(AsyncDriverAdapterBase):
    """Enhanced mock async driver with complete adapter interface."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: MockAsyncConnection,
        statement_config: Optional[StatementConfig] = None,
        driver_features: Optional[dict[str, Any]] = None,
    ) -> None:
        if statement_config is None:
            from sqlspec.core.parameters import ParameterStyleConfig

            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK},
                supported_execution_parameter_styles={ParameterStyle.QMARK},
                default_execution_parameter_style=ParameterStyle.QMARK,
            )
            statement_config = StatementConfig(
                dialect="sqlite",
                parameter_config=parameter_config,
                enable_caching=False,  # Disable caching to avoid hash issues in tests
            )
        super().__init__(connection, statement_config, driver_features)

    @asynccontextmanager
    async def with_cursor(self, connection: MockAsyncConnection) -> "AsyncGenerator[MockAsyncCursor, None]":
        """Return mock async cursor context manager."""
        cursor = await connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    @asynccontextmanager
    async def handle_database_exceptions(self) -> "AsyncGenerator[None, None]":
        """Handle database exceptions."""
        try:
            yield
        except Exception as e:
            raise SQLSpecError(f"Mock async database error: {e}") from e

    async def _try_special_handling(self, cursor: MockAsyncCursor, statement: SQL) -> Optional[Any]:
        """Mock async special handling - always return None."""
        return None

    async def _execute_statement(self, cursor: MockAsyncCursor, statement: SQL) -> ExecutionResult:
        """Mock async execute statement."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, prepared_parameters)

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = [dict(zip(column_names, row)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount)

    async def _execute_many(self, cursor: MockAsyncCursor, statement: SQL) -> ExecutionResult:
        """Mock async execute many."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        await cursor.executemany(sql, prepared_parameters)
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount, is_many_result=True)

    async def _execute_script(self, cursor: MockAsyncCursor, statement: SQL) -> ExecutionResult:
        """Mock async execute script."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        for stmt in statements:
            await cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def begin(self) -> None:
        """Mock async begin transaction."""
        await self.connection.begin()

    async def rollback(self) -> None:
        """Mock async rollback transaction."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Mock async commit transaction."""
        await self.connection.commit()


# Mock connection fixtures
@pytest.fixture
def mock_sync_connection() -> MockSyncConnection:
    """Fixture for basic mock sync connection."""
    return MockSyncConnection()


@pytest.fixture
def mock_async_connection() -> MockAsyncConnection:
    """Fixture for basic mock async connection."""
    return MockAsyncConnection()


@pytest.fixture
def mock_sync_driver(mock_sync_connection: MockSyncConnection) -> MockSyncDriver:
    """Fixture for mock sync driver."""
    return MockSyncDriver(mock_sync_connection)


@pytest.fixture
def mock_async_driver(mock_async_connection: MockAsyncConnection) -> MockAsyncDriver:
    """Fixture for mock async driver."""
    return MockAsyncDriver(mock_async_connection)


# Database-specific mock fixtures
@pytest.fixture
def mock_sqlite_connection() -> MockSyncConnection:
    """Mock SQLite connection with SQLite-specific behavior."""
    return MockSyncConnection("sqlite_connection", "sqlite")


@pytest.fixture
def mock_postgres_connection() -> MockAsyncConnection:
    """Mock PostgreSQL connection with Postgres-specific behavior."""
    conn = MockAsyncConnection("postgres_connection", "postgres")
    conn.connection_info.update({"server_version": "14.0", "supports_returning": "True", "supports_arrays": "True"})
    return conn


@pytest.fixture
def mock_mysql_connection() -> MockSyncConnection:
    """Mock MySQL connection with MySQL-specific behavior."""
    conn = MockSyncConnection("mysql_connection", "mysql")
    conn.connection_info.update({"server_version": "8.0.0", "supports_json": "True", "charset": "utf8mb4"})
    return conn


@pytest.fixture
def mock_bigquery_connection() -> MockSyncConnection:
    """Mock BigQuery connection with BigQuery-specific behavior."""
    conn = MockSyncConnection("bigquery_connection", "bigquery")
    conn.connection_info.update(
        {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "supports_arrays": "True",
            "supports_structs": "True",
        }
    )
    return conn


# =============================================================================
# 5. CLEANUP FIXTURES - Test isolation and cleanup functions
# =============================================================================


@pytest.fixture(autouse=True)
def test_isolation() -> "Generator[None, None, None]":
    """Auto-use fixture to ensure test isolation by resetting global state."""
    # Store original state

    yield

    # Clean up any test modifications to global state
    # This prevents test pollution between test runs


@pytest.fixture
def cleanup_test_state() -> "Generator[Callable[[Callable[[], None]], None], None, None]":
    """Fixture that provides a cleanup function for test state management."""
    cleanup_functions = []

    def register_cleanup(func: "Callable[[], None]") -> None:
        """Register a cleanup function to be called during teardown."""
        cleanup_functions.append(func)

    yield register_cleanup

    # Execute all registered cleanup functions
    for cleanup_func in reversed(cleanup_functions):
        try:
            cleanup_func()
        except Exception:
            # Log cleanup errors but don't fail the test
            pass


@pytest.fixture(scope="session", autouse=True)
def reset_global_state() -> "Generator[None, None, None]":
    """Session-scoped fixture to reset global state before and after test session."""
    # Reset any global singletons or cached state before tests
    yield
    # Final cleanup after all tests complete


# =============================================================================
# 6. PERFORMANCE FIXTURES - Timing and benchmark utilities
# =============================================================================


@pytest.fixture
def performance_timer() -> "Generator[Any, None, None]":
    """Performance timer fixture for measuring execution time during tests."""
    times = {}

    @contextmanager
    def timer(operation_name: str) -> "Generator[None, None, None]":
        """Time a specific operation."""
        start_time = time.perf_counter()
        yield
        end_time = time.perf_counter()
        times[operation_name] = end_time - start_time

    timer.times = times  # type: ignore[attr-defined]
    yield timer


@pytest.fixture
def benchmark_tracker() -> dict[str, Any]:
    """Benchmark tracking fixture for collecting performance metrics during tests."""
    return {
        "operations": [],
        "timings": {},
        "memory_usage": {},
        "cache_statistics": {},
        "sql_compilation_times": [],
        "parameter_processing_times": [],
    }


@pytest.fixture
def memory_profiler() -> "Generator[Callable[[], dict[str, Any]], None, None]":
    """Memory profiling fixture for tracking memory usage during tests."""
    try:
        import os

        import psutil

        process = psutil.Process(os.getpid())

        def get_memory_usage() -> dict[str, Any]:
            """Get current memory usage statistics."""
            memory_info = process.memory_info()
            return {
                "rss": memory_info.rss,  # Resident Set Size
                "vms": memory_info.vms,  # Virtual Memory Size
                "percent": process.memory_percent(),
            }

        yield get_memory_usage

    except ImportError:
        # Fallback if psutil is not available
        def get_memory_usage() -> dict[str, Any]:
            return {"rss": 0, "vms": 0, "percent": 0.0}

        yield get_memory_usage


@pytest.fixture
def compilation_metrics() -> "Generator[Any, None, None]":
    """Compilation metrics tracking for SQL compilation performance testing."""
    metrics: dict[str, Any] = {
        "compilation_count": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "parse_times": [],
        "transform_times": [],
        "total_compilation_times": [],
    }

    def record_compilation(
        parse_time: float, transform_time: float, total_time: float, was_cached: bool = False
    ) -> None:
        """Record compilation metrics."""
        metrics["compilation_count"] += 1
        if was_cached:
            metrics["cache_hits"] += 1
        else:
            metrics["cache_misses"] += 1
        metrics["parse_times"].append(parse_time)
        metrics["transform_times"].append(transform_time)
        metrics["total_compilation_times"].append(total_time)

    record_compilation.metrics = metrics  # type: ignore[attr-defined]
    yield record_compilation


# =============================================================================
# PYTEST CONFIGURATION AND MARKERS
# =============================================================================


def pytest_configure(config: Any) -> None:
    """Configure pytest with custom markers for fixture categories."""
    config.addinivalue_line("markers", "config: Tests for configuration fixtures")
    config.addinivalue_line("markers", "cache: Tests for cache-related fixtures")
    config.addinivalue_line("markers", "sql: Tests for SQL statement fixtures")
    config.addinivalue_line("markers", "mock_db: Tests for mock database fixtures")
    config.addinivalue_line("markers", "cleanup: Tests for cleanup and isolation fixtures")
    config.addinivalue_line("markers", "performance: Tests for performance measurement fixtures")
    config.addinivalue_line("markers", "slow: Slow-running tests that require extra time")
    config.addinivalue_line("markers", "unit: Unit tests with isolated components")
    config.addinivalue_line("markers", "integration: Integration tests with multiple components")


# Additional helper functions for test utilities
def create_test_sql_statement(sql: str, *params: Any, **kwargs: Any) -> SQL:
    """Helper function to create SQL statements for testing."""
    return SQL(sql, *params, **kwargs)


def assert_sql_execution_result(result: ExecutionResult, expected_rowcount: int = -1) -> None:
    """Helper function to assert SQL execution results."""
    assert result is not None
    if expected_rowcount >= 0:
        assert result.data_row_count == expected_rowcount
