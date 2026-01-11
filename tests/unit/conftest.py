"""Global conftest.py for SQLSpec unit tests.

Provides fixtures for configuration, caching, SQL statements, mock databases,
cleanup, and performance testing with proper scoping and test isolation.
"""

import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import asynccontextmanager, contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.core import (
    SQL,
    LRUCache,
    ParameterStyle,
    ParameterStyleConfig,
    StatementConfig,
    TypedParameter,
    get_default_cache,
)
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    ExecutionResult,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo
from tests.conftest import is_compiled

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator


class MockSyncExceptionHandler:
    """Mock sync exception handler for testing.

    Implements the SyncExceptionHandler protocol with deferred exception pattern.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "MockSyncExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, Exception):
            self.pending_exception = SQLSpecError(f"Mock database error: {exc_val}")
            return True
        return False


class MockAsyncExceptionHandler:
    """Mock async exception handler for testing.

    Implements the AsyncExceptionHandler protocol with deferred exception pattern.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "MockAsyncExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, Exception):
            self.pending_exception = SQLSpecError(f"Mock async database error: {exc_val}")
            return True
        return False


__all__ = (
    "MockAsyncConnection",
    "MockAsyncCursor",
    "MockAsyncDriver",
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
    "mock_lru_cache",
    "mock_mysql_connection",
    "mock_postgres_connection",
    "mock_sqlite_connection",
    "mock_sync_connection",
    "mock_sync_driver",
    "parameter_style_config_advanced",
    "parameter_style_config_basic",
    "performance_timer",
    "reset_cache_state",
    "reset_global_state",
    "sample_delete_sql",
    "sample_insert_sql",
    "sample_parameters_mixed",
    "sample_parameters_named",
    "sample_parameters_positional",
    "sample_select_sql",
    "sample_update_sql",
    "sql_with_typed_parameters",
    "statement_config_mysql",
    "statement_config_postgres",
    "statement_config_sqlite",
    "test_isolation",
)


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
        return f"{{{','.join(str(v) for v in value)}}}"

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


@pytest.fixture
def mock_lru_cache() -> LRUCache:
    """Mock LRU cache for testing cache behavior."""

    return get_default_cache()


@pytest.fixture
def cache_statistics_tracker() -> dict[str, Any]:
    """Cache statistics tracker for monitoring cache performance during tests."""
    return {"hits": 0, "misses": 0, "evictions": 0, "cache_sizes": defaultdict(int), "hit_rates": []}


@pytest.fixture(autouse=True)
def reset_cache_state() -> "Generator[None, None, None]":
    """Auto-use fixture to reset global cache state before each test."""

    yield


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


class MockSyncConnection:
    """Mock sync connection with database simulation."""

    def __init__(self, name: str = "mock_sync_connection", dialect: str = "sqlite") -> None:
        self.name = name
        self.dialect = dialect
        self.connected = True
        self.in_transaction = False
        self.autocommit = True
        self.execute_count = 0
        self.execute_many_count = 0
        self.last_sql: str | None = None
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
    """Mock async connection with database simulation."""

    def __init__(self, name: str = "mock_async_connection", dialect: str = "sqlite") -> None:
        self.name = name
        self.dialect = dialect
        self.connected = True
        self.in_transaction = False
        self.autocommit = True
        self.execute_count = 0
        self.execute_many_count = 0
        self.last_sql: str | None = None
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
    """Mock sync cursor with database cursor behavior."""

    def __init__(self, connection: MockSyncConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.description: list[tuple[str, ...]] | None = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.fetchone_result: tuple[Any, ...] | None = None
        self.closed = False
        self.arraysize = 1

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock execute method with smart result generation."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

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
            self.rowcount = 2
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

    def fetchone(self) -> tuple[Any, ...] | None:
        """Mock fetchone method."""
        return self.fetchone_result

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
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
    """Mock async cursor with database cursor behavior."""

    def __init__(self, connection: MockAsyncConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.description: list[tuple[str, ...]] | None = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.fetchone_result: tuple[Any, ...] | None = None
        self.closed = False
        self.arraysize = 1

    async def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock async execute method with smart result generation."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

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
            self.rowcount = 2
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

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Mock async fetchone method."""
        return self.fetchone_result

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
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


class MockSyncDataDictionary(SyncDataDictionaryBase):
    """Mock sync data dictionary for testing."""

    def get_version(self, driver: "MockSyncDriver") -> "VersionInfo | None":
        """Return mock version info."""
        return VersionInfo(3, 42, 0)

    def get_feature_flag(self, driver: "MockSyncDriver", feature: str) -> bool:
        """Return mock feature flag."""
        return feature in {"supports_transactions", "supports_prepared_statements"}

    def get_optimal_type(self, driver: "MockSyncDriver", type_category: str) -> str:
        """Return mock optimal type."""
        return {"text": "TEXT", "boolean": "INTEGER"}.get(type_category, "TEXT")

    def get_tables(self, driver: "MockSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Return mock table list."""
        _ = (driver, schema)
        return []

    def get_columns(
        self, driver: "MockSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Return mock column metadata."""
        _ = (driver, table, schema)
        return []

    def get_indexes(
        self, driver: "MockSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Return mock index metadata."""
        _ = (driver, table, schema)
        return []

    def get_foreign_keys(
        self, driver: "MockSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Return mock foreign key metadata."""
        _ = (driver, table, schema)
        return []

    def list_available_features(self) -> "list[str]":
        """Return mock available features."""
        return ["supports_transactions", "supports_prepared_statements"]


class MockAsyncDataDictionary(AsyncDataDictionaryBase):
    """Mock async data dictionary for testing."""

    async def get_version(self, driver: "MockAsyncDriver") -> "VersionInfo | None":
        """Return mock version info."""
        return VersionInfo(3, 42, 0)

    async def get_feature_flag(self, driver: "MockAsyncDriver", feature: str) -> bool:
        """Return mock feature flag."""
        return feature in {"supports_transactions", "supports_prepared_statements"}

    async def get_optimal_type(self, driver: "MockAsyncDriver", type_category: str) -> str:
        """Return mock optimal type."""
        return {"text": "TEXT", "boolean": "INTEGER"}.get(type_category, "TEXT")

    async def get_tables(self, driver: "MockAsyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Return mock table list."""
        _ = (driver, schema)
        return []

    async def get_columns(
        self, driver: "MockAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Return mock column metadata."""
        _ = (driver, table, schema)
        return []

    async def get_indexes(
        self, driver: "MockAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Return mock index metadata."""
        _ = (driver, table, schema)
        return []

    async def get_foreign_keys(
        self, driver: "MockAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Return mock foreign key metadata."""
        _ = (driver, table, schema)
        return []

    def list_available_features(self) -> "list[str]":
        """Return mock available features."""
        return ["supports_transactions", "supports_prepared_statements"]


class MockSyncDriver(SyncDriverAdapterBase):
    """Mock sync driver with adapter interface."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: MockSyncConnection,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None:
        if statement_config is None:
            from sqlspec.core import ParameterStyleConfig

            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK},
                supported_execution_parameter_styles={ParameterStyle.QMARK},
                default_execution_parameter_style=ParameterStyle.QMARK,
            )
            statement_config = StatementConfig(
                dialect="sqlite", parameter_config=parameter_config, enable_caching=False
            )
        super().__init__(connection, statement_config, driver_features)
        self._data_dictionary: SyncDataDictionaryBase | None = None

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver."""
        if self._data_dictionary is None:
            self._data_dictionary = MockSyncDataDictionary()
        return self._data_dictionary

    @contextmanager
    def with_cursor(self, connection: MockSyncConnection) -> "Generator[MockSyncCursor, None, None]":
        """Return mock cursor context manager."""
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def handle_database_exceptions(self) -> "MockSyncExceptionHandler":
        """Handle database exceptions."""
        return MockSyncExceptionHandler()

    def dispatch_special_handling(self, cursor: MockSyncCursor, statement: SQL) -> Any | None:
        """Mock special handling - always return None."""
        return None

    def dispatch_execute(self, cursor: MockSyncCursor, statement: SQL) -> ExecutionResult:
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

    def dispatch_execute_many(self, cursor: MockSyncCursor, statement: SQL) -> ExecutionResult:
        """Mock execute many."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        parameter_sets = cast("list[Any]", prepared_parameters)
        cursor.executemany(sql, parameter_sets)
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount, is_many_result=True)

    def dispatch_execute_script(self, cursor: MockSyncCursor, statement: SQL) -> ExecutionResult:
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

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.in_transaction)


class MockAsyncDriver(AsyncDriverAdapterBase):
    """Mock async driver with adapter interface."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: MockAsyncConnection,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None:
        if statement_config is None:
            from sqlspec.core import ParameterStyleConfig

            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK},
                supported_execution_parameter_styles={ParameterStyle.QMARK},
                default_execution_parameter_style=ParameterStyle.QMARK,
            )
            statement_config = StatementConfig(
                dialect="sqlite", parameter_config=parameter_config, enable_caching=False
            )
        super().__init__(connection, statement_config, driver_features)
        self._data_dictionary: AsyncDataDictionaryBase | None = None

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver."""
        if self._data_dictionary is None:
            self._data_dictionary = MockAsyncDataDictionary()
        return self._data_dictionary

    @asynccontextmanager
    async def with_cursor(self, connection: MockAsyncConnection) -> "AsyncGenerator[MockAsyncCursor, None]":
        """Return mock async cursor context manager."""
        cursor = await connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    def handle_database_exceptions(self) -> "MockAsyncExceptionHandler":
        """Handle database exceptions."""
        return MockAsyncExceptionHandler()

    async def dispatch_special_handling(self, cursor: MockAsyncCursor, statement: SQL) -> Any | None:
        """Mock async special handling - always return None."""
        return None

    async def dispatch_execute(self, cursor: MockAsyncCursor, statement: SQL) -> ExecutionResult:
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

    async def dispatch_execute_many(self, cursor: MockAsyncCursor, statement: SQL) -> ExecutionResult:
        """Mock async execute many."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        parameter_sets = cast("list[Any]", prepared_parameters)
        await cursor.executemany(sql, parameter_sets)
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount, is_many_result=True)

    async def dispatch_execute_script(self, cursor: MockAsyncCursor, statement: SQL) -> ExecutionResult:
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

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.in_transaction)


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
    if is_compiled():
        pytest.skip("Requires interpreted driver base")
    return MockSyncDriver(mock_sync_connection)


@pytest.fixture
def mock_async_driver(mock_async_connection: MockAsyncConnection) -> MockAsyncDriver:
    """Fixture for mock async driver."""
    if is_compiled():
        pytest.skip("Requires interpreted driver base")
    return MockAsyncDriver(mock_async_connection)


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
    conn.connection_info.update({
        "project_id": "test-project",
        "dataset_id": "test_dataset",
        "supports_arrays": "True",
        "supports_structs": "True",
    })
    return conn


@pytest.fixture(autouse=True)
def test_isolation() -> "Generator[None, None, None]":
    """Auto-use fixture to ensure test isolation by resetting global state."""

    yield


@pytest.fixture
def cleanup_test_state() -> "Generator[Callable[[Callable[[], None]], None], None, None]":
    """Fixture that provides a cleanup function for test state management."""
    cleanup_functions = []

    def register_cleanup(func: "Callable[[], None]") -> None:
        """Register a cleanup function to be called during teardown."""
        cleanup_functions.append(func)

    yield register_cleanup

    for cleanup_func in reversed(cleanup_functions):
        try:
            cleanup_func()
        except Exception:
            pass


@pytest.fixture(scope="session", autouse=True)
def reset_global_state() -> "Generator[None, None, None]":
    """Session-scoped fixture to reset global state before and after test session."""

    yield


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

    timer.times = times  # pyright: ignore[reportFunctionMemberAccess]
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
            return {"rss": memory_info.rss, "vms": memory_info.vms, "percent": process.memory_percent()}

        yield get_memory_usage

    except ImportError:

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

    record_compilation.metrics = metrics  # pyright: ignore[reportFunctionMemberAccess]
    yield record_compilation


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


def create_test_sql_statement(sql: str, *params: Any, **kwargs: Any) -> SQL:
    """Helper function to create SQL statements for testing."""
    return SQL(sql, *params, **kwargs)


def assert_sql_execution_result(result: ExecutionResult, expected_rowcount: int = -1) -> None:
    """Helper function to assert SQL execution results."""
    assert result is not None
    if expected_rowcount >= 0:
        assert result.data_row_count == expected_rowcount
