"""Shared fixtures for adapter testing."""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

import pytest

from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig
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
    "mock_async_connection",
    "mock_async_driver",
    "mock_sync_connection",
    "mock_sync_driver",
    "sample_sql_statement",
    "sample_statement_config",
)


class MockSyncConnection:
    """Mock sync connection for testing."""

    def __init__(self, name: str = "mock_sync_connection") -> None:
        self.name = name
        self.connected = True
        self.in_transaction = False
        self.cursor_results: list[dict[str, Any]] = []
        self.execute_count = 0
        self.execute_many_count = 0
        self.last_sql: str | None = None
        self.last_parameters: Any = None

    def cursor(self) -> "MockSyncCursor":
        """Return a mock cursor."""
        return MockSyncCursor(self)

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock execute method."""
        self.execute_count += 1
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


class MockAsyncConnection:
    """Mock async connection for testing."""

    def __init__(self, name: str = "mock_async_connection") -> None:
        self.name = name
        self.connected = True
        self.in_transaction = False
        self.cursor_results: list[dict[str, Any]] = []
        self.execute_count = 0
        self.execute_many_count = 0
        self.last_sql: str | None = None
        self.last_parameters: Any = None

    async def cursor(self) -> "MockAsyncCursor":
        """Return a mock async cursor."""
        return MockAsyncCursor(self)

    async def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock async execute method."""
        self.execute_count += 1
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


class MockSyncCursor:
    """Mock sync cursor for testing."""

    def __init__(self, connection: MockSyncConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.description: list[tuple[str, ...]] | None = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock execute method."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

        if sql.upper().strip().startswith("SELECT"):
            self.description = [("id", "INTEGER"), ("name", "TEXT")]

            self.fetchall_result = [(1, "test"), (2, "example")]
            self.rowcount = len(self.fetchall_result)
        else:
            self.description = None
            self.fetchall_result = []
            self.rowcount = 1

    def executemany(self, sql: str, parameters: "list[Any]") -> None:
        """Mock executemany method."""
        self.connection.execute_many_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters
        self.rowcount = len(parameters) if parameters else 0

    def fetchall(self) -> "list[tuple[Any, ...]]":
        """Mock fetchall method."""
        return self.fetchall_result

    def close(self) -> None:
        """Mock close method."""
        self.closed = True


class MockAsyncCursor:
    """Mock async cursor for testing."""

    def __init__(self, connection: MockAsyncConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.description: list[tuple[str, ...]] | None = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.closed = False

    async def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock async execute method."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

        if sql.upper().strip().startswith("SELECT"):
            self.description = [("id", "INTEGER"), ("name", "TEXT")]

            self.fetchall_result = [(1, "test"), (2, "example")]
            self.rowcount = len(self.fetchall_result)
        else:
            self.description = None
            self.fetchall_result = []
            self.rowcount = 1

    async def executemany(self, sql: str, parameters: "list[Any]") -> None:
        """Mock async executemany method."""
        self.connection.execute_many_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters
        self.rowcount = len(parameters) if parameters else 0

    async def fetchall(self) -> "list[tuple[Any, ...]]":
        """Mock async fetchall method."""
        return self.fetchall_result

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
    """Mock sync driver for testing."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: MockSyncConnection,
        statement_config: StatementConfig | None = None,
        driver_features: Optional["dict[str, Any]"] = None,
    ) -> None:
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="sqlite",
                enable_caching=False,
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
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
        self.connection.in_transaction = True

    def rollback(self) -> None:
        """Mock rollback transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Mock commit transaction."""
        self.connection.commit()


class MockAsyncDriver(AsyncDriverAdapterBase):
    """Mock async driver for testing."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: MockAsyncConnection,
        statement_config: StatementConfig | None = None,
        driver_features: Optional["dict[str, Any]"] = None,
    ) -> None:
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="sqlite",
                enable_caching=False,
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
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
        self.connection.in_transaction = True

    async def rollback(self) -> None:
        """Mock async rollback transaction."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Mock async commit transaction."""
        await self.connection.commit()


@pytest.fixture
def mock_sync_connection() -> MockSyncConnection:
    """Fixture for mock sync connection."""
    return MockSyncConnection()


@pytest.fixture
def mock_async_connection() -> MockAsyncConnection:
    """Fixture for mock async connection."""
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
def sample_statement_config() -> StatementConfig:
    """Sample statement configuration for testing."""
    return StatementConfig(
        dialect="sqlite",
        enable_caching=False,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
            default_execution_parameter_style=ParameterStyle.QMARK,
            supported_execution_parameter_styles={ParameterStyle.QMARK},
        ),
    )


@pytest.fixture
def sample_sql_statement(sample_statement_config: StatementConfig) -> SQL:
    """Sample SQL statement for testing."""
    return SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=sample_statement_config)
