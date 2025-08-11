"""Shared fixtures for adapter testing."""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional

import pytest

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

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
        self.last_sql: Optional[str] = None
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
        self.last_sql: Optional[str] = None
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
        self.description: Optional[list[tuple[str, ...]]] = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock execute method."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

        # Set mock results based on SQL type
        if sql.upper().strip().startswith("SELECT"):
            self.description = [("id", "INTEGER"), ("name", "TEXT")]
            # Always return 2 rows - tests expect this
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
        self.description: Optional[list[tuple[str, ...]]] = None
        self.fetchall_result: list[tuple[Any, ...]] = []
        self.closed = False

    async def execute(self, sql: str, parameters: Any = None) -> None:
        """Mock async execute method."""
        self.connection.execute_count += 1
        self.connection.last_sql = sql
        self.connection.last_parameters = parameters

        # Set mock results based on SQL type
        if sql.upper().strip().startswith("SELECT"):
            self.description = [("id", "INTEGER"), ("name", "TEXT")]
            # Always return 2 rows - tests expect this
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


class MockSyncDriver(SyncDriverAdapterBase):
    """Mock sync driver for testing."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: MockSyncConnection,
        statement_config: Optional[StatementConfig] = None,
        driver_features: Optional["dict[str, Any]"] = None,
    ) -> None:
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="sqlite",
                enable_caching=False,  # Disable caching to avoid hash issues
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
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
        # If this is an is_many statement, delegate to _execute_many
        if statement.is_many:
            return self._execute_many(cursor, statement)
        # If this is a script statement, delegate to _execute_script
        if statement.is_script:
            return self._execute_script(cursor, statement)

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
        statement_config: Optional[StatementConfig] = None,
        driver_features: Optional["dict[str, Any]"] = None,
    ) -> None:
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="sqlite",
                enable_caching=False,  # Disable caching to avoid hash issues
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
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
        # If this is an is_many statement, delegate to _execute_many
        if statement.is_many:
            return await self._execute_many(cursor, statement)
        # If this is a script statement, delegate to _execute_script
        if statement.is_script:
            return await self._execute_script(cursor, statement)

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
    return MockSyncDriver(mock_sync_connection)


@pytest.fixture
def mock_async_driver(mock_async_connection: MockAsyncConnection) -> MockAsyncDriver:
    """Fixture for mock async driver."""
    return MockAsyncDriver(mock_async_connection)


@pytest.fixture
def sample_statement_config() -> StatementConfig:
    """Sample statement configuration for testing."""
    return StatementConfig(
        dialect="sqlite",
        enable_caching=False,  # Disable caching to avoid hash issues
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
