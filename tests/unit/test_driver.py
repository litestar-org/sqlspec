"""Tests for sqlspec.driver module."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlglot import exp

from sqlspec import (
    SQL,
    AsyncDriverAdapterBase,
    ExecutionResult,
    ParameterStyle,
    ParameterStyleConfig,
    SQLResult,
    StatementConfig,
    SyncDriverAdapterBase,
)

# Test Fixtures and Mock Classes


def _create_test_statement_config() -> StatementConfig:
    """Create a StatementConfig for testing purposes."""
    return StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        )
    )


def _create_test_statement_config_no_validation() -> StatementConfig:
    """Create a StatementConfig for testing purposes with validation disabled."""
    return StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
        enable_validation=False,
    )


@pytest.fixture(autouse=True)
def clear_prometheus_registry() -> None:
    """Clear Prometheus registry before each test to avoid conflicts."""
    try:
        from prometheus_client import REGISTRY

        # Clear all collectors to avoid registration conflicts
        collectors = list(REGISTRY._collector_to_names.keys())
        for collector in collectors:
            try:
                REGISTRY.unregister(collector)
            except KeyError:
                pass  # Already unregistered
    except ImportError:
        pass  # Prometheus not available


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, name: str = "mock_connection") -> None:
        self.name = name
        self.connected = True

    def execute(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        return [{"result": "mock_data"}]

    def close(self) -> None:
        self.connected = False


class MockAsyncConnection:
    """Mock async connection for testing."""

    def __init__(self, name: str = "mock_async_connection") -> None:
        self.name = name
        self.connected = True

    async def execute(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        return [{"result": "mock_async_data"}]

    async def close(self) -> None:
        self.connected = False


class MockSyncDriver(SyncDriverAdapterBase):
    """Test sync driver implementation."""

    dialect = "sqlite"  # Use valid SQLGlot dialect

    def __init__(self, connection: MockConnection, statement_config: StatementConfig | None = None) -> None:
        if statement_config is None:
            statement_config = _create_test_statement_config()
        super().__init__(connection, statement_config)

    def _try_special_handling(self, cursor: Any, statement: SQL) -> SQLResult | None:
        """Hook for mock-specific special operations - none needed."""
        return None

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any, statement: SQL) -> ExecutionResult:
        """Execute single SQL statement using mock cursor."""
        cursor.execute(sql, prepared_params or ())

        # Determine if this is a SELECT statement
        sql_upper = sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            # Mock SELECT result - empty as expected by tests
            mock_data: list[dict[str, Any]] = []
            return self.create_execution_result(
                cursor_result=mock_data,
                selected_data=mock_data,
                column_names=["id", "name"],
                data_row_count=len(mock_data),
                is_select_result=True,
            )
        return self.create_execution_result(cursor_result=None, rowcount_override=1, is_select_result=False)

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: SQL) -> Any:
        """Execute SQL with multiple parameter sets using mock cursor."""
        return cursor.executemany(sql, prepared_params)

    def _get_selected_data(self, cursor: Any) -> tuple[list[dict[str, Any]], list[str], int]:
        """Extract data from cursor after SELECT execution."""
        result_data = cursor.fetchall() if hasattr(cursor, "fetchall") else []
        # If cursor.fetchall returns a non-list (e.g., Mock return value), use it directly
        if not isinstance(result_data, list) and hasattr(result_data, "__iter__"):
            result_data = list(result_data)
        column_names = list(result_data[0].keys()) if result_data else []
        return result_data, column_names, len(result_data)

    def _get_row_count(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return 1  # Mock always returns 1 affected row

    def _execute_script(
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: StatementConfig, statement: SQL
    ) -> Any:
        """Execute a SQL script (multiple statements)."""
        # Mock implementation - just execute as single statement
        return cursor.execute(sql, prepared_params or ())

    def begin(self) -> None:
        """Mock begin transaction."""
        pass

    def commit(self) -> None:
        """Mock commit transaction."""
        pass

    def rollback(self) -> None:
        """Mock rollback transaction."""
        pass

    def with_cursor(self, connection: Any = None) -> Any:
        """Mock cursor context manager."""
        from collections.abc import Iterator
        from contextlib import contextmanager

        @contextmanager
        def cursor_context() -> Iterator[Any]:
            cursor = Mock()
            cursor.execute = Mock()
            cursor.fetchall = Mock(return_value=[])
            yield cursor

        return cursor_context()

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NAMED_COLON

    def returns_rows(self, expression: Any) -> bool:
        """Mock implementation of returns_rows from CommonDriverAttributesMixin."""
        from sqlglot import expressions as exp

        if expression is None:
            return False

        # Row-returning expressions
        if isinstance(expression, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma)):
            return True

        # Handle WITH clauses
        if isinstance(expression, exp.With):
            if expression.expressions:
                return self.returns_rows(expression.expressions[0])

        # Handle RETURNING clause
        if hasattr(expression, "find") and expression.find(exp.Returning):
            return True

        return False

    def _execute_sql(self, statement: SQL, connection: Any | None = None, **kwargs: Any) -> SQLResult:
        conn = connection or self.connection
        if statement.is_script:
            return SQLResult(
                statement=statement,
                data=[],
                operation_type="SCRIPT",
                metadata={"message": "Script executed successfully"},
            )

        result_data = conn.execute(statement.sql, statement.parameters)  # pyright: ignore[reportAttributeAccessIssue]

        # Determine operation type from SQL
        sql_upper = statement.sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            operation_type = "SELECT"
        elif sql_upper.startswith("INSERT"):
            operation_type = "INSERT"
        elif sql_upper.startswith("UPDATE"):
            operation_type = "UPDATE"
        elif sql_upper.startswith("DELETE"):
            operation_type = "DELETE"
        else:
            operation_type = "EXECUTE"

        return SQLResult(
            statement=statement,
            data=result_data if operation_type == "SELECT" else [],
            column_names=list(result_data[0].keys()) if result_data and operation_type == "SELECT" else [],
            operation_type=operation_type,  # type: ignore  # operation_type is dynamically determined
            rows_affected=1 if operation_type != "SELECT" else 0,
        )

    def _wrap_select_result(self, statement: SQL, result: Any, schema_type: type | None = None, **kwargs: Any) -> Mock:
        mock_result = Mock()
        mock_result.rows = result
        mock_result.row_count = len(result) if hasattr(result, "__len__") and result else 0
        return mock_result  # type: ignore

    def _wrap_execute_result(self, statement: SQL, result: Any, **kwargs: Any) -> Mock:
        result = Mock()
        result.affected_count = 1
        result.last_insert_id = None
        return result  # type: ignore


class MockAsyncDriver(AsyncDriverAdapterBase):
    """Test async driver implementation."""

    dialect = "postgres"
    parameter_style = ParameterStyle.NAMED_COLON

    def __init__(self, connection: MockAsyncConnection, statement_config: StatementConfig | None = None) -> None:
        if statement_config is None:
            statement_config = _create_test_statement_config()
        super().__init__(connection, statement_config)

    async def _try_special_handling(self, cursor: Any, statement: SQL) -> SQLResult | None:
        """Hook for mock-specific special operations - none needed."""
        return None

    async def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any, statement: SQL) -> ExecutionResult:
        """Execute single SQL statement using mock cursor."""
        await cursor.execute(sql, prepared_params or ())

        # Determine if this is a SELECT statement
        sql_upper = sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            # Mock SELECT result - empty as expected by tests
            mock_data: list[dict[str, Any]] = []
            return self.create_execution_result(
                cursor_result=mock_data,
                selected_data=mock_data,
                column_names=["id", "name"],
                data_row_count=len(mock_data),
                is_select_result=True,
            )
        # Mock non-SELECT result
        return self.create_execution_result(cursor_result=None, rowcount_override=1, is_select_result=False)

    async def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: SQL) -> Any:
        """Execute SQL with multiple parameter sets using mock cursor."""
        return await cursor.executemany(sql, prepared_params)

    async def _get_selected_data(self, cursor: Any) -> tuple[list[dict[str, Any]], list[str], int]:
        """Extract data from cursor after SELECT execution."""
        result_data = await cursor.fetchall() if hasattr(cursor, "fetchall") else []
        # If cursor.fetchall returns a non-list (e.g., Mock return value), use it directly
        if not isinstance(result_data, list) and hasattr(result_data, "__iter__"):
            result_data = list(result_data)
        column_names = list(result_data[0].keys()) if result_data else []
        return result_data, column_names, len(result_data)

    def _get_row_count(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return 1  # Mock always returns 1 affected row

    async def begin(self) -> None:
        """Mock async begin transaction."""
        pass

    async def commit(self) -> None:
        """Mock async commit transaction."""
        pass

    async def rollback(self) -> None:
        """Mock async rollback transaction."""
        pass

    def with_cursor(self, connection: Any = None) -> Any:
        """Mock async cursor context manager."""
        from collections.abc import AsyncIterator
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def cursor_context() -> AsyncIterator[Any]:
            cursor = Mock()
            cursor.execute = Mock(return_value=None)
            cursor.fetchall = Mock(return_value=[])
            # Make the mocks async
            cursor.execute = AsyncMock()
            cursor.fetchall = AsyncMock(return_value=[])
            yield cursor

        return cursor_context()

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NAMED_COLON

    def returns_rows(self, expression: Any) -> bool:
        """Mock implementation of returns_rows from CommonDriverAttributesMixin."""
        from sqlglot import expressions as exp

        if expression is None:
            return False

        # Row-returning expressions
        if isinstance(expression, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma)):
            return True

        # Handle WITH clauses
        if isinstance(expression, exp.With):
            if expression.expressions:
                return self.returns_rows(expression.expressions[0])

        # Handle RETURNING clause
        if hasattr(expression, "find") and expression.find(exp.Returning):
            return True

        return False

    async def _execute_sql(self, statement: SQL, connection: Any | None = None, **kwargs: Any) -> SQLResult:
        conn = connection or self.connection
        if statement.is_script:
            return SQLResult(
                statement=statement,
                data=[],
                operation_type="SCRIPT",
                metadata={"message": "Async script executed successfully"},
            )

        result_data = await conn.execute(statement.sql, statement.parameters)  # pyright: ignore[reportAttributeAccessIssue]

        # Determine operation type from SQL
        sql_upper = statement.sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            operation_type = "SELECT"
        elif sql_upper.startswith("INSERT"):
            operation_type = "INSERT"
        elif sql_upper.startswith("UPDATE"):
            operation_type = "UPDATE"
        elif sql_upper.startswith("DELETE"):
            operation_type = "DELETE"
        else:
            operation_type = "EXECUTE"

        return SQLResult(
            statement=statement,
            data=result_data if operation_type == "SELECT" else [],
            column_names=list(result_data[0].keys()) if result_data and operation_type == "SELECT" else [],
            operation_type=operation_type,  # type: ignore  # operation_type is dynamically determined
            rows_affected=1 if operation_type != "SELECT" else 0,
        )

    async def _wrap_select_result(
        self, statement: SQL, result: Any, schema_type: type | None = None, **kwargs: Any
    ) -> Mock:
        mock_result = Mock()
        mock_result.rows = result
        mock_result.row_count = len(result) if hasattr(result, "__len__") and result else 0
        return mock_result  # type: ignore

    async def _wrap_execute_result(self, statement: SQL, result: Any, **kwargs: Any) -> Mock:
        mock_result = Mock()
        mock_result.affected_count = 1
        mock_result.last_insert_id = None
        return mock_result  # type: ignore


def test_common_driver_attributes_initialization() -> None:
    """Test CommonDriverAttributes initialization."""
    connection = MockConnection()
    config = _create_test_statement_config()

    driver = MockSyncDriver(connection, config)

    assert driver.connection is connection
    assert driver.statement_config is config


def test_common_driver_attributes_default_values() -> None:
    """Test CommonDriverAttributes with default values."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    assert driver.connection is connection
    assert isinstance(driver.statement_config, StatementConfig)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (exp.Select(), True),
        (exp.Values(), True),
        (exp.Table(), True),
        (exp.Show(), True),
        (exp.Describe(), True),
        (exp.Pragma(), True),
        (exp.Insert(), False),
        (exp.Update(), False),
        (exp.Delete(), False),
        (exp.Create(), False),
        (exp.Drop(), False),
        (None, False),
    ],
    ids=[
        "select",
        "values",
        "table",
        "show",
        "describe",
        "pragma",
        "insert",
        "update",
        "delete",
        "create",
        "drop",
        "none",
    ],
)
def test_common_driver_attributes_returns_rows(expression: exp.Expression | None, expected: bool) -> None:
    """Test returns_rows method."""
    # Create a driver instance to test the method
    driver = MockSyncDriver(MockConnection())
    result = driver.returns_rows(expression)
    assert result == expected


def test_common_driver_attributes_returns_rows_with_clause() -> None:
    """Test returns_rows with WITH clause."""
    driver = MockSyncDriver(MockConnection())

    # WITH clause with SELECT
    with_select = exp.With(expressions=[exp.Select()])
    assert driver.returns_rows(with_select) is True

    # WITH clause with INSERT
    with_insert = exp.With(expressions=[exp.Insert()])
    assert driver.returns_rows(with_insert) is False


def test_common_driver_attributes_returns_rows_returning_clause() -> None:
    """Test returns_rows with RETURNING clause."""
    driver = MockSyncDriver(MockConnection())

    # INSERT with RETURNING
    insert_returning = exp.Insert()
    insert_returning.set("expressions", [exp.Returning()])

    with patch.object(insert_returning, "find", return_value=exp.Returning()):
        assert driver.returns_rows(insert_returning) is True


def test_sync_driver_build_statement() -> None:
    """Test sync driver statement building."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    # Test with SQL string
    sql_string = "SELECT * FROM users"
    statement = driver.prepare_statement(sql_string, statement_config=_create_test_statement_config())
    assert isinstance(statement, SQL)
    assert statement.sql == sql_string


def test_sync_driver_build_statement_with_sql_object() -> None:
    """Test sync driver statement building with SQL object."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    sql_obj = SQL("SELECT * FROM users WHERE id = :id", id=1)
    statement = driver.prepare_statement(sql_obj, statement_config=_create_test_statement_config())
    # SQL objects are immutable, so a new instance is created
    assert isinstance(statement, SQL)
    assert statement._raw_sql == sql_obj._raw_sql
    assert statement._named_params == sql_obj._named_params  # type: ignore[attr-defined]


def test_sync_driver_build_statement_with_filters() -> None:
    """Test sync driver statement building with filters."""
    from sqlspec.statement.filters import StatementFilter

    connection = MockConnection()
    driver = MockSyncDriver(connection)

    # Create a real test filter class that properly implements the protocol
    class TestFilter(StatementFilter):
        def append_to_statement(self, statement: SQL) -> SQL:
            return SQL("SELECT * FROM users WHERE active = true")

        def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
            return [], {}

        def get_cache_key(self) -> tuple[Any, ...]:
            return ("test_filter", "active", True)

    # Create an instance and spy on its methods
    test_filter = TestFilter()

    # Mock the append_to_statement method to track calls
    original_append = test_filter.append_to_statement
    test_filter.append_to_statement = Mock(side_effect=original_append)

    sql_string = "SELECT * FROM users"
    statement = driver.prepare_statement(sql_string, test_filter, statement_config=_create_test_statement_config())

    # Access a property to trigger processing
    _ = statement.to_sql()

    test_filter.append_to_statement.assert_called_once()


def test_sync_driver_execute_select() -> None:
    """Test sync driver execute with SELECT statement."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # In the new architecture, dispatch_statement_execution returns SQLResult directly
        mock_result = Mock(spec=SQLResult)
        mock_result.data = [{"id": 1, "name": "test"}]
        mock_execute.return_value = mock_result

        result = driver.execute("SELECT * FROM users")

        mock_execute.assert_called_once()
        assert result is mock_result


def test_sync_driver_execute_insert() -> None:
    """Test sync driver execute with INSERT statement."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # In the new architecture, dispatch_statement_execution returns SQLResult directly
        mock_result = Mock(spec=SQLResult)
        mock_result.rows_affected = 1
        mock_result.operation_type = "INSERT"
        mock_execute.return_value = mock_result

        result = driver.execute("INSERT INTO users (name) VALUES ('test')")

        mock_execute.assert_called_once()
        assert result is mock_result


def test_sync_driver_execute_many() -> None:
    """Test sync driver execute_many."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    parameters = [{"name": "user1"}, {"name": "user2"}]

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # In the new architecture, dispatch_statement_execution returns SQLResult directly
        mock_result = Mock(spec=SQLResult)
        mock_result.rows_affected = 2
        mock_result.operation_type = "EXECUTE"
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid validation issues
        config = _create_test_statement_config()
        result = driver.execute_many("INSERT INTO users (name) VALUES (:name)", parameters, _config=config)

        mock_execute.assert_called_once()
        assert result is mock_result


def test_sync_driver_execute_script() -> None:
    """Test sync driver execute_script."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # In the new architecture, dispatch_statement_execution returns SQLResult directly
        mock_result = Mock(spec=SQLResult)
        mock_result.operation_type = "SCRIPT"
        mock_result.total_statements = 1
        mock_result.successful_statements = 1
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid DDL validation issues
        config = _create_test_statement_config_no_validation()
        result = driver.execute_script(script, _config=config)

        mock_execute.assert_called_once()
        # Check that the statement passed to dispatch_statement_execution has is_script=True
        call_args = mock_execute.call_args
        statement = call_args[1]["statement"]
        assert statement.is_script is True
        # Result should be wrapped in SQLResult object
        assert hasattr(result, "operation_type")
        assert result.operation_type == "SCRIPT"


def test_sync_driver_execute_with_parameters() -> None:
    """Test sync driver execute with parameters."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    # Only provide parameters that are actually used in the SQL

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # dispatch_statement_execution should return SQLResult
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users WHERE id = :id"),
            data=[{"id": 1, "name": "test"}],
            column_names=["id", "name"],
            operation_type="SELECT",
        )
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid validation issues
        config = _create_test_statement_config()
        # Pass named parameters as keyword arguments
        result = driver.execute("SELECT * FROM users WHERE id = :id", id=1, _config=config)

        mock_execute.assert_called_once()
        # Check that the statement passed to dispatch_statement_execution contains the parameters
        call_args = mock_execute.call_args
        statement = call_args[1]["statement"]
        # For NAMED_COLON style, parameters remain as a dict
        assert "id" in statement.parameters
        assert statement.parameters["id"] == 1
        assert result == mock_result


# AsyncDriverAdapterBase Tests


async def test_async_driver_build_statement() -> None:
    """Test async driver statement building."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    # Test with SQL string
    sql_string = "SELECT * FROM users"
    statement = driver.prepare_statement(sql_string, statement_config=_create_test_statement_config())
    assert isinstance(statement, SQL)
    assert statement.sql == sql_string


async def test_async_driver_execute_select() -> None:
    """Test async driver execute with SELECT statement."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # dispatch_statement_execution should return SQLResult directly
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users"),
            data=[{"id": 1, "name": "test"}],
            column_names=["id", "name"],
            operation_type="SELECT",
            rows_affected=1,
        )
        mock_execute.return_value = mock_result

        result = await driver.execute("SELECT * FROM users")

        mock_execute.assert_called_once()
        assert result == mock_result


async def test_async_driver_execute_insert() -> None:
    """Test async driver execute with INSERT statement."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # dispatch_statement_execution should return SQLResult directly
        mock_result = SQLResult(
            statement=SQL("INSERT INTO users (name) VALUES ('test')"),
            data=[],
            operation_type="INSERT",
            rows_affected=1,
            last_inserted_id=1,
        )
        mock_execute.return_value = mock_result

        result = await driver.execute("INSERT INTO users (name) VALUES ('test')")

        mock_execute.assert_called_once()
        assert result == mock_result


async def test_async_driver_execute_many() -> None:
    """Test async driver execute_many."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    parameters = [{"name": "user1"}, {"name": "user2"}]

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # dispatch_statement_execution should return SQLResult directly
        mock_result = SQLResult(
            statement=SQL("INSERT INTO users (name) VALUES (:name)"), data=[], operation_type="INSERT", rows_affected=2
        )
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid validation issues
        config = _create_test_statement_config()
        result = await driver.execute_many(
            "INSERT INTO users (name) VALUES (:name)", parameters, statement_config=config
        )

        mock_execute.assert_called_once()
        assert result == mock_result


async def test_async_driver_execute_script() -> None:
    """Test async driver execute_script."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # dispatch_statement_execution should return SQLResult directly
        mock_result = SQLResult(statement=SQL(script), data=[], operation_type="SCRIPT", metadata={"status": "success"})
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid DDL validation issues
        config = _create_test_statement_config_no_validation()
        result = await driver.execute_script(script, _config=config)

        mock_execute.assert_called_once()
        # Check that the statement passed to dispatch_statement_execution has is_script=True
        call_args = mock_execute.call_args
        statement = call_args[1]["statement"]
        assert statement.is_script is True
        assert result == mock_result
        # Result should be wrapped in SQLResult object
        assert hasattr(result, "operation_type")
        assert result.operation_type == "SCRIPT"


# Error Handling Tests


def test_sync_driverdispatch_statement_execution_exception() -> None:
    """Test sync driver dispatch_statement_execution exception handling."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution", side_effect=Exception("Database error")):
        with pytest.raises(Exception, match="Database error"):
            driver.execute("SELECT * FROM users")


async def test_async_driverdispatch_statement_execution_exception() -> None:
    """Test async driver dispatch_statement_execution exception handling."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution", side_effect=Exception("Async database error")):
        with pytest.raises(Exception, match="Async database error"):
            await driver.execute("SELECT * FROM users")


def test_sync_driver_wrap_result_exception() -> None:
    """Test sync driver exception handling."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution", side_effect=Exception("Execute error")):
        with pytest.raises(Exception, match="Execute error"):
            driver.execute("SELECT * FROM users")


async def test_async_driver_wrap_result_exception() -> None:
    """Test async driver exception handling."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution", side_effect=Exception("Async execute error")):
        with pytest.raises(Exception, match="Async execute error"):
            await driver.execute("SELECT * FROM users")


@pytest.mark.parametrize(
    ("statement_type", "expected_returns_rows"),
    [
        ("SELECT * FROM users", True),
        ("INSERT INTO users (name) VALUES ('test')", False),
        ("UPDATE users SET name = 'updated' WHERE id = 1", False),
        ("DELETE FROM users WHERE id = 1", False),
        ("CREATE TABLE test (id INT)", False),
        ("DROP TABLE test", False),
    ],
    ids=["select", "insert", "update", "delete", "create", "drop"],
)
def test_driver_returns_rows_detection(statement_type: str, expected_returns_rows: bool) -> None:
    """Test driver returns_rows detection for various statement types."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "dispatch_statement_execution") as mock_execute:
        # Determine operation type based on statement
        if "SELECT" in statement_type:
            operation_type = "SELECT"
        elif "INSERT" in statement_type:
            operation_type = "INSERT"
        elif "UPDATE" in statement_type:
            operation_type = "UPDATE"
        elif "DELETE" in statement_type:
            operation_type = "DELETE"
        else:
            operation_type = "EXECUTE"  # For DDL

        # dispatch_statement_execution should return SQLResult directly
        mock_result = SQLResult(
            statement=SQL(statement_type),
            data=[{"data": "test"}] if expected_returns_rows else [],
            column_names=["data"] if expected_returns_rows else [],
            operation_type=operation_type,  # type: ignore  # operation_type is dynamically determined
            rows_affected=1 if not expected_returns_rows else 0,
        )
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid DDL validation issues
        config = _create_test_statement_config_no_validation()
        result = driver.execute(statement_type, _config=config)

        mock_execute.assert_called_once()
        assert result == mock_result

        # Verify the result has appropriate data
        if expected_returns_rows:
            assert result.data  # Should have data for SELECT
        else:
            assert result.rows_affected is not None  # Should have rows_affected for DML/DDL


# Concurrent and Threading Tests


async def test_async_driver_concurrent_execution() -> None:
    """Test async driver concurrent execution."""
    import asyncio

    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    async def execute_query(query_id: int) -> Any:
        return await driver.execute(f"SELECT {query_id} as id")

    # Execute multiple queries concurrently
    tasks = [execute_query(i) for i in range(5)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 5


# Integration Tests


def test_driver_full_execution_flow() -> None:
    """Test complete driver execution flow."""
    connection = MockConnection()
    config = _create_test_statement_config()  # Use non-strict config
    driver = MockSyncDriver(connection, config)

    # Test actual execution with MockSyncDriver's built-in logic
    # The MockSyncDriver should handle the execution properly
    result = driver.execute("SELECT * FROM users WHERE id = :id", {"id": 1})

    # Verify result structure (should be SQLResult)
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    # MockSyncDriver doesn't actually execute queries, so data will be empty
    assert result.data == []


async def test_async_driver_full_execution_flow() -> None:
    """Test complete async driver execution flow."""
    connection = MockAsyncConnection()
    config = _create_test_statement_config()  # Use non-strict config

    driver = MockAsyncDriver(connection, config)

    # Test actual execution with MockAsyncDriver's built-in logic
    # The MockAsyncDriver should handle the execution properly
    result = await driver.execute("SELECT * FROM users WHERE id = :id", {"id": 1})

    # Verify result structure (should be SQLResult)
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    # MockAsyncDriver doesn't actually execute queries, so data will be empty
    assert result.data == []
