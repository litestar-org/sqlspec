"""Tests for sqlspec.driver module."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

import pytest
from sqlglot import exp

from sqlspec.driver import AsyncDriverAdapterProtocol, CommonDriverAttributesMixin, SyncDriverAdapterProtocol
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow

# Test Fixtures and Mock Classes


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


class MockSyncDriver(SyncDriverAdapterProtocol[MockConnection, DictRow]):
    """Test sync driver implementation."""

    dialect = "sqlite"  # Use valid SQLGlot dialect
    parameter_style = ParameterStyle.NAMED_COLON

    def __init__(
        self, connection: MockConnection, config: SQLConfig | None = None, default_row_type: type[DictRow] = DictRow
    ) -> None:
        super().__init__(connection, config, default_row_type)

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NAMED_COLON

    def _execute_statement(
        self, statement: SQL, connection: MockConnection | None = None, **kwargs: Any
    ) -> SQLResult[DictRow]:
        conn = connection or self.connection
        if statement.is_script:
            return SQLResult(
                statement=statement,
                data=[],
                operation_type="SCRIPT",
                metadata={"message": "Script executed successfully"},
            )

        result_data = conn.execute(statement.sql, statement.parameters)

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


class MockAsyncDriver(AsyncDriverAdapterProtocol[MockAsyncConnection, DictRow]):
    """Test async driver implementation."""

    dialect = "postgres"  # Use valid SQLGlot dialect
    parameter_style = ParameterStyle.NAMED_COLON

    def __init__(
        self,
        connection: MockAsyncConnection,
        config: SQLConfig | None = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(connection, config, default_row_type)

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NAMED_COLON

    async def _execute_statement(
        self, statement: SQL, connection: MockAsyncConnection | None = None, **kwargs: Any
    ) -> SQLResult[DictRow]:
        conn = connection or self.connection
        if statement.is_script:
            return SQLResult(
                statement=statement,
                data=[],
                operation_type="SCRIPT",
                metadata={"message": "Async script executed successfully"},
            )

        result_data = await conn.execute(statement.sql, statement.parameters)

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
    config = SQLConfig()

    driver = MockSyncDriver(connection, config, DictRow)

    assert driver.connection is connection
    assert driver.config is config
    assert driver.default_row_type is DictRow


def test_common_driver_attributes_default_values() -> None:
    """Test CommonDriverAttributes with default values."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    assert driver.connection is connection
    assert isinstance(driver.config, SQLConfig)
    assert driver.default_row_type is not None


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


def test_common_driver_attributes_check_not_found_success() -> None:
    """Test check_not_found with valid item."""
    item = "test_item"
    result = CommonDriverAttributesMixin.check_not_found(item)
    assert result == item


def test_common_driver_attributes_check_not_found_none() -> None:
    """Test check_not_found with None."""
    from sqlspec.exceptions import NotFoundError

    with pytest.raises(NotFoundError, match="No result found"):
        CommonDriverAttributesMixin.check_not_found(None)


def test_common_driver_attributes_check_not_found_falsy() -> None:
    """Test check_not_found with various falsy values."""
    from sqlspec.exceptions import NotFoundError

    # None should raise
    with pytest.raises(NotFoundError):
        CommonDriverAttributesMixin.check_not_found(None)

    # Empty list should not raise (it's not None)
    result: list[Any] = CommonDriverAttributesMixin.check_not_found([])
    assert result == []

    # Empty string should not raise
    result_str: str = CommonDriverAttributesMixin.check_not_found("")
    assert result_str == ""

    # Zero should not raise
    result_int: int = CommonDriverAttributesMixin.check_not_found(0)
    assert result_int == 0


def test_sync_driver_build_statement() -> None:
    """Test sync driver statement building."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    # Test with SQL string
    sql_string = "SELECT * FROM users"
    statement = driver._build_statement(sql_string, None, None)
    assert isinstance(statement, SQL)
    assert statement.sql == sql_string


def test_sync_driver_build_statement_with_sql_object() -> None:
    """Test sync driver statement building with SQL object."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    sql_obj = SQL("SELECT * FROM users WHERE id = :id", id=1)
    statement = driver._build_statement(sql_obj)
    # SQL objects are immutable, so a new instance is created
    assert isinstance(statement, SQL)
    assert statement._raw_sql == sql_obj._raw_sql
    assert statement._named_params == sql_obj._named_params  # type: ignore[attr-defined]


def test_sync_driver_build_statement_with_filters() -> None:
    """Test sync driver statement building with filters."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    # Mock filter - needs both methods
    mock_filter = Mock()

    def mock_append(stmt: Any) -> SQL:
        # Return a new SQL object with modified query
        return SQL("SELECT * FROM users WHERE active = true")

    mock_filter.append_to_statement = Mock(side_effect=mock_append)
    mock_filter.extract_parameters = Mock(return_value=([], {}))

    sql_string = "SELECT * FROM users"
    statement = driver._build_statement(sql_string, mock_filter)

    # Access a property to trigger processing
    _ = statement.to_sql()

    mock_filter.append_to_statement.assert_called_once()


def test_sync_driver_execute_select() -> None:
    """Test sync driver execute with SELECT statement."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "_execute_statement") as mock_execute:
        # In the new architecture, _execute_statement returns SQLResult directly
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

    with patch.object(driver, "_execute_statement") as mock_execute:
        # In the new architecture, _execute_statement returns SQLResult directly
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

    with patch.object(driver, "_execute_statement") as mock_execute:
        # In the new architecture, _execute_statement returns SQLResult directly
        mock_result = Mock(spec=SQLResult)
        mock_result.rows_affected = 2
        mock_result.operation_type = "EXECUTE"
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid validation issues
        config = SQLConfig()
        result = driver.execute_many("INSERT INTO users (name) VALUES (:name)", parameters, _config=config)

        mock_execute.assert_called_once()
        assert result is mock_result


def test_sync_driver_execute_script() -> None:
    """Test sync driver execute_script."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"

    with patch.object(driver, "_execute_statement") as mock_execute:
        # In the new architecture, _execute_statement returns SQLResult directly
        mock_result = Mock(spec=SQLResult)
        mock_result.operation_type = "SCRIPT"
        mock_result.total_statements = 1
        mock_result.successful_statements = 1
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid DDL validation issues
        config = SQLConfig(enable_validation=False)
        result = driver.execute_script(script, _config=config)

        mock_execute.assert_called_once()
        # Check that the statement passed to _execute_statement has is_script=True
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

    with patch.object(driver, "_execute_statement") as mock_execute:
        # _execute_statement should return SQLResult
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users WHERE id = :id"),
            data=[{"id": 1, "name": "test"}],
            column_names=["id", "name"],
            operation_type="SELECT",
        )
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid validation issues
        config = SQLConfig()
        # Pass named parameters as keyword arguments
        result = driver.execute("SELECT * FROM users WHERE id = :id", id=1, _config=config)

        mock_execute.assert_called_once()
        # Check that the statement passed to _execute_statement contains the parameters
        call_args = mock_execute.call_args
        statement = call_args[1]["statement"]
        # Named parameters should be in a dict
        assert statement.parameters == {"id": 1}
        assert result == mock_result


# AsyncDriverAdapterProtocol Tests


async def test_async_driver_build_statement() -> None:
    """Test async driver statement building."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    # Test with SQL string
    sql_string = "SELECT * FROM users"
    statement = driver._build_statement(sql_string, None, None)
    assert isinstance(statement, SQL)
    assert statement.sql == sql_string


async def test_async_driver_execute_select() -> None:
    """Test async driver execute with SELECT statement."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "_execute_statement") as mock_execute:
        # _execute_statement should return SQLResult directly
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

    with patch.object(driver, "_execute_statement") as mock_execute:
        # _execute_statement should return SQLResult directly
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

    with patch.object(driver, "_execute_statement") as mock_execute:
        # _execute_statement should return SQLResult directly
        mock_result = SQLResult(
            statement=SQL("INSERT INTO users (name) VALUES (:name)"), data=[], operation_type="INSERT", rows_affected=2
        )
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid validation issues
        config = SQLConfig()
        result = await driver.execute_many("INSERT INTO users (name) VALUES (:name)", parameters, _config=config)

        mock_execute.assert_called_once()
        assert result == mock_result


async def test_async_driver_execute_script() -> None:
    """Test async driver execute_script."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"

    with patch.object(driver, "_execute_statement") as mock_execute:
        # _execute_statement should return SQLResult directly
        mock_result = SQLResult(statement=SQL(script), data=[], operation_type="SCRIPT", metadata={"status": "success"})
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid DDL validation issues
        config = SQLConfig(enable_validation=False)
        result = await driver.execute_script(script, _config=config)

        mock_execute.assert_called_once()
        # Check that the statement passed to _execute_statement has is_script=True
        call_args = mock_execute.call_args
        statement = call_args[1]["statement"]
        assert statement.is_script is True
        assert result == mock_result
        # Result should be wrapped in SQLResult object
        assert hasattr(result, "operation_type")
        assert result.operation_type == "SCRIPT"


async def test_async_driver_execute_with_schema_type() -> None:
    """Test async driver execute with schema type."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "_execute_statement") as mock_execute:
        # _execute_statement should return SQLResult directly
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users"),
            data=[{"id": 1, "name": "test"}],
            column_names=["id", "name"],
            operation_type="SELECT",
            rows_affected=1,
        )
        mock_execute.return_value = mock_result

        # Note: This test may need adjustment based on actual schema_type support
        result = await driver.execute("SELECT * FROM users")

        mock_execute.assert_called_once()
        assert result == mock_result


# Error Handling Tests


def test_sync_driver_execute_statement_exception() -> None:
    """Test sync driver _execute_statement exception handling."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "_execute_statement", side_effect=Exception("Database error")):
        with pytest.raises(Exception, match="Database error"):
            driver.execute("SELECT * FROM users")


async def test_async_driver_execute_statement_exception() -> None:
    """Test async driver _execute_statement exception handling."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "_execute_statement", side_effect=Exception("Async database error")):
        with pytest.raises(Exception, match="Async database error"):
            await driver.execute("SELECT * FROM users")


def test_sync_driver_wrap_result_exception() -> None:
    """Test sync driver exception handling."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "_execute_statement", side_effect=Exception("Execute error")):
        with pytest.raises(Exception, match="Execute error"):
            driver.execute("SELECT * FROM users")


async def test_async_driver_wrap_result_exception() -> None:
    """Test async driver exception handling."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "_execute_statement", side_effect=Exception("Async execute error")):
        with pytest.raises(Exception, match="Async execute error"):
            await driver.execute("SELECT * FROM users")


def test_driver_connection_method() -> None:
    """Test driver _connection method."""
    connection1 = MockConnection("connection1")
    connection2 = MockConnection("connection2")
    driver = MockSyncDriver(connection1)

    # Without override, should return default connection
    assert driver._connection() is connection1

    # With override, should return override connection
    assert driver._connection(connection2) is connection2


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

    with patch.object(driver, "_execute_statement") as mock_execute:
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

        # _execute_statement should return SQLResult directly
        mock_result = SQLResult(
            statement=SQL(statement_type),
            data=[{"data": "test"}] if expected_returns_rows else [],
            column_names=["data"] if expected_returns_rows else [],
            operation_type=operation_type,  # type: ignore  # operation_type is dynamically determined
            rows_affected=1 if not expected_returns_rows else 0,
        )
        mock_execute.return_value = mock_result

        # Use a non-strict config to avoid DDL validation issues
        config = SQLConfig(enable_validation=False)
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


def test_sync_driver_multiple_connections() -> None:
    """Test sync driver with multiple connections."""
    connection1 = MockConnection("conn1")
    connection2 = MockConnection("conn2")
    driver = MockSyncDriver(connection1)

    # Execute with default connection
    with patch.object(driver, "_execute_statement") as mock_execute:
        mock_execute.return_value = []
        driver.execute("SELECT 1", _connection=None)
        _, kwargs = mock_execute.call_args
        assert kwargs["connection"] is connection1

    # Execute with override connection
    with patch.object(driver, "_execute_statement") as mock_execute:
        mock_execute.return_value = []
        driver.execute("SELECT 2", _connection=connection2)
        _, kwargs = mock_execute.call_args
        assert kwargs["connection"] is connection2


# Integration Tests


def test_driver_full_execution_flow() -> None:
    """Test complete driver execution flow."""
    connection = MockConnection()
    config = SQLConfig()  # Use non-strict config
    driver = MockSyncDriver(connection, config)

    # Mock the full execution flow
    with patch.object(connection, "execute", return_value=[{"id": 1, "name": "test"}]) as mock_conn_execute:
        result = driver.execute("SELECT * FROM users WHERE id = :id", {"id": 1})

        # Verify connection was called
        mock_conn_execute.assert_called_once()

        # Verify result structure (should be SQLResult)
        assert isinstance(result, SQLResult)
        assert result.data == [{"id": 1, "name": "test"}]
        assert result.column_names == ["id", "name"]
        assert result.operation_type == "SELECT"


async def test_async_driver_full_execution_flow() -> None:
    """Test complete async driver execution flow."""
    connection = MockAsyncConnection()
    config = SQLConfig()  # Use non-strict config

    driver = MockAsyncDriver(connection, config)

    # Mock the full async execution flow
    with patch.object(connection, "execute", return_value=[{"id": 1, "name": "test"}]) as mock_conn_execute:
        result = await driver.execute("SELECT * FROM users WHERE id = :id", {"id": 1})

        # Verify connection was called
        mock_conn_execute.assert_called_once()

        # Verify result structure (should be SQLResult)
        assert isinstance(result, SQLResult)
        assert result.data == [{"id": 1, "name": "test"}]
        assert result.column_names == ["id", "name"]
        assert result.operation_type == "SELECT"


def test_driver_supports_arrow_attribute() -> None:
    """Test driver __supports_arrow__ class attribute."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    # Default should be False
    assert driver.supports_native_arrow_export is False

    # Should be accessible as class attribute
    assert MockSyncDriver.supports_native_arrow_export is False
