"""Tests for sqlspec.driver module."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlglot import exp

from sqlspec.driver import AsyncDriverAdapterProtocol, CommonDriverAttributesMixin, SyncDriverAdapterProtocol
from sqlspec.statement.parameters import ParameterStyle
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

    def _execute_statement(self, statement: SQL, connection: MockConnection | None = None, **kwargs: Any) -> Any:
        conn = connection or self.connection
        if statement.is_script:
            return "Script executed successfully"
        return conn.execute(statement.sql, statement.parameters)

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
    ) -> Any:
        conn = connection or self.connection
        if statement.is_script:
            return "Async script executed successfully"
        return await conn.execute(statement.sql, statement.parameters)

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

    sql_obj = SQL("SELECT * FROM users WHERE id = :id", parameters={"id": 1})
    statement = driver._build_statement(sql_obj)
    # SQL objects are immutable, so a new instance is created
    assert isinstance(statement, SQL)
    assert statement._raw_sql == sql_obj._raw_sql
    assert statement._named_params == sql_obj._named_params


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
        with patch.object(driver, "_wrap_select_result") as mock_wrap:
            mock_execute.return_value = [{"id": 1, "name": "test"}]
            mock_result = Mock()
            mock_wrap.return_value = mock_result

            result = driver.execute("SELECT * FROM users")

            mock_execute.assert_called_once()
            mock_wrap.assert_called_once()
            assert result is mock_result


def test_sync_driver_execute_insert() -> None:
    """Test sync driver execute with INSERT statement."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "_execute_statement") as mock_execute:
        with patch.object(driver, "_wrap_execute_result") as mock_wrap:
            mock_execute.return_value = 1
            mock_result = Mock()
            mock_wrap.return_value = mock_result

            result = driver.execute("INSERT INTO users (name) VALUES ('test')")

            mock_execute.assert_called_once()
            mock_wrap.assert_called_once()
            assert result is mock_result


def test_sync_driver_execute_many() -> None:
    """Test sync driver execute_many."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    parameters = [{"name": "user1"}, {"name": "user2"}]

    with patch.object(driver, "_execute_statement") as mock_execute:
        with patch.object(driver, "_wrap_execute_result") as mock_wrap:
            mock_execute.return_value = 2
            mock_result = Mock()
            mock_wrap.return_value = mock_result

            # Use a non-strict config to avoid validation issues
            config = SQLConfig(strict_mode=False)
            result = driver.execute_many("INSERT INTO users (name) VALUES (:name)", parameters, _config=config)

            mock_execute.assert_called_once()
            _, kwargs = mock_execute.call_args
            assert kwargs["is_many"] is True
            assert result is mock_result


def test_sync_driver_execute_script() -> None:
    """Test sync driver execute_script."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"

    with patch.object(driver, "_execute_statement") as mock_execute:
        mock_execute.return_value = "Script executed successfully"

        # Use a non-strict config to avoid DDL validation issues
        config = SQLConfig(strict_mode=False, enable_validation=False)
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
    parameters = {"id": 1}

    with patch.object(driver, "_execute_statement") as mock_execute:
        with patch.object(driver, "_wrap_select_result") as mock_wrap:
            mock_execute.return_value = [{"id": 1, "name": "test"}]
            mock_wrap.return_value = Mock()

            # Use a non-strict config to avoid validation issues
            config = SQLConfig(strict_mode=False)
            driver.execute("SELECT * FROM users WHERE id = :id", parameters, _config=config)

            mock_execute.assert_called_once()
            # Check that the statement passed to _execute_statement contains the parameters
            call_args = mock_execute.call_args
            statement = call_args[1]["statement"]
            assert statement.parameters == parameters


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
        with patch.object(driver, "_wrap_select_result") as mock_wrap:
            mock_execute.return_value = AsyncMock(return_value=[{"id": 1, "name": "test"}])
            mock_result = Mock()
            mock_wrap.return_value = AsyncMock(return_value=mock_result)

            await driver.execute("SELECT * FROM users")

            mock_execute.assert_called_once()
            mock_wrap.assert_called_once()


async def test_async_driver_execute_insert() -> None:
    """Test async driver execute with INSERT statement."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "_execute_statement") as mock_execute:
        with patch.object(driver, "_wrap_execute_result") as mock_wrap:
            mock_execute.return_value = AsyncMock(return_value=1)
            mock_result = Mock()
            mock_wrap.return_value = AsyncMock(return_value=mock_result)

            await driver.execute("INSERT INTO users (name) VALUES ('test')")

            mock_execute.assert_called_once()
            mock_wrap.assert_called_once()


async def test_async_driver_execute_many() -> None:
    """Test async driver execute_many."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    parameters = [{"name": "user1"}, {"name": "user2"}]

    with patch.object(driver, "_execute_statement") as mock_execute:
        with patch.object(driver, "_wrap_execute_result") as mock_wrap:
            mock_execute.return_value = AsyncMock(return_value=2)
            mock_result = Mock()
            mock_wrap.return_value = AsyncMock(return_value=mock_result)

            # Use a non-strict config to avoid validation issues
            config = SQLConfig(strict_mode=False)
            await driver.execute_many("INSERT INTO users (name) VALUES (:name)", parameters, _config=config)

            mock_execute.assert_called_once()
            _, kwargs = mock_execute.call_args
            assert kwargs["is_many"] is True


async def test_async_driver_execute_script() -> None:
    """Test async driver execute_script."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"

    with patch.object(driver, "_execute_statement") as mock_execute:
        # For async, we need to return the actual value, not an AsyncMock
        mock_execute.return_value = "Async script executed successfully"

        # Use a non-strict config to avoid DDL validation issues
        config = SQLConfig(strict_mode=False, enable_validation=False)
        result = await driver.execute_script(script, _config=config)

        mock_execute.assert_called_once()
        # Check that the statement passed to _execute_statement has is_script=True
        call_args = mock_execute.call_args
        statement = call_args[1]["statement"]
        assert statement.is_script is True
        # Result should be wrapped in SQLResult object
        assert hasattr(result, "operation_type")
        assert result.operation_type == "SCRIPT"


async def test_async_driver_execute_with_schema_type() -> None:
    """Test async driver execute with schema type."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "_execute_statement") as mock_execute:
        with patch.object(driver, "_wrap_select_result") as mock_wrap:
            mock_execute.return_value = AsyncMock(return_value=[{"id": 1, "name": "test"}])
            mock_wrap.return_value = AsyncMock(return_value=Mock())

            # Note: This test may need adjustment based on actual schema_type support
            await driver.execute("SELECT * FROM users")

            mock_wrap.assert_called_once()


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
    """Test sync driver result wrapping exception handling."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    with patch.object(driver, "_execute_statement", return_value=[{"data": "test"}]):
        with patch.object(driver, "_wrap_select_result", side_effect=Exception("Wrap error")):
            with pytest.raises(Exception, match="Wrap error"):
                driver.execute("SELECT * FROM users")


async def test_async_driver_wrap_result_exception() -> None:
    """Test async driver result wrapping exception handling."""
    connection = MockAsyncConnection()
    driver = MockAsyncDriver(connection)

    with patch.object(driver, "_execute_statement", return_value=AsyncMock(return_value=[{"data": "test"}])):
        with patch.object(driver, "_wrap_select_result", side_effect=Exception("Async wrap error")):
            with pytest.raises(Exception, match="Async wrap error"):
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
        with patch.object(driver, "_wrap_select_result") as mock_wrap_select:
            with patch.object(driver, "_wrap_execute_result") as mock_wrap_execute:
                mock_execute.return_value = [{"data": "test"}]
                mock_wrap_select.return_value = Mock()
                mock_wrap_execute.return_value = Mock()

                # Use a non-strict config to avoid DDL validation issues
                config = SQLConfig(strict_mode=False, enable_validation=False)
                driver.execute(statement_type, _config=config)

                if expected_returns_rows:
                    mock_wrap_select.assert_called_once()
                    mock_wrap_execute.assert_not_called()
                else:
                    mock_wrap_execute.assert_called_once()
                    mock_wrap_select.assert_not_called()


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
    config = SQLConfig(strict_mode=False)  # Use non-strict config
    driver = MockSyncDriver(connection, config)

    # Mock the full execution flow
    with patch.object(connection, "execute", return_value=[{"id": 1, "name": "test"}]) as mock_conn_execute:
        result = driver.execute("SELECT * FROM users WHERE id = :id", {"id": 1})

        # Verify connection was called
        mock_conn_execute.assert_called_once()

        # Verify result structure
        assert hasattr(result, "rows")
        assert hasattr(result, "row_count")


async def test_async_driver_full_execution_flow() -> None:
    """Test complete async driver execution flow."""
    connection = MockAsyncConnection()
    config = SQLConfig(strict_mode=False)  # Use non-strict config

    driver = MockAsyncDriver(connection, config)

    # Mock the full async execution flow
    with patch.object(connection, "execute", return_value=[{"id": 1, "name": "test"}]) as mock_conn_execute:
        result = await driver.execute("SELECT * FROM users WHERE id = :id", {"id": 1})

        # Verify connection was called
        mock_conn_execute.assert_called_once()

        # Verify result structure
        assert hasattr(result, "rows")
        assert hasattr(result, "row_count")


def test_driver_supports_arrow_attribute() -> None:
    """Test driver __supports_arrow__ class attribute."""
    connection = MockConnection()
    driver = MockSyncDriver(connection)

    # Default should be False
    assert driver.supports_native_arrow_export is False

    # Should be accessible as class attribute
    assert MockSyncDriver.supports_native_arrow_export is False
