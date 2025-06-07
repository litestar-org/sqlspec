"""Unit tests for the database service layer."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.config import InstrumentationConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.service.service import AsyncDatabaseService, DatabaseService
from sqlspec.statement import SQL
from sqlspec.statement.result import SQLResult


class TestDatabaseService:
    """Test sync database service."""

    @pytest.fixture
    def mock_driver(self):
        """Create a mock sync driver."""
        driver = MagicMock()
        driver.config = MagicMock()
        driver.config.instrumentation = InstrumentationConfig()
        return driver

    @pytest.fixture
    def mock_result(self):
        """Create a mock SQL result."""
        result = MagicMock(spec=SQLResult)
        result.rowcount = 3
        # Set up methods
        result.all = MagicMock()
        result.one = MagicMock()
        result.one_or_none = MagicMock()
        result.scalar = MagicMock()
        result.scalar_or_none = MagicMock()
        return result

    @pytest.fixture
    def service(self, mock_driver):
        """Create a database service with mock driver."""
        return DatabaseService(
            driver=mock_driver,
            instrumentation_config=InstrumentationConfig(
                log_service_operations=True,
                log_queries=True
            ),
            service_name="TestService"
        )

    def test_init(self, mock_driver) -> None:
        """Test service initialization."""
        service = DatabaseService(mock_driver)
        assert service.driver is mock_driver
        assert service.service_name == "DatabaseService"
        assert service.instrumentation_config is not None

    def test_init_with_custom_name(self, mock_driver) -> None:
        """Test service initialization with custom name."""
        service = DatabaseService(mock_driver, service_name="CustomService")
        assert service.service_name == "CustomService"

    @patch("sqlspec.utils.correlation.CorrelationContext.get")
    def test_execute(self, mock_correlation, service, mock_driver, mock_result) -> None:
        """Test execute method."""
        mock_correlation.return_value = "test-correlation-id"
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users")
        params = {"id": 1}

        result = service.execute(statement, params)

        assert result is mock_result
        mock_driver.execute.assert_called_once_with(
            statement, params,
            connection=None,
            config=None,
            schema_type=None
        )

    def test_execute_with_schema_type(self, service, mock_driver, mock_result) -> None:
        """Test execute with schema type."""
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users")

        class UserDTO:
            pass

        result = service.execute(statement, schema_type=UserDTO)

        assert result is mock_result
        mock_driver.execute.assert_called_once_with(
            statement, None,
            connection=None,
            config=None,
            schema_type=UserDTO
        )

    def test_execute_many(self, service, mock_driver, mock_result) -> None:
        """Test execute_many method."""
        mock_driver.execute_many.return_value = mock_result
        statement = SQL("INSERT INTO users (name) VALUES (?)")
        params = [("Alice",), ("Bob",), ("Charlie",)]

        result = service.execute_many(statement, params)

        assert result is mock_result
        mock_driver.execute_many.assert_called_once_with(
            statement, params,
            connection=None,
            config=None
        )

    def test_select(self, service, mock_driver, mock_result) -> None:
        """Test select method."""
        mock_result.all.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users")

        result = service.select(statement)

        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_driver.execute.assert_called_once()
        mock_result.all.assert_called_once()

    def test_select_one(self, service, mock_driver, mock_result) -> None:
        """Test select_one method."""
        mock_result.one.return_value = {"id": 1, "name": "Alice"}
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users WHERE id = ?")

        result = service.select_one(statement, parameters=(1,))

        assert result == {"id": 1, "name": "Alice"}
        mock_driver.execute.assert_called_once()
        mock_result.one.assert_called_once()

    def test_select_one_or_none(self, service, mock_driver, mock_result) -> None:
        """Test select_one_or_none method."""
        mock_result.one_or_none.return_value = {"id": 1, "name": "Alice"}
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users WHERE id = ?")

        result = service.select_one_or_none(statement, parameters=(1,))

        assert result == {"id": 1, "name": "Alice"}
        mock_driver.execute.assert_called_once()
        mock_result.one_or_none.assert_called_once()

    def test_select_one_or_none_returns_none(self, service, mock_driver, mock_result) -> None:
        """Test select_one_or_none when no results."""
        mock_result.one_or_none.return_value = None
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users WHERE id = ?")

        result = service.select_one_or_none(statement, parameters=(999,))

        assert result is None

    def test_select_value(self, service, mock_driver, mock_result) -> None:
        """Test select_value method."""
        mock_result.scalar.return_value = 42
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT COUNT(*) FROM users")

        result = service.select_value(statement)

        assert result == 42
        mock_driver.execute.assert_called_once()
        mock_result.scalar.assert_called_once()

    def test_select_value_or_none(self, service, mock_driver, mock_result) -> None:
        """Test select_value_or_none method."""
        mock_result.scalar_or_none.return_value = 42
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT COUNT(*) FROM users")

        result = service.select_value_or_none(statement)

        assert result == 42
        mock_driver.execute.assert_called_once()
        mock_result.scalar_or_none.assert_called_once()

    def test_insert(self, service, mock_driver, mock_result) -> None:
        """Test insert method."""
        mock_result.rowcount = 1
        mock_driver.execute.return_value = mock_result
        statement = SQL("INSERT INTO users (name) VALUES (?)")

        result = service.insert(statement, parameters=("Alice",))

        assert result is mock_result
        mock_driver.execute.assert_called_once()

    def test_update(self, service, mock_driver, mock_result) -> None:
        """Test update method."""
        mock_result.rowcount = 5
        mock_driver.execute.return_value = mock_result
        statement = SQL("UPDATE users SET status = ? WHERE active = ?")

        result = service.update(statement, parameters=("inactive", True))

        assert result is mock_result
        assert result.rowcount == 5
        mock_driver.execute.assert_called_once()

    def test_delete(self, service, mock_driver, mock_result) -> None:
        """Test delete method."""
        mock_result.rowcount = 3
        mock_driver.execute.return_value = mock_result
        statement = SQL("DELETE FROM users WHERE created_at < ?")

        result = service.delete(statement, parameters=("2023-01-01",))

        assert result is mock_result
        assert result.rowcount == 3
        mock_driver.execute.assert_called_once()

    def test_error_handling(self, service, mock_driver) -> None:
        """Test error handling and logging."""
        mock_driver.execute.side_effect = SQLSpecError("Database error")
        statement = SQL("SELECT * FROM users")

        with pytest.raises(SQLSpecError, match="Database error"):
            service.execute(statement)

    @patch("sqlspec.service.base.get_logger")
    def test_logging_operations(self, mock_logger, mock_driver) -> None:
        """Test that operations are properly logged."""
        # Create a mock logger
        logger_instance = MagicMock()
        mock_logger.return_value = logger_instance

        # Create service which will use the mocked logger
        service = DatabaseService(
            mock_driver,
            instrumentation_config=InstrumentationConfig(log_service_operations=True)
        )

        # Execute operation
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_driver.execute.return_value = mock_result

        service.execute(SQL("SELECT 1"))

        # Verify logging calls
        assert logger_instance.info.call_count >= 2  # Start and complete logs

    def test_track_operation_context(self, service, mock_driver, mock_result) -> None:
        """Test operation tracking context."""
        mock_driver.execute.return_value = mock_result
        mock_result.rowcount = 10

        with patch.object(service, "_log_operation_start") as mock_start, \
             patch.object(service, "_log_operation_complete") as mock_complete:

            service.execute(SQL("SELECT * FROM users"))

            mock_start.assert_called_once()
            mock_complete.assert_called_once()

            # Check that result_count is passed to complete log
            complete_kwargs = mock_complete.call_args[1]
            assert complete_kwargs.get("result_count") == 10


class TestAsyncDatabaseService:
    """Test async database service."""

    @pytest.fixture
    def mock_driver(self):
        """Create a mock async driver."""
        driver = AsyncMock()
        driver.config = MagicMock()
        driver.config.instrumentation = InstrumentationConfig()
        return driver

    @pytest.fixture
    def mock_result(self):
        """Create a mock async SQL result."""
        result = AsyncMock(spec=SQLResult)
        result.rowcount = 3
        # Set up async methods
        result.all = AsyncMock()
        result.one = AsyncMock()
        result.one_or_none = AsyncMock()
        result.scalar = AsyncMock()
        result.scalar_or_none = AsyncMock()
        return result

    @pytest.fixture
    def service(self, mock_driver):
        """Create an async database service with mock driver."""
        return AsyncDatabaseService(
            driver=mock_driver,
            instrumentation_config=InstrumentationConfig(
                log_service_operations=True,
                log_queries=True
            ),
            service_name="TestAsyncService"
        )

    def test_init(self, mock_driver) -> None:
        """Test async service initialization."""
        service = AsyncDatabaseService(mock_driver)
        assert service.driver is mock_driver
        assert service.service_name == "AsyncDatabaseService"
        assert service.instrumentation_config is not None

    @pytest.mark.asyncio
    async def test_execute(self, service, mock_driver, mock_result) -> None:
        """Test async execute method."""
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users")
        params = {"id": 1}

        result = await service.execute(statement, params)

        assert result is mock_result
        mock_driver.execute.assert_called_once_with(
            statement, params,
            connection=None,
            config=None,
            schema_type=None
        )

    @pytest.mark.asyncio
    async def test_execute_many(self, service, mock_driver, mock_result) -> None:
        """Test async execute_many method."""
        mock_driver.execute_many.return_value = mock_result
        statement = SQL("INSERT INTO users (name) VALUES (?)")
        params = [("Alice",), ("Bob",), ("Charlie",)]

        result = await service.execute_many(statement, params)

        assert result is mock_result
        mock_driver.execute_many.assert_called_once_with(
            statement, params,
            connection=None,
            config=None
        )

    @pytest.mark.asyncio
    async def test_select(self, service, mock_driver, mock_result) -> None:
        """Test async select method."""
        mock_result.all.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users")

        result = await service.select(statement)

        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_driver.execute.assert_called_once()
        mock_result.all.assert_called_once()

    @pytest.mark.asyncio
    async def test_select_one(self, service, mock_driver, mock_result) -> None:
        """Test async select_one method."""
        mock_result.one.return_value = {"id": 1, "name": "Alice"}
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users WHERE id = ?")

        result = await service.select_one(statement, parameters=(1,))

        assert result == {"id": 1, "name": "Alice"}
        mock_driver.execute.assert_called_once()
        mock_result.one.assert_called_once()

    @pytest.mark.asyncio
    async def test_select_one_or_none(self, service, mock_driver, mock_result) -> None:
        """Test async select_one_or_none method."""
        mock_result.one_or_none.return_value = {"id": 1, "name": "Alice"}
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT * FROM users WHERE id = ?")

        result = await service.select_one_or_none(statement, parameters=(1,))

        assert result == {"id": 1, "name": "Alice"}
        mock_driver.execute.assert_called_once()
        mock_result.one_or_none.assert_called_once()

    @pytest.mark.asyncio
    async def test_select_value(self, service, mock_driver, mock_result) -> None:
        """Test async select_value method."""
        mock_result.scalar.return_value = 42
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT COUNT(*) FROM users")

        result = await service.select_value(statement)

        assert result == 42
        mock_driver.execute.assert_called_once()
        mock_result.scalar.assert_called_once()

    @pytest.mark.asyncio
    async def test_select_value_or_none(self, service, mock_driver, mock_result) -> None:
        """Test async select_value_or_none method."""
        mock_result.scalar_or_none.return_value = 42
        mock_driver.execute.return_value = mock_result
        statement = SQL("SELECT COUNT(*) FROM users")

        result = await service.select_value_or_none(statement)

        assert result == 42
        mock_driver.execute.assert_called_once()
        mock_result.scalar_or_none.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert(self, service, mock_driver, mock_result) -> None:
        """Test async insert method."""
        mock_result.rowcount = 1
        mock_driver.execute.return_value = mock_result
        statement = SQL("INSERT INTO users (name) VALUES (?)")

        result = await service.insert(statement, parameters=("Alice",))

        assert result is mock_result
        mock_driver.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_update(self, service, mock_driver, mock_result) -> None:
        """Test async update method."""
        mock_result.rowcount = 5
        mock_driver.execute.return_value = mock_result
        statement = SQL("UPDATE users SET status = ? WHERE active = ?")

        result = await service.update(statement, parameters=("inactive", True))

        assert result is mock_result
        assert result.rowcount == 5
        mock_driver.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete(self, service, mock_driver, mock_result) -> None:
        """Test async delete method."""
        mock_result.rowcount = 3
        mock_driver.execute.return_value = mock_result
        statement = SQL("DELETE FROM users WHERE created_at < ?")

        result = await service.delete(statement, parameters=("2023-01-01",))

        assert result is mock_result
        assert result.rowcount == 3
        mock_driver.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self, service, mock_driver) -> None:
        """Test async error handling."""
        mock_driver.execute.side_effect = SQLSpecError("Async database error")
        statement = SQL("SELECT * FROM users")

        with pytest.raises(SQLSpecError, match="Async database error"):
            await service.execute(statement)

    @pytest.mark.asyncio
    @patch("sqlspec.service.base.get_logger")
    async def test_logging_operations(self, mock_logger, mock_driver) -> None:
        """Test that async operations are properly logged."""
        # Create a mock logger
        logger_instance = MagicMock()
        mock_logger.return_value = logger_instance

        # Create service which will use the mocked logger
        service = AsyncDatabaseService(
            mock_driver,
            instrumentation_config=InstrumentationConfig(log_service_operations=True)
        )

        # Execute operation
        mock_result = AsyncMock()
        mock_result.rowcount = 1
        mock_driver.execute.return_value = mock_result

        await service.execute(SQL("SELECT 1"))

        # Verify logging calls
        assert logger_instance.info.call_count >= 2  # Start and complete logs

    @pytest.mark.asyncio
    async def test_correlation_tracking(self, service, mock_driver, mock_result) -> None:
        """Test correlation ID tracking in async operations."""
        mock_driver.execute.return_value = mock_result

        with patch("sqlspec.utils.correlation.CorrelationContext.get") as mock_get:
            mock_get.return_value = "async-correlation-123"

            await service.execute(SQL("SELECT 1"))

            mock_get.assert_called()

    @pytest.mark.asyncio
    async def test_with_schema_type(self, service, mock_driver, mock_result) -> None:
        """Test async operations with schema type."""
        class UserDTO:
            id: int
            name: str

        # For async mock, we need to set up the async method properly
        mock_result.all = AsyncMock(return_value=[UserDTO()])
        mock_driver.execute.return_value = mock_result

        result = await service.select(SQL("SELECT * FROM users"), schema_type=UserDTO)

        assert isinstance(result, list)
        mock_driver.execute.assert_called_with(
            SQL("SELECT * FROM users"),
            None,
            connection=None,
            config=None,
            schema_type=UserDTO
        )
