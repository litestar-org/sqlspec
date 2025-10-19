"""Tests for enhanced migration context functionality."""

import asyncio
from unittest.mock import Mock

from sqlspec.migrations.context import MigrationContext


class TestMigrationContext:
    """Test the enhanced migration context."""

    def test_migration_context_initialization(self) -> None:
        """Test basic migration context initialization."""
        context = MigrationContext(dialect="postgres", config=Mock(), metadata={"test": "value"})

        assert context.dialect == "postgres"
        assert context.metadata is not None
        assert context.metadata["test"] == "value"
        assert context._execution_metadata == {}  # type: ignore[reportPrivateUsage]

    def test_execution_metadata_operations(self) -> None:
        """Test execution metadata set/get operations."""
        context = MigrationContext()

        context.set_execution_metadata("test_key", "test_value")
        assert context.get_execution_metadata("test_key") == "test_value"
        assert context.get_execution_metadata("nonexistent", "default") == "default"

    def test_is_async_execution_detection(self) -> None:
        """Test async execution context detection."""
        context = MigrationContext()

        # Should be False outside of async context
        assert not context.is_async_execution

        # Test inside async context
        async def test_async() -> None:
            assert context.is_async_execution

        # Run the async test
        asyncio.run(test_async())

    def test_is_async_driver_detection(self) -> None:
        """Test async driver detection."""
        context = MigrationContext()

        # No driver - should be False
        assert not context.is_async_driver

        # Mock sync driver
        sync_driver = Mock()
        sync_driver.execute_script = Mock()  # Regular function
        context.driver = sync_driver
        assert not context.is_async_driver

        # Mock async driver
        async_driver = Mock()

        async def mock_execute() -> None:
            pass

        async_driver.execute_script = mock_execute
        context.driver = async_driver
        assert context.is_async_driver

    def test_execution_mode_property(self) -> None:
        """Test execution mode property."""
        context = MigrationContext()

        # Should be 'sync' outside async context
        assert context.execution_mode == "sync"

        # Test inside async context
        async def test_async_mode() -> None:
            assert context.execution_mode == "async"

        asyncio.run(test_async_mode())

    def test_validate_async_usage_with_async_function(self) -> None:
        """Test async function validation."""
        context = MigrationContext()

        async def async_migration() -> list[str]:
            return ["CREATE TABLE test (id INT);"]

        # Should log warning when async function used in non-async context
        # Since we're outside async context and no driver is set, both should be False
        context.validate_async_usage(async_migration)

    def test_validate_async_usage_with_sync_function(self) -> None:
        """Test sync function validation in async context."""
        context = MigrationContext()

        def sync_migration() -> list[str]:
            return ["CREATE TABLE test (id INT);"]

        # Mock async driver by setting a mock driver
        mock_async_driver = Mock()

        async def mock_execute() -> None:
            pass

        mock_async_driver.execute_script = mock_execute
        context.driver = mock_async_driver

        context.validate_async_usage(sync_migration)
        # Should set mixed execution metadata
        assert context.get_execution_metadata("mixed_execution") is True

    def test_from_config_class_method(self) -> None:
        """Test creating context from config."""
        mock_config = Mock()
        mock_config.statement_config = Mock()
        mock_config.statement_config.dialect = "postgres"

        context = MigrationContext.from_config(mock_config)

        assert context.config is mock_config
        assert context.dialect == "postgres"

    def test_from_config_with_callable_statement_config(self) -> None:
        """Test creating context from config with callable statement config."""
        mock_config = Mock()
        mock_stmt_config = Mock()
        mock_stmt_config.dialect = "mysql"
        mock_config.statement_config = None
        mock_config._create_statement_config = Mock(return_value=mock_stmt_config)

        context = MigrationContext.from_config(mock_config)

        assert context.config is mock_config
        assert context.dialect == "mysql"

    def test_from_config_no_dialect_available(self) -> None:
        """Test creating context when no dialect is available."""
        mock_config = Mock()
        mock_config.statement_config = None
        del mock_config._create_statement_config  # Remove the method

        context = MigrationContext.from_config(mock_config)

        assert context.config is mock_config
        assert context.dialect is None

    def test_from_config_exception_handling(self) -> None:
        """Test exception handling in from_config method."""
        mock_config = Mock()
        mock_config.statement_config = None
        mock_config._create_statement_config = Mock(side_effect=Exception("Test exception"))

        # Should not raise exception, just log debug message
        context = MigrationContext.from_config(mock_config)

        assert context.config is mock_config
        assert context.dialect is None

    def test_post_init_metadata_initialization(self) -> None:
        """Test __post_init__ metadata initialization."""
        # Test with None values
        context = MigrationContext(metadata=None, extension_config=None)

        assert context.metadata == {}
        assert context.extension_config == {}

        # Test with existing values
        existing_metadata = {"key": "value"}
        existing_extension_config = {"ext": "config"}

        context = MigrationContext(metadata=existing_metadata, extension_config=existing_extension_config)

        assert context.metadata is existing_metadata
        assert context.extension_config is existing_extension_config
