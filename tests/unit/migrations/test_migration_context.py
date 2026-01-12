"""Test migration context functionality."""

import asyncio
from unittest.mock import Mock

from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.context import MigrationContext


def test_migration_context_from_sqlite_config() -> None:
    """Test creating migration context from SQLite config."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    context = MigrationContext.from_config(config)

    assert context.dialect == "sqlite"
    assert context.config is config
    assert context.driver is None
    assert context.metadata == {}


def test_migration_context_from_postgres_config() -> None:
    """Test creating migration context from PostgreSQL config."""
    config = PsycopgSyncConfig(
        connection_config={"host": "localhost", "dbname": "test", "user": "test", "password": "test"}
    )
    context = MigrationContext.from_config(config)

    # PostgreSQL config should have postgres dialect
    assert context.dialect in {"postgres", "postgresql"}
    assert context.config is config


def test_migration_context_manual_creation() -> None:
    """Test manually creating migration context."""
    context = MigrationContext(dialect="mysql", metadata={"custom_key": "custom_value"})

    assert context.dialect == "mysql"
    assert context.config is None
    assert context.driver is None
    assert context.metadata == {"custom_key": "custom_value"}


def test_migration_context_initialization() -> None:
    """Test basic migration context initialization."""
    context = MigrationContext(dialect="postgres", config=Mock(), metadata={"test": "value"})

    assert context.dialect == "postgres"
    assert context.metadata is not None
    assert context.metadata["test"] == "value"
    assert getattr(context, "_execution_metadata") == {}


def test_execution_metadata_operations() -> None:
    """Test execution metadata set/get operations."""
    context = MigrationContext()

    context.set_execution_metadata("test_key", "test_value")
    assert context.get_execution_metadata("test_key") == "test_value"
    assert context.get_execution_metadata("nonexistent", "default") == "default"


def test_is_async_execution_detection() -> None:
    """Test async execution context detection."""
    context = MigrationContext()

    assert not context.is_async_execution

    async def test_async() -> None:
        assert context.is_async_execution

    asyncio.run(test_async())


def test_is_async_driver_detection() -> None:
    """Test async driver detection."""
    context = MigrationContext()

    assert not context.is_async_driver

    sync_driver = Mock()
    sync_driver.execute_script = Mock()
    context.driver = sync_driver
    assert not context.is_async_driver

    async_driver = Mock()

    async def mock_execute() -> None:
        return None

    async_driver.execute_script = mock_execute
    context.driver = async_driver
    assert context.is_async_driver


def test_execution_mode_property() -> None:
    """Test execution mode property."""
    context = MigrationContext()

    assert context.execution_mode == "sync"

    async def test_async_mode() -> None:
        assert context.execution_mode == "async"

    asyncio.run(test_async_mode())


def test_validate_async_usage_with_async_function() -> None:
    """Test async function validation."""
    context = MigrationContext()

    async def async_migration() -> list[str]:
        return ["CREATE TABLE test (id INT);"]

    context.validate_async_usage(async_migration)


def test_validate_async_usage_with_sync_function() -> None:
    """Test sync function validation in async context."""
    context = MigrationContext()

    def sync_migration() -> list[str]:
        return ["CREATE TABLE test (id INT);"]

    mock_async_driver = Mock()

    async def mock_execute() -> None:
        return None

    mock_async_driver.execute_script = mock_execute
    context.driver = mock_async_driver

    context.validate_async_usage(sync_migration)
    assert context.get_execution_metadata("mixed_execution") is True


def test_from_config_class_method() -> None:
    """Test creating context from config."""
    mock_config = Mock()
    mock_config.statement_config = Mock()
    mock_config.statement_config.dialect = "postgres"

    context = MigrationContext.from_config(mock_config)

    assert context.config is mock_config
    assert context.dialect == "postgres"


def test_from_config_with_callable_statement_config() -> None:
    """Test creating context from config with callable statement config."""
    mock_config = Mock()
    mock_stmt_config = Mock()
    mock_stmt_config.dialect = "mysql"
    mock_config.statement_config = None
    mock_config._create_statement_config = Mock(return_value=mock_stmt_config)

    context = MigrationContext.from_config(mock_config)

    assert context.config is mock_config
    assert context.dialect == "mysql"


def test_from_config_no_dialect_available() -> None:
    """Test creating context when no dialect is available."""
    mock_config = Mock()
    mock_config.statement_config = None
    del mock_config._create_statement_config

    context = MigrationContext.from_config(mock_config)

    assert context.config is mock_config
    assert context.dialect is None


def test_from_config_exception_handling() -> None:
    """Test exception handling in from_config method."""
    mock_config = Mock()
    mock_config.statement_config = None
    mock_config._create_statement_config = Mock(side_effect=Exception("Test exception"))

    context = MigrationContext.from_config(mock_config)

    assert context.config is mock_config
    assert context.dialect is None


def test_post_init_metadata_initialization() -> None:
    """Test __post_init__ metadata initialization."""
    context = MigrationContext(metadata=None, extension_config=None)

    assert context.metadata == {}
    assert context.extension_config == {}

    existing_metadata = {"key": "value"}
    existing_extension_config = {"ext": "config"}

    context = MigrationContext(metadata=existing_metadata, extension_config=existing_extension_config)

    assert context.metadata is existing_metadata
    assert context.extension_config is existing_extension_config
