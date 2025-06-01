"""Unit tests for DuckDB configuration."""

from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBConnectionConfig, DuckDBDriver
from sqlspec.adapters.duckdb.config import DuckDBExtensionConfig, DuckDBSecretConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


def test_duckdb_connection_config_creation() -> None:
    """Test DuckDB connection config creation with valid parameters."""
    # Test basic config creation
    config = DuckDBConnectionConfig(database=":memory:")
    assert config["database"] == ":memory:"

    # Test with all core parameters
    config_full = DuckDBConnectionConfig(
        database="/tmp/test.db",
        read_only=True,
        config={"memory_limit": "1GB"},
        memory_limit="2GB",
        threads=4,
        temp_directory="/tmp/duckdb",
        max_temp_directory_size="500MB",
    )
    assert config_full["database"] == "/tmp/test.db"
    assert config_full.get("read_only") is True
    assert config_full.get("config") == {"memory_limit": "1GB"}
    assert config_full.get("memory_limit") == "2GB"
    assert config_full.get("threads") == 4
    assert config_full.get("temp_directory") == "/tmp/duckdb"
    assert config_full.get("max_temp_directory_size") == "500MB"


def test_duckdb_connection_config_extensions() -> None:
    """Test DuckDB connection config with extension parameters."""
    config = DuckDBConnectionConfig(
        database=":memory:",
        autoload_known_extensions=True,
        autoinstall_known_extensions=True,
        allow_community_extensions=True,
        allow_unsigned_extensions=False,
        extension_directory="/tmp/extensions",
        custom_extension_repository="https://custom.repo",
        autoinstall_extension_repository="https://autoinstall.repo",
    )
    assert config.get("autoload_known_extensions") is True
    assert config.get("autoinstall_known_extensions") is True
    assert config.get("allow_community_extensions") is True
    assert config.get("allow_unsigned_extensions") is False
    assert config.get("extension_directory") == "/tmp/extensions"
    assert config.get("custom_extension_repository") == "https://custom.repo"
    assert config.get("autoinstall_extension_repository") == "https://autoinstall.repo"


def test_duckdb_connection_config_security() -> None:
    """Test DuckDB connection config with security parameters."""
    config = DuckDBConnectionConfig(
        database=":memory:",
        allow_persistent_secrets=True,
        enable_external_access=False,
        secret_directory="/tmp/secrets",
    )
    assert config.get("allow_persistent_secrets") is True
    assert config.get("enable_external_access") is False
    assert config.get("secret_directory") == "/tmp/secrets"


def test_duckdb_connection_config_performance() -> None:
    """Test DuckDB connection config with performance parameters."""
    config = DuckDBConnectionConfig(
        database=":memory:",
        enable_object_cache=True,
        parquet_metadata_cache=True,
        enable_external_file_cache=True,
        checkpoint_threshold="1GB",
        enable_progress_bar=True,
        progress_bar_time=5000,
    )
    assert config.get("enable_object_cache") is True
    assert config.get("parquet_metadata_cache") is True
    assert config.get("enable_external_file_cache") is True
    assert config.get("checkpoint_threshold") == "1GB"
    assert config.get("enable_progress_bar") is True
    assert config.get("progress_bar_time") == 5000


def test_duckdb_extension_config_creation() -> None:
    """Test DuckDB extension config creation."""
    # Basic extension config
    ext_config = DuckDBExtensionConfig(name="spatial")
    assert ext_config["name"] == "spatial"

    # Full extension config
    ext_config_full = DuckDBExtensionConfig(
        name="aws",
        version="1.0.0",
        repository="core",
        force_install=True,
    )
    assert ext_config_full["name"] == "aws"
    assert ext_config_full.get("version") == "1.0.0"
    assert ext_config_full.get("repository") == "core"
    assert ext_config_full.get("force_install") is True


def test_duckdb_secret_config_creation() -> None:
    """Test DuckDB secret config creation."""
    # Basic secret config
    secret_config = DuckDBSecretConfig(
        secret_type="openai",
        name="my_openai_secret",
        value={"api_key": "sk-test123"},
    )
    assert secret_config["secret_type"] == "openai"
    assert secret_config["name"] == "my_openai_secret"
    assert secret_config["value"] == {"api_key": "sk-test123"}

    # Full secret config
    secret_config_full = DuckDBSecretConfig(
        secret_type="aws",
        name="my_aws_secret",
        value={"access_key_id": "AKIA123", "secret_access_key": "secret123"},
        scope="PERSISTENT",
    )
    assert secret_config_full["secret_type"] == "aws"
    assert secret_config_full["name"] == "my_aws_secret"
    assert secret_config_full["value"] == {"access_key_id": "AKIA123", "secret_access_key": "secret123"}
    assert secret_config_full.get("scope") == "PERSISTENT"


def test_duckdb_config_initialization() -> None:
    """Test DuckDB config initialization."""
    # Test with default parameters
    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config["database"] == ":memory:"
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.extensions == []
    assert config.secrets == []
    assert config.on_connection_create is None

    # Test with custom parameters
    custom_connection_config = DuckDBConnectionConfig(
        database="/tmp/custom.db",
        memory_limit="1GB",
        threads=8,
    )
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)
    extensions = [DuckDBExtensionConfig(name="spatial")]
    secrets = [DuckDBSecretConfig(secret_type="openai", name="test", value={"key": "value"})]

    def connection_hook(conn: "DuckDBPyConnection") -> None:
        pass

    config = DuckDBConfig(
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
        extensions=extensions,
        secrets=secrets,
        on_connection_create=connection_hook,
    )
    assert config.connection_config["database"] == "/tmp/custom.db"
    assert config.connection_config.get("memory_limit") == "1GB"
    assert config.connection_config.get("threads") == 8
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True
    assert config.extensions == extensions
    assert config.secrets == secrets
    assert config.on_connection_create is connection_hook


def test_duckdb_config_connection_config_dict() -> None:
    """Test DuckDB config connection_config_dict property."""
    connection_config = DuckDBConnectionConfig(
        database="/tmp/test.db",
        read_only=True,
        memory_limit="1GB",
        threads=4,
        autoload_known_extensions=True,
        config={"custom_setting": "value"},
    )
    config = DuckDBConfig(connection_config=connection_config)

    config_dict = config.connection_config_dict

    # Direct parameters should be preserved
    assert config_dict["database"] == "/tmp/test.db"
    assert config_dict["read_only"] is True

    # DuckDB settings should be merged into config
    expected_config = {
        "custom_setting": "value",
        "memory_limit": "1GB",
        "threads": 4,
        "autoload_known_extensions": True,
    }
    assert config_dict["config"] == expected_config


@patch("duckdb.connect")
def test_duckdb_config_connection_creation(mock_connect: Mock) -> None:
    """Test DuckDB config connection creation (mocked)."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = DuckDBConnectionConfig(
        database="/tmp/test.db",
        memory_limit="1GB",
    )
    config = DuckDBConfig(connection_config=connection_config)

    connection = config.create_connection()

    # Verify connection creation was called with correct parameters
    expected_config = {"memory_limit": "1GB"}
    mock_connect.assert_called_once_with(
        database="/tmp/test.db",
        config=expected_config,
    )
    assert connection is mock_connection


@patch("duckdb.connect")
def test_duckdb_config_connection_creation_with_extensions(mock_connect: Mock) -> None:
    """Test DuckDB config connection creation with extensions (mocked)."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    extensions = [
        DuckDBExtensionConfig(name="spatial", repository="core"),
        DuckDBExtensionConfig(name="aws", version="1.0.0", force_install=True),
    ]

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(
        connection_config=connection_config,
        extensions=extensions,
        instrumentation=InstrumentationConfig(log_pool_operations=True),
    )

    connection = config.create_connection()

    # Verify connection creation
    mock_connect.assert_called_once()
    assert connection is mock_connection

    # Verify install_extension calls
    assert mock_connection.install_extension.call_count == 2
    assert mock_connection.load_extension.call_count == 2


@patch("duckdb.connect")
def test_duckdb_config_connection_creation_with_secrets(mock_connect: Mock) -> None:
    """Test DuckDB config connection creation with secrets (mocked)."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    secrets = [
        DuckDBSecretConfig(
            secret_type="openai",
            name="my_openai_secret",
            value={"api_key": "sk-test123"},
        ),
        DuckDBSecretConfig(
            secret_type="aws",
            name="my_aws_secret",
            value={"access_key_id": "AKIA123", "secret_access_key": "secret123"},
            scope="PERSISTENT",
        ),
    ]

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(
        connection_config=connection_config,
        secrets=secrets,
        instrumentation=InstrumentationConfig(log_pool_operations=True),
    )

    connection = config.create_connection()

    # Verify connection creation
    mock_connect.assert_called_once()
    assert connection is mock_connection

    # Verify secret creation SQL execution
    assert mock_connection.execute.call_count == 2

    # Check the SQL statements contain expected elements
    execute_calls = mock_connection.execute.call_args_list
    first_sql = execute_calls[0][0][0]
    second_sql = execute_calls[1][0][0]

    assert "CREATE SECRET my_openai_secret" in first_sql
    assert "TYPE openai" in first_sql
    assert "api_key" in first_sql

    assert "CREATE SECRET my_aws_secret" in second_sql
    assert "TYPE aws" in second_sql
    assert "SCOPE 'PERSISTENT'" in second_sql


@patch("duckdb.connect")
def test_duckdb_config_connection_creation_with_hook(mock_connect: Mock) -> None:
    """Test DuckDB config connection creation with connection hook (mocked)."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    hook_called = False

    def connection_hook(conn: "DuckDBPyConnection") -> None:
        nonlocal hook_called
        hook_called = True
        # In real usage, conn would be DuckDBPyConnection, but for testing we use Mock
        assert conn is mock_connection

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(
        connection_config=connection_config,
        on_connection_create=connection_hook,
    )

    connection = config.create_connection()

    # Verify connection creation and hook execution
    mock_connect.assert_called_once()
    assert connection is mock_connection
    assert hook_called is True


@patch("duckdb.connect")
def test_duckdb_config_provide_connection(mock_connect: Mock) -> None:
    """Test DuckDB config provide_connection context manager."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)

    # Test context manager behavior
    with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("duckdb.connect")
def test_duckdb_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test DuckDB config provide_connection error handling."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("duckdb.connect")
def test_duckdb_config_provide_session(mock_connect: Mock) -> None:
    """Test DuckDB config provide_session context manager."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, DuckDBDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        # Check instrumentation config attributes instead of object identity
        assert session.instrumentation_config.log_queries == config.instrumentation.log_queries
        assert session.instrumentation_config.log_parameters == config.instrumentation.log_parameters
        assert session.instrumentation_config.log_results_count == config.instrumentation.log_results_count
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_duckdb_config_driver_type() -> None:
    """Test DuckDB config driver_type property."""
    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)
    assert config.driver_type is DuckDBDriver


def test_duckdb_config_connection_type() -> None:
    """Test DuckDB config connection_type property."""
    from duckdb import DuckDBPyConnection

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_type is DuckDBPyConnection


def test_duckdb_config_file_database_path() -> None:
    """Test DuckDB config with file database path."""
    test_path = "/tmp/test_database.db"
    connection_config = DuckDBConnectionConfig(database=test_path)
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config["database"] == test_path


def test_duckdb_config_memory_database() -> None:
    """Test DuckDB config with in-memory database."""
    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config["database"] == ":memory:"


def test_duckdb_config_read_only_mode() -> None:
    """Test DuckDB config with read-only mode."""
    connection_config = DuckDBConnectionConfig(database="/tmp/readonly.db", read_only=True)
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config.get("read_only") is True


def test_duckdb_config_memory_and_thread_settings() -> None:
    """Test DuckDB config with memory and thread settings."""
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        memory_limit="2GB",
        threads=8,
        temp_directory="/tmp/duckdb_temp",
        max_temp_directory_size="1GB",
    )
    config = DuckDBConfig(connection_config=connection_config)

    config_dict = config.connection_config_dict
    expected_config = {
        "memory_limit": "2GB",
        "threads": 8,
        "temp_directory": "/tmp/duckdb_temp",
        "max_temp_directory_size": "1GB",
    }
    assert config_dict["config"] == expected_config


def test_duckdb_config_logging_and_debugging() -> None:
    """Test DuckDB config with logging and debugging settings."""
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        enable_logging=True,
        log_query_path="/tmp/duckdb.log",
        logging_level="DEBUG",
        errors_as_json=True,
    )
    config = DuckDBConfig(connection_config=connection_config)

    config_dict = config.connection_config_dict
    expected_config = {
        "enable_logging": True,
        "log_query_path": "/tmp/duckdb.log",
        "logging_level": "DEBUG",
        "errors_as_json": True,
    }
    assert config_dict["config"] == expected_config


def test_duckdb_config_data_processing_settings() -> None:
    """Test DuckDB config with data processing settings."""
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        preserve_insertion_order=True,
        default_null_order="NULLS_FIRST",
        default_order="DESC",
        ieee_floating_point_ops=True,
        binary_as_string=True,
        arrow_large_buffer_size=True,
    )
    config = DuckDBConfig(connection_config=connection_config)

    config_dict = config.connection_config_dict
    expected_config = {
        "preserve_insertion_order": True,
        "default_null_order": "NULLS_FIRST",
        "default_order": "DESC",
        "ieee_floating_point_ops": True,
        "binary_as_string": True,
        "arrow_large_buffer_size": True,
    }
    assert config_dict["config"] == expected_config


@patch("duckdb.connect")
def test_duckdb_config_extension_error_handling(mock_connect: Mock) -> None:
    """Test DuckDB config extension error handling."""
    mock_connection = Mock()
    mock_connection.install_extension.side_effect = Exception("Extension install failed")
    mock_connect.return_value = mock_connection

    extensions = [DuckDBExtensionConfig(name="failing_extension")]
    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(
        connection_config=connection_config,
        extensions=extensions,
        instrumentation=InstrumentationConfig(log_pool_operations=True),
    )

    # Should not raise exception, just log warning
    connection = config.create_connection()
    assert connection is mock_connection


@patch("duckdb.connect")
def test_duckdb_config_secret_error_handling(mock_connect: Mock) -> None:
    """Test DuckDB config secret error handling."""
    mock_connection = Mock()
    mock_connection.execute.side_effect = Exception("Secret creation failed")
    mock_connect.return_value = mock_connection

    secrets = [DuckDBSecretConfig(secret_type="openai", name="test", value={"key": "value"})]
    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(
        connection_config=connection_config,
        secrets=secrets,
        instrumentation=InstrumentationConfig(log_pool_operations=True),
    )

    # Should not raise exception, just log warning
    connection = config.create_connection()
    assert connection is mock_connection


@patch("duckdb.connect")
def test_duckdb_config_hook_error_handling(mock_connect: Mock) -> None:
    """Test DuckDB config connection hook error handling."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    def failing_hook(conn: "DuckDBPyConnection") -> None:
        raise Exception("Hook failed")

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(
        connection_config=connection_config,
        on_connection_create=failing_hook,
        instrumentation=InstrumentationConfig(log_pool_operations=True),
    )

    # Should not raise exception, just log warning
    connection = config.create_connection()
    assert connection is mock_connection
