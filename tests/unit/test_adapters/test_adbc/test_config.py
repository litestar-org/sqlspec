"""Unit tests for ADBC configuration."""

from unittest.mock import Mock, patch

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig, AdbcConnectionConfig
from sqlspec.adapters.adbc.driver import AdbcConnection, AdbcDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow


def test_adbc_connection_config_creation() -> None:
    """Test AdbcConnectionConfig creation with various parameters."""
    config: AdbcConnectionConfig = {
        "uri": "postgresql://user:pass@localhost:5432/db",
        "driver_name": "adbc_driver_postgresql",
        "autocommit": True,
    }

    assert config["uri"] == "postgresql://user:pass@localhost:5432/db"
    assert config["driver_name"] == "adbc_driver_postgresql"
    assert config["autocommit"] is True


def test_adbc_connection_config_comprehensive_features() -> None:
    """Test AdbcConnectionConfig with comprehensive ADBC features."""
    db_kwargs = {"read_only": False, "cache_size": 1000}
    conn_kwargs = {"fetch_size": 100}
    grpc_options = {"grpc.max_message_length": 1024 * 1024}

    config: AdbcConnectionConfig = {
        "uri": "duckdb://mydata.db",
        "driver_name": "duckdb",
        "db_kwargs": db_kwargs,
        "conn_kwargs": conn_kwargs,
        "autocommit": False,
        "isolation_level": "READ_COMMITTED",
        "batch_size": 1000,
        "query_timeout": 30,
        "connection_timeout": 10,
        "ssl_mode": "require",
        "ssl_cert": "/path/to/cert.pem",
        "ssl_key": "/path/to/key.pem",
        "ssl_ca": "/path/to/ca.pem",
        "username": "testuser",
        "password": "testpass",
        "token": "auth-token-123",
        "project_id": "my-project",
        "dataset_id": "my_dataset",
        "account": "my-account",
        "warehouse": "my-warehouse",
        "database": "my_database",
        "schema": "public",
        "role": "analyst",
        "authorization_header": "Bearer token123",
        "grpc_options": grpc_options,
    }

    # Test that all features are properly set
    assert config["uri"] == "duckdb://mydata.db"
    assert config["driver_name"] == "duckdb"
    assert config["db_kwargs"] == db_kwargs
    assert config["conn_kwargs"] == conn_kwargs
    assert config["autocommit"] is False
    assert config["isolation_level"] == "READ_COMMITTED"
    assert config["batch_size"] == 1000
    assert config["query_timeout"] == 30
    assert config["connection_timeout"] == 10
    assert config["ssl_mode"] == "require"
    assert config["username"] == "testuser"
    assert config["password"] == "testpass"
    assert config["token"] == "auth-token-123"
    assert config["project_id"] == "my-project"
    assert config["dataset_id"] == "my_dataset"
    assert config["grpc_options"] == grpc_options


def test_adbc_connection_config_minimal() -> None:
    """Test AdbcConnectionConfig with minimal configuration."""
    config: AdbcConnectionConfig = {"driver_name": "sqlite"}

    assert config["driver_name"] == "sqlite"
    assert len(config) == 1


def test_adbc_config_initialization() -> None:
    """Test AdbcConfig initialization with default parameters."""
    config = AdbcConfig()

    assert isinstance(config.connection_config, dict)
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.default_row_type == DictRow
    assert config.connection_type == AdbcConnection
    assert config.driver_type == AdbcDriver


def test_adbc_config_with_connection_config() -> None:
    """Test AdbcConfig initialization with connection configuration."""
    connection_config: AdbcConnectionConfig = {
        "uri": "postgresql://user:pass@localhost/db",
        "driver_name": "postgresql",
        "autocommit": True,
        "batch_size": 500,
    }

    config = AdbcConfig(connection_config=connection_config)

    assert config.connection_config["uri"] == "postgresql://user:pass@localhost/db"
    assert config.connection_config["driver_name"] == "postgresql"
    assert config.connection_config["autocommit"] is True
    assert config.connection_config["batch_size"] == 500


def test_adbc_config_with_statement_config() -> None:
    """Test AdbcConfig initialization with statement configuration."""
    statement_config = SQLConfig(
        strict_mode=False,
        enable_validation=True,
    )

    config = AdbcConfig(statement_config=statement_config)

    assert config.statement_config.strict_mode is False
    assert config.statement_config.enable_validation is True


def test_adbc_config_with_instrumentation() -> None:
    """Test AdbcConfig initialization with instrumentation configuration."""
    instrumentation = InstrumentationConfig(
        log_queries=True,
        log_runtime=True,
        log_pool_operations=True,
    )

    config = AdbcConfig(instrumentation=instrumentation)

    assert config.instrumentation.log_queries is True
    assert config.instrumentation.log_runtime is True
    assert config.instrumentation.log_pool_operations is True


def test_adbc_config_with_callback() -> None:
    """Test AdbcConfig initialization with connection creation callback."""
    connection_callback = Mock()

    config = AdbcConfig(on_connection_create=connection_callback)

    assert config.on_connection_create == connection_callback


def test_adbc_config_resolve_driver_name_explicit_alias() -> None:
    """Test AdbcConfig._resolve_driver_name with driver aliases."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "adbc_driver_sqlite.dbapi.connect"


def test_adbc_config_resolve_driver_name_postgresql_aliases() -> None:
    """Test AdbcConfig._resolve_driver_name with PostgreSQL aliases."""
    test_cases = [
        ("postgres", "adbc_driver_postgresql.dbapi.connect"),
        ("postgresql", "adbc_driver_postgresql.dbapi.connect"),
        ("pg", "adbc_driver_postgresql.dbapi.connect"),
        ("adbc_driver_postgresql", "adbc_driver_postgresql.dbapi.connect"),
    ]

    for alias, expected in test_cases:
        config = AdbcConfig(connection_config={"driver_name": alias})
        driver_name = config._resolve_driver_name()
        assert driver_name == expected


def test_adbc_config_resolve_driver_name_duckdb_aliases() -> None:
    """Test AdbcConfig._resolve_driver_name with DuckDB aliases."""
    test_cases = [
        ("duckdb", "adbc_driver_duckdb.dbapi.connect"),
        ("adbc_driver_duckdb", "adbc_driver_duckdb.dbapi.connect"),
    ]

    for alias, expected in test_cases:
        config = AdbcConfig(connection_config={"driver_name": alias})
        driver_name = config._resolve_driver_name()
        assert driver_name == expected


def test_adbc_config_resolve_driver_name_bigquery_aliases() -> None:
    """Test AdbcConfig._resolve_driver_name with BigQuery aliases."""
    test_cases = [
        ("bigquery", "adbc_driver_bigquery.dbapi.connect"),
        ("bq", "adbc_driver_bigquery.dbapi.connect"),
        ("adbc_driver_bigquery", "adbc_driver_bigquery.dbapi.connect"),
    ]

    for alias, expected in test_cases:
        config = AdbcConfig(connection_config={"driver_name": alias})
        driver_name = config._resolve_driver_name()
        assert driver_name == expected


def test_adbc_config_resolve_driver_name_snowflake_aliases() -> None:
    """Test AdbcConfig._resolve_driver_name with Snowflake aliases."""
    test_cases = [
        ("snowflake", "adbc_driver_snowflake.dbapi.connect"),
        ("sf", "adbc_driver_snowflake.dbapi.connect"),
        ("adbc_driver_snowflake", "adbc_driver_snowflake.dbapi.connect"),
    ]

    for alias, expected in test_cases:
        config = AdbcConfig(connection_config={"driver_name": alias})
        driver_name = config._resolve_driver_name()
        assert driver_name == expected


def test_adbc_config_resolve_driver_name_flightsql_aliases() -> None:
    """Test AdbcConfig._resolve_driver_name with Flight SQL aliases."""
    test_cases = [
        ("flightsql", "adbc_driver_flightsql.dbapi.connect"),
        ("adbc_driver_flightsql", "adbc_driver_flightsql.dbapi.connect"),
        ("grpc", "adbc_driver_flightsql.dbapi.connect"),
    ]

    for alias, expected in test_cases:
        config = AdbcConfig(connection_config={"driver_name": alias})
        driver_name = config._resolve_driver_name()
        assert driver_name == expected


def test_adbc_config_resolve_driver_name_full_path() -> None:
    """Test AdbcConfig._resolve_driver_name with full driver path."""
    config = AdbcConfig(connection_config={"driver_name": "my.custom.driver.connect"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "my.custom.driver.connect.dbapi.connect"


def test_adbc_config_resolve_driver_name_full_path_with_suffix() -> None:
    """Test AdbcConfig._resolve_driver_name with full path already containing suffix."""
    config = AdbcConfig(connection_config={"driver_name": "my.custom.driver.dbapi.connect"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "my.custom.driver.dbapi.connect"


def test_adbc_config_resolve_driver_name_from_postgresql_uri() -> None:
    """Test AdbcConfig._resolve_driver_name auto-detection from PostgreSQL URI."""
    config = AdbcConfig(connection_config={"uri": "postgresql://user:pass@localhost/db"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "adbc_driver_postgresql.dbapi.connect"


def test_adbc_config_resolve_driver_name_from_sqlite_uri() -> None:
    """Test AdbcConfig._resolve_driver_name auto-detection from SQLite URI."""
    config = AdbcConfig(connection_config={"uri": "sqlite:///path/to/db.sqlite"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "adbc_driver_sqlite.dbapi.connect"


def test_adbc_config_resolve_driver_name_from_duckdb_uri() -> None:
    """Test AdbcConfig._resolve_driver_name auto-detection from DuckDB URI."""
    config = AdbcConfig(connection_config={"uri": "duckdb://mydata.db"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "adbc_driver_duckdb.dbapi.connect"


def test_adbc_config_resolve_driver_name_from_grpc_uri() -> None:
    """Test AdbcConfig._resolve_driver_name auto-detection from gRPC URI."""
    config = AdbcConfig(connection_config={"uri": "grpc://localhost:8080"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "adbc_driver_flightsql.dbapi.connect"


def test_adbc_config_resolve_driver_name_from_snowflake_uri() -> None:
    """Test AdbcConfig._resolve_driver_name auto-detection from Snowflake URI."""
    config = AdbcConfig(connection_config={"uri": "snowflake://account.region.snowflake"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "adbc_driver_snowflake.dbapi.connect"


def test_adbc_config_resolve_driver_name_from_bigquery_uri() -> None:
    """Test AdbcConfig._resolve_driver_name auto-detection from BigQuery URI."""
    config = AdbcConfig(connection_config={"uri": "bigquery://project/dataset"})

    driver_name = config._resolve_driver_name()
    assert driver_name == "adbc_driver_bigquery.dbapi.connect"


def test_adbc_config_resolve_driver_name_no_driver_or_uri() -> None:
    """Test AdbcConfig._resolve_driver_name raises error when no driver or URI specified."""
    config = AdbcConfig(connection_config={})

    with pytest.raises(ImproperConfigurationError, match="Could not determine ADBC driver connect path"):
        config._resolve_driver_name()


def test_adbc_config_resolve_driver_name_unsupported_uri() -> None:
    """Test AdbcConfig._resolve_driver_name raises error for unsupported URI scheme."""
    config = AdbcConfig(connection_config={"uri": "unsupported://example.com"})

    with pytest.raises(ImproperConfigurationError, match="Could not determine ADBC driver connect path"):
        config._resolve_driver_name()


def test_adbc_config_connection_config_dict_sqlite() -> None:
    """Test AdbcConfig.connection_config_dict for SQLite."""
    connection_config: AdbcConnectionConfig = {
        "uri": "sqlite:///path/to/database.sqlite",
        "driver_name": "sqlite",
        "autocommit": True,
        "query_timeout": 30,
    }

    config = AdbcConfig(connection_config=connection_config)
    config_dict = config.connection_config_dict

    # SQLite URI should be processed
    assert config_dict["uri"] == "/path/to/database.sqlite"
    assert config_dict["autocommit"] is True
    assert config_dict["query_timeout"] == 30


def test_adbc_config_connection_config_dict_duckdb() -> None:
    """Test AdbcConfig.connection_config_dict for DuckDB."""
    connection_config: AdbcConnectionConfig = {
        "uri": "duckdb://mydata.db",
        "driver_name": "duckdb",
        "db_kwargs": {"read_only": False},
        "conn_kwargs": {"fetch_size": 100},
    }

    config = AdbcConfig(connection_config=connection_config)
    config_dict = config.connection_config_dict

    # DuckDB URI should be processed as path
    assert config_dict["path"] == "mydata.db"
    assert config_dict["conn_kwargs"] == {"fetch_size": 100}


def test_adbc_config_connection_config_dict_postgresql() -> None:
    """Test AdbcConfig.connection_config_dict for PostgreSQL."""
    connection_config: AdbcConnectionConfig = {
        "uri": "postgresql://user:pass@localhost:5432/db",
        "driver_name": "postgresql",
        "ssl_mode": "require",
        "conn_kwargs": {"autocommit": False},
    }

    config = AdbcConfig(connection_config=connection_config)
    config_dict = config.connection_config_dict

    # PostgreSQL URI should be preserved
    assert config_dict["uri"] == "postgresql://user:pass@localhost:5432/db"
    assert config_dict["ssl_mode"] == "require"
    assert config_dict["conn_kwargs"] == {"autocommit": False}


def test_adbc_config_connection_config_dict_bigquery() -> None:
    """Test AdbcConfig.connection_config_dict for BigQuery."""
    connection_config: AdbcConnectionConfig = {
        "driver_name": "bigquery",
        "project_id": "my-project",
        "dataset_id": "my_dataset",
        "token": "auth-token",
    }

    config = AdbcConfig(connection_config=connection_config)
    config_dict = config.connection_config_dict

    # BigQuery parameters should be in db_kwargs
    assert "db_kwargs" in config_dict
    assert config_dict["db_kwargs"]["project_id"] == "my-project"
    assert config_dict["db_kwargs"]["dataset_id"] == "my_dataset"
    assert config_dict["db_kwargs"]["token"] == "auth-token"


def test_adbc_config_connection_config_dict_with_db_kwargs() -> None:
    """Test AdbcConfig.connection_config_dict with existing db_kwargs."""
    connection_config: AdbcConnectionConfig = {
        "driver_name": "postgresql",
        "db_kwargs": {"existing_param": "value"},
        "username": "testuser",
        "password": "testpass",
    }

    config = AdbcConfig(connection_config=connection_config)
    config_dict = config.connection_config_dict

    # Should merge existing db_kwargs with connection parameters
    assert config_dict["existing_param"] == "value"
    assert config_dict["username"] == "testuser"
    assert config_dict["password"] == "testpass"


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_get_connect_func_success(mock_import_string: Mock) -> None:
    """Test AdbcConfig._get_connect_func loads driver successfully."""
    mock_connect_func = Mock()
    mock_import_string.return_value = mock_connect_func

    config = AdbcConfig(connection_config={"driver_name": "sqlite"})

    connect_func = config._get_connect_func()

    assert connect_func == mock_connect_func
    mock_import_string.assert_called_once_with("adbc_driver_sqlite.dbapi.connect")


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_get_connect_func_with_suffix_fallback(mock_import_string: Mock) -> None:
    """Test AdbcConfig._get_connect_func tries adding suffix on ImportError."""
    mock_connect_func = Mock()

    # First call fails, second succeeds
    mock_import_string.side_effect = [ImportError("Module not found"), mock_connect_func]

    config = AdbcConfig(connection_config={"driver_name": "custom_driver"})

    connect_func = config._get_connect_func()

    assert connect_func == mock_connect_func
    assert mock_import_string.call_count == 2
    mock_import_string.assert_any_call("custom_driver.dbapi.connect")
    mock_import_string.assert_any_call("custom_driver.dbapi.connect.dbapi.connect")


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_get_connect_func_import_error(mock_import_string: Mock) -> None:
    """Test AdbcConfig._get_connect_func raises ImproperConfigurationError on ImportError."""
    mock_import_string.side_effect = ImportError("Module not found")

    config = AdbcConfig(connection_config={"driver_name": "nonexistent.driver.dbapi.connect"})

    with pytest.raises(ImproperConfigurationError, match="Failed to import ADBC connect function"):
        config._get_connect_func()


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_get_connect_func_not_callable(mock_import_string: Mock) -> None:
    """Test AdbcConfig._get_connect_func raises error if imported object is not callable."""
    mock_import_string.return_value = "not_a_function"

    config = AdbcConfig(connection_config={"driver_name": "sqlite"})

    with pytest.raises(ImproperConfigurationError, match="did not resolve to a callable function"):
        config._get_connect_func()


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_create_connection_success(mock_import_string: Mock) -> None:
    """Test AdbcConfig.create_connection creates connection successfully."""
    mock_connect_func = Mock()
    mock_connection = Mock()
    mock_connect_func.return_value = mock_connection
    mock_import_string.return_value = mock_connect_func

    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": "sqlite:///test.db"})

    connection = config.create_connection()

    assert connection == mock_connection
    mock_connect_func.assert_called_once_with(uri="/test.db")


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_create_connection_with_callback(mock_import_string: Mock) -> None:
    """Test AdbcConfig.create_connection executes connection creation callback."""
    mock_connect_func = Mock()
    mock_connection = Mock()
    mock_connect_func.return_value = mock_connection
    mock_import_string.return_value = mock_connect_func
    connection_callback = Mock()

    config = AdbcConfig(
        connection_config={"driver_name": "sqlite"},
        on_connection_create=connection_callback,
    )

    connection = config.create_connection()

    assert connection == mock_connection
    connection_callback.assert_called_once_with(mock_connection)


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_create_connection_callback_exception(mock_import_string: Mock) -> None:
    """Test AdbcConfig.create_connection handles callback exceptions gracefully."""
    mock_connect_func = Mock()
    mock_connection = Mock()
    mock_connect_func.return_value = mock_connection
    mock_import_string.return_value = mock_connect_func
    connection_callback = Mock(side_effect=Exception("Callback error"))

    config = AdbcConfig(
        connection_config={"driver_name": "sqlite"},
        on_connection_create=connection_callback,
    )

    # Should not raise exception even if callback fails
    connection = config.create_connection()
    assert connection == mock_connection


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_create_connection_failure(mock_import_string: Mock) -> None:
    """Test AdbcConfig.create_connection raises ImproperConfigurationError on failure."""
    mock_connect_func = Mock()
    mock_connect_func.side_effect = Exception("Connection failed")
    mock_import_string.return_value = mock_connect_func

    config = AdbcConfig(connection_config={"driver_name": "sqlite"})

    with pytest.raises(ImproperConfigurationError, match="Could not configure ADBC connection"):
        config.create_connection()


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_provide_connection(mock_import_string: Mock) -> None:
    """Test AdbcConfig.provide_connection context manager."""
    mock_connect_func = Mock()
    mock_connection = Mock()
    mock_connect_func.return_value = mock_connection
    mock_import_string.return_value = mock_connect_func

    config = AdbcConfig(connection_config={"driver_name": "sqlite"})

    with config.provide_connection() as connection:
        assert connection == mock_connection

    # Connection should be closed after context exit
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.adbc.config.import_string")
def test_adbc_config_provide_session(mock_import_string: Mock) -> None:
    """Test AdbcConfig.provide_session context manager."""
    mock_connect_func = Mock()
    mock_connection = Mock()
    mock_connect_func.return_value = mock_connection
    mock_import_string.return_value = mock_connect_func

    config = AdbcConfig(connection_config={"driver_name": "sqlite"})

    with patch("sqlspec.adapters.adbc.config.AdbcDriver") as mock_driver_class:
        mock_driver = Mock(spec=AdbcDriver)
        mock_driver_class.return_value = mock_driver

        with config.provide_session() as driver:
            assert driver == mock_driver

    # Connection should be closed after session
    mock_connection.close.assert_called_once()

    # Driver should be created with correct arguments
    mock_driver_class.assert_called_once_with(
        connection=mock_connection,
        config=config.statement_config,
    )


def test_adbc_config_class_variables() -> None:
    """Test AdbcConfig class variables are set correctly."""
    config = AdbcConfig()

    assert config.__is_async__ is False
    assert config.__supports_connection_pooling__ is False


def test_adbc_config_postgresql_connection() -> None:
    """Test AdbcConfig with PostgreSQL-specific configuration."""
    connection_config: AdbcConnectionConfig = {
        "uri": "postgresql://user:pass@localhost:5432/mydb",
        "driver_name": "postgresql",
        "ssl_mode": "require",
        "ssl_cert": "/path/to/cert.pem",
        "ssl_key": "/path/to/key.pem",
        "ssl_ca": "/path/to/ca.pem",
        "autocommit": False,
        "isolation_level": "READ_COMMITTED",
    }

    config = AdbcConfig(connection_config=connection_config)

    assert config.connection_config["uri"] == "postgresql://user:pass@localhost:5432/mydb"
    assert config.connection_config["ssl_mode"] == "require"
    assert config.connection_config["ssl_cert"] == "/path/to/cert.pem"
    assert config.connection_config["autocommit"] is False
    assert config.connection_config["isolation_level"] == "READ_COMMITTED"


def test_adbc_config_bigquery_connection() -> None:
    """Test AdbcConfig with BigQuery-specific configuration."""
    connection_config: AdbcConnectionConfig = {
        "driver_name": "bigquery",
        "project_id": "my-gcp-project",
        "dataset_id": "analytics_dataset",
        "token": "oauth-token-123",
        "query_timeout": 300,
    }

    config = AdbcConfig(connection_config=connection_config)

    assert config.connection_config["project_id"] == "my-gcp-project"
    assert config.connection_config["dataset_id"] == "analytics_dataset"
    assert config.connection_config["token"] == "oauth-token-123"
    assert config.connection_config["query_timeout"] == 300


def test_adbc_config_snowflake_connection() -> None:
    """Test AdbcConfig with Snowflake-specific configuration."""
    connection_config: AdbcConnectionConfig = {
        "driver_name": "snowflake",
        "account": "myaccount.snowflake",
        "warehouse": "COMPUTE_WH",
        "database": "PRODUCTION",
        "schema": "PUBLIC",
        "role": "ANALYST",
        "username": "analyst_user",
        "password": "secure_password",
    }

    config = AdbcConfig(connection_config=connection_config)

    assert config.connection_config["account"] == "myaccount.snowflake"
    assert config.connection_config["warehouse"] == "COMPUTE_WH"
    assert config.connection_config["database"] == "PRODUCTION"
    assert config.connection_config["schema"] == "PUBLIC"
    assert config.connection_config["role"] == "ANALYST"


def test_adbc_config_flightsql_connection() -> None:
    """Test AdbcConfig with Flight SQL-specific configuration."""
    grpc_options = {"grpc.max_message_length": 2 * 1024 * 1024}

    connection_config: AdbcConnectionConfig = {
        "uri": "grpc://flightsql.example.com:443",
        "driver_name": "flightsql",
        "authorization_header": "Bearer jwt-token-xyz",
        "grpc_options": grpc_options,
        "ssl_mode": "enable",
    }

    config = AdbcConfig(connection_config=connection_config)

    assert config.connection_config["uri"] == "grpc://flightsql.example.com:443"
    assert config.connection_config["authorization_header"] == "Bearer jwt-token-xyz"
    assert config.connection_config["grpc_options"] == grpc_options
    assert config.connection_config["ssl_mode"] == "enable"


def test_adbc_config_performance_options() -> None:
    """Test AdbcConfig with performance and optimization settings."""
    connection_config: AdbcConnectionConfig = {
        "driver_name": "duckdb",
        "batch_size": 10000,
        "query_timeout": 120,
        "connection_timeout": 30,
        "db_kwargs": {
            "threads": 4,
            "memory_limit": "2GB",
            "max_memory": "4GB",
        },
    }

    config = AdbcConfig(connection_config=connection_config)

    assert config.connection_config["batch_size"] == 10000
    assert config.connection_config["query_timeout"] == 120
    assert config.connection_config["connection_timeout"] == 30
    assert config.connection_config["db_kwargs"]["threads"] == 4
    assert config.connection_config["db_kwargs"]["memory_limit"] == "2GB"
