"""Unit tests for Psycopg configuration.

This module tests the PsycopgSyncConfig and PsycopgAsyncConfig classes including:
- Basic configuration initialization
- Connection and pool parameter handling
- Context manager behavior (sync and async)
- SSL configuration
- Error handling
- Property accessors
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.psycopg import (
    CONNECTION_FIELDS,
    POOL_FIELDS,
    PsycopgAsyncConfig,
    PsycopgAsyncDriver,
    PsycopgSyncConfig,
    PsycopgSyncDriver,
)
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    pass


# Constants Tests
def test_connection_fields_constant() -> None:
    """Test CONNECTION_FIELDS constant contains all expected fields."""
    expected_fields = frozenset(
        {
            "conninfo",
            "host",
            "port",
            "user",
            "password",
            "dbname",
            "connect_timeout",
            "options",
            "application_name",
            "sslmode",
            "sslcert",
            "sslkey",
            "sslrootcert",
            "autocommit",
        }
    )
    assert CONNECTION_FIELDS == expected_fields


def test_pool_fields_constant() -> None:
    """Test POOL_FIELDS constant contains connection fields plus pool-specific fields."""
    # POOL_FIELDS should be a superset of CONNECTION_FIELDS
    assert CONNECTION_FIELDS.issubset(POOL_FIELDS)

    # Check pool-specific fields
    pool_specific = POOL_FIELDS - CONNECTION_FIELDS
    expected_pool_specific = {
        "min_size",
        "max_size",
        "name",
        "timeout",
        "max_waiting",
        "max_lifetime",
        "max_idle",
        "reconnect_timeout",
        "num_workers",
        "configure",
        "kwargs",
    }
    assert pool_specific == expected_pool_specific


# Sync Config Initialization Tests
@pytest.mark.parametrize(
    "kwargs,expected_attrs",
    [
        (
            {"host": "localhost", "port": 5432, "user": "test_user", "password": "test_password", "dbname": "test_db"},
            {
                "host": "localhost",
                "port": 5432,
                "user": "test_user",
                "password": "test_password",
                "dbname": "test_db",
                "conninfo": None,
                "extras": {},
            },
        ),
        (
            {"conninfo": "postgresql://user:pass@localhost:5432/testdb"},
            {
                "conninfo": "postgresql://user:pass@localhost:5432/testdb",
                "host": None,
                "port": None,
                "user": None,
                "password": None,
                "dbname": None,
                "extras": {},
            },
        ),
    ],
    ids=["individual_params", "conninfo"],
)
def test_sync_config_initialization(kwargs: dict[str, Any], expected_attrs: dict[str, Any]) -> None:
    """Test sync config initialization with various parameters."""
    config = PsycopgSyncConfig(**kwargs)

    for attr, expected_value in expected_attrs.items():
        assert getattr(config, attr) == expected_value

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is DictRow


@pytest.mark.parametrize(
    "init_kwargs,expected_extras",
    [
        (
            {"host": "localhost", "port": 5432, "custom_param": "value", "debug": True},
            {"custom_param": "value", "debug": True},
        ),
        (
            {"conninfo": "postgresql://localhost/test", "unknown_param": "test", "another_param": 42},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"host": "localhost", "port": 5432}, {}),
    ],
    ids=["with_custom_params", "with_conninfo_extras", "no_extras"],
)
def test_sync_config_extras_handling(init_kwargs: dict[str, Any], expected_extras: dict[str, Any]) -> None:
    """Test handling of extra parameters in sync config."""
    config = PsycopgSyncConfig(**init_kwargs)
    assert config.extras == expected_extras


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(strict_mode=True), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_sync_config_statement_config_initialization(
    statement_config: "SQLConfig | None", expected_type: type[SQLConfig]
) -> None:
    """Test sync config statement config initialization."""
    config = PsycopgSyncConfig(host="localhost", statement_config=statement_config)
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Async Config Initialization Tests
@pytest.mark.parametrize(
    "kwargs,expected_attrs",
    [
        (
            {"host": "localhost", "port": 5432, "user": "test_user", "password": "test_password", "dbname": "test_db"},
            {
                "host": "localhost",
                "port": 5432,
                "user": "test_user",
                "password": "test_password",
                "dbname": "test_db",
                "conninfo": None,
                "extras": {},
            },
        ),
        (
            {"conninfo": "postgresql://user:pass@localhost:5432/testdb"},
            {
                "conninfo": "postgresql://user:pass@localhost:5432/testdb",
                "host": None,
                "port": None,
                "user": None,
                "password": None,
                "dbname": None,
                "extras": {},
            },
        ),
    ],
    ids=["individual_params", "conninfo"],
)
def test_async_config_initialization(kwargs: dict[str, Any], expected_attrs: dict[str, Any]) -> None:
    """Test async config initialization with various parameters."""
    config = PsycopgAsyncConfig(**kwargs)

    for attr, expected_value in expected_attrs.items():
        assert getattr(config, attr) == expected_value

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is DictRow


# Connection Configuration Tests
@pytest.mark.parametrize(
    "timeout_type,value", [("connect_timeout", 30.0), ("timeout", 60.0)], ids=["connect_timeout", "pool_timeout"]
)
def test_timeout_configuration(timeout_type: str, value: float) -> None:
    """Test timeout configuration."""
    config = PsycopgSyncConfig(host="localhost", **{timeout_type: value})
    assert getattr(config, timeout_type) == value


def test_application_settings() -> None:
    """Test application-specific settings."""
    config = PsycopgSyncConfig(
        host="localhost", application_name="test_app", options="-c search_path=public", autocommit=True
    )

    assert config.application_name == "test_app"
    assert config.options == "-c search_path=public"
    assert config.autocommit is True


# SSL Configuration Tests
@pytest.mark.parametrize(
    "ssl_param,value",
    [
        ("sslmode", "require"),
        ("sslcert", "/path/to/cert.pem"),
        ("sslkey", "/path/to/key.pem"),
        ("sslrootcert", "/path/to/ca.pem"),
    ],
    ids=["sslmode", "sslcert", "sslkey", "sslrootcert"],
)
def test_ssl_configuration(ssl_param: str, value: str) -> None:
    """Test SSL configuration parameters."""
    config = PsycopgSyncConfig(host="localhost", **{ssl_param: value})
    assert getattr(config, ssl_param) == value


def test_complete_ssl_configuration() -> None:
    """Test complete SSL configuration."""
    config = PsycopgSyncConfig(
        host="localhost",
        sslmode="require",
        sslcert="/path/to/cert.pem",
        sslkey="/path/to/key.pem",
        sslrootcert="/path/to/ca.pem",
    )

    assert config.sslmode == "require"
    assert config.sslcert == "/path/to/cert.pem"
    assert config.sslkey == "/path/to/key.pem"
    assert config.sslrootcert == "/path/to/ca.pem"


# Pool Configuration Tests
@pytest.mark.parametrize(
    "pool_param,value",
    [
        ("min_size", 5),
        ("max_size", 20),
        ("max_waiting", 10),
        ("max_lifetime", 3600.0),
        ("max_idle", 600.0),
        ("reconnect_timeout", 30.0),
        ("num_workers", 4),
    ],
    ids=["min_size", "max_size", "max_waiting", "max_lifetime", "max_idle", "reconnect_timeout", "num_workers"],
)
def test_pool_parameters(pool_param: str, value: Any) -> None:
    """Test pool-specific parameters."""
    config = PsycopgSyncConfig(host="localhost", **{pool_param: value})
    assert getattr(config, pool_param) == value


def test_pool_callbacks() -> None:
    """Test pool setup callbacks."""

    def configure_callback(conn: Any) -> None:
        pass

    kwargs = {"custom_setting": "value"}

    config = PsycopgSyncConfig(host="localhost", name="test_pool", configure=configure_callback, kwargs=kwargs)

    assert config.name == "test_pool"
    assert config.configure is configure_callback
    assert config.kwargs == kwargs


# Sync Connection Creation Tests
@patch("sqlspec.adapters.psycopg.config.connect")
def test_sync_create_connection(mock_connect: MagicMock) -> None:
    """Test sync connection creation."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = PsycopgSyncConfig(
        host="localhost", port=5432, user="test_user", password="test_password", dbname="test_db", connect_timeout=30.0
    )

    connection = config.create_connection()

    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["host"] == "localhost"
    assert call_kwargs["port"] == 5432
    assert call_kwargs["connect_timeout"] == 30.0
    assert connection is mock_connection


@patch("sqlspec.adapters.psycopg.config.connect")
def test_sync_create_connection_with_conninfo(mock_connect: MagicMock) -> None:
    """Test sync connection creation with conninfo."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    conninfo = "postgresql://user:pass@localhost:5432/testdb"
    config = PsycopgSyncConfig(conninfo=conninfo)

    connection = config.create_connection()

    mock_connect.assert_called_once_with(conninfo=conninfo, row_factory=MagicMock)
    assert connection is mock_connection


# Async Connection Creation Tests
@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_async_create_connection(mock_connect: MagicMock) -> None:
    """Test async connection creation."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(
        host="localhost", port=5432, user="test_user", password="test_password", dbname="test_db", connect_timeout=30.0
    )

    connection = await config.create_connection()

    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["host"] == "localhost"
    assert call_kwargs["port"] == 5432
    assert call_kwargs["connect_timeout"] == 30.0
    assert connection is mock_connection


# Sync Context Manager Tests
@patch("sqlspec.adapters.psycopg.config.connect")
def test_sync_provide_connection_success(mock_connect: MagicMock) -> None:
    """Test sync provide_connection context manager normal flow."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = PsycopgSyncConfig(host="localhost")

    with config.provide_connection() as conn:
        assert conn is mock_connection
        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.connect")
def test_sync_provide_connection_error_handling(mock_connect: MagicMock) -> None:
    """Test sync provide_connection context manager error handling."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = PsycopgSyncConfig(host="localhost")

    with pytest.raises(ValueError, match="Test error"):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Connection should still be closed on error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.connect")
def test_sync_provide_session(mock_connect: MagicMock) -> None:
    """Test sync provide_session context manager."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = PsycopgSyncConfig(host="localhost", dbname="test_db")

    with config.provide_session() as session:
        assert isinstance(session, PsycopgSyncDriver)
        assert session.connection is mock_connection

        # Check parameter style injection
        assert session.config.allowed_parameter_styles == ("pyformat_positional", "pyformat_named")
        assert session.config.target_parameter_style == "pyformat_positional"

        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


# Async Context Manager Tests
@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_async_provide_connection_success(mock_connect: MagicMock) -> None:
    """Test async provide_connection context manager normal flow."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(host="localhost")

    async with config.provide_connection() as conn:
        assert conn is mock_connection
        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_async_provide_connection_error_handling(mock_connect: MagicMock) -> None:
    """Test async provide_connection context manager error handling."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(host="localhost")

    with pytest.raises(ValueError, match="Test error"):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Connection should still be closed on error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_async_provide_session(mock_connect: MagicMock) -> None:
    """Test async provide_session context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(host="localhost", dbname="test_db")

    async with config.provide_session() as session:
        assert isinstance(session, PsycopgAsyncDriver)
        assert session.connection is mock_connection

        # Check parameter style injection
        assert session.config.allowed_parameter_styles == ("pyformat_positional", "pyformat_named")
        assert session.config.target_parameter_style == "pyformat_positional"

        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


# Pool Creation Tests
@patch("sqlspec.adapters.psycopg.config.ConnectionPool")
def test_sync_create_pool(mock_pool_class: MagicMock) -> None:
    """Test sync pool creation."""
    mock_pool = MagicMock()
    mock_pool_class.return_value = mock_pool

    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=5,
        max_size=20,
    )

    pool = config._create_pool()

    mock_pool_class.assert_called_once()
    assert pool is mock_pool


@patch("sqlspec.adapters.psycopg.config.AsyncConnectionPool")
@pytest.mark.asyncio
async def test_async_create_pool(mock_pool_class: MagicMock) -> None:
    """Test async pool creation."""
    mock_pool = AsyncMock()
    mock_pool_class.return_value = mock_pool

    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=5,
        max_size=20,
    )

    pool = await config._create_pool()

    mock_pool_class.assert_called_once()
    assert pool is mock_pool


# Property Tests
def test_sync_connection_config_dict() -> None:
    """Test sync connection_config_dict property."""
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        connect_timeout=30.0,
        application_name="test_app",
        min_size=5,  # Pool parameter, should not be in connection dict
        max_size=10,  # Pool parameter, should not be in connection dict
    )

    conn_dict = config.connection_config_dict

    # Should include connection parameters
    assert conn_dict["host"] == "localhost"
    assert conn_dict["port"] == 5432
    assert conn_dict["user"] == "test_user"
    assert conn_dict["password"] == "test_password"
    assert conn_dict["dbname"] == "test_db"
    assert conn_dict["connect_timeout"] == 30.0
    assert conn_dict["application_name"] == "test_app"

    # Should not include pool parameters
    assert "min_size" not in conn_dict
    assert "max_size" not in conn_dict

    # Should include row_factory
    assert "row_factory" in conn_dict


def test_sync_pool_config_dict() -> None:
    """Test sync pool_config_dict property."""
    config = PsycopgSyncConfig(host="localhost", port=5432, min_size=5, max_size=10, timeout=30.0)

    pool_dict = config.pool_config_dict

    # Should include all parameters
    assert pool_dict["host"] == "localhost"
    assert pool_dict["port"] == 5432
    assert pool_dict["min_size"] == 5
    assert pool_dict["max_size"] == 10
    assert pool_dict["timeout"] == 30.0


def test_sync_driver_type() -> None:
    """Test sync driver_type class attribute."""
    config = PsycopgSyncConfig(host="localhost")
    assert config.driver_type is PsycopgSyncDriver


def test_async_driver_type() -> None:
    """Test async driver_type class attribute."""
    config = PsycopgAsyncConfig(host="localhost")
    assert config.driver_type is PsycopgAsyncDriver


def test_sync_connection_type() -> None:
    """Test sync connection_type class attribute."""
    from psycopg import Connection

    config = PsycopgSyncConfig(host="localhost")
    assert config.connection_type is Connection


def test_async_connection_type() -> None:
    """Test async connection_type class attribute."""
    from psycopg import AsyncConnection

    config = PsycopgAsyncConfig(host="localhost")
    assert config.connection_type is AsyncConnection


def test_sync_is_async() -> None:
    """Test sync is_async class attribute."""
    assert PsycopgSyncConfig.is_async is False

    config = PsycopgSyncConfig(host="localhost")
    assert config.is_async is False


def test_async_is_async() -> None:
    """Test async is_async class attribute."""
    assert PsycopgAsyncConfig.is_async is True

    config = PsycopgAsyncConfig(host="localhost")
    assert config.is_async is True


def test_sync_supports_connection_pooling() -> None:
    """Test sync supports_connection_pooling class attribute."""
    assert PsycopgSyncConfig.supports_connection_pooling is True

    config = PsycopgSyncConfig(host="localhost")
    assert config.supports_connection_pooling is True


def test_async_supports_connection_pooling() -> None:
    """Test async supports_connection_pooling class attribute."""
    assert PsycopgAsyncConfig.supports_connection_pooling is True

    config = PsycopgAsyncConfig(host="localhost")
    assert config.supports_connection_pooling is True


# Parameter Style Tests
def test_sync_supported_parameter_styles() -> None:
    """Test sync supported parameter styles class attribute."""
    assert PsycopgSyncConfig.supported_parameter_styles == ("pyformat_positional", "pyformat_named")


def test_sync_preferred_parameter_style() -> None:
    """Test sync preferred parameter style class attribute."""
    assert PsycopgSyncConfig.preferred_parameter_style == "pyformat_positional"


def test_async_supported_parameter_styles() -> None:
    """Test async supported parameter styles class attribute."""
    assert PsycopgAsyncConfig.supported_parameter_styles == ("pyformat_positional", "pyformat_named")


def test_async_preferred_parameter_style() -> None:
    """Test async preferred parameter style class attribute."""
    assert PsycopgAsyncConfig.preferred_parameter_style == "pyformat_positional"


# Slots Tests
def test_sync_slots_defined() -> None:
    """Test that sync config __slots__ is properly defined."""
    assert hasattr(PsycopgSyncConfig, "__slots__")
    expected_slots = {
        "application_name",
        "autocommit",
        "configure",
        "connect_timeout",
        "conninfo",
        "dbname",
        "default_row_type",
        "extras",
        "host",
        "kwargs",
        "max_idle",
        "max_lifetime",
        "max_size",
        "max_waiting",
        "min_size",
        "name",
        "num_workers",
        "options",
        "password",
        "pool_instance",
        "port",
        "reconnect_timeout",
        "sslcert",
        "sslkey",
        "sslmode",
        "sslrootcert",
        "statement_config",
        "timeout",
        "user",
    }
    assert set(PsycopgSyncConfig.__slots__) == expected_slots


def test_async_slots_defined() -> None:
    """Test that async config __slots__ is properly defined."""
    assert hasattr(PsycopgAsyncConfig, "__slots__")
    # Should be the same as sync config
    assert set(PsycopgAsyncConfig.__slots__) == set(PsycopgSyncConfig.__slots__)


# Edge Cases
def test_config_with_both_conninfo_and_individual_params() -> None:
    """Test config with both conninfo and individual parameters."""
    config = PsycopgSyncConfig(
        conninfo="postgresql://user:pass@host:5432/db",
        host="different_host",  # Individual params alongside conninfo
        port=5433,
    )

    # Both should be stored
    assert config.conninfo == "postgresql://user:pass@host:5432/db"
    assert config.host == "different_host"
    assert config.port == 5433
    # Note: The actual precedence is handled in create_connection


def test_config_minimal_conninfo() -> None:
    """Test config with minimal conninfo."""
    config = PsycopgSyncConfig(conninfo="postgresql://localhost/test")
    assert config.conninfo == "postgresql://localhost/test"
    assert config.host is None
    assert config.port is None
    assert config.user is None
    assert config.password is None


def test_config_with_pool_instance() -> None:
    """Test config with existing pool instance."""
    mock_pool = MagicMock()
    config = PsycopgSyncConfig(host="localhost", pool_instance=mock_pool)
    assert config.pool_instance is mock_pool


def test_config_comprehensive_parameters() -> None:
    """Test config with comprehensive parameter set."""
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        connect_timeout=30.0,
        options="-c search_path=public",
        application_name="test_app",
        sslmode="require",
        autocommit=False,
        min_size=2,
        max_size=15,
        timeout=60.0,
        max_waiting=5,
        max_lifetime=7200.0,
        max_idle=300.0,
        reconnect_timeout=10.0,
        num_workers=3,
    )

    # Connection parameters
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.connect_timeout == 30.0
    assert config.application_name == "test_app"
    assert config.sslmode == "require"
    assert config.autocommit is False

    # Pool parameters
    assert config.min_size == 2
    assert config.max_size == 15
    assert config.timeout == 60.0
    assert config.max_waiting == 5
    assert config.num_workers == 3
