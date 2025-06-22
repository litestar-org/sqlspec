"""Unit tests for AsyncPG configuration.

This module tests the AsyncpgConfig class including:
- Basic configuration initialization
- Connection and pool parameter handling
- DSN vs individual parameter configuration
- SSL configuration
- Context manager behavior (async)
- Connection pooling support
- Error handling
- Property accessors
"""

import ssl
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.asyncpg import CONNECTION_FIELDS, POOL_FIELDS, AsyncpgConfig, AsyncpgDriver
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# Constants Tests
def test_connection_fields_constant() -> None:
    """Test CONNECTION_FIELDS constant contains all expected fields."""
    expected_fields = {
        "dsn",
        "host",
        "port",
        "user",
        "password",
        "database",
        "ssl",
        "passfile",
        "direct_tls",
        "connect_timeout",
        "command_timeout",
        "statement_cache_size",
        "max_cached_statement_lifetime",
        "max_cacheable_statement_size",
        "server_settings",
    }
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
        "max_queries",
        "max_inactive_connection_lifetime",
        "setup",
        "init",
        "loop",
        "connection_class",
        "record_class",
    }
    assert pool_specific == expected_pool_specific


# Initialization Tests
@pytest.mark.parametrize(
    "kwargs,expected_attrs",
    [
        (
            {
                "host": "localhost",
                "port": 5432,
                "user": "test_user",
                "password": "test_password",
                "database": "test_db",
            },
            {
                "host": "localhost",
                "port": 5432,
                "user": "test_user",
                "password": "test_password",
                "database": "test_db",
                "dsn": None,
                "ssl": None,
                "extras": {},
            },
        ),
        (
            {"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"},
            {
                "dsn": "postgresql://test_user:test_password@localhost:5432/test_db",
                "host": None,
                "port": None,
                "user": None,
                "password": None,
                "database": None,
                "extras": {},
            },
        ),
    ],
    ids=["individual_params", "dsn"],
)
def test_config_initialization(kwargs: dict[str, Any], expected_attrs: dict[str, Any]) -> None:
    """Test config initialization with various parameters."""
    config = AsyncpgConfig(**kwargs)

    for attr, expected_value in expected_attrs.items():
        assert getattr(config, attr) == expected_value

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type == dict[str, Any]


@pytest.mark.parametrize(
    "init_kwargs,expected_extras",
    [
        (
            {"host": "localhost", "port": 5432, "custom_param": "value", "debug": True},
            {"custom_param": "value", "debug": True},
        ),
        (
            {"dsn": "postgresql://localhost/test", "unknown_param": "test", "another_param": 42},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"host": "localhost", "port": 5432}, {}),
    ],
    ids=["with_custom_params", "with_dsn_extras", "no_extras"],
)
def test_extras_handling(init_kwargs: dict[str, Any], expected_extras: dict[str, Any]) -> None:
    """Test handling of extra parameters."""
    config = AsyncpgConfig(**init_kwargs)
    assert config.extras == expected_extras


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(strict_mode=True), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: "SQLConfig | None", expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    config = AsyncpgConfig(host="localhost", statement_config=statement_config)  # type: ignore[arg-type]
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Connection Configuration Tests
@pytest.mark.parametrize(
    "timeout_type,value",
    [("connect_timeout", "30"), ("command_timeout", "60")],
    ids=["connect_timeout", "command_timeout"],
)
def test_timeout_configuration(timeout_type: str, value: str) -> None:
    """Test timeout configuration."""
    config = AsyncpgConfig(host="localhost", **{timeout_type: value})  # type: ignore[arg-type]
    assert getattr(config, timeout_type) == value


def test_statement_cache_configuration() -> None:
    """Test statement cache configuration."""
    config = AsyncpgConfig(
        host="localhost",
        statement_cache_size=200,
        max_cached_statement_lifetime=600,
        max_cacheable_statement_size=16384,
    )

    assert config.statement_cache_size == 200
    assert config.max_cached_statement_lifetime == 600
    assert config.max_cacheable_statement_size == 16384


def test_server_settings() -> None:
    """Test server settings configuration."""
    server_settings = {"application_name": "test_app", "timezone": "UTC", "search_path": "public,test_schema"}

    config = AsyncpgConfig(host="localhost", server_settings=server_settings)
    assert config.server_settings == server_settings


# SSL Configuration Tests
def test_ssl_boolean() -> None:
    """Test SSL configuration with boolean value."""
    config = AsyncpgConfig(host="localhost", ssl=True)
    assert config.ssl is True

    config = AsyncpgConfig(host="localhost", ssl=False)
    assert config.ssl is False


def test_ssl_context() -> None:
    """Test SSL configuration with SSLContext."""
    ssl_context = ssl.create_default_context()
    config = AsyncpgConfig(host="localhost", ssl=ssl_context)
    assert config.ssl is ssl_context


def test_ssl_passfile() -> None:
    """Test SSL configuration with passfile."""
    config = AsyncpgConfig(host="localhost", passfile="/path/to/.pgpass", direct_tls=True)
    assert config.passfile == "/path/to/.pgpass"
    assert config.direct_tls is True


# Pool Configuration Tests
@pytest.mark.parametrize(
    "pool_param,value",
    [("min_size", 5), ("max_size", 20), ("max_queries", 50000), ("max_inactive_connection_lifetime", 300.0)],
    ids=["min_size", "max_size", "max_queries", "max_inactive_lifetime"],
)
def test_pool_parameters(pool_param: str, value: Any) -> None:
    """Test pool-specific parameters."""
    config = AsyncpgConfig(host="localhost", **{pool_param: value})
    assert getattr(config, pool_param) == value


def test_pool_callbacks() -> None:
    """Test pool setup and init callbacks."""

    async def setup(conn: Any) -> None:
        pass

    async def init(conn: Any) -> None:
        pass

    config = AsyncpgConfig(host="localhost", setup=setup, init=init)

    assert config.setup is setup
    assert config.init is init


# Connection Creation Tests
@pytest.mark.asyncio
async def test_create_connection() -> None:
    """Test connection creation."""
    mock_connection = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value = mock_connection

    with patch(
        "sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock, return_value=mock_pool
    ) as mock_create_pool:
        config = AsyncpgConfig(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
            connect_timeout=30.0,
        )

        connection = await config.create_connection()

        mock_create_pool.assert_called_once()
        call_kwargs = mock_create_pool.call_args[1]
        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["port"] == 5432
        assert call_kwargs["user"] == "test_user"
        assert call_kwargs["password"] == "test_password"
        assert call_kwargs["database"] == "test_db"
        assert call_kwargs["connect_timeout"] == 30.0

        mock_pool.acquire.assert_called_once()
        assert connection is mock_connection


@pytest.mark.asyncio
async def test_create_connection_with_dsn() -> None:
    """Test connection creation with DSN."""
    mock_connection = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value = mock_connection

    with patch(
        "sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock, return_value=mock_pool
    ) as mock_create_pool:
        dsn = "postgresql://test_user:test_password@localhost:5432/test_db"
        config = AsyncpgConfig(dsn=dsn)

        connection = await config.create_connection()

        mock_create_pool.assert_called_once()
        call_kwargs = mock_create_pool.call_args[1]
        assert call_kwargs["dsn"] == dsn

        mock_pool.acquire.assert_called_once()
        assert connection is mock_connection


# Pool Creation Tests
@pytest.mark.asyncio
async def test_create_pool() -> None:
    """Test pool creation."""
    mock_pool = AsyncMock()

    with patch(
        "sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock, return_value=mock_pool
    ) as mock_create_pool:
        config = AsyncpgConfig(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
            min_size=5,
            max_size=20,
        )

        pool = await config.create_pool()

        mock_create_pool.assert_called_once()
        call_kwargs = mock_create_pool.call_args[1]
        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["port"] == 5432
        assert call_kwargs["min_size"] == 5
        assert call_kwargs["max_size"] == 20
        assert pool is mock_pool


# Context Manager Tests
@pytest.mark.asyncio
async def test_provide_connection_no_pool() -> None:
    """Test provide_connection without pool (creates pool and acquires connection)."""
    mock_connection = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value = mock_connection
    mock_pool.release = AsyncMock()

    with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock, return_value=mock_pool):
        config = AsyncpgConfig(host="localhost")

        async with config.provide_connection() as conn:
            assert conn is mock_connection
            mock_pool.acquire.assert_called_once()
            mock_pool.release.assert_not_called()

        mock_pool.release.assert_called_once_with(mock_connection)


@pytest.mark.asyncio
async def test_provide_connection_with_pool() -> None:
    """Test provide_connection with existing pool."""
    mock_pool = AsyncMock()
    mock_connection = AsyncMock()
    mock_pool.acquire.return_value = mock_connection
    mock_pool.release = AsyncMock()

    # Create config without host to avoid actual connection attempts
    config = AsyncpgConfig()
    # Set the pool instance directly
    config.pool_instance = mock_pool

    async with config.provide_connection() as conn:
        assert conn is mock_connection
        mock_pool.acquire.assert_called_once()

    mock_pool.release.assert_called_once_with(mock_connection)


@pytest.mark.asyncio
async def test_provide_connection_error_handling() -> None:
    """Test provide_connection error handling."""
    mock_connection = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value = mock_connection
    mock_pool.release = AsyncMock()

    with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock, return_value=mock_pool):
        config = AsyncpgConfig(host="localhost")

        with pytest.raises(ValueError, match="Test error"):
            async with config.provide_connection() as conn:
                assert conn is mock_connection
                raise ValueError("Test error")

        # Connection should still be released
        mock_pool.release.assert_called_once_with(mock_connection)


@pytest.mark.asyncio
async def test_provide_session() -> None:
    """Test provide_session context manager."""
    mock_connection = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value = mock_connection
    mock_pool.release = AsyncMock()

    with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock, return_value=mock_pool):
        config = AsyncpgConfig(host="localhost", database="test_db")

        async with config.provide_session() as session:
            assert isinstance(session, AsyncpgDriver)
            assert session.connection is mock_connection

            # Check parameter style injection
            assert session.config.allowed_parameter_styles == ("numeric",)
            assert session.config.target_parameter_style == "numeric"

            mock_pool.release.assert_not_called()

        mock_pool.release.assert_called_once_with(mock_connection)


# Property Tests
def test_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    config = AsyncpgConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
        command_timeout=60.0,
        min_size=5,  # Pool parameter, should not be in connection dict
        max_size=10,  # Pool parameter, should not be in connection dict
    )

    conn_dict = config.connection_config_dict

    # Should include connection parameters
    assert conn_dict["host"] == "localhost"
    assert conn_dict["port"] == 5432
    assert conn_dict["user"] == "test_user"
    assert conn_dict["password"] == "test_password"
    assert conn_dict["database"] == "test_db"
    assert conn_dict["connect_timeout"] == 30.0
    assert conn_dict["command_timeout"] == 60.0

    # Should not include pool parameters
    assert "min_size" not in conn_dict
    assert "max_size" not in conn_dict


def test_pool_config_dict() -> None:
    """Test pool_config_dict property."""
    config = AsyncpgConfig(host="localhost", port=5432, min_size=5, max_size=10, max_queries=50000)

    pool_dict = config.pool_config_dict

    # Should include all parameters
    assert pool_dict["host"] == "localhost"
    assert pool_dict["port"] == 5432
    assert pool_dict["min_size"] == 5
    assert pool_dict["max_size"] == 10
    assert pool_dict["max_queries"] == 50000


def test_driver_type() -> None:
    """Test driver_type class attribute."""
    config = AsyncpgConfig(host="localhost")
    assert config.driver_type is AsyncpgDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    config = AsyncpgConfig(host="localhost")
    # The connection_type is set to type(AsyncpgConnection) which is a Union type
    # In runtime, this becomes type(Union[...]) which is not a specific class
    assert config.connection_type is not None
    assert hasattr(config, "connection_type")


def test_is_async() -> None:
    """Test is_async class attribute."""
    assert AsyncpgConfig.is_async is True

    config = AsyncpgConfig(host="localhost")
    assert config.is_async is True


def test_supports_connection_pooling() -> None:
    """Test supports_connection_pooling class attribute."""
    assert AsyncpgConfig.supports_connection_pooling is True

    config = AsyncpgConfig(host="localhost")
    assert config.supports_connection_pooling is True


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert AsyncpgConfig.supported_parameter_styles == ("numeric",)


def test_preferred_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert AsyncpgConfig.preferred_parameter_style == "numeric"


# JSON Serialization Tests
def test_json_serializer_configuration() -> None:
    """Test custom JSON serializer configuration."""

    def custom_serializer(obj: Any) -> str:
        return f"custom:{obj}"

    def custom_deserializer(data: str) -> Any:
        return data.replace("custom:", "")

    config = AsyncpgConfig(host="localhost", json_serializer=custom_serializer, json_deserializer=custom_deserializer)

    assert config.json_serializer is custom_serializer
    assert config.json_deserializer is custom_deserializer


# Slots Test
def test_slots_defined() -> None:
    """Test that __slots__ is properly defined."""
    assert hasattr(AsyncpgConfig, "__slots__")
    expected_slots = {
        "_dialect",
        "command_timeout",
        "connect_timeout",
        "connection_class",
        "database",
        "default_row_type",
        "direct_tls",
        "dsn",
        "extras",
        "host",
        "init",
        "json_deserializer",
        "json_serializer",
        "loop",
        "max_cacheable_statement_size",
        "max_cached_statement_lifetime",
        "max_inactive_connection_lifetime",
        "max_queries",
        "max_size",
        "min_size",
        "passfile",
        "password",
        "pool_instance",
        "port",
        "record_class",
        "server_settings",
        "setup",
        "ssl",
        "statement_cache_size",
        "statement_config",
        "user",
    }
    assert set(AsyncpgConfig.__slots__) == expected_slots


# Edge Cases
def test_config_with_both_dsn_and_individual_params() -> None:
    """Test config with both DSN and individual parameters."""
    config = AsyncpgConfig(
        dsn="postgresql://user:pass@host:5432/db",
        host="different_host",  # Individual params alongside DSN
        port=5433,
    )

    # Both should be stored
    assert config.dsn == "postgresql://user:pass@host:5432/db"
    assert config.host == "different_host"
    assert config.port == 5433
    # Note: The actual precedence is handled in create_connection


def test_config_minimal_dsn() -> None:
    """Test config with minimal DSN."""
    config = AsyncpgConfig(dsn="postgresql://localhost/test")
    assert config.dsn == "postgresql://localhost/test"
    assert config.host is None
    assert config.port is None
    assert config.user is None
    assert config.password is None


def test_config_with_pool_instance() -> None:
    """Test config with existing pool instance."""
    mock_pool = MagicMock()
    config = AsyncpgConfig(host="localhost", pool_instance=mock_pool)
    assert config.pool_instance is mock_pool
