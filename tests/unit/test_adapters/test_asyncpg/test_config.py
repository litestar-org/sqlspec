"""Unit tests for AsyncPG configuration.

This module tests the AsyncpgConfig class including:
- TypedDict configuration initialization
- Pool configuration handling
- Context manager behavior (async)
- Connection pooling support
- Property accessors
"""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# TypedDict Tests
def test_typeddict_inheritance() -> None:
    """Test that AsyncpgPoolConfig inherits from AsyncpgConnectionConfig."""
    from sqlspec.adapters.asyncpg import AsyncpgConnectionConfig, AsyncpgPoolConfig

    # Check that pool config has all connection fields
    connection_annotations = AsyncpgConnectionConfig.__annotations__
    pool_annotations = AsyncpgPoolConfig.__annotations__

    # All connection fields should be in pool config
    for field in connection_annotations:
        assert field in pool_annotations or field == "extra"  # extra might be overridden


# Initialization Tests
@pytest.mark.parametrize(
    "pool_config,expected_config",
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
            },
        ),
        (
            {"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"},
            {"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"},
        ),
        (None, {}),
    ],
    ids=["individual_params", "dsn", "empty"],
)
def test_config_initialization(pool_config: dict[str, Any] | None, expected_config: dict[str, Any]) -> None:
    """Test config initialization with various parameters."""
    config = AsyncpgConfig(pool_config=pool_config)

    # Check that pool_config contains expected values
    assert config.pool_config == expected_config

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is dict


@pytest.mark.parametrize(
    "pool_config,expected_extra",
    [
        (
            {"host": "localhost", "port": 5432, "extra": {"custom_param": "value", "debug": True}},
            {"custom_param": "value", "debug": True},
        ),
        (
            {"dsn": "postgresql://localhost/test", "extra": {"unknown_param": "test", "another_param": 42}},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"host": "localhost", "port": 5432}, {}),
    ],
    ids=["with_custom_params", "with_dsn_extras", "no_extras"],
)
def test_extras_handling(pool_config: dict[str, Any], expected_extra: dict[str, Any]) -> None:
    """Test handling of extra parameters."""
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.pool_config.get("extra", {}) == expected_extra


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(parse_errors_as_warnings=False), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: SQLConfig | None, expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    config = AsyncpgConfig(pool_config={"host": "localhost"}, statement_config=statement_config)
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Pool Configuration Tests
@pytest.mark.parametrize(
    "pool_param,value",
    [("min_size", 5), ("max_size", 20), ("max_queries", 50000), ("max_inactive_connection_lifetime", 300.0)],
    ids=["min_size", "max_size", "max_queries", "max_inactive_lifetime"],
)
def test_pool_parameters(pool_param: str, value: Any) -> None:
    """Test pool-specific parameters."""
    config = AsyncpgConfig(pool_config={"host": "localhost", pool_param: value})  # type: ignore[misc]
    assert config.pool_config[pool_param] == value  # type: ignore[misc]


def test_pool_callbacks() -> None:
    """Test pool setup and init callbacks."""

    async def setup(conn: Any) -> None:
        pass

    async def init(conn: Any) -> None:
        pass

    config = AsyncpgConfig(pool_config={"host": "localhost", "setup": setup, "init": init})

    assert config.pool_config["setup"] is setup
    assert config.pool_config["init"] is init


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
            pool_config={
                "host": "localhost",
                "port": 5432,
                "user": "test_user",
                "password": "test_password",
                "database": "test_db",
                "connect_timeout": 30.0,
            }
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
        config = AsyncpgConfig(pool_config={"dsn": dsn})

        connection = await config.create_connection()

        mock_create_pool.assert_called_once()
        call_kwargs = mock_create_pool.call_args[1]
        assert call_kwargs["dsn"] == dsn

        mock_pool.acquire.assert_called_once()
        assert connection is mock_connection


@pytest.mark.asyncio
async def test_create_connection_with_extra() -> None:
    """Test connection creation with extra parameters."""
    mock_connection = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value = mock_connection

    with patch(
        "sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock, return_value=mock_pool
    ) as mock_create_pool:
        config = AsyncpgConfig(
            pool_config={"host": "localhost", "port": 5432, "extra": {"custom_param": "value", "debug": True}}
        )

        connection = await config.create_connection()

        mock_create_pool.assert_called_once()
        call_kwargs = mock_create_pool.call_args[1]
        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["port"] == 5432
        assert call_kwargs["custom_param"] == "value"
        assert call_kwargs["debug"] is True

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
            pool_config={
                "host": "localhost",
                "port": 5432,
                "user": "test_user",
                "password": "test_password",
                "database": "test_db",
                "min_size": 5,
                "max_size": 20,
            }
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
        config = AsyncpgConfig(pool_config={"host": "localhost"})

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

    # Create config and set pool instance directly
    config = AsyncpgConfig(pool_config={})
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
        config = AsyncpgConfig(pool_config={"host": "localhost"})

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
        config = AsyncpgConfig(pool_config={"host": "localhost", "database": "test_db"})

        async with config.provide_session() as session:
            assert isinstance(session, AsyncpgDriver)
            assert session.connection is mock_connection

            # Check parameter style injection
            assert session.config.allowed_parameter_styles == ("numeric",)
            assert session.config.default_parameter_style == "numeric"

            mock_pool.release.assert_not_called()

        mock_pool.release.assert_called_once_with(mock_connection)


# SSL Configuration Tests
def test_ssl_boolean() -> None:
    """Test SSL configuration with boolean value."""
    config = AsyncpgConfig(pool_config={"host": "localhost", "ssl": True})
    assert config.pool_config["ssl"] is True

    config = AsyncpgConfig(pool_config={"host": "localhost", "ssl": False})
    assert config.pool_config["ssl"] is False


def test_ssl_context() -> None:
    """Test SSL configuration with SSLContext."""
    ssl_context = ssl.create_default_context()
    config = AsyncpgConfig(pool_config={"host": "localhost", "ssl": ssl_context})
    assert config.pool_config["ssl"] is ssl_context


def test_ssl_passfile() -> None:
    """Test SSL configuration with passfile."""
    config = AsyncpgConfig(pool_config={"host": "localhost", "passfile": "/path/to/.pgpass", "direct_tls": True})
    assert config.pool_config["passfile"] == "/path/to/.pgpass"
    assert config.pool_config["direct_tls"] is True


def test_driver_type() -> None:
    """Test driver_type class attribute."""
    config = AsyncpgConfig(pool_config={"host": "localhost"})
    assert config.driver_type is AsyncpgDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    config = AsyncpgConfig(pool_config={"host": "localhost"})
    # The connection_type is set to type(AsyncpgConnection) which is a Union type
    # In runtime, this becomes type(Union[...]) which is not a specific class
    assert config.connection_type is not None
    assert hasattr(config, "connection_type")


def test_is_async() -> None:
    """Test is_async class attribute."""
    assert AsyncpgConfig.is_async is True

    config = AsyncpgConfig(pool_config={"host": "localhost"})
    assert config.is_async is True


def test_supports_connection_pooling() -> None:
    """Test supports_connection_pooling class attribute."""
    assert AsyncpgConfig.supports_connection_pooling is True

    config = AsyncpgConfig(pool_config={"host": "localhost"})
    assert config.supports_connection_pooling is True


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert AsyncpgConfig.supported_parameter_styles == ("numeric",)


def test_default_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert AsyncpgConfig.default_parameter_style == "numeric"


# JSON Serialization Tests
def test_json_serializer_configuration() -> None:
    """Test custom JSON serializer configuration."""

    def custom_serializer(obj: Any) -> str:
        return f"custom:{obj}"

    def custom_deserializer(data: str) -> Any:
        return data.replace("custom:", "")

    config = AsyncpgConfig(
        pool_config={"host": "localhost"}, json_serializer=custom_serializer, json_deserializer=custom_deserializer
    )

    assert config.json_serializer is custom_serializer
    assert config.json_deserializer is custom_deserializer


def test_config_with_pool_instance() -> None:
    """Test config with existing pool instance."""
    mock_pool = MagicMock()
    config = AsyncpgConfig(pool_config={"host": "localhost"}, pool_instance=mock_pool)
    assert config.pool_instance is mock_pool


# Statement cache configuration
def test_statement_cache_configuration() -> None:
    """Test statement cache configuration."""
    config = AsyncpgConfig(
        pool_config={
            "host": "localhost",
            "statement_cache_size": 200,
            "max_cached_statement_lifetime": 600,
            "max_cacheable_statement_size": 16384,
        }
    )

    assert config.pool_config["statement_cache_size"] == 200
    assert config.pool_config["max_cached_statement_lifetime"] == 600
    assert config.pool_config["max_cacheable_statement_size"] == 16384


def test_server_settings() -> None:
    """Test server settings configuration."""
    server_settings = {"application_name": "test_app", "timezone": "UTC", "search_path": "public,test_schema"}

    config = AsyncpgConfig(pool_config={"host": "localhost", "server_settings": server_settings})
    assert config.pool_config["server_settings"] == server_settings


# Timeout configuration
@pytest.mark.parametrize(
    "timeout_type,value", [("connect_timeout", 30), ("command_timeout", 60)], ids=["connect_timeout", "command_timeout"]
)
def test_timeout_configuration(timeout_type: str, value: int) -> None:
    """Test timeout configuration."""
    config = AsyncpgConfig(pool_config={"host": "localhost", timeout_type: value})  # type: ignore[misc]
    assert config.pool_config[timeout_type] == value  # type: ignore[misc]
