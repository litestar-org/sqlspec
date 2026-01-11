"""Unit tests for Asyncmy configuration."""

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import (
    AsyncmyConfig,
    AsyncmyConnectionParams,
    AsyncmyDriver,
    AsyncmyDriverFeatures,
    AsyncmyPoolParams,
)
from sqlspec.core import StatementConfig

pytestmark = pytest.mark.xdist_group("mysql")


def test_asyncmy_typed_dict_structure() -> None:
    """Test Asyncmy TypedDict structure."""

    connection_parameters: AsyncmyConnectionParams = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    assert connection_parameters["host"] == "localhost"
    assert connection_parameters["port"] == 3306

    pool_parameters: AsyncmyPoolParams = {"host": "localhost", "port": 3306, "minsize": 5, "maxsize": 20, "echo": True}
    assert pool_parameters["host"] == "localhost"
    assert pool_parameters["minsize"] == 5


def test_asyncmy_config_basic_creation() -> None:
    """Test Asyncmy config creation with basic parameters."""

    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(connection_config=connection_config)
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["user"] == "test_user"
    assert config.connection_config["password"] == "test_password"
    assert config.connection_config["database"] == "test_db"

    connection_config_full = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "custom": "value",
    }
    config_full = AsyncmyConfig(connection_config=connection_config_full)
    assert config_full.connection_config["host"] == "localhost"
    assert config_full.connection_config["port"] == 3306
    assert config_full.connection_config["user"] == "test_user"
    assert config_full.connection_config["password"] == "test_password"
    assert config_full.connection_config["database"] == "test_db"
    assert config_full.connection_config["custom"] == "value"


def test_asyncmy_config_initialization() -> None:
    """Test Asyncmy config initialization."""

    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(connection_config=connection_config)
    assert isinstance(config.statement_config, StatementConfig)

    custom_statement_config = StatementConfig(dialect="custom")
    config = AsyncmyConfig(connection_config=connection_config, statement_config=custom_statement_config)
    assert config.statement_config.dialect == "custom"


async def test_asyncmy_config_provide_session(mysql_service: MySQLService) -> None:
    """Test Asyncmy config provide_session context manager."""

    connection_config = {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        "database": mysql_service.db,
    }
    config = AsyncmyConfig(connection_config=connection_config)

    async with config.provide_session() as session:
        assert isinstance(session, AsyncmyDriver)

        assert session.statement_config is not None
        assert session.statement_config.parameter_config is not None


def test_asyncmy_config_driver_type() -> None:
    """Test Asyncmy config driver_type property."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(connection_config=connection_config)
    assert config.driver_type is AsyncmyDriver


def test_asyncmy_config_is_async() -> None:
    """Test Asyncmy config is_async attribute."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(connection_config=connection_config)
    assert config.is_async is True
    assert AsyncmyConfig.is_async is True


def test_asyncmy_config_supports_connection_pooling() -> None:
    """Test Asyncmy config supports_connection_pooling attribute."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(connection_config=connection_config)
    assert config.supports_connection_pooling is True
    assert AsyncmyConfig.supports_connection_pooling is True


def test_asyncmy_driver_features_typed_dict_structure() -> None:
    """Test AsyncmyDriverFeatures TypedDict structure."""
    features: AsyncmyDriverFeatures = {"json_serializer": lambda x: str(x), "json_deserializer": lambda x: x}

    assert "json_serializer" in features
    assert "json_deserializer" in features
    assert callable(features["json_serializer"])
    assert callable(features["json_deserializer"])


def test_asyncmy_driver_features_partial_dict() -> None:
    """Test AsyncmyDriverFeatures with partial configuration."""
    features: AsyncmyDriverFeatures = {"json_serializer": lambda x: str(x)}

    assert "json_serializer" in features
    assert "json_deserializer" not in features


def test_asyncmy_driver_features_empty_dict() -> None:
    """Test AsyncmyDriverFeatures with empty configuration."""
    features: AsyncmyDriverFeatures = {}

    assert len(features) == 0


def test_asyncmy_config_with_driver_features() -> None:
    """Test AsyncmyConfig initialization with driver_features."""

    def custom_serializer(data: object) -> str:
        return str(data)

    def custom_deserializer(data: str) -> object:
        return data

    features: AsyncmyDriverFeatures = {"json_serializer": custom_serializer, "json_deserializer": custom_deserializer}

    config = AsyncmyConfig(connection_config={"host": "localhost", "port": 3306}, driver_features=features)

    assert config.driver_features["json_serializer"] is custom_serializer
    assert config.driver_features["json_deserializer"] is custom_deserializer


def test_asyncmy_config_with_empty_driver_features() -> None:
    """Test AsyncmyConfig with empty driver_features still provides defaults."""
    config = AsyncmyConfig(connection_config={"host": "localhost", "port": 3306}, driver_features={})

    assert "json_serializer" in config.driver_features
    assert "json_deserializer" in config.driver_features
    assert callable(config.driver_features["json_serializer"])
    assert callable(config.driver_features["json_deserializer"])


def test_asyncmy_config_without_driver_features() -> None:
    """Test AsyncmyConfig without driver_features provides sensible defaults."""
    config = AsyncmyConfig(connection_config={"host": "localhost", "port": 3306})

    assert "json_serializer" in config.driver_features
    assert "json_deserializer" in config.driver_features
    assert callable(config.driver_features["json_serializer"])
    assert callable(config.driver_features["json_deserializer"])


def test_asyncmy_config_driver_features_as_plain_dict() -> None:
    """Test AsyncmyConfig with driver_features as plain dict."""

    def custom_serializer(data: object) -> str:
        return str(data)

    config = AsyncmyConfig(
        connection_config={"host": "localhost", "port": 3306}, driver_features={"json_serializer": custom_serializer}
    )

    assert config.driver_features["json_serializer"] is custom_serializer
