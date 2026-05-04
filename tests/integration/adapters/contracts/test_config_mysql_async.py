"""MySQL async-family configuration contract tests."""

from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConnectionParams, AiomysqlDriverFeatures, AiomysqlPoolParams
from sqlspec.adapters.asyncmy import AsyncmyConnectionParams, AsyncmyDriverFeatures, AsyncmyPoolParams
from sqlspec.core import StatementConfig
from tests.integration.adapters.contracts._mysql_async import (
    MYSQL_ASYNC_ADAPTERS,
    close_mysql_async_config,
    mysql_async_config,
    mysql_async_config_type,
    mysql_async_connection_config,
    mysql_async_database_key,
    mysql_async_driver_type,
)

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture(params=MYSQL_ASYNC_ADAPTERS)
def adapter(request: pytest.FixtureRequest) -> str:
    """Return the concrete MySQL async-family adapter under test."""
    return str(request.param)


def test_mysql_async_typed_dict_structure(adapter: str) -> None:
    """Test adapter-specific TypedDict structures."""
    if adapter == "aiomysql":
        aiomysql_connection_parameters: AiomysqlConnectionParams = {
            "host": "localhost",
            "port": 3306,
            "user": "test_user",
            "password": "test_password",
            "db": "test_db",
        }
        aiomysql_pool_parameters: AiomysqlPoolParams = {
            "host": "localhost",
            "port": 3306,
            "minsize": 5,
            "maxsize": 20,
            "echo": True,
        }
        assert aiomysql_connection_parameters["host"] == "localhost"
        assert aiomysql_connection_parameters["port"] == 3306
        assert aiomysql_pool_parameters["host"] == "localhost"
        assert aiomysql_pool_parameters["minsize"] == 5
        return

    asyncmy_connection_parameters: AsyncmyConnectionParams = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    asyncmy_pool_parameters: AsyncmyPoolParams = {
        "host": "localhost",
        "port": 3306,
        "minsize": 5,
        "maxsize": 20,
        "echo": True,
    }
    assert asyncmy_connection_parameters["host"] == "localhost"
    assert asyncmy_connection_parameters["port"] == 3306
    assert asyncmy_pool_parameters["host"] == "localhost"
    assert asyncmy_pool_parameters["minsize"] == 5


def test_mysql_async_config_basic_creation(adapter: str) -> None:
    """Test config creation with basic parameters."""
    config_type = mysql_async_config_type(adapter)
    database_key = mysql_async_database_key(adapter)

    connection_config = mysql_async_connection_config(adapter)
    config = config_type(connection_config=connection_config)
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["user"] == "test_user"
    assert config.connection_config["password"] == "test_password"
    assert config.connection_config[database_key] == "test_db"

    connection_config_full = mysql_async_connection_config(adapter, custom="value")
    config_full = config_type(connection_config=connection_config_full)
    assert config_full.connection_config["host"] == "localhost"
    assert config_full.connection_config["port"] == 3306
    assert config_full.connection_config["user"] == "test_user"
    assert config_full.connection_config["password"] == "test_password"
    assert config_full.connection_config[database_key] == "test_db"
    assert config_full.connection_config["custom"] == "value"


def test_mysql_async_config_initialization(adapter: str) -> None:
    """Test config statement initialization."""
    config_type = mysql_async_config_type(adapter)
    connection_config = mysql_async_connection_config(adapter)

    config = config_type(connection_config=connection_config)
    assert isinstance(config.statement_config, StatementConfig)

    custom_statement_config = StatementConfig(dialect="custom")
    config = config_type(connection_config=connection_config, statement_config=custom_statement_config)
    assert config.statement_config.dialect == "custom"


async def test_mysql_async_config_provide_session(adapter: str, mysql_service: MySQLService) -> None:
    """Test config provide_session context manager."""
    config = mysql_async_config(adapter, mysql_service)

    try:
        async with config.provide_session() as session:
            assert isinstance(session, mysql_async_driver_type(adapter))
            assert session.statement_config is not None
            assert session.statement_config.parameter_config is not None
    finally:
        await close_mysql_async_config(config)


def test_mysql_async_config_driver_type(adapter: str) -> None:
    """Test config driver_type property."""
    config_type = mysql_async_config_type(adapter)
    config = config_type(connection_config=mysql_async_connection_config(adapter))
    assert config.driver_type is mysql_async_driver_type(adapter)


def test_mysql_async_config_is_async(adapter: str) -> None:
    """Test config is_async attribute."""
    config_type = mysql_async_config_type(adapter)
    config = config_type(connection_config=mysql_async_connection_config(adapter))
    assert config.is_async is True
    assert config_type.is_async is True


def test_mysql_async_config_supports_connection_pooling(adapter: str) -> None:
    """Test config supports_connection_pooling attribute."""
    config_type = mysql_async_config_type(adapter)
    config = config_type(connection_config=mysql_async_connection_config(adapter))
    assert config.supports_connection_pooling is True
    assert config_type.supports_connection_pooling is True


def test_mysql_async_driver_features_typed_dict_structure(adapter: str) -> None:
    """Test driver feature TypedDict structure."""
    if adapter == "aiomysql":
        aiomysql_features: AiomysqlDriverFeatures = {
            "json_serializer": lambda value: str(value),
            "json_deserializer": lambda value: value,
        }
        assert "json_serializer" in aiomysql_features
        assert "json_deserializer" in aiomysql_features
        assert callable(aiomysql_features["json_serializer"])
        assert callable(aiomysql_features["json_deserializer"])
        return

    asyncmy_features: AsyncmyDriverFeatures = {
        "json_serializer": lambda value: str(value),
        "json_deserializer": lambda value: value,
    }
    assert "json_serializer" in asyncmy_features
    assert "json_deserializer" in asyncmy_features
    assert callable(asyncmy_features["json_serializer"])
    assert callable(asyncmy_features["json_deserializer"])


def test_mysql_async_driver_features_partial_dict(adapter: str) -> None:
    """Test driver features with partial configuration."""
    if adapter == "aiomysql":
        aiomysql_features: AiomysqlDriverFeatures = {"json_serializer": lambda value: str(value)}
        assert "json_serializer" in aiomysql_features
        assert "json_deserializer" not in aiomysql_features
        return

    asyncmy_features: AsyncmyDriverFeatures = {"json_serializer": lambda value: str(value)}
    assert "json_serializer" in asyncmy_features
    assert "json_deserializer" not in asyncmy_features


def test_mysql_async_driver_features_empty_dict(adapter: str) -> None:
    """Test driver features with empty configuration."""
    if adapter == "aiomysql":
        aiomysql_features: AiomysqlDriverFeatures = {}
        assert len(aiomysql_features) == 0
        return

    asyncmy_features: AsyncmyDriverFeatures = {}
    assert len(asyncmy_features) == 0


def test_mysql_async_config_with_driver_features(adapter: str) -> None:
    """Test config initialization with driver_features."""

    def custom_serializer(data: object) -> str:
        return str(data)

    def custom_deserializer(data: str) -> object:
        return data

    config = mysql_async_config_type(adapter)(
        connection_config=mysql_async_connection_config(adapter),
        driver_features={"json_serializer": custom_serializer, "json_deserializer": custom_deserializer},
    )

    assert config.driver_features["json_serializer"] is custom_serializer
    assert config.driver_features["json_deserializer"] is custom_deserializer


def test_mysql_async_config_with_empty_driver_features(adapter: str) -> None:
    """Test config with empty driver_features still provides defaults."""
    config = mysql_async_config_type(adapter)(
        connection_config=mysql_async_connection_config(adapter), driver_features={}
    )

    assert "json_serializer" in config.driver_features
    assert "json_deserializer" in config.driver_features
    assert callable(config.driver_features["json_serializer"])
    assert callable(config.driver_features["json_deserializer"])


def test_mysql_async_config_without_driver_features(adapter: str) -> None:
    """Test config without driver_features provides sensible defaults."""
    config = mysql_async_config_type(adapter)(connection_config=mysql_async_connection_config(adapter))

    assert "json_serializer" in config.driver_features
    assert "json_deserializer" in config.driver_features
    assert callable(config.driver_features["json_serializer"])
    assert callable(config.driver_features["json_deserializer"])


def test_mysql_async_config_driver_features_as_plain_dict(adapter: str) -> None:
    """Test config with driver_features as a plain dict."""

    def custom_serializer(data: object) -> str:
        return str(data)

    driver_features: dict[str, Any] = {"json_serializer": custom_serializer}
    config = mysql_async_config_type(adapter)(
        connection_config=mysql_async_connection_config(adapter), driver_features=driver_features
    )

    assert config.driver_features["json_serializer"] is custom_serializer
