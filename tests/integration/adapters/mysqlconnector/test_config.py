"""Unit tests for MysqlConnector configuration."""

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncConnectionParams,
    MysqlConnectorDriverFeatures,
    MysqlConnectorPoolParams,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncConnectionParams,
    MysqlConnectorSyncDriver,
)
from sqlspec.core import StatementConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector]


def test_mysqlconnector_typed_dict_structure() -> None:
    """Test MysqlConnector TypedDict structure."""
    connection_parameters: MysqlConnectorSyncConnectionParams = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    assert connection_parameters["host"] == "localhost"
    assert connection_parameters["port"] == 3306

    pool_parameters: MysqlConnectorPoolParams = {"host": "localhost", "port": 3306, "pool_size": 10}
    assert pool_parameters["host"] == "localhost"
    assert pool_parameters["pool_size"] == 10

    async_parameters: MysqlConnectorAsyncConnectionParams = {"host": "localhost", "port": 3306}
    assert async_parameters["host"] == "localhost"


def test_mysqlconnector_sync_config_basic_creation() -> None:
    """Test MysqlConnector sync config creation with basic parameters."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = MysqlConnectorSyncConfig(connection_config=connection_config)
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["user"] == "test_user"
    assert config.connection_config["password"] == "test_password"
    assert config.connection_config["database"] == "test_db"


def test_mysqlconnector_async_config_basic_creation() -> None:
    """Test MysqlConnector async config creation with basic parameters."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = MysqlConnectorAsyncConfig(connection_config=connection_config)
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["user"] == "test_user"
    assert config.connection_config["password"] == "test_password"
    assert config.connection_config["database"] == "test_db"


def test_mysqlconnector_config_initialization() -> None:
    """Test MysqlConnector config initialization."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = MysqlConnectorSyncConfig(connection_config=connection_config)
    assert isinstance(config.statement_config, StatementConfig)

    custom_statement_config = StatementConfig(dialect="custom")
    config = MysqlConnectorSyncConfig(connection_config=connection_config, statement_config=custom_statement_config)
    assert config.statement_config.dialect == "custom"


async def test_mysqlconnector_async_config_provide_session(mysql_service: MySQLService) -> None:
    """Test MysqlConnector async config provide_session context manager."""
    connection_config = {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        "database": mysql_service.db,
        "use_pure": True,
    }
    config = MysqlConnectorAsyncConfig(connection_config=connection_config)

    async with config.provide_session() as session:
        assert session.statement_config is not None
        assert session.statement_config.parameter_config is not None


def test_mysqlconnector_sync_config_driver_type() -> None:
    """Test MysqlConnector sync config driver_type property."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = MysqlConnectorSyncConfig(connection_config=connection_config)
    assert config.driver_type is MysqlConnectorSyncDriver


def test_mysqlconnector_config_is_async_flags() -> None:
    """Test MysqlConnector config is_async attribute."""
    sync_config = MysqlConnectorSyncConfig(connection_config={"host": "localhost", "port": 3306})
    async_config = MysqlConnectorAsyncConfig(connection_config={"host": "localhost", "port": 3306})

    assert sync_config.is_async is False
    assert async_config.is_async is True


def test_mysqlconnector_driver_features_typed_dict_structure() -> None:
    """Test MysqlConnectorDriverFeatures TypedDict structure."""
    features: MysqlConnectorDriverFeatures = {"json_serializer": lambda x: str(x), "json_deserializer": lambda x: x}

    assert "json_serializer" in features
    assert "json_deserializer" in features
    assert callable(features["json_serializer"])
    assert callable(features["json_deserializer"])


def test_mysqlconnector_driver_features_partial_dict() -> None:
    """Test MysqlConnectorDriverFeatures with partial configuration."""
    features: MysqlConnectorDriverFeatures = {"json_serializer": lambda x: str(x)}

    assert "json_serializer" in features
    assert "json_deserializer" not in features


def test_mysqlconnector_config_with_driver_features() -> None:
    """Test MysqlConnector config initialization with driver_features."""

    def custom_serializer(data: object) -> str:
        return str(data)

    def custom_deserializer(data: str) -> object:
        return data

    features: MysqlConnectorDriverFeatures = {
        "json_serializer": custom_serializer,
        "json_deserializer": custom_deserializer,
    }

    config = MysqlConnectorSyncConfig(connection_config={"host": "localhost", "port": 3306}, driver_features=features)

    assert config.driver_features["json_serializer"] is custom_serializer
    assert config.driver_features["json_deserializer"] is custom_deserializer
