"""Unit tests for PyMySQL configuration."""

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlConnectionParams, PyMysqlDriverFeatures, PyMysqlPoolParams
from sqlspec.core import StatementConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.pymysql]


def test_pymysql_typed_dict_structure() -> None:
    """Test PyMySQL TypedDict structure."""
    connection_parameters: PyMysqlConnectionParams = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    assert connection_parameters["host"] == "localhost"
    assert connection_parameters["port"] == 3306

    pool_parameters: PyMysqlPoolParams = {"host": "localhost", "port": 3306, "pool_recycle_seconds": 60}
    assert pool_parameters["host"] == "localhost"
    assert pool_parameters["pool_recycle_seconds"] == 60


def test_pymysql_config_basic_creation() -> None:
    """Test PyMySQL config creation with basic parameters."""
    connection_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = PyMysqlConfig(connection_config=connection_config)
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["user"] == "test_user"
    assert config.connection_config["password"] == "test_password"
    assert config.connection_config["database"] == "test_db"


def test_pymysql_config_initialization() -> None:
    """Test PyMySQL config initialization."""
    connection_config = {"host": "localhost", "port": 3306}
    config = PyMysqlConfig(connection_config=connection_config)
    assert isinstance(config.statement_config, StatementConfig)

    custom_statement_config = StatementConfig(dialect="custom")
    config = PyMysqlConfig(connection_config=connection_config, statement_config=custom_statement_config)
    assert config.statement_config.dialect == "custom"


def test_pymysql_driver_features_typed_dict_structure() -> None:
    """Test PyMySQLDriverFeatures TypedDict structure."""
    features: PyMysqlDriverFeatures = {"json_serializer": lambda x: str(x), "json_deserializer": lambda x: x}

    assert "json_serializer" in features
    assert "json_deserializer" in features
    assert callable(features["json_serializer"])
    assert callable(features["json_deserializer"])


def test_pymysql_config_provide_session(mysql_service: MySQLService) -> None:
    """Test PyMySQL config provide_session context manager."""
    connection_config = {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        "database": mysql_service.db,
    }
    config = PyMysqlConfig(connection_config=connection_config)

    with config.provide_session() as session:
        assert session.statement_config is not None
        assert session.statement_config.parameter_config is not None
