"""Unit tests for PyMySQL configuration."""

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlConnectionParams, PyMysqlDriverFeatures, PyMysqlPoolParams
from sqlspec.adapters.pymysql._typing import PyMysqlConnection
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


@pytest.mark.parametrize("read_only", [True, False])
def test_pymysql_replica_preserves_init_command_and_creation_hook(mysql_service: MySQLService, read_only: bool) -> None:
    """Run caller initialization before hooks while enforcing replica session mode."""
    hook_values: list[object] = []

    def on_create(connection: PyMysqlConnection) -> None:
        with connection.cursor() as cursor:
            cursor.execute("SELECT @probe, @@session.transaction_read_only")
            hook_values.append(cursor.fetchone())

    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "init_command": "SET @probe=1",
        },
        driver_features={"on_connection_create": on_create},
        replicas=[{"host": mysql_service.host, "read_only": read_only}],
    )
    replica = config.replica_groups["default"].configs[0]
    try:
        with config.provide_session() as writer:
            assert writer.select_value("SELECT 1") == 1
        for _ in range(2):
            with config.provide_read_session() as reader:
                assert reader.select_value("SELECT @@session.transaction_read_only") == int(read_only)
        connection = replica.create_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT @probe, @@session.transaction_read_only")
                assert cursor.fetchone() == (1, int(read_only))
        finally:
            connection.close()
        assert hook_values == [(1, 0), (1, int(read_only)), (1, int(read_only))]
    finally:
        config.close_pool()
