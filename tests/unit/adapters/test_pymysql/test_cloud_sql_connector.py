"""Unit tests for PyMySQL Cloud SQL connector integration."""

import sys
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.pymysql.config import PyMysqlConfig
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError

# pyright: reportPrivateUsage=false


@pytest.fixture(autouse=True)
def disable_cloud_sql_by_default() -> Generator[None, None, None]:
    """Disable Cloud SQL by default for clean test state."""
    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", False, create=True):
        yield


@pytest.fixture
def mock_cloud_sql_module() -> Generator[MagicMock, None, None]:
    """Create and register mock google.cloud.sql module."""
    mock_connector_class = MagicMock()
    mock_module = MagicMock()
    mock_module.connector.Connector = mock_connector_class

    sys.modules["google.cloud.sql"] = mock_module
    sys.modules["google.cloud.sql.connector"] = mock_module.connector

    yield mock_connector_class

    sys.modules.pop("google.cloud.sql", None)
    sys.modules.pop("google.cloud.sql.connector", None)


def test_cloud_sql_defaults_to_false() -> None:
    """Cloud SQL connector should require explicit opt-in."""
    config = PyMysqlConfig(connection_config={})

    assert config.driver_features["enable_cloud_sql"] is False


def test_cloud_sql_explicit_disable_uses_direct_pymysql_path() -> None:
    """Disabling Cloud SQL should leave the pool on the normal PyMySQL path."""
    config = PyMysqlConfig(connection_config={}, driver_features={"enable_cloud_sql": False})
    pool = config._create_pool()

    assert config.driver_features["enable_cloud_sql"] is False
    assert pool._connection_factory is None
    assert pool._connection_parameters["host"] == "localhost"
    assert pool._connection_parameters["port"] == 3306


def test_cloud_sql_missing_package_raises_error() -> None:
    """Enabling Cloud SQL without the connector package should raise."""
    with pytest.raises(MissingDependencyError, match="cloud-sql-python-connector"):
        PyMysqlConfig(
            connection_config={},
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
        )


def test_cloud_sql_missing_instance_raises_error() -> None:
    """Cloud SQL requires an instance connection name."""
    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        with pytest.raises(
            ImproperConfigurationError, match="cloud_sql_instance required when enable_cloud_sql is True"
        ):
            PyMysqlConfig(connection_config={}, driver_features={"enable_cloud_sql": True})


@pytest.mark.parametrize("instance", ["invalid-format", "project:region:instance:extra"])
def test_cloud_sql_invalid_instance_format_raises_error(instance: str) -> None:
    """Cloud SQL instance names must be project:region:instance."""
    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        with pytest.raises(ImproperConfigurationError, match="Invalid Cloud SQL instance format"):
            PyMysqlConfig(
                connection_config={}, driver_features={"enable_cloud_sql": True, "cloud_sql_instance": instance}
            )


def test_cloud_sql_setup_strips_direct_connection_parameters(mock_cloud_sql_module: MagicMock) -> None:
    """Cloud SQL setup should remove direct network/auth params from pool kwargs."""
    mock_connector = MagicMock()
    mock_cloud_sql_module.return_value = mock_connector

    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        config = PyMysqlConfig(
            connection_config={
                "host": "127.0.0.1",
                "port": 3307,
                "unix_socket": "/cloudsql/project:region:instance",
                "bind_address": "127.0.0.1",
                "user": "testuser",
                "password": "testpass",
                "database": "testdb",
                "autocommit": True,
                "charset": "utf8mb4",
            },
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
        )
        pool = config._create_pool()

    mock_cloud_sql_module.assert_called_once()
    assert config.get_cloud_sql_connector() is mock_connector
    assert pool._connection_factory is not None
    assert "host" not in pool._connection_parameters
    assert "port" not in pool._connection_parameters
    assert "unix_socket" not in pool._connection_parameters
    assert "bind_address" not in pool._connection_parameters
    assert "user" not in pool._connection_parameters
    assert "password" not in pool._connection_parameters
    assert "database" not in pool._connection_parameters
    assert pool._connection_parameters["autocommit"] is True
    assert pool._connection_parameters["charset"] == "utf8mb4"


def test_cloud_sql_connection_factory_calls_connector(mock_cloud_sql_module: MagicMock) -> None:
    """The pool factory should connect through google.cloud.sql.connector."""
    cloud_connection = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.return_value = cloud_connection
    mock_cloud_sql_module.return_value = mock_connector

    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        config = PyMysqlConfig(
            connection_config={"user": "testuser", "password": "testpass", "database": "testdb", "autocommit": True},
            driver_features={
                "enable_cloud_sql": True,
                "cloud_sql_instance": "project:region:instance",
                "cloud_sql_enable_iam_auth": True,
                "cloud_sql_ip_type": "PUBLIC",
            },
        )
        pool = config._create_pool()

    with patch("sqlspec.adapters.pymysql.pool.pymysql.connect") as mock_pymysql_connect:
        connection = pool._create_connection()

    assert connection is cloud_connection
    mock_pymysql_connect.assert_not_called()
    mock_connector.connect.assert_called_once_with(
        instance_connection_string="project:region:instance",
        driver="pymysql",
        enable_iam_auth=True,
        ip_type="PUBLIC",
        autocommit=True,
        local_infile=False,
        user="testuser",
        password="testpass",
        db="testdb",
    )


def test_pool_runs_connection_create_callback_after_direct_or_factory_paths() -> None:
    """Connection creation callbacks should run after direct and factory creation."""
    direct_connection = MagicMock()
    cloud_connection = MagicMock()
    seen_connections: list[Any] = []

    def on_connection_create(connection: Any) -> None:
        seen_connections.append(connection)

    direct_pool = PyMysqlConnectionPool({"host": "localhost"}, on_connection_create=on_connection_create)
    with patch("sqlspec.adapters.pymysql.pool.pymysql.connect", return_value=direct_connection):
        assert direct_pool._create_connection() is direct_connection

    factory_pool = PyMysqlConnectionPool(
        {}, connection_factory=lambda: cloud_connection, on_connection_create=on_connection_create
    )
    assert factory_pool._create_connection() is cloud_connection

    assert seen_connections == [direct_connection, cloud_connection]


def test_cloud_sql_connector_cleanup(mock_cloud_sql_module: MagicMock) -> None:
    """Cloud SQL connector should be closed when the config closes."""
    mock_connector = MagicMock()
    mock_cloud_sql_module.return_value = mock_connector

    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        config = PyMysqlConfig(
            connection_config={},
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
        )
        config.connection_instance = config._create_pool()
        config._close_pool()

    mock_connector.close.assert_called_once()
    assert config.get_cloud_sql_connector() is None
