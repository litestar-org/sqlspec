"""Tests for IBM Db2 database configuration."""

from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.db2.config import (
    Db2ConnectionParams,
    Db2DriverFeatures,
    Db2PoolParams,
    Db2SyncConfig,
    Db2SyncConnectionContext,
)
from sqlspec.adapters.db2.core import parse_db2_dsn
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.adapters.db2.pool import Db2SyncConnectionPool


def test_parse_db2_dsn_cli_format() -> None:
    """Parse standard Db2 CLI DSN semicolon-separated key-value pairs."""
    dsn = "DATABASE=mytestdb;HOSTNAME=127.0.0.1;PORT=50000;PROTOCOL=TCPIP;UID=db2admin;PWD=secretpass;"
    parsed = parse_db2_dsn(dsn)
    assert parsed["database"] == "mytestdb"
    assert parsed["hostname"] == "127.0.0.1"
    assert parsed["port"] == 50000
    assert parsed["protocol"] == "TCPIP"
    assert parsed["username"] == "db2admin"
    assert parsed["password"] == "secretpass"


def test_parse_db2_dsn_url_format() -> None:
    """Parse db2:// connection URI into keyword parameters."""
    url = "db2://db2inst1:mysecret@localhost:50000/proddb?autocommit=false"
    parsed = parse_db2_dsn(url)
    assert parsed["database"] == "proddb"
    assert parsed["hostname"] == "localhost"
    assert parsed["port"] == 50000
    assert parsed["username"] == "db2inst1"
    assert parsed["password"] == "mysecret"
    assert parsed["autocommit"] is False


def test_db2_config_defaults() -> None:
    """Validate default configuration properties and capabilities."""
    config = Db2SyncConfig()
    assert config.connection_config["database"] == "SAMPLE"
    assert config.driver_type is Db2SyncDriver
    assert config.supports_transactional_ddl is True
    assert config.supports_native_row_streaming is True
    assert config.supports_native_arrow_export is False
    assert config.type_coercion_capabilities.datetime_binding == "native"
    assert config.type_coercion_capabilities.uuid_binding == "text"


def test_db2_config_with_dsn_string() -> None:
    """Validate Db2SyncConfig connection_config populated from DSN."""
    dsn = "DATABASE=appdb;HOSTNAME=dbhost;PORT=50001;UID=user1;PWD=pass1;"
    config = Db2SyncConfig(connection_config={"dsn": dsn})
    assert config.connection_config["database"] == "appdb"
    assert config.connection_config["hostname"] == "dbhost"
    assert config.connection_config["port"] == 50001
    assert config.connection_config["username"] == "user1"
    assert config.connection_config["password"] == "pass1"


def test_db2_config_with_url_string() -> None:
    """Validate Db2SyncConfig connection_config populated from URL."""
    url = "db2://user2:pass2@remotehost:50002/customdb"
    config = Db2SyncConfig(connection_config={"url": url})
    assert config.connection_config["database"] == "customdb"
    assert config.connection_config["hostname"] == "remotehost"
    assert config.connection_config["port"] == 50002
    assert config.connection_config["username"] == "user2"
    assert config.connection_config["password"] == "pass2"


def test_db2_config_get_connection_string() -> None:
    """Generate CLI DSN connection string matching normalized configuration."""
    config = Db2SyncConfig(
        connection_config={
            "database": "SAMPLE",
            "hostname": "localhost",
            "port": 50000,
            "username": "db2inst1",
            "password": "password",
            "protocol": "TCPIP",
        }
    )
    conn_str = config.get_connection_string()
    assert "DATABASE=SAMPLE" in conn_str
    assert "HOSTNAME=localhost" in conn_str
    assert "PORT=50000" in conn_str
    assert "UID=db2inst1" in conn_str
    assert "PWD=password" in conn_str
    assert "PROTOCOL=TCPIP" in conn_str


def test_db2_config_provide_pool_and_create_connection() -> None:
    """Verify pool instantiation and connection factory delegation."""
    mock_conn = MagicMock()
    mock_factory = MagicMock(return_value=mock_conn)
    hook_calls: list[Any] = []

    def hook(conn: Any) -> None:
        hook_calls.append(conn)

    config = Db2SyncConfig(
        connection_config={"database": "TESTDB"},
        driver_features={"connection_factory": mock_factory, "on_connection_create": hook},
    )

    pool = config.provide_pool()
    assert isinstance(pool, Db2SyncConnectionPool)
    assert pool._connection_parameters["database"] == "TESTDB"

    conn = config.create_connection()
    assert conn is mock_conn
    assert mock_factory.call_count == 1
    assert hook_calls == [mock_conn]

    config._close_pool()
    assert config.connection_instance is None


def test_db2_config_signature_namespace() -> None:
    """Verify exported symbols in signature namespace for dependency injection."""
    config = Db2SyncConfig()
    namespace = config.get_signature_namespace()
    assert namespace["Db2SyncConfig"] is Db2SyncConfig
    assert namespace["Db2SyncConnectionContext"] is Db2SyncConnectionContext
    assert namespace["Db2ConnectionParams"] is Db2ConnectionParams
    assert namespace["Db2SyncConnectionPool"] is Db2SyncConnectionPool
    assert namespace["Db2SyncDriver"] is Db2SyncDriver
    assert namespace["Db2DriverFeatures"] is Db2DriverFeatures
    assert namespace["Db2PoolParams"] is Db2PoolParams


def test_db2_config_event_runtime_hints() -> None:
    """Verify default runtime hints for event subscription channels."""
    config = Db2SyncConfig()
    hints = config.get_event_runtime_hints()
    assert hints.poll_interval == 0.25
    assert hints.lease_seconds == 5
