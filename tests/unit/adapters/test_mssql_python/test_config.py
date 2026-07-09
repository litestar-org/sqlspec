"""Unit tests for mssql_python adapter configuration."""

import warnings
from typing import Any, cast, get_type_hints

import pytest

import sqlspec.adapters.mssql_python.pool as _mssql_pool
from sqlspec.adapters.mssql_python.config import (
    MssqlPythonConfig,
    MssqlPythonConnectionParams,
    _normalize_mssql_python_init,
)
from sqlspec.adapters.mssql_python.pool import MssqlPythonConnectionPool


def test_config_applies_driver_feature_json_serializer_to_statement_config() -> None:
    """Custom JSON serializers should reach the statement parameter config."""

    def serializer(value: object) -> str:
        return f"json:{value!r}"

    config = MssqlPythonConfig(
        connection_config={"server": "localhost"}, driver_features={"json_serializer": serializer}
    )

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.type_coercion_map[dict] is serializer
    assert parameter_config.type_coercion_map[list] is serializer


class DummyConnection:
    """Minimal connection object for pool lifecycle assertions."""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_connection_pool_configures_mssql_python_pooling(monkeypatch: pytest.MonkeyPatch) -> None:
    """The SQLSpec pool wrapper should configure driver-level pooling before connect."""
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
    connection = DummyConnection()
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)

    def fake_pooling(*args: Any, **kwargs: Any) -> None:
        calls.append(("pooling", args, kwargs))

    def fake_connect(connection_string: str, **kwargs: Any) -> DummyConnection:
        calls.append(("connect", (connection_string,), kwargs))
        return connection

    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", fake_pooling)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.connect", fake_connect)

    pool = MssqlPythonConnectionPool(
        connection_string="Server=localhost;", connect_kwargs={"timeout": 5}, max_size=7, idle_timeout=30, enabled=True
    )
    acquired = pool.acquire()
    pool.release(acquired)

    assert calls == [
        ("pooling", (), {"max_size": 7, "idle_timeout": 30, "enabled": True}),
        ("connect", ("Server=localhost;",), {"timeout": 5}),
    ]
    assert connection.closed is True


def test_config_create_pool_splits_connection_and_pool_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """MssqlPythonConfig should pass connection options and pool options to the right APIs."""
    pooling_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)

    def fake_pooling(**kwargs: Any) -> None:
        pooling_calls.append(kwargs)

    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", fake_pooling)

    config = MssqlPythonConfig(
        connection_config={
            "server": "localhost",
            "database": "tempdb",
            "pool_size": 3,
            "pool_idle_timeout": 10,
            "timeout": 20,
        }
    )

    pool = config.create_pool()

    assert pool.connection_string == "Server=localhost;Database=tempdb;"
    assert pool.connect_kwargs == {"timeout": 20}
    assert pooling_calls == [{"max_size": 3, "idle_timeout": 10, "enabled": True}]


def test_connection_params_include_current_mssql_python_odbc_keywords() -> None:
    """Typed connection params should cover the mssql-python ODBC keyword allowlist."""
    annotations = get_type_hints(MssqlPythonConnectionParams, include_extras=True)

    expected_keys = {
        "addr",
        "address",
        "server",
        "database",
        "uid",
        "pwd",
        "authentication",
        "trusted_connection",
        "encrypt",
        "trust_server_certificate",
        "hostname_in_certificate",
        "server_certificate",
        "server_spn",
        "multi_subnet_failover",
        "application_intent",
        "connect_retry_count",
        "connect_retry_interval",
        "keep_alive",
        "keep_alive_interval",
        "ip_address_preference",
        "packet_size",
        "timeout",
        "connection_timeout",
        "login_timeout",
        "native_uuid",
    }

    assert expected_keys <= set(annotations)
    assert "driver" not in annotations
    assert "application_name" not in annotations
    assert "workstation_id" not in annotations


def test_config_create_pool_normalizes_current_odbc_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Current mssql-python ODBC aliases should become canonical connection fields."""
    pooling_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)

    def fake_pooling(**kwargs: Any) -> None:
        pooling_calls.append(kwargs)

    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", fake_pooling)

    config = MssqlPythonConfig(
        connection_config={
            "address": "sql.example.test",
            "port": 1433,
            "db": "app",
            "username": "sa",
            "password": "secret",
            "trust": True,
            "trust_server_certificate": False,
            "encrypt": True,
            "authentication": "ActiveDirectoryMsi",
            "hostname_in_certificate": "*.database.windows.net",
            "server_certificate": "/certs/server.pem",
            "server_spn": "MSSQLSvc/sql.example.test:1433",
            "multi_subnet_failover": True,
            "application_intent": "ReadOnly",
            "connect_retry_count": 3,
            "connect_retry_interval": 5,
            "keep_alive": 30,
            "keep_alive_interval": 10,
            "ip_address_preference": "IPv4First",
            "packet_size": 32767,
            "connection_timeout": 15,
            "driver": "ODBC Driver 17 for SQL Server",
            "application_name": "ignored",
        }
    )

    pool = config.create_pool()

    assert pool.connection_string == (
        "Server=sql.example.test,1433;Database=app;UID=sa;PWD=secret;"
        "Authentication=ActiveDirectoryMsi;Encrypt=yes;TrustServerCertificate=no;"
        "HostnameInCertificate=*.database.windows.net;ServerCertificate=/certs/server.pem;"
        "ServerSPN=MSSQLSvc/sql.example.test:1433;MultiSubnetFailover=yes;ApplicationIntent=ReadOnly;"
        "ConnectRetryCount=3;ConnectRetryInterval=5;KeepAlive=30;KeepAliveInterval=10;"
        "IpAddressPreference=IPv4First;PacketSize=32767;"
    )
    assert pool.connect_kwargs == {"timeout": 15}
    assert "Driver=" not in pool.connection_string
    assert "APP=" not in pool.connection_string
    assert "Application Name=" not in pool.connection_string
    assert pooling_calls == [{"max_size": 100, "idle_timeout": 600, "enabled": True}]


def test_config_create_pool_rejects_conflicting_server_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Conflicting canonical ODBC aliases should fail instead of silently choosing one."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)

    config = MssqlPythonConfig(connection_config={"server": "srv1", "addr": "srv2"})

    with pytest.raises(ValueError, match="server"):
        config.create_pool()


def test_config_connection_hook_runs_for_session_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    """on_connection_create should run for connections acquired by provide_session()."""
    connection = DummyConnection()
    seen: list[DummyConnection] = []
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)

    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: connection
    )

    config = MssqlPythonConfig(
        connection_config={"server": "localhost"}, driver_features={"on_connection_create": seen.append}
    )

    with config.provide_session():
        pass

    assert seen == [connection]


def test_second_pool_warns_on_different_params(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second pool with different process-wide pooling params emits one warning."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: DummyConnection()
    )

    MssqlPythonConnectionPool(connection_string="Server=srv1;", max_size=10, idle_timeout=60, enabled=True)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        MssqlPythonConnectionPool(connection_string="Server=srv2;", max_size=20, idle_timeout=60, enabled=True)

    assert len(caught) == 1
    message = str(caught[0].message)
    assert "overwriting" in message
    assert "(10, 60, True)" in message
    assert "(20, 60, True)" in message


def test_second_pool_same_params_no_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second pool with identical process-wide pooling params emits no warning."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: DummyConnection()
    )

    MssqlPythonConnectionPool(connection_string="Server=srv1;", max_size=10, idle_timeout=60, enabled=True)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        MssqlPythonConnectionPool(connection_string="Server=srv2;", max_size=10, idle_timeout=60, enabled=True)

    assert len(caught) == 0


def test_normalize_mssql_python_init_returns_config_features_and_hook() -> None:
    seen: list[DummyConnection] = []

    normalized, features, hook = _normalize_mssql_python_init(
        {"server": "localhost"}, {"on_connection_create": seen.append, "use_pool": False}
    )

    assert normalized == {"server": "localhost"}
    assert features["use_pool"] is False
    assert "on_connection_create" not in features
    assert hook is not None
    hook(cast(Any, DummyConnection()))
    assert len(seen) == 1


def test_sync_config_delegates_to_shared_init_helper() -> None:
    def hook(_connection: Any) -> None:
        return None

    sync_config = MssqlPythonConfig(
        connection_config={"server": "localhost"}, driver_features={"on_connection_create": hook}
    )

    assert sync_config.connection_config == {"server": "localhost"}
    assert sync_config._user_connection_hook is hook
