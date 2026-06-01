"""Unit tests for mssql_python adapter configuration."""

import warnings
from typing import Any, cast

import pytest

import sqlspec.adapters.mssql_python.config as _mssql_config
from sqlspec.adapters.mssql_python.config import (
    MssqlPythonAsyncConfig,
    MssqlPythonConfig,
    MssqlPythonConnectionPool,
    _normalize_mssql_python_init,
)


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
    monkeypatch.setattr(_mssql_config, "_POOLING_PARAMS", None)

    def fake_pooling(*args: Any, **kwargs: Any) -> None:
        calls.append(("pooling", args, kwargs))

    def fake_connect(connection_string: str, **kwargs: Any) -> DummyConnection:
        calls.append(("connect", (connection_string,), kwargs))
        return connection

    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.pooling", fake_pooling)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.connect", fake_connect)

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
    monkeypatch.setattr(_mssql_config, "_POOLING_PARAMS", None)

    def fake_pooling(**kwargs: Any) -> None:
        pooling_calls.append(kwargs)

    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.pooling", fake_pooling)

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


def test_config_connection_hook_runs_for_session_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    """on_connection_create should run for connections acquired by provide_session()."""
    connection = DummyConnection()
    seen: list[DummyConnection] = []
    monkeypatch.setattr(_mssql_config, "_POOLING_PARAMS", None)

    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: connection
    )

    config = MssqlPythonConfig(
        connection_config={"server": "localhost"}, driver_features={"on_connection_create": seen.append}
    )

    with config.provide_session():
        pass

    assert seen == [connection]


def test_second_pool_warns_on_different_params(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second pool with different process-wide pooling params emits one warning."""
    monkeypatch.setattr(_mssql_config, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: DummyConnection()
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
    monkeypatch.setattr(_mssql_config, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: DummyConnection()
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


def test_sync_and_async_config_delegate_to_shared_init_helper() -> None:
    def hook(_connection: Any) -> None:
        return None

    sync_config = MssqlPythonConfig(
        connection_config={"server": "localhost"}, driver_features={"on_connection_create": hook}
    )
    async_config = MssqlPythonAsyncConfig(
        connection_config={"server": "localhost"}, driver_features={"on_connection_create": hook}
    )

    assert sync_config.connection_config == {"server": "localhost"}
    assert async_config.connection_config == {"server": "localhost"}
    assert sync_config._user_connection_hook is hook
    assert async_config._user_connection_hook is hook
