"""Unit tests for mssql_python adapter configuration."""

from typing import Any

import pytest

from sqlspec.adapters.mssql_python.config import MssqlPythonConfig, MssqlPythonConnectionPool


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
