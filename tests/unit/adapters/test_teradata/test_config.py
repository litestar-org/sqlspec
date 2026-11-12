"""Exercise session hooks and exceptional lease cleanup through public configs."""

import sys
from collections.abc import Generator
from types import SimpleNamespace
from typing import Any, NoReturn
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.teradata import TeradataConfig, TeradataDriver
from sqlspec.exceptions import TransactionError
from sqlspec.utils.module_loader import reset_dependency_cache


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> Generator[list[Any], None, None]:
    result = []

    def connect(**kwargs: Any) -> Any:
        connection = MagicMock()
        connection.autocommit = True
        result.append(connection)
        return connection

    monkeypatch.setitem(sys.modules, "teradatasql", SimpleNamespace(connect=connect))
    reset_dependency_cache("teradatasql")
    try:
        yield result
    finally:
        reset_dependency_cache("teradatasql")


def test_config_session_releases_connection_after_success(connections: list[Any]) -> None:
    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()
    with config.provide_session() as session:
        assert isinstance(session, TeradataDriver)
        assert pool.checked_out() == 1
    assert pool.checked_out() == 0
    with config.provide_session() as session:
        assert session.connection is connections[0]
    pool.close()


def test_session_prepare_hook_failure_discards_lease(connections: list[Any]) -> None:
    def fail(session: Any) -> NoReturn:
        raise ValueError("session setup failed")

    from sqlspec.adapters.teradata import TeradataSessionContext

    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()

    def release(connection: Any, **kwargs: Any) -> None:
        pool.release(connection, discard=kwargs.get("exc_type") is not None)

    context = TeradataSessionContext(pool.acquire, release, config.statement_config, dict(config.driver_features), fail)
    with pytest.raises(ValueError, match="session setup failed"):
        with context:
            pytest.fail("setup hook must fail before yielding session")
    assert pool.checked_out() == 0
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()
    pool.close()


def test_session_rollback_failure_discards_lease(connections: list[Any]) -> None:
    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()
    with pytest.raises(TransactionError, match="rollback"):
        with config.provide_session() as session:
            session.begin()
            session.connection.rollback.side_effect = RuntimeError("socket gone")
    assert pool.checked_out() == 0
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()
    pool.close()


def test_config_signature_namespace_and_standalone_connection(connections: list[Any]) -> None:
    config = TeradataConfig()
    namespace = config.get_signature_namespace()
    assert namespace["TeradataConfig"] is TeradataConfig
    assert namespace["TeradataDriver"] is TeradataDriver
    standalone = config.create_connection()
    assert standalone is connections[0]
    assert config.provide_pool().checked_out() == 0
    standalone.close()
    config.close_pool()


@pytest.mark.parametrize("error", [False, True])
def test_native_cursor_context_closes_after_use(error: bool) -> None:
    from sqlspec.adapters.teradata import TeradataCursor

    connection = MagicMock()
    if error:
        with pytest.raises(ValueError, match="application failed"):
            with TeradataCursor(connection) as cursor:
                cursor.execute("SELECT 1")
                raise ValueError("application failed")
    else:
        with TeradataCursor(connection) as cursor:
            cursor.execute("SELECT 1")
    assert connection.cursor.return_value.execute.call_args.args == ("SELECT 1",)
    connection.cursor.return_value.close.assert_called_once_with()


def test_config_exports_oauth_logmechs_and_extended_connection_params() -> None:
    from types import MappingProxyType
    from typing import get_args

    from sqlspec.adapters.teradata import config as config_mod

    for name in ("TeradataLogMech", "TeradataSSLMode", "TeradataTransactionMode"):
        assert name in config_mod.__all__

    logmechs = set(get_args(config_mod.TeradataLogMech))
    assert {"BEARER", "CODE", "CRED", "ROPC", "SECRET"} <= logmechs

    params: config_mod.TeradataConnectionParams = {
        "host": "td.local",
        "logmech": "BEARER",
        "account": "$M",
        "logdata": "token",
        "charset": "UTF8",
        "sslca": "/etc/ssl/ca.pem",
        "sslcapath": "/etc/ssl/certs",
        "lob_support": True,
        "connect_timeout": 10,
        "request_timeout": 30.5,
        "cop": True,
        "coplast": False,
        "https_port": 443,
    }
    features: config_mod.TeradataDriverFeatures = {"query_band": MappingProxyType({"app": "etl"})}
    cfg = config_mod.TeradataConfig(connection_config=dict(params), driver_features=features)
    assert cfg.connection_config["cop"] == "true"
    assert cfg.connection_config["coplast"] == "false"
    assert cfg.connection_config["https_port"] == 443
    hints = cfg.get_event_runtime_hints()
    assert hints.poll_interval == 0.5
    cfg.close_pool()
