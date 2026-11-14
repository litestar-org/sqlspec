"""Exercise session hooks and exceptional lease cleanup through public configs."""

import sys
from collections.abc import Generator
from types import SimpleNamespace
from typing import Any, NoReturn
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.teradata import TeradataConfig, TeradataDriver, TeradataSessionContext
from sqlspec.driver._common import _DETACHED_CONNECTION
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
    assert session.connection is _DETACHED_CONNECTION
    assert pool.checked_out() == 0
    with config.provide_session() as session:
        assert session.connection is connections[0]
    assert session.connection is _DETACHED_CONNECTION
    pool.close()


def test_session_prepare_hook_failure_discards_lease(connections: list[Any]) -> None:
    captured: list[Any] = []

    def fail(session: Any) -> NoReturn:
        captured.append(session)
        raise ValueError("session setup failed")

    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()

    def release(connection: Any, **kwargs: Any) -> None:
        pool.release(connection, discard=kwargs.get("exc_type") is not None)

    context = TeradataSessionContext(pool.acquire, release, config.statement_config, dict(config.driver_features), fail)
    with pytest.raises(ValueError, match="session setup failed"):
        with context:
            pytest.fail("setup hook must fail before yielding session")
    assert captured[0].connection is _DETACHED_CONNECTION
    assert pool.checked_out() == 0
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()
    pool.close()


def test_session_rollback_failure_discards_lease(connections: list[Any]) -> None:
    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()
    session: Any = None
    with pytest.raises(TransactionError, match="rollback"):
        with config.provide_session() as session:
            session.begin()
            session.connection.rollback.side_effect = RuntimeError("socket gone")
    assert session.connection is _DETACHED_CONNECTION
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


def test_replica_derivation_preserves_pool_controls_and_alias_precedence() -> None:
    def hook(connection: Any) -> None:
        return None

    primary = TeradataConfig(
        connection_config={
            "host": "writer",
            "user": "writer",
            "pool_min_size": 2,
            "pool_max_size": 6,
            "pool_timeout": 11,
            "pool_recycle_seconds": 120,
            "health_check_interval": 8,
        },
        driver_features={"on_connection_create": hook},
    )
    replica = primary.derive_pool(host="reader", username="reader", port=1026, pool_max_size=8)
    assert replica.connection_config["user"] == "reader"
    assert replica.connection_config["dbs_port"] == "1026"
    assert replica._user_connection_hook is hook
    assert (
        replica._pool_min_size,
        replica._pool_max_size,
        replica._pool_timeout,
        replica._pool_recycle_seconds,
        replica._health_check_interval,
    ) == (2, 8, 11, 120, 8)
    json_replica = primary.derive_pool(sJson='{"host":"json-reader"}')
    assert json_replica.connection_config == {"sJson": '{"host":"json-reader"}'}
    assert primary.connection_config["host"] == "writer"


def test_replica_endpoint_shorthands_build_independent_teradata_configs() -> None:
    primary = TeradataConfig(
        connection_config={"host": "writer"}, replicas=["reader:1026", "teradata://reader:pw@reader-two:1025/analytics"]
    )
    readers = primary.replica_groups["default"].configs
    assert readers[0].connection_config["dbs_port"] == "1026"
    assert readers[1].connection_config["database"] == "analytics"


def test_replica_url_and_sjson_transitions_preserve_pool_capacity() -> None:
    primary = TeradataConfig(connection_config={"host": "writer", "pool_min_size": 2, "pool_max_size": 7})
    json_reader = primary.derive_pool(sJson='{"host":"json-reader"}')
    assert (json_reader._pool_min_size, json_reader._pool_max_size) == (2, 7)
    tcp_reader = json_reader.derive_pool(url="teradata://reader:pw@reader:1026/analytics")
    assert tcp_reader.connection_config["host"] == "reader"
    assert tcp_reader.connection_config["dbs_port"] == "1026"
    assert tcp_reader.connection_config.get("sJson") is None


@pytest.mark.parametrize("endpoint", ("", "postgresql://reader/app", "reader:invalid"))
def test_invalid_replica_endpoints_fail_before_pool_creation(endpoint: str) -> None:
    from sqlspec.exceptions import ImproperConfigurationError

    with pytest.raises(ImproperConfigurationError):
        TeradataConfig(connection_config={"host": "writer"}, replicas=[endpoint])
