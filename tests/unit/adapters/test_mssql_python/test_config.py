"""Unit tests for mssql_python adapter configuration."""

import gc
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, cast, get_type_hints

import pytest

import sqlspec.adapters.mssql_python.pool as _mssql_pool
from sqlspec.adapters.mssql_python._typing import MssqlPythonSessionContext
from sqlspec.adapters.mssql_python.config import (
    MssqlPythonConfig,
    MssqlPythonConnectionParams,
    MssqlPythonDriverFeatures,
    _normalize_mssql_python_init,
)
from sqlspec.adapters.mssql_python.core import build_connection_config
from sqlspec.adapters.mssql_python.pool import MssqlPythonConnectionPool
from sqlspec.adapters.mssql_python.tenant import MssqlPythonSessionContextStrategy
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload


@pytest.fixture(autouse=True)
def _reset_mssql_pool_state() -> Iterator[None]:
    """Isolate process-global mssql-python pool tracking across unit tests."""
    _mssql_pool._ACTIVE_ENABLED_POOLS.clear()
    _mssql_pool._ACTIVE_DISABLED_POOLS.clear()
    _mssql_pool._POOLING_PARAMS = None
    try:
        yield
    finally:
        _mssql_pool._ACTIVE_ENABLED_POOLS.clear()
        _mssql_pool._ACTIVE_DISABLED_POOLS.clear()
        _mssql_pool._POOLING_PARAMS = None


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


def test_config_connection_string_with_discrete_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """MssqlPythonConfig should merge discrete database overrides over connection_string."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kw: None)

    config = MssqlPythonConfig(
        connection_config={
            "connection_string": "Server=localhost,1433;UID=app;PWD=secret;Encrypt=yes;",
            "database": "sales",
        }
    )

    pool = config.create_pool()
    assert "Database=sales" in pool.connection_string
    assert "Server=localhost,1433" in pool.connection_string


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

    with config.provide_session() as session:
        assert session.connection is connection

    assert session.connection is _DETACHED_CONNECTION
    assert seen == [connection]


def test_on_connection_create_runs_once_per_physical_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """on_connection_create hook runs once per physical connection, not on every warm pool checkout."""
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

    pool = config.create_pool()
    c1 = pool.acquire()
    pool.release(c1)
    c2 = pool.acquire()
    pool.release(c2)

    assert len(seen) == 1
    assert seen == [connection]


def test_pool_min_size_not_in_connection_string(monkeypatch: pytest.MonkeyPatch) -> None:
    """pool_min_size, pool_max_size, and pool_size are stripped from the raw ODBC connection string."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)

    config = MssqlPythonConfig(
        connection_config={"server": "localhost", "pool_min_size": 5, "pool_max_size": 20, "pool_size": 15}
    )
    pool = config.create_pool()
    assert "pool_min_size" not in pool.connection_string
    assert "pool_max_size" not in pool.connection_string
    assert "pool_size" not in pool.connection_string


def test_second_pool_warns_on_different_params(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second pool with different process-wide pooling params emits one warning."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: DummyConnection()
    )

    pool_a = MssqlPythonConnectionPool(connection_string="Server=srv1;", max_size=10, idle_timeout=60, enabled=True)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pool_b = MssqlPythonConnectionPool(connection_string="Server=srv2;", max_size=20, idle_timeout=60, enabled=True)

    assert len(caught) == 1
    message = str(caught[0].message)
    assert "overwriting" in message
    assert "(10, 60, True)" in message
    assert "(20, 60, True)" in message
    pool_a.close()
    pool_b.close()


def test_second_pool_same_params_no_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second pool with identical process-wide pooling params emits no warning."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: DummyConnection()
    )

    pool_a = MssqlPythonConnectionPool(connection_string="Server=srv1;", max_size=10, idle_timeout=60, enabled=True)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pool_b = MssqlPythonConnectionPool(connection_string="Server=srv2;", max_size=10, idle_timeout=60, enabled=True)

    assert len(caught) == 0
    pool_a.close()
    pool_b.close()


def test_normalize_mssql_python_init_returns_config_features_and_hook() -> None:
    seen: list[DummyConnection] = []

    assert "use_pool" not in MssqlPythonDriverFeatures.__annotations__
    assert "enable_pooling" in MssqlPythonDriverFeatures.__annotations__

    normalized, features, hook = _normalize_mssql_python_init(
        {"server": "localhost"}, {"on_connection_create": seen.append, "enable_pooling": False}
    )

    assert normalized == {"server": "localhost"}
    assert features["enable_pooling"] is False
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


def test_token_provider_reaches_connect() -> None:
    """An Azure Entra ID token provider must be forwarded, not silently dropped."""

    def _token_provider() -> tuple[str, int]:
        return ("token", 0)

    _, connect_kwargs = build_connection_config({"server": "sql.example.test", "token_provider": _token_provider})

    assert connect_kwargs["token_provider"] is _token_provider


def test_token_provider_is_a_declared_connection_parameter() -> None:
    """The typed parameters must cover every connect keyword the driver accepts."""
    assert "token_provider" in MssqlPythonConnectionParams.__annotations__


def test_failed_session_rolls_back_before_release() -> None:
    """A session exiting with an exception must roll back rather than leave work open."""
    calls: list[str] = []

    class _Connection:
        def cursor(self) -> object:
            return object()

        def rollback(self) -> None:
            calls.append("rollback")

        def commit(self) -> None:
            calls.append("commit")

    context = MssqlPythonSessionContext(
        acquire_connection=cast("Any", _Connection),
        release_connection=lambda _conn, **_kwargs: calls.append("release"),
        statement_config=MssqlPythonConfig().statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with pytest.raises(RuntimeError), context as session:
        raise RuntimeError

    assert calls == ["rollback", "release"]
    assert session.connection is _DETACHED_CONNECTION


def test_pool_does_not_reconfigure_when_params_match(monkeypatch: pytest.MonkeyPatch) -> None:
    """Identical pool configuration should avoid touching the native process-wide pool."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", (10, 60, True))
    calls: list[dict[str, object]] = []
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kw: calls.append(kw))

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        pool = MssqlPythonConnectionPool(
            connection_string="Server=localhost;", max_size=10, idle_timeout=60, enabled=True
        )
    assert len(recorded) == 0
    assert calls == []
    pool.close()


def test_close_pool_clears_connection_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    """MssqlPythonConfig._close_pool closes the pool and clears connection_instance."""
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    _mssql_pool._ACTIVE_ENABLED_POOLS.clear()
    _mssql_pool._ACTIVE_DISABLED_POOLS.clear()
    pooling_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kw: pooling_calls.append(kw)
    )
    config = MssqlPythonConfig(connection_config={"server": "localhost"})
    pool = config.create_pool()
    assert config.connection_instance is pool

    config.close_pool()

    assert config.connection_instance is None
    assert pooling_calls == [{"max_size": 100, "idle_timeout": 600, "enabled": True}, {"enabled": False}]
    assert _mssql_pool._POOLING_PARAMS is None


def test_session_clean_exit_rolls_back_active_transaction() -> None:
    """A session exiting cleanly with an uncommitted transaction must roll back before release."""
    calls: list[str] = []

    class _Connection:
        autocommit = False

        def cursor(self) -> object:
            return object()

        def rollback(self) -> None:
            calls.append("rollback")

        def commit(self) -> None:
            calls.append("commit")

    context = MssqlPythonSessionContext(
        acquire_connection=cast("Any", _Connection),
        release_connection=lambda _conn, **_kwargs: calls.append("release"),
        statement_config=MssqlPythonConfig().statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as session:
        session.begin()

    assert calls == ["rollback", "release"]
    assert session.connection is _DETACHED_CONNECTION


def test_connection_context_rolls_back_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """provide_connection should roll back the connection when exiting with an exception."""
    rolled_back = False

    class _Conn(DummyConnection):
        def rollback(self) -> None:
            nonlocal rolled_back
            rolled_back = True

    conn = _Conn()
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.connect", lambda *_a, **_kw: conn)
    config = MssqlPythonConfig(connection_config={"server": "localhost"})

    with pytest.raises(RuntimeError), config.provide_connection():
        raise RuntimeError

    assert rolled_back is True
    assert conn.closed is True


@pytest.mark.parametrize(
    ("connection_config", "driver_features"),
    [({"server": "localhost", "pool_enabled": False}, None), ({"server": "localhost"}, {"enable_pooling": False})],
    ids=["pool_enabled_false", "driver_feature_enable_pooling_false"],
)
def test_pool_disabled_explicitly_disables_native_pooling(
    monkeypatch: pytest.MonkeyPatch, connection_config: dict[str, Any], driver_features: dict[str, Any] | None
) -> None:
    """Disabling pooling initializes mssql_python.PoolingManager as disabled so connect() will not auto-enable pooling."""
    from mssql_python.pooling import PoolingManager

    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    _mssql_pool._ACTIVE_ENABLED_POOLS.clear()
    _mssql_pool._ACTIVE_DISABLED_POOLS.clear()
    PoolingManager._reset_for_testing()
    config = MssqlPythonConfig(connection_config=connection_config, driver_features=driver_features)
    try:
        pool = config.create_pool()

        assert pool.enabled is False
        assert PoolingManager.is_initialized() is True
        assert PoolingManager.is_enabled() is False
    finally:
        config.close_pool()
        PoolingManager._reset_for_testing()


def test_multiple_disabled_pools_do_not_warn_or_reconfigure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Multiple disabled pools share process-wide disabled state without warnings or repeated pooling() calls."""
    pooling_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    _mssql_pool._ACTIVE_ENABLED_POOLS.clear()
    _mssql_pool._ACTIVE_DISABLED_POOLS.clear()
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kw: pooling_calls.append(kw)
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pool_a = MssqlPythonConnectionPool(connection_string="Server=srv1;Database=db1;", max_size=5, enabled=False)
        pool_b = MssqlPythonConnectionPool(connection_string="Server=srv1;Database=db2;", max_size=25, enabled=False)

    assert len(caught) == 0
    assert pooling_calls == [{"enabled": False}]
    pool_a.close()
    assert _mssql_pool._POOLING_PARAMS == (0, 0, False)
    pool_b.close()
    assert _mssql_pool._POOLING_PARAMS is None
    assert pooling_calls == [{"enabled": False}]


def test_close_disables_native_pooling_only_when_last_enabled_pool_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Closing one enabled pool keeps native pooling active until the final enabled pool closes."""
    pooling_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    _mssql_pool._ACTIVE_ENABLED_POOLS.clear()
    _mssql_pool._ACTIVE_DISABLED_POOLS.clear()
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kw: pooling_calls.append(kw)
    )

    pool_a = MssqlPythonConnectionPool(connection_string="Server=srv1;Database=db1;", max_size=10, idle_timeout=60)
    pool_b = MssqlPythonConnectionPool(connection_string="Server=srv1;Database=db2;", max_size=10, idle_timeout=60)

    pool_a.close()
    pool_a.close()
    assert pooling_calls == [{"max_size": 10, "idle_timeout": 60, "enabled": True}]
    assert _mssql_pool._POOLING_PARAMS == (10, 60, True)

    pool_b.close()
    assert pooling_calls == [{"max_size": 10, "idle_timeout": 60, "enabled": True}, {"enabled": False}]
    assert _mssql_pool._POOLING_PARAMS is None

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pool_c = MssqlPythonConnectionPool(connection_string="Server=srv1;Database=db3;", max_size=20, idle_timeout=30)

    assert len(caught) == 0
    assert pooling_calls[-1] == {"max_size": 20, "idle_timeout": 30, "enabled": True}
    pool_c.close()


@pytest.mark.parametrize(
    ("first_kwargs", "second_kwargs", "expected_pooling_calls", "expected_active_params"),
    [
        (
            {"max_size": 10, "idle_timeout": 60, "enabled": True},
            {"enabled": False},
            [{"max_size": 10, "idle_timeout": 60, "enabled": True}, {"enabled": False}],
            (0, 0, False),
        ),
        (
            {"enabled": False},
            {"max_size": 10, "idle_timeout": 60, "enabled": True},
            [{"enabled": False}, {"max_size": 10, "idle_timeout": 60, "enabled": True}],
            (10, 60, True),
        ),
    ],
    ids=["enabled_then_disabled", "disabled_then_enabled"],
)
def test_superseded_pool_close_preserves_active_pooling_state(
    monkeypatch: pytest.MonkeyPatch,
    first_kwargs: dict[str, Any],
    second_kwargs: dict[str, Any],
    expected_pooling_calls: list[dict[str, Any]],
    expected_active_params: tuple[int, int, bool],
) -> None:
    """Closing a superseded pool must not disturb the replacing pooling state."""
    pooling_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kw: pooling_calls.append(kw)
    )

    first_pool = MssqlPythonConnectionPool(connection_string="Server=srv1;", **first_kwargs)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        second_pool = MssqlPythonConnectionPool(connection_string="Server=srv2;", **second_kwargs)

    assert len(caught) == 1
    assert pooling_calls == expected_pooling_calls
    assert _mssql_pool._POOLING_PARAMS == expected_active_params

    first_pool.close()
    assert _mssql_pool._POOLING_PARAMS == expected_active_params
    assert pooling_calls == expected_pooling_calls

    second_pool.close()
    assert _mssql_pool._POOLING_PARAMS is None


def test_gc_evicted_pool_does_not_trigger_overwrite_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """When all prior pools are garbage-collected without close(), new configs do not warn."""
    pooling_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kw: pooling_calls.append(kw)
    )

    def _create_ephemeral_pool() -> None:
        MssqlPythonConnectionPool(connection_string="Server=srv1;", max_size=10, idle_timeout=60, enabled=True)

    _create_ephemeral_pool()
    gc.collect()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        replacement = MssqlPythonConnectionPool(
            connection_string="Server=srv2;", max_size=20, idle_timeout=30, enabled=True
        )

    assert len(caught) == 0
    assert pooling_calls == [
        {"max_size": 10, "idle_timeout": 60, "enabled": True},
        {"max_size": 20, "idle_timeout": 30, "enabled": True},
    ]
    replacement.close()


def test_acquire_on_closed_pool_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Acquiring a connection after pool.close() raises RuntimeError."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    pool = MssqlPythonConnectionPool(connection_string="Server=srv1;", enabled=False)
    pool.close()

    with pytest.raises(RuntimeError, match="closed mssql-python pool"):
        pool.acquire()


def test_config_applies_driver_features_to_custom_statement_config() -> None:
    """driver_features json_serializer/json_deserializer should apply even when statement_config is passed explicitly."""
    from sqlspec.core import StatementConfig

    def custom_to_json(val: Any) -> str:
        return f"custom:{val}"

    def custom_from_json(val: str) -> Any:
        return {"raw": val}

    custom_stmt = StatementConfig(dialect="tsql")
    config = MssqlPythonConfig(
        connection_config={"server": "localhost"},
        statement_config=custom_stmt,
        driver_features={"json_serializer": custom_to_json, "json_deserializer": custom_from_json},
    )

    assert config.statement_config.parameter_config.json_serializer is custom_to_json
    assert config.statement_config.parameter_config.json_deserializer is custom_from_json


def test_config_event_runtime_hints_and_exports() -> None:
    """MssqlPythonConfig exposes poll_queue event runtime hints and does not re-export MssqlPythonConnectionPool in config.__all__."""
    import sqlspec.adapters.mssql_python.config as config_mod

    config = MssqlPythonConfig(connection_config={"server": "localhost"})
    hints = config.get_event_runtime_hints()

    assert hints.poll_interval == 0.25
    assert hints.lease_seconds == 5
    assert hints.select_for_update is False
    assert hints.skip_locked is False
    assert "MssqlPythonConnectionPool" not in config_mod.__all__


@pytest.fixture
def tenant_pool_manager(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    state = {"initialized": False, "enabled": False}

    class Manager:
        @staticmethod
        def is_initialized() -> bool:
            return state["initialized"]

        @staticmethod
        def is_enabled() -> bool:
            return state["enabled"]

    def pooling(**kwargs: Any) -> None:
        calls.append(kwargs)
        state.update(initialized=True, enabled=kwargs["enabled"])

    monkeypatch.setattr(_mssql_pool, "_POOLING_PARAMS", None)
    monkeypatch.setattr(_mssql_pool, "MssqlPythonPoolingManager", Manager, raising=False)
    monkeypatch.setattr(_mssql_pool.MSSQL_PYTHON_MODULE, "pooling", pooling)
    return calls


@pytest.mark.parametrize("tenant_first", [False, True])
def test_tenant_pool_disables_sdk_and_accepts_unpooled_admin(
    tenant_pool_manager: list[dict[str, Any]], tenant_first: bool
) -> None:
    pools = [
        MssqlPythonConnectionPool(connection_string="Server=localhost;", enabled=False, require_unpooled=tenant)
        for tenant in (tenant_first, not tenant_first)
    ]
    assert tenant_pool_manager == [{"enabled": False}]
    for pool in pools:
        pool.close()


@pytest.mark.parametrize("tenant_first", [False, True])
def test_tenant_pool_rejects_pooled_admin(tenant_pool_manager: list[dict[str, Any]], tenant_first: bool) -> None:
    from sqlspec.exceptions import ImproperConfigurationError

    first = MssqlPythonConnectionPool(
        connection_string="Server=localhost;", enabled=not tenant_first, require_unpooled=tenant_first
    )
    with pytest.raises(ImproperConfigurationError, match="pool_enabled=False"):
        MssqlPythonConnectionPool(
            connection_string="Server=localhost;", enabled=tenant_first, require_unpooled=not tenant_first
        )
    first.close()


def test_tenant_pool_reuses_verified_disabled_mode(tenant_pool_manager: list[dict[str, Any]]) -> None:
    for _ in range(2):
        pool = MssqlPythonConnectionPool(connection_string="Server=localhost;", enabled=False, require_unpooled=True)
        assert pool.require_unpooled is True
    assert tenant_pool_manager == [{"enabled": False}]


@dataclass
class RecordingTenantStrategy:
    label: str
    calls: list[str]
    fail_cleanup: bool = False

    def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        self.calls.append("checkout:" + payload.tenant_id)
        return TenantLeaseState(payload)

    def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        self.calls.append("checkin:" + state.payload.tenant_id)
        if self.fail_cleanup:
            raise ValueError("injected reset failure")


class TenantConnection:
    autocommit = False

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def cursor(self) -> Any:
        return None

    def close(self) -> None:
        self.calls.append("close")

    def rollback(self) -> None:
        self.calls.append("rollback")


@pytest.mark.parametrize("provider", ["provide_connection", "provide_session"])
@pytest.mark.parametrize("failure", ["", "body", "cleanup", "prepare"])
def test_tenant_config_providers_capture_and_cleanup(
    monkeypatch: pytest.MonkeyPatch, tenant_pool_manager: list[dict[str, Any]], provider: str, failure: str
) -> None:
    calls: list[str] = []
    strategy = RecordingTenantStrategy("custom", calls, fail_cleanup=failure == "cleanup")

    def connect(*args: Any, **kwargs: Any) -> TenantConnection:
        calls.append("connect")
        return TenantConnection(calls)

    monkeypatch.setattr(_mssql_pool.MSSQL_PYTHON_MODULE, "connect", connect)
    config = MssqlPythonConfig(connection_config={"server": "localhost"}, driver_features={"tenant_strategy": strategy})
    assert config.driver_features["tenant_strategy"] is strategy
    if failure == "prepare" and provider == "provide_session":

        def fail_prepare(self: Any, driver: Any) -> Any:
            raise RuntimeError("prepare")

        monkeypatch.setattr(MssqlPythonConfig, "_prepare_driver", fail_prepare)
    expected = TenantCleanupError if failure == "cleanup" else RuntimeError

    def invoke() -> None:
        with TenantContext.scope("a"), getattr(config, provider)():
            if failure == "body":
                raise RuntimeError("body")

    if failure in {"body", "cleanup"} or (failure == "prepare" and provider == "provide_session"):
        with pytest.raises(expected):
            invoke()
    else:
        invoke()
    assert calls == ["connect", "checkout:a", "rollback", "checkin:a", "close"]


@pytest.mark.parametrize("provider", ["provide_connection", "provide_session"])
def test_tenant_config_rejects_missing_context_before_connect(
    monkeypatch: pytest.MonkeyPatch, tenant_pool_manager: list[dict[str, Any]], provider: str
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(_mssql_pool.MSSQL_PYTHON_MODULE, "connect", lambda *args, **kwargs: calls.append("connect"))
    config = MssqlPythonConfig(driver_features={"tenant_strategy": RecordingTenantStrategy("custom", calls)})
    with pytest.raises(TenantContextError), getattr(config, provider)():
        pass
    assert calls == []
    with pytest.raises(ImproperConfigurationError):
        config.create_connection()


@pytest.mark.parametrize("shorthand", [True, "default", "rls", "session_context"])
def test_tenant_config_shorthand(shorthand: Any) -> None:
    config = MssqlPythonConfig(driver_features={"tenant_strategy": shorthand})
    assert isinstance(config.driver_features["tenant_strategy"], MssqlPythonSessionContextStrategy)


@pytest.mark.parametrize("shorthand", ["vpd", "identifier"])
def test_tenant_config_invalid_shorthand(shorthand: str) -> None:
    with pytest.raises(ImproperConfigurationError):
        MssqlPythonConfig(driver_features={"tenant_strategy": shorthand})


@pytest.mark.parametrize("disabled", [None, False])
def test_tenant_config_disabled_strategy(disabled: Any) -> None:
    config = MssqlPythonConfig(driver_features={"tenant_strategy": disabled})
    assert config.driver_features["tenant_strategy"] is None


@pytest.mark.parametrize(
    "kwargs", [{"connection_config": {"pool_enabled": True}}, {"driver_features": {"enable_pooling": True}}]
)
def test_tenant_config_rejects_explicit_pooling(kwargs: dict[str, Any]) -> None:
    features = {"tenant_strategy": True, **kwargs.get("driver_features", {})}
    with pytest.raises(ImproperConfigurationError, match="pool_enabled=False"):
        MssqlPythonConfig(connection_config=kwargs.get("connection_config"), driver_features=features)


def test_tenant_config_rejects_foreign_pool(tenant_pool_manager: list[dict[str, Any]]) -> None:
    foreign = MssqlPythonConnectionPool(connection_string="Server=localhost;", enabled=False)
    with pytest.raises(ImproperConfigurationError, match="verified unpooled"):
        MssqlPythonConfig(connection_instance=foreign, driver_features={"tenant_strategy": True})


def test_tenant_pool_rejects_external_sdk_pooling(
    monkeypatch: pytest.MonkeyPatch, tenant_pool_manager: list[dict[str, Any]]
) -> None:
    monkeypatch.setattr(_mssql_pool.MssqlPythonPoolingManager, "is_enabled", lambda: True)
    with pytest.raises(ImproperConfigurationError, match="pool_enabled=False"):
        MssqlPythonConnectionPool(connection_string="Server=localhost;", enabled=False, require_unpooled=True)
    assert tenant_pool_manager == []


def test_tenant_connection_hook_failure_closes_acquired_connection(
    monkeypatch: pytest.MonkeyPatch, tenant_pool_manager: list[dict[str, Any]]
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(_mssql_pool.MSSQL_PYTHON_MODULE, "connect", lambda *args, **kwargs: TenantConnection(calls))

    def fail_hook(connection: Any) -> None:
        raise RuntimeError("hook")

    config = MssqlPythonConfig(
        connection_config={"server": "localhost"},
        driver_features={"tenant_strategy": True, "on_connection_create": fail_hook},
    )
    with TenantContext.scope("a"), pytest.raises(RuntimeError, match="hook"), config.provide_connection():
        pass
    assert calls == ["close"]


def test_ag_replica_defaults_and_database_guard() -> None:
    config = MssqlPythonConfig(connection_config={"server": "listener", "database": "app"}, replicas=True)
    replica = config.replica_groups["default"].configs[0]
    connection_string, _ = build_connection_config(dict(replica.connection_config))
    assert "ApplicationIntent=ReadOnly" in connection_string
    assert "MultiSubnetFailover=yes" in connection_string
    assert config.connection_config == {"server": "listener", "database": "app"}
    with pytest.raises(ImproperConfigurationError, match="database"):
        MssqlPythonConfig(connection_config={"server": "listener"}, replicas=True)


def test_ag_derive_alias_and_tenant_pooling_hook_are_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MSSQL_PYTHON_MODULE.pooling", lambda **kwargs: None)
    monkeypatch.setattr("sqlspec.adapters.mssql_python.pool.MssqlPythonPoolingManager.is_enabled", lambda: False)
    seen: list[object] = []
    hook = seen.append
    strategy = MssqlPythonSessionContextStrategy()
    config = MssqlPythonConfig(
        connection_config={"server": "primary", "database": "app"},
        driver_features={"tenant_strategy": strategy, "on_connection_create": hook},
        replicas=True,
    )
    replica = config.replica_groups["default"].configs[0]
    assert replica.driver_features["enable_pooling"] is False
    pool = replica.create_pool()
    try:
        assert pool.enabled is False
        assert pool.require_unpooled
        assert pool.on_connection_create is hook
    finally:
        replica.close_pool()
    derived = config.derive_pool(host="standby")
    assert build_connection_config(dict(derived.connection_config))[0].startswith("Server=standby;")


def test_ag_derive_new_connection_string_replaces_inherited_aliases() -> None:
    config = MssqlPythonConfig(connection_config={"server": "primary", "database": "old", "uid": "old-user"})
    derived = config.derive_pool(connection_string="Server=standby;Database=new;UID=new-user;")
    assert build_connection_config(dict(derived.connection_config))[0] == "Server=standby;Database=new;UID=new-user;"
    assert config.connection_config["database"] == "old"


def test_ag_acquire_maps_unavailable_replica(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.exceptions import ReplicaUnavailableError

    native = Exception(976, "The target database is not available for queries")

    def fail_connect(*args: Any, **kwargs: Any) -> None:
        raise native

    monkeypatch.setattr(_mssql_pool.MSSQL_PYTHON_MODULE, "pooling", lambda **kwargs: None)
    monkeypatch.setattr(_mssql_pool.MSSQL_PYTHON_MODULE, "connect", fail_connect)
    config = MssqlPythonConfig(connection_config={"server": "listener", "database": "app"}, replicas=True)
    try:
        replica = config.replica_groups["default"].configs[0]
        with pytest.raises(ReplicaUnavailableError) as caught, replica.provide_session():
            pass
        assert caught.value.__cause__ is native
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    "shorthand",
    [
        True,
        "Server=reader;Database=app;",
        {"host": "reader", "application_intent": "ReadWrite", "multi_subnet_failover": False},
    ],
)
def test_ag_shorthand_uses_connection_string_database_and_explicit_intent(shorthand: Any) -> None:
    config = MssqlPythonConfig(
        connection_config={"connection_string": "Server=primary;Database={app};"}, replicas=shorthand
    )
    replica = config.replica_groups["default"].configs[0]
    text = build_connection_config(dict(replica.connection_config))[0]
    if isinstance(shorthand, dict):
        assert "ApplicationIntent=ReadWrite" in text
        assert "MultiSubnetFailover=no" in text
    else:
        assert "ApplicationIntent=ReadOnly" in text
        assert "MultiSubnetFailover=yes" in text


@pytest.mark.parametrize(
    "replicas",
    [
        "Server=reader;Database=app;ApplicationIntent=ReadWrite;MultiSubnetFailover=no;",
        {"host": "reader", "applicationintent": "ReadWrite", "multisubnetfailover": False},
    ],
)
def test_ag_replica_preserves_explicit_odbc_intent_aliases(replicas: Any) -> None:
    config = MssqlPythonConfig(connection_config={"server": "primary", "database": "app"}, replicas=replicas)
    replica = config.replica_groups["default"].configs[0]
    text = build_connection_config(dict(replica.connection_config))[0]
    assert "ApplicationIntent=ReadWrite" in text
    assert "MultiSubnetFailover=no" in text


def test_ag_clone_replaces_inherited_extra_endpoint_without_mutation() -> None:
    extra = {"Server": "primary", "KeepAlive": 60}
    parent = MssqlPythonConfig(connection_config={"server": "primary", "database": "app", "extra": extra})
    replica = parent.derive_pool(host="standby")
    text = build_connection_config(dict(replica.connection_config))[0]
    assert "Server=standby" in text
    assert "primary" not in text
    assert "KeepAlive=60" in text
    assert extra == {"Server": "primary", "KeepAlive": 60}
    assert parent.connection_config["server"] == "primary"
