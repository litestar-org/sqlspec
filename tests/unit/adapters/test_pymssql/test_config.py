"""pymssql configuration tests."""

from dataclasses import dataclass
from typing import Any, get_type_hints

import pytest

import sqlspec.adapters.pymssql.pool as _pymssql_pool
from sqlspec.adapters.pymssql import build_connection_config
from sqlspec.adapters.pymssql.config import PymssqlConfig, PymssqlConnectionParams, PymssqlDriverFeatures
from sqlspec.adapters.pymssql.driver import PymssqlDriver
from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool
from sqlspec.adapters.pymssql.tenant import PymssqlSessionContextStrategy
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload


def test_connection_params_cover_common_pymssql_keywords() -> None:
    """Typed connection params should cover the common pymssql connect kwargs."""
    annotations = get_type_hints(PymssqlConnectionParams, include_extras=True)

    expected_keys = {
        "server",
        "host",
        "user",
        "password",
        "database",
        "port",
        "timeout",
        "login_timeout",
        "charset",
        "as_dict",
        "appname",
        "conn_properties",
        "autocommit",
        "tds_version",
        "pool_recycle_seconds",
        "health_check_interval",
    }

    assert expected_keys <= set(annotations)


def test_config_defaults_server_port_and_features() -> None:
    """PymssqlConfig should normalize connection defaults and driver features."""
    config = PymssqlConfig(connection_config={}, driver_features={"enable_events": True})

    assert config.connection_config["server"] == "localhost"
    assert config.connection_config["port"] == 1433
    assert config.driver_type is PymssqlDriver
    assert config.supports_transactional_ddl is True
    assert config.supports_native_arrow_export is True
    assert config.driver_features["enable_events"] is True


def test_build_connection_config_normalizes_aliases() -> None:
    """build_connection_config should normalize host, db, and username aliases."""
    normalized = build_connection_config({"host": "sql.local", "db": "analytics", "username": "admin"})
    assert normalized == {"server": "sql.local", "port": 1433, "database": "analytics", "user": "admin"}


def test_pymssql_config_normalizes_aliases() -> None:
    """PymssqlConfig should map host, db, and username to server, database, and user."""
    config = PymssqlConfig(connection_config={"host": "sql.local", "db": "analytics", "username": "admin"})
    assert config.connection_config["server"] == "sql.local"
    assert config.connection_config["database"] == "analytics"
    assert config.connection_config["user"] == "admin"
    assert "host" not in config.connection_config
    assert "db" not in config.connection_config
    assert "username" not in config.connection_config


def test_config_create_pool_splits_pool_options_and_hook() -> None:
    """Pool lifecycle options should not be forwarded to pymssql.connect."""
    seen: list[object] = []
    config = PymssqlConfig(
        connection_config={
            "server": "sql.example.test",
            "user": "sa",
            "password": "secret",
            "database": "app",
            "pool_recycle_seconds": 5,
            "health_check_interval": 0.25,
        },
        driver_features={"on_connection_create": seen.append},
    )

    pool = config.create_pool()

    assert isinstance(pool, PymssqlConnectionPool)
    assert pool._connection_parameters == {
        "server": "sql.example.test",
        "user": "sa",
        "password": "secret",
        "database": "app",
        "port": 1433,
    }
    assert pool._recycle_seconds == 5
    assert pool._health_check_interval == 0.25
    assert "on_connection_create" not in config.driver_features


def test_config_close_pool_clears_connection_instance() -> None:
    """Closing the config pool should clear the stored pool reference."""
    config = PymssqlConfig(connection_instance=PymssqlConnectionPool({}))

    config._close_pool()

    assert config.connection_instance is None


def test_signature_namespace_exposes_public_adapter_types() -> None:
    """Config signature namespaces should include public pymssql types."""
    config = PymssqlConfig(connection_config={"server": "localhost"})

    namespace = config.get_signature_namespace()

    assert namespace["PymssqlConfig"] is PymssqlConfig
    assert namespace["PymssqlConnectionParams"] is PymssqlConnectionParams
    assert namespace["PymssqlConnectionPool"] is PymssqlConnectionPool
    assert namespace["PymssqlDriver"] is PymssqlDriver


def test_pymssql_runtime_aliases_resolve_to_installed_classes() -> None:
    """pymssql public runtime aliases should expose installed pymssql classes."""
    pymssql = pytest.importorskip("pymssql")
    from sqlspec.adapters.pymssql import PymssqlConnection as PublicPymssqlConnection
    from sqlspec.adapters.pymssql._typing import PYMSSQL_MODULE, PymssqlConnection, PymssqlRawCursor

    namespace = PymssqlConfig().get_signature_namespace()

    assert PYMSSQL_MODULE is pymssql
    assert PymssqlConnection is pymssql.Connection
    assert PublicPymssqlConnection is pymssql.Connection
    assert PymssqlRawCursor is pymssql.Cursor
    assert PymssqlConfig.connection_type is pymssql.Connection
    assert namespace["PymssqlConnection"] is pymssql.Connection
    assert namespace["PymssqlRawCursor"] is pymssql.Cursor
    assert isinstance(object(), PymssqlConnection) is False


def test_pymssql_session_clean_exit_rolls_back_active_transaction() -> None:
    """PymssqlSessionContext rolls back an active transaction even when exiting without an exception."""
    from typing import cast

    from sqlspec.adapters.pymssql._typing import PymssqlSessionContext
    from tests.unit.adapters.test_pymssql._fakes import FakeConnection

    calls: list[str] = []
    conn = FakeConnection()
    conn.autocommit(False)

    context = PymssqlSessionContext(
        acquire_connection=lambda: cast("Any", conn),
        release_connection=lambda _c, **_kw: calls.append("release"),
        statement_config=PymssqlConfig().statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as session:
        session.begin()

    assert conn.rollbacks == 1
    assert calls == ["release"]


def test_use_datetime2_default() -> None:
    """PymssqlConfig connection_config supports use_datetime2."""
    config = PymssqlConfig(connection_config={"server": "localhost", "use_datetime2": True})
    assert config.connection_config.get("use_datetime2") is True


def test_driver_features_mssql_parity() -> None:
    """PymssqlConfig defaults enable_pooling to True and harmonizes driver features."""
    assert "use_pool" not in PymssqlDriverFeatures.__annotations__
    assert "enable_pooling" in PymssqlDriverFeatures.__annotations__
    config = PymssqlConfig()
    assert config.driver_features.get("enable_pooling") is True
    assert config.driver_features.get("events_backend") == "poll_queue"
    assert config.driver_features.get("enable_uuid_conversion") is False


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
    monkeypatch: pytest.MonkeyPatch, provider: str, failure: str
) -> None:
    calls: list[str] = []
    strategy = RecordingTenantStrategy("custom", calls, fail_cleanup=failure == "cleanup")

    def connect(*args: Any, **kwargs: Any) -> TenantConnection:
        calls.append("connect")
        return TenantConnection(calls)

    monkeypatch.setattr(_pymssql_pool.pymssql, "connect", connect)
    config = PymssqlConfig(driver_features={"tenant_strategy": strategy})
    assert config.driver_features["tenant_strategy"] is strategy
    if failure == "prepare" and provider == "provide_session":

        def fail_prepare(self: Any, driver: Any) -> Any:
            raise RuntimeError("prepare")

        monkeypatch.setattr(PymssqlConfig, "_prepare_driver", fail_prepare)
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
def test_tenant_config_rejects_missing_context_before_connect(monkeypatch: pytest.MonkeyPatch, provider: str) -> None:
    calls: list[str] = []
    monkeypatch.setattr(_pymssql_pool.pymssql, "connect", lambda *args, **kwargs: calls.append("connect"))
    config = PymssqlConfig(driver_features={"tenant_strategy": RecordingTenantStrategy("custom", calls)})
    with pytest.raises(TenantContextError), getattr(config, provider)():
        pass
    assert calls == []
    with pytest.raises(ImproperConfigurationError):
        config.create_connection()


@pytest.mark.parametrize("shorthand", [True, "default", "rls", "session_context"])
def test_tenant_config_shorthand(shorthand: Any) -> None:
    config = PymssqlConfig(driver_features={"tenant_strategy": shorthand})
    assert isinstance(config.driver_features["tenant_strategy"], PymssqlSessionContextStrategy)


@pytest.mark.parametrize("shorthand", ["vpd", "identifier"])
def test_tenant_config_invalid_shorthand(shorthand: str) -> None:
    with pytest.raises(ImproperConfigurationError):
        PymssqlConfig(driver_features={"tenant_strategy": shorthand})


@pytest.mark.parametrize("disabled", [None, False])
def test_tenant_config_disabled_strategy(disabled: Any) -> None:
    config = PymssqlConfig(driver_features={"tenant_strategy": disabled})
    assert config.driver_features["tenant_strategy"] is None


def test_tenant_config_nested_sessions_have_distinct_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    connections: list[TenantConnection] = []

    def connect(**kwargs: Any) -> TenantConnection:
        connection = TenantConnection(calls)
        connections.append(connection)
        return connection

    monkeypatch.setattr(_pymssql_pool.pymssql, "connect", connect)
    config = PymssqlConfig(driver_features={"tenant_strategy": RecordingTenantStrategy("custom", calls)})
    with TenantContext.scope("a"), config.provide_session() as first, config.provide_session() as second:
        assert first.connection is connections[0]
        assert second.connection is connections[1]
        assert first.connection is not second.connection
    assert calls == ["checkout:a", "checkout:a", "rollback", "checkin:a", "close", "rollback", "checkin:a", "close"]


def test_tenant_connection_hook_failure_closes_acquired_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(_pymssql_pool.pymssql, "connect", lambda **kwargs: TenantConnection(calls))

    def fail_hook(connection: Any) -> None:
        raise RuntimeError("hook")

    config = PymssqlConfig(driver_features={"tenant_strategy": True, "on_connection_create": fail_hook})
    with TenantContext.scope("a"), pytest.raises(RuntimeError, match="hook"), config.provide_connection():
        pass
    assert calls == ["close"]
