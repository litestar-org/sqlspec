"""Psqlpy configuration tests covering statement config builders."""

from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.psqlpy._typing import PsqlpyConnection, PsqlpyConnectionPool, PsqlpySessionContext
from sqlspec.adapters.psqlpy.config import (
    PsqlpyConfig,
    PsqlpyConnectionContext,
    PsqlpyDriverFeatures,
    _PsqlpySessionFactory,
)
from sqlspec.adapters.psqlpy.core import (
    build_connection_config,
    build_postgres_extension_probe_names,
    build_statement_config,
    resolve_postgres_extension_state,
)
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.core import SQL, StatementConfig
from sqlspec.exceptions import ImproperConfigurationError


class _ExtensionQueryResult:
    def __init__(self, extension_names: set[str]) -> None:
        self._extension_names = extension_names

    def result(self) -> list[dict[str, str]]:
        return [{"extname": name} for name in self._extension_names]


class _ExtensionConnection:
    __slots__ = ("_extension_names", "queries")

    def __init__(self, extension_names: set[str]) -> None:
        self._extension_names = extension_names
        self.queries: list[tuple[str, list[list[str]]]] = []

    async def fetch(self, query: str, parameters: list[list[str]]) -> _ExtensionQueryResult:
        self.queries.append((query, parameters))
        return _ExtensionQueryResult(self._extension_names)


def test_build_default_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_build_default_statement_config_custom_deserializer() -> None:
    """Custom deserializer should propagate into the parameter configuration."""

    def deserializer(_: str) -> object:
        return {"deserialized": True}

    statement_config = build_statement_config(json_deserializer=deserializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_deserializer is deserializer


def test_psqlpy_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the Psqlpy statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    config = PsqlpyConfig(driver_features={"json_serializer": serializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_psqlpy_config_applies_driver_feature_deserializer() -> None:
    """Driver features should mutate the Psqlpy JSON deserializer."""

    def deserializer(_: str) -> object:
        return {"feature": True}

    config = PsqlpyConfig(driver_features={"json_deserializer": deserializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_deserializer is deserializer


def test_psqlpy_driver_features_declares_cast_detection() -> None:
    """Driver feature typing should expose every consumed feature flag."""
    assert "enable_cast_detection" in PsqlpyDriverFeatures.__annotations__


def test_psqlpy_config_defaults_cast_detection_feature() -> None:
    """Cast-aware parameter processing should be declared and enabled by default."""
    config = PsqlpyConfig()

    assert config.driver_features["enable_cast_detection"] is True


def test_psqlpy_config_preserves_cast_detection_override() -> None:
    """Callers should be able to disable cast-aware parameter processing."""
    config = PsqlpyConfig(driver_features={"enable_cast_detection": False})

    assert config.driver_features["enable_cast_detection"] is False


def test_psqlpy_runtime_aliases_resolve_to_installed_classes() -> None:
    """Psqlpy public runtime aliases should expose installed psqlpy classes."""
    psqlpy = pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy import PsqlpyConnection as PublicPsqlpyConnection
    from sqlspec.adapters.psqlpy._typing import PsqlpyListener

    namespace = PsqlpyConfig().get_signature_namespace()

    assert PsqlpyConnection is psqlpy.Connection
    assert PublicPsqlpyConnection is psqlpy.Connection
    assert PsqlpyListener is psqlpy.Listener
    assert PsqlpyConfig.connection_type is psqlpy.Connection
    assert namespace["PsqlpyConnection"] is psqlpy.Connection
    assert isinstance(object(), PsqlpyConnection) is False


def test_psqlpy_build_postgres_extension_probe_names_filters_disabled_features() -> None:
    """Only enabled extension probes should be returned."""
    assert build_postgres_extension_probe_names({"enable_pgvector": True, "enable_paradedb": False}) == ["vector"]


def test_psqlpy_resolve_postgres_extension_state_promotes_paradedb() -> None:
    """Detected extensions should promote the runtime dialect."""
    statement_config, pgvector_available, paradedb_available = resolve_postgres_extension_state(
        StatementConfig(dialect="postgres"), {"enable_pgvector": True, "enable_paradedb": True}, {"vector", "pg_search"}
    )

    assert statement_config.dialect == "paradedb"
    assert pgvector_available is True
    assert paradedb_available is True


@pytest.mark.anyio
async def test_psqlpy_enable_pgvector_detects_extension_and_promotes_dialect() -> None:
    """Psqlpy pgvector support should mean extension detection and dialect promotion."""
    config = PsqlpyConfig(
        driver_features={"enable_pgvector": True, "enable_paradedb": False, "enable_pg_textsearch": False}
    )
    connection = _ExtensionConnection({"vector"})

    await config._ensure_connection(cast("PsqlpyConnection", connection))  # pyright: ignore[reportPrivateUsage]

    assert connection.queries[0][1] == [["vector"]]
    assert config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_psqlpy_enable_pg_textsearch_detects_extension_and_promotes_dialect() -> None:
    """Psqlpy pg_textsearch support should mean extension detection and dialect promotion."""
    config = PsqlpyConfig(
        driver_features={"enable_pgvector": False, "enable_paradedb": False, "enable_pg_textsearch": True}
    )
    connection = _ExtensionConnection({"pg_textsearch"})

    assert config.pg_textsearch_available is False
    await config._ensure_connection(cast("PsqlpyConnection", connection))  # pyright: ignore[reportPrivateUsage]

    assert connection.queries[0][1] == [["pg_textsearch"]]
    assert config.pg_textsearch_available is True
    assert config.statement_config.dialect == "pg_textsearch"


@pytest.mark.anyio
async def test_psqlpy_ensure_connection_supports_slotted_connections_and_calls_hook_once() -> None:
    """_ensure_connection should track connection IDs without setting attributes on slotted connections."""
    hook_calls: list[object] = []

    async def on_connection_create(conn: PsqlpyConnection) -> None:
        hook_calls.append(conn)

    config = PsqlpyConfig(
        driver_features={
            "enable_pgvector": False,
            "enable_paradedb": False,
            "enable_pg_textsearch": False,
            "on_connection_create": on_connection_create,
        }
    )
    connection = _ExtensionConnection(set())

    await config._ensure_connection(cast("PsqlpyConnection", connection))  # pyright: ignore[reportPrivateUsage]
    await config._ensure_connection(cast("PsqlpyConnection", connection))  # pyright: ignore[reportPrivateUsage]

    assert hook_calls == [connection]


@pytest.mark.anyio
async def test_psqlpy_session_context_resolves_callable_statement_config() -> None:
    """Session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = PsqlpySessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_psqlpy_session_context_preserves_explicit_statement_config() -> None:
    """Explicit StatementConfig should be used directly without calling."""
    explicit_config = StatementConfig(dialect="postgres")
    context = PsqlpySessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=explicit_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config is explicit_config


def test_psqlpy_provide_session_tracks_promoted_statement_config() -> None:
    """Runtime statement config should resolve the current config dialect lazily."""
    config = PsqlpyConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"


def test_build_connection_config_resolves_aliases() -> None:
    """build_connection_config should map conninfo/url to dsn, database/dbname/db to db_name, and user to username."""
    cfg = build_connection_config({
        "url": "postgresql://usr:pwd@host.internal:5432/main",
        "database": "override_db",
        "user": "override_usr",
    })
    assert cfg["dsn"] == "postgresql://usr:pwd@host.internal:5432/main"
    assert cfg["db_name"] == "override_db"
    assert cfg["username"] == "override_usr"
    assert "url" not in cfg
    assert "database" not in cfg
    assert "user" not in cfg


def test_psqlpy_config_normalizes_aliases() -> None:
    """PsqlpyConfig should normalize aliases in connection_config."""
    config = PsqlpyConfig(
        connection_config={"conninfo": "postgresql://localhost:5432/test", "dbname": "target_db", "user": "target_user"}
    )
    assert config.connection_config["dsn"] == "postgresql://localhost:5432/test"
    assert config.connection_config["db_name"] == "target_db"
    assert config.connection_config["username"] == "target_user"
    assert "conninfo" not in config.connection_config
    assert "dbname" not in config.connection_config
    assert "user" not in config.connection_config


@pytest.mark.anyio
async def test_psqlpy_session_context_rolls_back_open_transaction_on_exit() -> None:
    """PsqlpySessionContext.__aexit__ should roll back an active driver transaction before releasing the connection."""
    conn = AsyncMock()
    conn.in_transaction = False
    context = PsqlpySessionContext(
        acquire_connection=AsyncMock(return_value=conn),
        release_connection=AsyncMock(),
        statement_config=StatementConfig(dialect="postgres"),
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        await driver.begin()
        assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]

    conn.execute.assert_any_await("ROLLBACK")
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.anyio
async def test_psqlpy_pooler_mode_transaction_disables_prepared_statements() -> None:
    """Transaction pooler mode should construct a valid ConnectionPool and pass prepared=False on execution."""
    assert "pgbouncer" not in PsqlpyDriverFeatures.__annotations__
    assert "pgbouncer_mode" not in PsqlpyDriverFeatures.__annotations__
    config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://localhost:6432/test"}, driver_features={"pooler_mode": "transaction"}
    )
    assert config.driver_features["pooler_mode"] == "transaction"
    pool = await config.create_pool()
    pool.close()

    conn = AsyncMock()
    conn.fetch.return_value = _ExtensionQueryResult(set())
    conn.execute.return_value = "CREATE TABLE"
    driver = PsqlpyDriver(
        cast("PsqlpyConnection", conn), statement_config=config.statement_config, driver_features=config.driver_features
    )

    select_stmt = SQL("SELECT 1", statement_config=config.statement_config)
    await driver.dispatch_execute(cast("PsqlpyConnection", conn), select_stmt)
    assert conn.fetch.await_args.kwargs.get("prepared") is False

    ddl_stmt = SQL("CREATE TABLE items (id INT)", statement_config=config.statement_config)
    await driver.dispatch_execute(cast("PsqlpyConnection", conn), ddl_stmt)
    assert conn.execute.await_args.kwargs.get("prepared") is False

    many_stmt = SQL(
        "INSERT INTO items (id) VALUES ($1)", [[1], [2]], statement_config=config.statement_config, is_many=True
    )
    await driver.dispatch_execute_many(cast("PsqlpyConnection", conn), many_stmt)
    assert conn.execute_many.await_args.kwargs.get("prepared") is False


@pytest.mark.anyio
async def test_psqlpy_derive_direct_config_and_columnar_engine() -> None:
    """derive_direct_config and enable_alloydb_columnar should configure session and transaction modes."""
    base_config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost:5432/test"})
    assert base_config.derive_direct_config() is base_config

    auto_direct_config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://localhost:5432/test"},
        driver_features={"pooler_mode": "auto", "enable_alloydb_columnar": True},
    )
    assert auto_direct_config.connection_config["options"] == "-c google_columnar_engine.enable_columnar_scan=on"

    tx_columnar_config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://pooler:6432/test"},
        driver_features={"pooler_mode": "transaction", "enable_alloydb_columnar": True},
    )
    assert tx_columnar_config.driver_features["pooler_mode"] == "transaction"
    assert "options" not in tx_columnar_config.connection_config

    with pytest.raises(ImproperConfigurationError, match="Invalid pooler_mode"):
        PsqlpyConfig(
            connection_config={"dsn": "postgresql://localhost:5432/test"},
            driver_features=cast("Any", {"pooler_mode": "invalid"}),
        )

    pooled_config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://pooler:6432/test"},
        driver_features={
            "pooler_mode": "transaction",
            "direct_dsn": "postgresql://direct:5432/test",
            "enable_alloydb_columnar": True,
        },
    )
    assert "options" not in pooled_config.connection_config

    direct_config = pooled_config.derive_direct_config()
    assert direct_config is not pooled_config
    assert direct_config.connection_config["dsn"] == "postgresql://direct:5432/test"
    assert direct_config.driver_features["pooler_mode"] == "session"
    assert direct_config.driver_features["direct_dsn"] is None
    assert direct_config.connection_config["options"] == "-c google_columnar_engine.enable_columnar_scan=on"

    conn = AsyncMock()
    conn.in_transaction = False
    driver = PsqlpyDriver(
        cast("PsqlpyConnection", conn),
        statement_config=pooled_config.statement_config,
        driver_features=pooled_config.driver_features,
    )
    await driver.begin()
    conn.execute.assert_any_await("BEGIN")
    conn.execute.assert_any_await("SET LOCAL google_columnar_engine.enable_columnar_scan = 'on'")


@pytest.mark.anyio
async def test_psqlpy_auth_token_provider_evaluated_on_create_pool_not_init() -> None:
    """Synchronous and async auth_token_provider is evaluated when creating a pool, not during __init__."""
    call_count = 0

    def token_provider() -> str:
        nonlocal call_count
        call_count += 1
        return "dynamic_token"

    config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://localhost:5432/test", "auth_token_provider": token_provider}
    )
    assert call_count == 0

    pool = await config.create_pool()
    assert call_count == 1
    pool.close()


@pytest.mark.anyio
async def test_psqlpy_sync_on_connection_create_callback() -> None:
    """Synchronous callback for on_connection_create should execute without awaiting."""
    hook_called = False

    def sync_hook(conn: Any) -> None:
        nonlocal hook_called
        hook_called = True

    config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://localhost:5432/test"},
        driver_features={"on_connection_create": sync_hook},
    )
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=None)
    await config._ensure_connection(mock_conn)  # pyright: ignore[reportPrivateUsage]
    assert hook_called is True


class _RecordingAsyncContext:
    def __init__(self) -> None:
        self.recorded_exit_args: tuple[object, object, object] | None = None

    async def __aenter__(self) -> object:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object | None
    ) -> bool | None:
        self.recorded_exit_args = (exc_type, exc_val, exc_tb)
        return None


@pytest.mark.anyio
async def test_psqlpy_connection_context_aexit_forwards_none_on_exception() -> None:
    """Forwarded exceptions must pass (None, None, None) to the inner driver context."""
    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost:5432/test"})
    context = PsqlpyConnectionContext(config)
    inner_ctx = _RecordingAsyncContext()
    context._ctx = inner_ctx

    exc = ValueError("forwarded failure")
    result = await context.__aexit__(ValueError, exc, None)

    assert result is None
    assert inner_ctx.recorded_exit_args == (None, None, None)
    assert context._ctx is None


@pytest.mark.anyio
async def test_psqlpy_session_factory_release_connection_forwards_none_on_exception() -> None:
    """Factory release must pass (None, None, None) to the inner driver context."""
    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost:5432/test"})
    factory = _PsqlpySessionFactory(config)
    inner_ctx = _RecordingAsyncContext()
    factory._ctx = inner_ctx

    conn = AsyncMock()
    exc = ValueError("forwarded failure")
    await factory.release_connection(conn, exc_type=ValueError, exc_val=exc, exc_tb=None)

    conn.execute.assert_awaited_once_with("ROLLBACK")
    assert inner_ctx.recorded_exit_args == (None, None, None)
    assert factory._ctx is None


@pytest.mark.anyio
async def test_psqlpy_connection_context_aexit_executes_rollback_on_exception() -> None:
    """PsqlpyConnectionContext.__aexit__ must execute ROLLBACK on exception and suppress rollback errors."""
    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost:5432/test"})
    context = PsqlpyConnectionContext(config)
    inner_ctx = _RecordingAsyncContext()
    conn = AsyncMock()
    conn.execute.side_effect = RuntimeError("connection lost")
    context._ctx = inner_ctx
    context._connection = conn

    exc = ValueError("raw begin aborted")
    result = await context.__aexit__(ValueError, exc, None)

    assert result is None
    conn.execute.assert_awaited_once_with("ROLLBACK")
    assert context._connection is None
    assert inner_ctx.recorded_exit_args == (None, None, None)
    assert context._ctx is None

    clean_context = PsqlpyConnectionContext(config)
    clean_inner_ctx = _RecordingAsyncContext()
    clean_conn = AsyncMock()
    clean_context._ctx = clean_inner_ctx
    clean_context._connection = clean_conn

    await clean_context.__aexit__(None, None, None)

    clean_conn.execute.assert_not_awaited()
    assert clean_context._connection is None
    assert clean_inner_ctx.recorded_exit_args == (None, None, None)


@pytest.mark.anyio
async def test_psqlpy_session_factory_release_connection_skips_rollback_on_clean_exit() -> None:
    """_PsqlpySessionFactory.release_connection skips ROLLBACK on clean exit and suppresses rollback errors."""
    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost:5432/test"})
    factory = _PsqlpySessionFactory(config)
    inner_ctx = _RecordingAsyncContext()
    factory._ctx = inner_ctx
    conn = AsyncMock()

    await factory.release_connection(conn, exc_type=None, exc_val=None, exc_tb=None)

    conn.execute.assert_not_awaited()
    assert inner_ctx.recorded_exit_args == (None, None, None)

    failing_conn = AsyncMock()
    failing_conn.execute.side_effect = RuntimeError("socket closed")
    err_ctx = _RecordingAsyncContext()
    factory._ctx = err_ctx

    await factory.release_connection(failing_conn, exc_type=RuntimeError, exc_val=RuntimeError("boom"), exc_tb=None)

    failing_conn.execute.assert_awaited_once_with("ROLLBACK")
    assert err_ctx.recorded_exit_args == (None, None, None)
    assert factory._ctx is None


@pytest.mark.parametrize("strategy", ["default", "rls", True])
def test_tenant_shorthand_defaults_to_session(strategy: Any) -> None:
    from sqlspec.adapters.psqlpy import PsqlpyRLSStrategy

    config = PsqlpyConfig(driver_features={"tenant_strategy": strategy})
    assert isinstance(config.driver_features["tenant_strategy"], PsqlpyRLSStrategy)
    assert config.driver_features["pooler_mode"] == "session"


@pytest.mark.parametrize("mode", ["transaction", "statement", "auto"])
def test_tenant_rejects_non_session_pooler(mode: str) -> None:
    with pytest.raises(ImproperConfigurationError, match="session"):
        PsqlpyConfig(driver_features={"tenant_strategy": "rls", "pooler_mode": mode})


@pytest.mark.parametrize("disabled", [None, False])
async def test_tenant_disabled_retains_native_provider(disabled: Any) -> None:
    from types import SimpleNamespace

    connection = AsyncMock()
    connection.fetch.return_value = SimpleNamespace(result=lambda: [])
    context = AsyncMock()
    context.__aenter__.return_value = connection
    pool = SimpleNamespace(acquire=lambda: context)
    config = PsqlpyConfig(
        connection_instance=cast("PsqlpyConnectionPool", pool),
        driver_features={"tenant_strategy": disabled, "pooler_mode": "transaction"},
    )
    assert config.driver_features["tenant_strategy"] is None
    assert config.driver_features["pooler_mode"] == "transaction"
    async with config.provide_connection() as acquired:
        assert acquired is connection
    connection.execute.assert_not_awaited()
    context.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("provider", ["provide_session", "provide_connection"])
async def test_tenant_public_provider_applies_captured_strategy(provider: str) -> None:
    from dataclasses import dataclass
    from types import SimpleNamespace

    from sqlspec.core.tenant import TenantLeaseState
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    events: list[str] = []

    @dataclass
    class Strategy:
        label: str

        async def checkout(self, _connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
            events.append(f"checkout:{payload.tenant_id}")
            return TenantLeaseState(payload)

        async def checkin(self, _connection: Any, state: TenantLeaseState) -> None:
            events.append(f"checkin:{state.payload.tenant_id}")

    connection = AsyncMock()
    connection.fetch.return_value = SimpleNamespace(result=lambda: [])
    connection.execute.side_effect = lambda _: events.append("rollback")
    context = AsyncMock()
    context.__aenter__.return_value = connection
    pool = SimpleNamespace(acquire=lambda: context)
    strategy = Strategy("custom")
    config = PsqlpyConfig(
        connection_instance=cast("PsqlpyConnectionPool", pool), driver_features={"tenant_strategy": strategy}
    )
    assert config.driver_features["tenant_strategy"] is strategy
    with TenantContext.scope("a"):
        async with getattr(config, provider)() as acquired:
            assert (acquired.connection if provider == "provide_session" else acquired) is connection
            assert events == ["checkout:a"]
    assert events == ["checkout:a", "rollback", "checkin:a"]
    context.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("provider", ["provide_session", "provide_connection"])
async def test_tenant_missing_scope_prevents_acquire(provider: str) -> None:
    from types import SimpleNamespace
    from unittest.mock import Mock

    from sqlspec.exceptions import TenantContextError

    pool = SimpleNamespace(acquire=Mock())
    config = PsqlpyConfig(
        connection_instance=cast("PsqlpyConnectionPool", pool), driver_features={"tenant_strategy": "rls"}
    )
    with pytest.raises(TenantContextError):
        async with getattr(config, provider)():
            pass
    pool.acquire.assert_not_called()


async def test_tenant_standalone_connection_rejected() -> None:
    config = PsqlpyConfig(driver_features={"tenant_strategy": "rls"})
    with pytest.raises(ImproperConfigurationError, match="lease"):
        await config.create_connection()


async def test_tenant_preparation_failure_releases_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from sqlspec.core.tenant import TenantLeaseState
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    strategy = AsyncMock()
    strategy.checkout.return_value = TenantLeaseState(TenantContextPayload("a"))
    connection = AsyncMock()
    connection.fetch.return_value = SimpleNamespace(result=lambda: [])
    context = AsyncMock()
    context.__aenter__.return_value = connection
    config = PsqlpyConfig(
        connection_instance=cast("PsqlpyConnectionPool", SimpleNamespace(acquire=lambda: context)),
        driver_features={"tenant_strategy": strategy},
    )
    error = RuntimeError("prepare failure")

    def fail_prepare(_driver: Any) -> Any:
        raise error

    monkeypatch.setattr(config, "_prepare_driver", fail_prepare)
    with TenantContext.scope("a"), pytest.raises(RuntimeError) as caught:
        async with config.provide_session():
            pass
    assert caught.value is error
    context.__aexit__.assert_awaited_once()
    strategy.checkin.assert_awaited_once()


@pytest.mark.parametrize("session", ("any", "read-write", "read-only", "primary", "standby", "prefer-standby"))
def test_replica_derivation_normalizes_native_session_enums(session: str) -> None:
    from psqlpy import LoadBalanceHosts, TargetSessionAttrs

    primary = PsqlpyConfig(connection_config={"dsn": "postgresql://writer/app"})
    replica = primary.derive_pool(
        hosts=["reader-a", "reader-b"], target_session_attrs=session, load_balance_hosts="random"
    )
    expected = (
        TargetSessionAttrs.Any
        if session == "any"
        else (TargetSessionAttrs.ReadWrite if session in {"read-write", "primary"} else TargetSessionAttrs.ReadOnly)
    )
    assert "dbname='app'" in replica.connection_config["dsn"]
    assert "host='reader-a,reader-b'" in replica.connection_config["dsn"]
    assert replica.connection_config["target_session_attrs"] == expected
    expected_attr = (
        "any" if session == "any" else ("read-write" if session in {"primary", "read-write"} else "read-only")
    )
    assert f"target_session_attrs='{expected_attr}'" in replica.connection_config["dsn"]
    assert replica.connection_config["load_balance_hosts"] == LoadBalanceHosts.Random
    assert primary.connection_config["dsn"] == "postgresql://writer/app"


@pytest.mark.parametrize("field", ("target_session_attrs", "load_balance_hosts"))
@pytest.mark.parametrize("value", ("invalid", True, 1))
def test_invalid_replica_enum_options_fail_configuration(field: str, value: Any) -> None:
    with pytest.raises(ImproperConfigurationError):
        PsqlpyConfig(connection_config={field: value})


def test_replica_alias_overrides_replace_primary_values() -> None:
    primary = PsqlpyConfig(connection_config={"dsn": "postgresql://writer/app", "db_name": "app", "username": "writer"})
    replica = primary.derive_pool(host="reader", database="reader_app", user="reader")
    assert "dbname='reader_app'" in replica.connection_config["dsn"]
    assert "host='reader'" in replica.connection_config["dsn"]
    assert replica.connection_config["db_name"] == "reader_app"
    assert replica.connection_config["username"] == "reader"


def test_native_replica_enums_and_uppercase_strings_are_supported() -> None:
    from psqlpy import LoadBalanceHosts, TargetSessionAttrs

    native = PsqlpyConfig(
        connection_config={
            "target_session_attrs": TargetSessionAttrs.ReadOnly,
            "load_balance_hosts": LoadBalanceHosts.Disable,
        }
    )
    uppercase = PsqlpyConfig(connection_config={"target_session_attrs": "READ-ONLY", "load_balance_hosts": "DISABLE"})
    assert native.connection_config["target_session_attrs"] == uppercase.connection_config["target_session_attrs"]
    assert native.connection_config["load_balance_hosts"] == uppercase.connection_config["load_balance_hosts"]


def test_replica_session_attributes_preserve_inherited_endpoint() -> None:
    from psqlpy import LoadBalanceHosts, TargetSessionAttrs

    primary = PsqlpyConfig(connection_config={"dsn": "postgresql://writer/app"})
    reader = primary.derive_pool(
        connection_config_overrides={
            "options": "-c default_transaction_read_only=on",
            "target_session_attrs": "read-only",
            "load_balance_hosts": "random",
        }
    )
    assert "host='writer'" in reader.connection_config["dsn"]
    assert "dbname='app'" in reader.connection_config["dsn"]
    assert "target_session_attrs='read-only'" in reader.connection_config["dsn"]
    assert reader.connection_config["target_session_attrs"] == TargetSessionAttrs.ReadOnly
    assert reader.connection_config["load_balance_hosts"] == LoadBalanceHosts.Random
    assert reader.connection_config["options"] == "-c default_transaction_read_only=on"


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://app:sec%27ret@writer/orders?sslmode=require&application_name=reports",
        "postgresql://app:sec%27ret@[::1]:5433,writer:5434/orders?sslmode=require&application_name=reports",
        "host=writer user=app password='sec\\'ret' dbname=orders sslmode=require application_name=reports",
    ],
)
def test_derived_host_preserves_dsn_credentials_database_and_tls(dsn: str) -> None:
    primary = PsqlpyConfig(connection_config={"dsn": dsn})
    reader = primary.derive_pool(host="reader", port=6432)
    merged = reader.connection_config["dsn"]
    assert "host='reader'" in merged
    assert "port='6432'" in merged
    assert "user='app'" in merged
    assert "password='sec\\'ret'" in merged
    assert "dbname='orders'" in merged
    assert "sslmode='require'" in merged
    assert "application_name='reports'" in merged
    assert primary.connection_config["dsn"] == dsn
    alternate = primary.derive_pool(host="reader", user="reporter", password="new-secret", database="reports")
    assert "user='reporter'" in alternate.connection_config["dsn"]
    assert "password='new-secret'" in alternate.connection_config["dsn"]
    assert "dbname='reports'" in alternate.connection_config["dsn"]


@pytest.mark.parametrize("dsn", ["host=writer invalid", "invalid host=writer", "postgresql://writer:invalid/orders"])
def test_invalid_dsn_host_derivation_fails_before_pool_creation(dsn: str) -> None:
    primary = PsqlpyConfig(connection_config={"dsn": dsn})
    with pytest.raises(ImproperConfigurationError, match="Invalid PostgreSQL"):
        primary.derive_pool(host="reader")


def test_derived_dsn_preserves_literal_plus_in_security_paths() -> None:
    primary = PsqlpyConfig(connection_config={"dsn": "postgresql://app@writer/orders?sslrootcert=/keys/ca+extra.pem"})
    reader = primary.derive_pool(host="reader")
    assert "sslrootcert='/keys/ca+extra.pem'" in reader.connection_config["dsn"]
