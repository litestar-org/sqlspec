"""AsyncPG configuration tests covering statement config builders."""

import inspect
from typing import Any, NotRequired, cast, get_args, get_origin
from unittest.mock import AsyncMock

import asyncpg
import asyncpg.connect_utils
import pytest
from asyncpg.pool import PoolConnectionProxy, PoolConnectionProxyMeta

import sqlspec.adapters.asyncpg.config as config_mod
from sqlspec.adapters.asyncpg._typing import AsyncpgSessionContext
from sqlspec.adapters.asyncpg.config import (
    AsyncpgConfig,
    AsyncpgConnectionConfig,
    AsyncpgDriverFeatures,
    AsyncpgPoolConfig,
)
from sqlspec.adapters.asyncpg.core import build_connection_config, build_statement_config
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.core import StatementConfig, StatementStack
from sqlspec.core.config_runtime import build_postgres_extension_probe_names, resolve_postgres_extension_state
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.exceptions import ImproperConfigurationError


def test_build_default_statement_config_custom_serializers() -> None:
    """Custom serializers should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    def deserializer(_: str) -> object:
        return {"value": "deserialized"}

    statement_config = build_statement_config(json_serializer=serializer, json_deserializer=deserializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_asyncpg_config_applies_driver_feature_serializers() -> None:
    """Driver features should mutate the AsyncPG statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    def deserializer(_: str) -> object:
        return {"feature": True}

    config = AsyncpgConfig(driver_features={"json_serializer": serializer, "json_deserializer": deserializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_asyncpg_config_connection_type_is_not_metaclass() -> None:
    connection_type = cast("object", AsyncpgConfig.connection_type)
    assert connection_type is PoolConnectionProxy
    assert connection_type is not cast("object", PoolConnectionProxyMeta)


def test_asyncpg_connection_config_types_current_service_and_gss_options() -> None:
    """Typed connection config should include current asyncpg connection options."""
    expected_options = {"service", "servicefile", "timeout", "target_session_attrs", "krbsrvname", "gsslib"}

    assert expected_options <= set(AsyncpgConnectionConfig.__annotations__)

    target_session_attrs_annotation = cast("Any", AsyncpgConnectionConfig.__annotations__["target_session_attrs"])
    assert get_origin(target_session_attrs_annotation) is NotRequired
    assert set(get_args(get_args(target_session_attrs_annotation)[0])) == {
        "any",
        "primary",
        "standby",
        "read-write",
        "read-only",
        "prefer-standby",
    }

    gsslib_annotation = cast("Any", AsyncpgConnectionConfig.__annotations__["gsslib"])
    assert get_origin(gsslib_annotation) is NotRequired
    assert set(get_args(get_args(gsslib_annotation)[0])) == {"gssapi", "sspi"}


def test_asyncpg_pool_config_types_current_pool_callbacks() -> None:
    """Typed pool config should include current asyncpg pool callback options."""
    assert {"connect", "setup", "init", "reset"} <= set(AsyncpgPoolConfig.__annotations__)


async def test_asyncpg_create_pool_routes_current_connect_and_pool_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Current connect kwargs should reach create_pool while reset remains pool-scoped."""
    import sqlspec.adapters.asyncpg.config as config_mod

    captured_connect_kwargs: dict[str, Any] = {}
    captured_pool_kwargs: dict[str, Any] = {}

    async def reset_connection(_: object) -> None:
        return None

    async def connect_factory(**_: Any) -> object:
        return object()

    async def fake_create_pool(*, connect: Any = None, init: Any = None, reset: Any = None, **kwargs: Any) -> object:
        captured_pool_kwargs.update({"connect": connect, "init": init, "reset": reset})
        captured_connect_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(config_mod, "asyncpg_create_pool", fake_create_pool)

    config = AsyncpgConfig(
        connection_config={
            "connect": connect_factory,
            "dsn": "postgresql://localhost/test",
            "gsslib": "gssapi",
            "krbsrvname": "postgres",
            "min_size": 1,
            "reset": reset_connection,
            "service": "analytics",
            "servicefile": "/tmp/pg_service.conf",
            "target_session_attrs": "read-only",
            "timeout": 4.5,
        }
    )

    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

    assert captured_pool_kwargs["connect"] is connect_factory
    assert captured_pool_kwargs["init"].__func__ is config._init_connection.__func__  # pyright: ignore[reportPrivateUsage]
    assert captured_pool_kwargs["init"].__self__ is config
    assert captured_pool_kwargs["reset"] is reset_connection
    assert captured_connect_kwargs["dsn"] == "postgresql://localhost/test"
    assert captured_connect_kwargs["gsslib"] == "gssapi"
    assert captured_connect_kwargs["krbsrvname"] == "postgres"
    assert captured_connect_kwargs["service"] == "analytics"
    assert captured_connect_kwargs["servicefile"] == "/tmp/pg_service.conf"
    assert captured_connect_kwargs["target_session_attrs"] == "read-only"
    assert captured_connect_kwargs["timeout"] == 4.5
    assert "reset" not in captured_connect_kwargs


async def test_asyncpg_minimal_create_pool_omits_tuning_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal pool config should not forward optional cache tuning keys."""
    import sqlspec.adapters.asyncpg.config as config_mod

    captured_connect_kwargs: dict[str, Any] = {}
    captured_pool_kwargs: dict[str, Any] = {}

    async def fake_create_pool(*, init: Any = None, **kwargs: Any) -> object:
        captured_pool_kwargs.update({"init": init})
        captured_connect_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(config_mod, "asyncpg_create_pool", fake_create_pool)

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

    assert captured_pool_kwargs["init"].__func__ is config._init_connection.__func__  # pyright: ignore[reportPrivateUsage]
    assert captured_connect_kwargs == {"dsn": "postgresql://localhost/test"}


def test_asyncpg_build_postgres_extension_probe_names_filters_disabled_features() -> None:
    """Only enabled extension probes should be returned."""
    assert build_postgres_extension_probe_names({"enable_pgvector": True, "enable_paradedb": False}) == ["vector"]


def test_asyncpg_resolve_postgres_extension_state_promotes_paradedb() -> None:
    """Detected extensions should promote the runtime dialect."""
    statement_config, pgvector_available, paradedb_available = resolve_postgres_extension_state(
        StatementConfig(dialect="postgres"), {"enable_pgvector": True, "enable_paradedb": True}, {"vector", "pg_search"}
    )

    assert statement_config.dialect == "paradedb"
    assert pgvector_available is True
    assert paradedb_available is True


async def test_asyncpg_bm25_extension_probe_is_required_and_cached() -> None:
    """BM25 probes pg_textsearch once and accepts only that extension."""
    connection = AsyncMock()
    connection.fetch.return_value = [{"extname": "pg_search"}]
    config = AsyncpgConfig(
        driver_features={"enable_json_codecs": False, "enable_pgvector": False, "enable_paradedb": True},
        extension_config={"adk": {"enable_memory": True, "enable_bm25": True}},
    )

    await config._init_connection(connection)  # pyright: ignore[reportPrivateUsage]
    await config._init_connection(connection)  # pyright: ignore[reportPrivateUsage]

    connection.fetch.assert_awaited_once()
    assert connection.fetch.await_args.args[1] == ["pg_search", "pg_textsearch"]
    with pytest.raises(ImproperConfigurationError, match="requires the pg_textsearch"):
        config._ensure_pg_textsearch_available()  # pyright: ignore[reportPrivateUsage]


async def test_asyncpg_bm25_probe_failure_is_preserved_as_cause() -> None:
    """A failed capability query remains distinguishable from an absent extension."""
    probe_error = RuntimeError("probe failed")
    connection = AsyncMock()
    connection.fetch.side_effect = probe_error
    config = AsyncpgConfig(
        driver_features={"enable_json_codecs": False, "enable_pgvector": False, "enable_paradedb": False},
        extension_config={"adk": {"enable_bm25": True}},
    )

    await config._init_connection(connection)  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(ImproperConfigurationError) as exc_info:
        config._ensure_pg_textsearch_available()  # pyright: ignore[reportPrivateUsage]
    assert exc_info.value.__cause__ is probe_error


async def test_asyncpg_does_not_probe_pg_textsearch_when_disabled() -> None:
    """Extension probe is omitted when all extension flags are disabled."""
    connection = AsyncMock()
    config = AsyncpgConfig(
        driver_features={
            "enable_json_codecs": False,
            "enable_pgvector": False,
            "enable_paradedb": False,
            "enable_pg_textsearch": False,
        }
    )

    await config._init_connection(connection)  # pyright: ignore[reportPrivateUsage]

    connection.fetch.assert_not_awaited()


async def test_asyncpg_pg_textsearch_available_property() -> None:
    """pg_textsearch_available reflects detected extension state."""
    connection = AsyncMock()
    connection.fetch.return_value = [{"extname": "pg_textsearch"}]
    config = AsyncpgConfig(
        driver_features={"enable_json_codecs": False, "enable_pgvector": False, "enable_paradedb": False}
    )

    assert config.pg_textsearch_available is False
    await config._init_connection(connection)  # pyright: ignore[reportPrivateUsage]
    assert config.pg_textsearch_available is True
    assert config.statement_config.dialect == "pg_textsearch"


@pytest.mark.anyio
async def test_asyncpg_session_context_resolves_callable_statement_config() -> None:
    """Session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = AsyncpgSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_asyncpg_session_context_preserves_explicit_statement_config() -> None:
    """Explicit StatementConfig should be used directly without calling."""
    explicit_config = StatementConfig(dialect="postgres")
    context = AsyncpgSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=explicit_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config is explicit_config


def test_asyncpg_provide_session_tracks_promoted_statement_config() -> None:
    """Runtime statement config should resolve the current config dialect lazily."""
    config = AsyncpgConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"


def test_build_connection_config_resolves_aliases() -> None:
    """build_connection_config should map conninfo/url to dsn, dbname/db to database, and username to user."""
    cfg = build_connection_config({
        "url": "postgresql://usr:pwd@host.internal:5432/main",
        "dbname": "override_db",
        "username": "override_usr",
    })
    assert cfg["dsn"] == "postgresql://usr:pwd@host.internal:5432/main"
    assert cfg["database"] == "override_db"
    assert cfg["user"] == "override_usr"
    assert "url" not in cfg
    assert "dbname" not in cfg
    assert "username" not in cfg


def test_asyncpg_config_normalizes_aliases() -> None:
    """AsyncpgConfig should normalize aliases in connection_config."""
    config = AsyncpgConfig(
        connection_config={
            "conninfo": "postgresql://localhost:5432/test",
            "dbname": "target_db",
            "username": "target_user",
        }
    )
    assert config.connection_config["dsn"] == "postgresql://localhost:5432/test"
    assert config.connection_config["database"] == "target_db"
    assert config.connection_config["user"] == "target_user"
    assert "conninfo" not in config.connection_config
    assert "dbname" not in config.connection_config
    assert "username" not in config.connection_config


def test_asyncpg_driver_features_contract_retains_configured_keys() -> None:
    """AsyncpgConfig retains on_connection_create and type_codecs and populates event defaults."""
    assert "connection_instance" not in AsyncpgDriverFeatures.__annotations__
    assert "enable_pgcopy_binary" in AsyncpgDriverFeatures.__annotations__
    assert AsyncpgConfig.supports_native_arrow_export is False
    assert AsyncpgConfig.supports_native_parquet_export is False
    assert AsyncpgConfig.supports_native_parquet_import is False

    async def hook(_: object) -> None:
        return None

    codecs = [{"typename": "citext", "encoder": str, "decoder": str}]
    config = AsyncpgConfig(driver_features={"on_connection_create": hook, "type_codecs": codecs})

    assert config.driver_features["on_connection_create"] is hook
    assert config.driver_features["type_codecs"] == codecs
    assert config.driver_features["enable_events"] is True
    assert config.driver_features["events_backend"] == "notify"


async def test_asyncpg_user_init_callback_chains_after_internal_init(monkeypatch: pytest.MonkeyPatch) -> None:
    """Custom connection_config['init'] should run after _init_connection in both _create_pool and create_connection."""
    order: list[str] = []

    async def user_init(_: object) -> None:
        order.append("user_init")

    captured_init: dict[str, Any] = {}

    async def fake_create_pool(*, init: Any = None, **_: Any) -> object:
        captured_init["init"] = init
        return object()

    monkeypatch.setattr(config_mod, "asyncpg_create_pool", fake_create_pool)

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test", "init": user_init},
        driver_features={
            "enable_json_codecs": False,
            "enable_pgvector": False,
            "enable_paradedb": False,
            "enable_pg_textsearch": False,
            "on_connection_create": AsyncMock(side_effect=lambda _: order.append("hook")),
        },
    )

    await config._create_pool()  # pyright: ignore[reportPrivateUsage]
    await captured_init["init"](AsyncMock())
    assert order == ["hook", "user_init"]

    order.clear()
    monkeypatch.setattr(config_mod, "asyncpg_connect", AsyncMock(return_value=AsyncMock()))
    await config.create_connection()
    assert order == ["hook", "user_init"]


async def test_asyncpg_extension_probe_retries_after_transient_error() -> None:
    """A failed extension probe should not permanently poison _pgvector_available."""
    failing_conn = AsyncMock()
    failing_conn.fetch.side_effect = RuntimeError("transient connection error")
    healthy_conn = AsyncMock()
    healthy_conn.fetch.return_value = [{"extname": "pg_textsearch"}]

    config = AsyncpgConfig(
        driver_features={"enable_json_codecs": False, "enable_pgvector": False, "enable_paradedb": False}
    )

    await config._init_connection(failing_conn)  # pyright: ignore[reportPrivateUsage]
    assert config._pgvector_available is None  # pyright: ignore[reportPrivateUsage]
    assert config.pg_textsearch_available is False

    await config._init_connection(healthy_conn)  # pyright: ignore[reportPrivateUsage]
    assert config._pgvector_available is False  # pyright: ignore[reportPrivateUsage]
    assert config.pg_textsearch_available is True
    assert config.statement_config.dialect == "pg_textsearch"


async def test_asyncpg_pooler_mode_transaction_sets_statement_cache_size_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transaction pooler mode and pgbouncer aliases should disable statement_cache_size and prepared stack metadata."""
    captured_connect_kwargs: dict[str, Any] = {}

    async def fake_create_pool(*, init: Any = None, **kwargs: Any) -> object:
        _ = init
        captured_connect_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(config_mod, "asyncpg_create_pool", fake_create_pool)

    assert "pgbouncer" not in AsyncpgDriverFeatures.__annotations__
    assert "pgbouncer_mode" not in AsyncpgDriverFeatures.__annotations__
    assert "pgbouncer" not in AsyncpgConnectionConfig.__annotations__
    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"}, driver_features={"pooler_mode": "transaction"}
    )
    assert config.connection_config["statement_cache_size"] == 0
    await config._create_pool()  # pyright: ignore[reportPrivateUsage]
    assert captured_connect_kwargs["statement_cache_size"] == 0

    conn = AsyncMock()
    conn.is_in_transaction = lambda: True
    conn.execute.return_value = "UPDATE 1"
    driver = AsyncpgDriver(
        cast("Any", conn), statement_config=config.statement_config, driver_features=config.driver_features
    )

    stack = StatementStack().push_execute("UPDATE items SET name = :name", {"name": "x"})
    results = await driver.execute_stack(stack)
    assert results[0].result is not None
    assert "prepared_statement" not in results[0].result.metadata


def test_asyncpg_derive_direct_config_uses_direct_dsn_and_session_mode() -> None:
    """derive_direct_config should return a session-mode config targeting direct_dsn or self when unset."""
    base_config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    assert base_config.derive_direct_config() is base_config

    pooled_config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://pooler:6432/test"},
        driver_features={
            "pooler_mode": "transaction",
            "direct_dsn": "postgresql://direct:5432/test",
            "enable_alloydb_columnar": True,
        },
    )
    direct_config = pooled_config.derive_direct_config()
    assert direct_config is not pooled_config
    assert direct_config.connection_config["dsn"] == "postgresql://direct:5432/test"
    assert "statement_cache_size" not in direct_config.connection_config
    assert direct_config.driver_features["pooler_mode"] == "session"
    assert direct_config.driver_features["direct_dsn"] is None
    assert direct_config.connection_config["server_settings"] == {"google_columnar_engine.enable_columnar_scan": "on"}

    mapping_config = AsyncpgConfig(
        connection_config={"host": "pooler.internal", "port": 6432, "database": "app"},
        driver_features={"pooler_mode": "transaction", "direct_dsn": {"host": "direct.internal", "port": 5432}},
    )
    derived_mapping = mapping_config.derive_direct_config()
    assert derived_mapping.connection_config["host"] == "direct.internal"
    assert derived_mapping.connection_config["port"] == 5432
    assert derived_mapping.connection_config["database"] == "app"
    assert derived_mapping.driver_features["pooler_mode"] == "session"


async def test_asyncpg_columnar_engine_session_vs_transaction() -> None:
    """enable_alloydb_columnar should use server_settings in session mode and SET LOCAL in transaction mode."""
    session_config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test", "server_settings": {"application_name": "sqlspec"}},
        driver_features={"pooler_mode": "session", "enable_alloydb_columnar": True},
    )
    assert session_config.connection_config["server_settings"] == {
        "application_name": "sqlspec",
        "google_columnar_engine.enable_columnar_scan": "on",
    }

    auto_direct_config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        driver_features={"pooler_mode": "auto", "enable_alloydb_columnar": True},
    )
    assert auto_direct_config.connection_config["server_settings"] == {
        "google_columnar_engine.enable_columnar_scan": "on"
    }

    tx_columnar_config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        driver_features={"pooler_mode": "transaction", "enable_alloydb_columnar": True},
    )
    assert tx_columnar_config.driver_features["pooler_mode"] == "transaction"
    assert "server_settings" not in tx_columnar_config.connection_config

    tx_config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        driver_features={"pooler_mode": "transaction", "enable_alloydb_columnar": True},
    )
    assert "server_settings" not in tx_config.connection_config

    tx_handle = AsyncMock()
    conn = AsyncMock()
    conn.is_in_transaction = lambda: False
    conn.transaction = lambda: tx_handle
    driver = AsyncpgDriver(
        cast("Any", conn), statement_config=tx_config.statement_config, driver_features=tx_config.driver_features
    )
    await driver.begin()
    tx_handle.start.assert_awaited_once()
    conn.execute.assert_awaited_once_with("SET LOCAL google_columnar_engine.enable_columnar_scan = 'on'")


@pytest.mark.parametrize("strategy", ["default", "rls", True])
def test_tenant_shorthand_defaults_to_session_pooler(strategy: Any) -> None:
    from sqlspec.adapters.asyncpg import AsyncpgRLSStrategy

    config = AsyncpgConfig(driver_features={"tenant_strategy": strategy})
    assert isinstance(config.driver_features["tenant_strategy"], AsyncpgRLSStrategy)
    assert config.driver_features["pooler_mode"] == "session"


@pytest.mark.parametrize("mode", ["transaction", "statement", "auto"])
def test_tenant_rejects_non_session_poolers(mode: str) -> None:
    with pytest.raises(ImproperConfigurationError, match="session"):
        AsyncpgConfig(driver_features={"tenant_strategy": "rls", "pooler_mode": mode})


@pytest.mark.parametrize("disabled", [False, None])
def test_tenant_disabled_preserves_native_configuration(disabled: Any) -> None:
    config = AsyncpgConfig(driver_features={"tenant_strategy": disabled, "pooler_mode": "transaction"})
    assert config.driver_features["tenant_strategy"] is None
    assert config.driver_features["pooler_mode"] == "transaction"


async def test_tenant_standalone_connections_are_rejected() -> None:
    config = AsyncpgConfig(driver_features={"tenant_strategy": "rls"})
    with pytest.raises(ImproperConfigurationError, match="lease"):
        await config.create_connection()


@pytest.mark.parametrize("provider", ["provide_connection", "provide_session"])
async def test_tenant_public_providers_apply_and_restore(provider: str) -> None:
    from dataclasses import dataclass

    from sqlspec.core.tenant import TenantLeaseState
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    calls: list[str] = []

    @dataclass
    class RecordingStrategy:
        label: str

        async def checkout(self, _connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
            calls.append(f"checkout:{payload.tenant_id}")
            return TenantLeaseState(payload)

        async def checkin(self, _connection: Any, state: TenantLeaseState) -> None:
            calls.append(f"checkin:{state.payload.tenant_id}")

    connection = AsyncMock()
    connection.is_in_transaction = lambda: False
    pool = AsyncMock()
    pool.acquire.return_value = connection
    strategy = RecordingStrategy("custom")
    config = AsyncpgConfig(connection_instance=pool, driver_features={"tenant_strategy": strategy})
    assert config.driver_features["tenant_strategy"] is strategy
    with TenantContext.scope("a"):
        async with getattr(config, provider)() as value:
            assert calls == ["checkout:a"]
            assert (value if provider == "provide_connection" else value.connection) is connection
    assert calls == ["checkout:a", "checkin:a"]
    pool.release.assert_awaited_once()
    assert pool.release.call_args.args == (connection,)
    assert pool.release.call_args.kwargs["timeout"] > 0


@pytest.mark.parametrize("provider", ["provide_connection", "provide_session"])
async def test_tenant_missing_scope_prevents_pool_access(provider: str) -> None:
    from sqlspec.exceptions import TenantContextError

    pool = AsyncMock()
    config = AsyncpgConfig(connection_instance=pool, driver_features={"tenant_strategy": "rls"})
    with pytest.raises(TenantContextError):
        async with getattr(config, provider)():
            pass
    pool.acquire.assert_not_awaited()


async def test_tenant_prepare_failure_releases_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.core.tenant import TenantLeaseState
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    payload = TenantContextPayload("a")
    strategy = AsyncMock()
    strategy.checkout.return_value = TenantLeaseState(payload)
    connection = AsyncMock()
    connection.is_in_transaction = lambda: False
    pool = AsyncMock()
    pool.acquire.return_value = connection
    config = AsyncpgConfig(connection_instance=pool, driver_features={"tenant_strategy": strategy})
    error = RuntimeError("prepare failed")

    def fail_prepare(_driver: Any) -> Any:
        raise error

    monkeypatch.setattr(config, "_prepare_driver", fail_prepare)
    with TenantContext.scope(payload), pytest.raises(RuntimeError) as caught:
        async with config.provide_session():
            pass
    assert caught.value is error
    strategy.checkin.assert_awaited_once()
    pool.release.assert_awaited_once()
    assert pool.release.call_args.args == (connection,)
    assert pool.release.call_args.kwargs["timeout"] > 0


async def test_tenant_failed_restore_terminates_before_release() -> None:
    from unittest.mock import Mock

    from sqlspec.core.tenant import TenantLeaseState
    from sqlspec.exceptions import TenantCleanupError
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    payload = TenantContextPayload("a")
    strategy = AsyncMock()
    strategy.checkout.return_value = TenantLeaseState(payload)
    strategy.checkin.side_effect = RuntimeError("failed readback")
    events: list[str] = []
    connection = AsyncMock()
    connection.is_in_transaction = lambda: False
    connection.terminate = Mock(side_effect=lambda: events.append("terminate"))
    pool = AsyncMock()
    pool.acquire.return_value = connection
    pool.release.side_effect = lambda _, **_kwargs: events.append("release")
    config = AsyncpgConfig(connection_instance=pool, driver_features={"tenant_strategy": strategy})
    with TenantContext.scope(payload), pytest.raises(TenantCleanupError):
        async with config.provide_connection():
            pass
    assert events == ["terminate", "release"]


@pytest.mark.parametrize("explicit_cache", (False, True))
def test_replica_derivation_preserves_explicit_cache_and_primary_dsn(explicit_cache: bool) -> None:
    connection: dict[str, Any] = {"dsn": "postgresql://app:secret@writer/orders?sslmode=require"}
    if explicit_cache:
        connection["statement_cache_size"] = 0
    primary = AsyncpgConfig(connection_config=connection, driver_features={"pooler_mode": "transaction"})
    replica = primary.derive_pool(host=["reader-a", "reader-b"], port=[5432, 5433], pooler_mode="session")
    assert replica.connection_config["dsn"] == "postgresql://app:secret@writer/orders?sslmode=require"
    assert replica.connection_config["host"] == ["reader-a", "reader-b"]
    assert replica.connection_config.get("statement_cache_size") == (0 if explicit_cache else None)
    assert primary.connection_config["dsn"] == "postgresql://app:secret@writer/orders?sslmode=require"


@pytest.mark.parametrize("alias", ("url", "conninfo"))
def test_replica_uri_override_replaces_inherited_dsn(alias: str) -> None:
    primary = AsyncpgConfig(connection_config={"dsn": "postgresql://writer/app"})
    overrides: dict[str, Any] = {alias: "postgresql://reader/app"}
    replica = primary.derive_pool(**overrides)
    assert replica.connection_config["dsn"] == "postgresql://reader/app"


def test_read_only_transaction_maps_native_error_with_cause() -> None:
    from asyncpg.exceptions import ReadOnlySQLTransactionError

    from sqlspec.adapters.asyncpg.core import create_mapped_exception
    from sqlspec.exceptions import ReadOnlyTransactionError

    error = ReadOnlySQLTransactionError("cannot write")

    mapped = create_mapped_exception(error)
    assert isinstance(mapped, ReadOnlyTransactionError)
    assert mapped.__cause__ is error
    assert "read-only transaction" in str(mapped).lower()


async def test_wal_helpers_bind_locations_validate_and_map_native_errors() -> None:
    from asyncpg.exceptions import InvalidTextRepresentationError

    from sqlspec.exceptions import DataError, SQLSpecError

    connection = AsyncMock()
    config = AsyncpgConfig()
    driver = AsyncpgDriver(connection, statement_config=config.statement_config)
    connection.fetchval.return_value = "0/16B3748"
    assert await driver.current_wal_lsn() == "0/16B3748"
    connection.fetchval.return_value = False
    assert await driver.has_replayed_wal_lsn("0/16B3748") is False
    assert connection.fetchval.call_args.args[1] == "0/16B3748"
    with pytest.raises(ImproperConfigurationError):
        await driver.has_replayed_wal_lsn(" ")
    connection.fetchval.return_value = None
    with pytest.raises(SQLSpecError):
        await driver.current_wal_lsn()
    connection.fetchval.side_effect = InvalidTextRepresentationError("invalid WAL location")
    with pytest.raises(DataError):
        await driver.has_replayed_wal_lsn("bad-lsn")


def test_replica_derivation_distinguishes_explicit_and_injected_server_settings() -> None:
    primary = AsyncpgConfig(driver_features={"enable_alloydb_columnar": True})
    replica = primary.derive_pool(pooler_mode="transaction")
    assert replica.connection_config.get("server_settings") is None
    explicit = AsyncpgConfig(
        connection_config={"server_settings": {"application_name": "explicit"}},
        driver_features={"enable_alloydb_columnar": True},
    )
    replica = explicit.derive_pool(pooler_mode="transaction")
    assert replica.connection_config["server_settings"] == {"application_name": "explicit"}


def test_derived_host_allows_explicit_connection_identity_overrides() -> None:
    primary = AsyncpgConfig(connection_config={"dsn": "postgresql://app:secret@writer/orders?sslmode=require"})
    reader = primary.derive_pool(host="reader", user="reporter", password="reader-secret", database="reports")
    assert reader.connection_config["dsn"] == "postgresql://app:secret@writer/orders?sslmode=require"
    assert reader.connection_config["user"] == "reporter"
    assert reader.connection_config["password"] == "reader-secret"
    assert reader.connection_config["database"] == "reports"


async def test_provide_session_detaches_driver_on_exit() -> None:
    connection = AsyncMock()
    connection.is_in_transaction = lambda: False
    pool = AsyncMock()
    pool.acquire.return_value = connection
    config = AsyncpgConfig(connection_instance=pool)
    async with config.provide_session() as session:
        assert session.connection is connection
    assert session.connection is _DETACHED_CONNECTION


async def test_session_context_aenter_failure_releases_and_detaches_driver() -> None:
    connection = AsyncMock()
    captured: list[Any] = []
    release_events: list[tuple[bool, bool, Any]] = []

    async def acquire_connection() -> Any:
        return connection

    async def release_connection(conn: Any, **kwargs: Any) -> None:
        release_events.append((conn is connection, captured[0].connection is connection, kwargs.get("exc_type")))

    def fail_prepare(driver: Any) -> Any:
        captured.append(driver)
        raise RuntimeError("prepare boom")

    ctx = AsyncpgSessionContext(
        acquire_connection=acquire_connection,
        release_connection=release_connection,
        statement_config=StatementConfig(dialect="postgres"),
        driver_features={},
        prepare_driver=fail_prepare,
    )
    with pytest.raises(RuntimeError, match="prepare boom"):
        async with ctx:
            pytest.fail("prepare failure must abort __aenter__")

    assert release_events == [(True, True, RuntimeError)]
    assert captured[0].connection is _DETACHED_CONNECTION


@pytest.mark.parametrize("failure_stage", ["pool", "acquire"])
async def test_asyncpg_read_replica_failover_maps_driver_errors(
    failure_stage: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Asyncpg pool creation and acquire errors map to DatabaseConnectionError so replicas fail over."""
    good_conn = object()
    good_pool = AsyncMock()
    good_pool.acquire = AsyncMock(return_value=good_conn)
    good_pool.release = AsyncMock()
    bad_pool = AsyncMock()
    bad_pool.acquire = AsyncMock(side_effect=asyncpg.InterfaceError("closed"))

    async def fake_create_pool(**kwargs: Any) -> Any:
        if "bad" in str(kwargs.get("dsn", "")):
            if failure_stage == "pool":
                raise asyncpg.CannotConnectNowError("replica starting")
            return bad_pool
        return good_pool

    monkeypatch.setattr("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", fake_create_pool)
    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://primary"}, replicas=["postgresql://bad", "postgresql://good"]
    )
    async with config.provide_read_connection() as conn:
        assert conn is good_conn
    async with config.provide_read_session() as reader:
        assert reader.connection is good_conn


def test_asyncpg_replica_dsn_and_direct_dsn_clear_inherited_endpoints() -> None:
    """String replicas, dsn overrides, and direct_dsn clear inherited field-based primary options."""
    primary = AsyncpgConfig(
        connection_config={"host": "primary", "port": 5432, "database": "rw", "user": "u", "password": "pw"},
        driver_features={"pooler_mode": "transaction", "direct_dsn": "postgresql://du:dp@direct:5439/direct_db"},
        replicas=["postgresql://ro:ropw@replica:6432/ro_db"],
    )
    derived = primary.derive_pool(dsn="postgresql://ro:ropw@replica:6432/ro_db")
    replica = primary.replica_groups["default"].configs[0]
    direct = primary.derive_direct_config()

    parse_args = asyncpg.connect_utils._parse_connect_arguments  # type: ignore[attr-defined]
    parse_defaults: dict[str, Any] = dict.fromkeys(inspect.signature(parse_args).parameters)
    parse_defaults.update({
        "statement_cache_size": 100,
        "max_cached_statement_lifetime": 300,
        "max_cacheable_statement_size": 307200,
    })
    for cfg in (derived, replica):
        assert cfg.connection_config["dsn"] == "postgresql://ro:ropw@replica:6432/ro_db"
        for stale_key in ("host", "port", "database", "user", "password"):
            assert stale_key not in cfg.connection_config
        addrs, params, _ = parse_args(**{**parse_defaults, **build_connection_config(cfg.connection_config)})
        assert addrs == [("replica", 6432)]
        assert params.database == "ro_db"
        assert params.user == "ro"

    assert direct.connection_config["dsn"] == "postgresql://du:dp@direct:5439/direct_db"
    addrs, params, _ = parse_args(**{**parse_defaults, **build_connection_config(direct.connection_config)})
    assert addrs == [("direct", 5439)]
    assert params.database == "direct_db"
    assert params.user == "du"
