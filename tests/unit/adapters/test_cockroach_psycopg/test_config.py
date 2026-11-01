# pyright: reportAttributeAccessIssue=false
"""Unit tests for CockroachDB psycopg configuration.

Tests cover:
- CockroachPsycopgSyncConfig initialization and defaults
- CockroachPsycopgAsyncConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncSessionContext,
    CockroachPsycopgDriverFeatures,
    CockroachPsycopgRetryConfig,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
    CockroachPsycopgSyncSessionContext,
    build_connection_config,
)
from sqlspec.adapters.cockroach_psycopg.adk.store import (
    CockroachPsycopgAsyncADKMemoryStore,
    CockroachPsycopgAsyncADKStore,
    CockroachPsycopgSyncADKMemoryStore,
    CockroachPsycopgSyncADKStore,
)
from sqlspec.adapters.cockroach_psycopg.config import default_statement_config
from sqlspec.adapters.cockroach_psycopg.core import build_native_export
from sqlspec.adapters.cockroach_psycopg.litestar.store import CockroachPsycopgSyncStore
from sqlspec.core import SQL
from sqlspec.exceptions import ImproperConfigurationError


@pytest.mark.parametrize("config_type", [CockroachPsycopgSyncConfig, CockroachPsycopgAsyncConfig])
def test_cockroach_psycopg_native_storage_defaults_off(
    config_type: type[CockroachPsycopgSyncConfig] | type[CockroachPsycopgAsyncConfig],
) -> None:
    assert config_type().driver_features["enable_native_storage"] is False


@pytest.mark.parametrize("config_type", [CockroachPsycopgSyncConfig, CockroachPsycopgAsyncConfig])
def test_cockroach_psycopg_native_storage_options_reach_driver(
    config_type: type[CockroachPsycopgSyncConfig] | type[CockroachPsycopgAsyncConfig],
) -> None:
    options = {"nullas": "NULL", "nullif": "NULL", "skip": 0}
    config = config_type(
        connection_config={"autocommit": True},
        driver_features={"enable_native_storage": True, "native_storage_csv_options": options},
    )
    driver = config.driver_type(connection=cast("Any", object()), driver_features=config.driver_features)
    assert driver.driver_features["enable_native_storage"] is True
    assert driver.driver_features["native_storage_csv_options"] == options


@pytest.mark.parametrize("config_type", [CockroachPsycopgSyncConfig, CockroachPsycopgAsyncConfig])
@pytest.mark.parametrize(
    "connection_config", [{}, {"autocommit": False}, {"autocommit": False, "kwargs": {"autocommit": True}}]
)
def test_cockroach_psycopg_native_storage_requires_autocommit(
    config_type: type[CockroachPsycopgSyncConfig] | type[CockroachPsycopgAsyncConfig], connection_config: dict[str, Any]
) -> None:
    with pytest.raises(ImproperConfigurationError, match="autocommit=True"):
        config_type(connection_config=connection_config, driver_features={"enable_native_storage": True})


@pytest.mark.parametrize("config_type", [CockroachPsycopgSyncConfig, CockroachPsycopgAsyncConfig])
@pytest.mark.parametrize(
    "connection_config",
    [
        {"kwargs": {"autocommit": True}},
        {"extra": {"autocommit": True}},
        {"autocommit": True, "kwargs": {"autocommit": False}},
    ],
)
def test_cockroach_psycopg_native_storage_honors_effective_autocommit(
    config_type: type[CockroachPsycopgSyncConfig] | type[CockroachPsycopgAsyncConfig], connection_config: dict[str, Any]
) -> None:
    config = config_type(connection_config=connection_config, driver_features={"enable_native_storage": True})
    assert config.driver_features["enable_native_storage"] is True


@pytest.mark.parametrize("config_type", [CockroachPsycopgSyncConfig, CockroachPsycopgAsyncConfig])
@pytest.mark.parametrize(
    "options",
    [None, [], {"invalid": "fragment"}, {"nullas": 1}, {"nullif": None}, {"skip": -1}, {"skip": True}, {"skip": "1"}],
)
def test_cockroach_psycopg_native_storage_rejects_invalid_csv_options(
    config_type: type[CockroachPsycopgSyncConfig] | type[CockroachPsycopgAsyncConfig], options: Any
) -> None:
    with pytest.raises(ImproperConfigurationError, match="native_storage_csv_options"):
        config_type(driver_features={"native_storage_csv_options": options})


@pytest.mark.parametrize("config_type", [CockroachPsycopgSyncConfig, CockroachPsycopgAsyncConfig])
@pytest.mark.parametrize("options", [{}, {"skip": 1}, {"nullas": ""}, {"nullif": ""}])
def test_cockroach_psycopg_native_storage_preserves_explicit_csv_options(
    config_type: type[CockroachPsycopgSyncConfig] | type[CockroachPsycopgAsyncConfig], options: dict[str, Any]
) -> None:
    config = config_type(driver_features={"native_storage_csv_options": options})
    assert config.driver_features["native_storage_csv_options"] == options


class _SyncPoolSpy:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        self.__class__.calls.append((args, kwargs))

    def close(self) -> None:
        return None


class _AsyncPoolSpy:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        self.open_called = False
        self.__class__.calls.append((args, kwargs))

    async def open(self) -> None:
        self.open_called = True

    async def close(self) -> None:
        return None


@pytest.fixture(autouse=True)
def reset_pool_spies() -> None:
    _SyncPoolSpy.calls = []
    _AsyncPoolSpy.calls = []


def test_cockroach_psycopg_sync_config_default_initialization() -> None:
    """Config should initialize with sensible defaults."""
    config = CockroachPsycopgSyncConfig()
    assert config.connection_config is not None
    assert config.statement_config is not None
    assert config.driver_features is not None


def test_cockroach_psycopg_sync_config_auto_retry_enabled_by_default() -> None:
    """Auto retry should be enabled by default."""
    config = CockroachPsycopgSyncConfig()
    assert config.driver_features.get("enable_auto_retry") is True


def test_cockroach_psycopg_sync_config_retry_config_extraction() -> None:
    """Retry config should be extractable from driver features."""
    config = CockroachPsycopgSyncConfig(driver_features={"max_retries": 5, "retry_delay_base_ms": 100.0})
    retry_config = CockroachPsycopgRetryConfig.from_features(config.driver_features)
    assert retry_config.max_retries == 5
    assert retry_config.base_delay_ms == 100.0


def test_cockroach_psycopg_sync_config_disable_auto_retry() -> None:
    """Auto retry can be explicitly disabled."""
    config = CockroachPsycopgSyncConfig(driver_features={"enable_auto_retry": False})
    assert config.driver_features.get("enable_auto_retry") is False


def test_cockroach_psycopg_sync_config_follower_reads_configuration() -> None:
    """Follower reads settings should be stored in driver features."""
    config = CockroachPsycopgSyncConfig(driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"})
    assert config.driver_features.get("enable_follower_reads") is True
    assert config.driver_features.get("default_staleness") == "'-10s'"


def test_cockroach_psycopg_sync_config_json_serializer_propagation() -> None:
    """JSON serializer should propagate to statement config.

    Note: psycopg only uses json_serializer for parameter encoding.
    JSON deserialization is handled by psycopg's built-in type adapters.
    """

    def custom_serializer(obj: object) -> str:
        return f"custom:{obj}"

    config = CockroachPsycopgSyncConfig(driver_features={"json_serializer": custom_serializer})
    param_config = config.statement_config.parameter_config
    assert param_config.json_serializer is custom_serializer


def test_cockroach_psycopg_sync_config_connection_config_dict_normalization() -> None:
    """Connection config dict should be normalized."""
    config = CockroachPsycopgSyncConfig(connection_config={"host": "localhost", "port": 26257, "dbname": "testdb"})
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 26257


def test_cockroach_psycopg_sync_config_conninfo_in_connection_config() -> None:
    """Conninfo string should be accepted in connection config."""
    config = CockroachPsycopgSyncConfig(connection_config={"conninfo": "postgresql://user:pass@localhost:26257/testdb"})
    assert "conninfo" in config.connection_config


def test_cockroach_psycopg_sync_pool_preserves_conninfo_connection_kwargs() -> None:
    """Conninfo should not cause explicit psycopg connection kwargs to be dropped."""
    config = CockroachPsycopgSyncConfig(
        connection_config={
            "conninfo": "postgresql://user:pass@localhost:26257/testdb",
            "prepare_threshold": 0,
            "row_factory": "rows",
            "cursor_factory": "cursor",
            "context": "adapt-context",
            "kwargs": {"application_name": "sqlspec-test"},
        }
    )

    with patch("sqlspec.adapters.cockroach_psycopg.config.ConnectionPool", _SyncPoolSpy):
        pool = config.create_pool()

    assert isinstance(pool, _SyncPoolSpy)
    args, kwargs = _SyncPoolSpy.calls[0]
    assert args == ("postgresql://user:pass@localhost:26257/testdb",)
    assert kwargs["kwargs"] == {
        "prepare_threshold": 0,
        "row_factory": "rows",
        "cursor_factory": "cursor",
        "context": "adapt-context",
        "application_name": "sqlspec-test",
    }


def test_cockroach_psycopg_sync_pool_forwards_lifecycle_options() -> None:
    """psycopg-pool lifecycle options should route to ConnectionPool."""

    def check(_: object) -> None:
        return None

    def reset(_: object) -> None:
        return None

    def reconnect_failed(_: object) -> None:
        return None

    config = CockroachPsycopgSyncConfig(
        connection_config={
            "host": "localhost",
            "check": check,
            "reset": reset,
            "reconnect_failed": reconnect_failed,
            "open": False,
        }
    )

    with patch("sqlspec.adapters.cockroach_psycopg.config.ConnectionPool", _SyncPoolSpy):
        config.create_pool()

    _, kwargs = _SyncPoolSpy.calls[0]
    assert kwargs["check"] is check
    assert kwargs["reset"] is reset
    assert kwargs["reconnect_failed"] is reconnect_failed
    assert kwargs["open"] is False
    assert kwargs["kwargs"] == {"host": "localhost"}


def test_cockroach_psycopg_sync_config_bind_key_configuration() -> None:
    """Bind key should be stored for multi-database setups."""
    config = CockroachPsycopgSyncConfig(bind_key="cockroach_primary")
    assert config.bind_key == "cockroach_primary"


def test_cockroach_psycopg_sync_config_class_attributes() -> None:
    """Config should have correct class attributes."""
    assert CockroachPsycopgSyncConfig.supports_transactional_ddl is False
    assert CockroachPsycopgSyncConfig.supports_native_arrow_export is True
    assert CockroachPsycopgSyncConfig.supports_native_arrow_import is True


def test_cockroach_psycopg_sync_session_context_resolves_callable_statement_config() -> None:
    """Sync session context should match psycopg callable statement_config behavior."""
    connection = object()
    statement_config = default_statement_config.replace(dialect="cockroach")
    calls: list[str] = []

    def statement_config_factory() -> Any:
        calls.append("factory")
        return statement_config

    ctx = CockroachPsycopgSyncSessionContext(
        acquire_connection=lambda: connection,
        release_connection=lambda _connection, **_kwargs: None,
        statement_config=statement_config_factory,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with patch("sqlspec.adapters.cockroach_psycopg.driver.CockroachPsycopgSyncDriver") as driver_type:
        with ctx:
            pass

    assert calls == ["factory"]
    driver_type.assert_called_once_with(connection=connection, statement_config=statement_config, driver_features={})


def test_cockroach_psycopg_async_config_default_initialization() -> None:
    """Config should initialize with sensible defaults."""
    config = CockroachPsycopgAsyncConfig()
    assert config.connection_config is not None
    assert config.statement_config is not None
    assert config.driver_features is not None


def test_cockroach_psycopg_async_config_auto_retry_enabled_by_default() -> None:
    """Auto retry should be enabled by default."""
    config = CockroachPsycopgAsyncConfig()
    assert config.driver_features.get("enable_auto_retry") is True


def test_cockroach_psycopg_async_config_retry_config_extraction() -> None:
    """Retry config should be extractable from driver features."""
    config = CockroachPsycopgAsyncConfig(driver_features={"max_retries": 7, "retry_delay_base_ms": 75.0})
    retry_config = CockroachPsycopgRetryConfig.from_features(config.driver_features)
    assert retry_config.max_retries == 7
    assert retry_config.base_delay_ms == 75.0


def test_cockroach_psycopg_async_config_disable_auto_retry() -> None:
    """Auto retry can be explicitly disabled."""
    config = CockroachPsycopgAsyncConfig(driver_features={"enable_auto_retry": False})
    assert config.driver_features.get("enable_auto_retry") is False


def test_cockroach_psycopg_async_config_follower_reads_configuration() -> None:
    """Follower reads settings should be stored in driver features."""
    config = CockroachPsycopgAsyncConfig(driver_features={"enable_follower_reads": True, "default_staleness": "'-5s'"})
    assert config.driver_features.get("enable_follower_reads") is True
    assert config.driver_features.get("default_staleness") == "'-5s'"


def test_cockroach_psycopg_async_config_json_serializer_propagation() -> None:
    """JSON serializer should propagate to statement config.

    Note: psycopg only uses json_serializer for parameter encoding.
    JSON deserialization is handled by psycopg's built-in type adapters.
    """

    def custom_serializer(obj: object) -> str:
        return f"async:{obj}"

    config = CockroachPsycopgAsyncConfig(driver_features={"json_serializer": custom_serializer})
    param_config = config.statement_config.parameter_config
    assert param_config.json_serializer is custom_serializer


def test_cockroach_psycopg_async_config_connection_config_dict_normalization() -> None:
    """Connection config dict should be normalized."""
    config = CockroachPsycopgAsyncConfig(
        connection_config={"host": "cockroach-node", "port": 26258, "dbname": "asyncdb"}
    )
    assert config.connection_config["host"] == "cockroach-node"
    assert config.connection_config["port"] == 26258


def test_cockroach_psycopg_async_config_bind_key_configuration() -> None:
    """Bind key should be stored for multi-database setups."""
    config = CockroachPsycopgAsyncConfig(bind_key="cockroach_async")
    assert config.bind_key == "cockroach_async"


async def test_cockroach_psycopg_async_session_context_resolves_callable_statement_config() -> None:
    """Async session context should match psycopg callable statement_config behavior."""
    connection = object()
    statement_config = default_statement_config.replace(dialect="cockroach")
    calls: list[str] = []

    def statement_config_factory() -> Any:
        calls.append("factory")
        return statement_config

    async def acquire_connection() -> object:
        return connection

    async def release_connection(_connection: object, **_kwargs: object) -> None:
        return None

    ctx = CockroachPsycopgAsyncSessionContext(
        acquire_connection=acquire_connection,
        release_connection=release_connection,
        statement_config=statement_config_factory,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with patch("sqlspec.adapters.cockroach_psycopg.driver.CockroachPsycopgAsyncDriver") as driver_type:
        async with ctx:
            pass

    assert calls == ["factory"]
    driver_type.assert_called_once_with(connection=connection, statement_config=statement_config, driver_features={})


@pytest.mark.anyio
async def test_cockroach_psycopg_async_pool_preserves_conninfo_connection_kwargs() -> None:
    """Conninfo should not cause explicit async psycopg connection kwargs to be dropped."""
    config = CockroachPsycopgAsyncConfig(
        connection_config={
            "conninfo": "postgresql://user:pass@localhost:26257/testdb",
            "prepare_threshold": None,
            "row_factory": "async-rows",
            "cursor_factory": "async-cursor",
            "context": "async-adapt-context",
            "kwargs": {"application_name": "sqlspec-async-test"},
        }
    )

    with patch("sqlspec.adapters.cockroach_psycopg.config.AsyncConnectionPool", _AsyncPoolSpy):
        pool = await config.create_pool()

    assert isinstance(pool, _AsyncPoolSpy)
    args, kwargs = _AsyncPoolSpy.calls[0]
    assert args == ("postgresql://user:pass@localhost:26257/testdb",)
    assert kwargs["kwargs"] == {
        "prepare_threshold": None,
        "row_factory": "async-rows",
        "cursor_factory": "async-cursor",
        "context": "async-adapt-context",
        "application_name": "sqlspec-async-test",
    }


@pytest.mark.anyio
async def test_cockroach_psycopg_async_pool_forwards_lifecycle_options() -> None:
    """psycopg-pool async lifecycle options should route to AsyncConnectionPool."""

    async def check(_: object) -> None:
        return None

    async def reset(_: object) -> None:
        return None

    async def reconnect_failed(_: object) -> None:
        return None

    config = CockroachPsycopgAsyncConfig(
        connection_config={
            "host": "cockroach-node",
            "check": check,
            "reset": reset,
            "reconnect_failed": reconnect_failed,
            "open": False,
        }
    )

    with patch("sqlspec.adapters.cockroach_psycopg.config.AsyncConnectionPool", _AsyncPoolSpy):
        pool = await config.create_pool()

    _, kwargs = _AsyncPoolSpy.calls[0]
    assert kwargs["check"] is check
    assert kwargs["reset"] is reset
    assert kwargs["reconnect_failed"] is reconnect_failed
    assert kwargs["open"] is False
    assert kwargs["kwargs"] == {"host": "cockroach-node"}
    assert pool.open_called is False


def test_cockroach_psycopg_async_config_class_attributes() -> None:
    """Config should have correct class attributes."""
    assert CockroachPsycopgAsyncConfig.supports_transactional_ddl is False
    assert CockroachPsycopgAsyncConfig.supports_native_arrow_export is True
    assert CockroachPsycopgAsyncConfig.supports_native_arrow_import is True


def test_cockroach_psycopg_async_config_provide_session_uses_default_statement_config_constant_when_config_missing() -> (
    None
):
    """Async session fallback should reuse the module-level default config."""
    config = CockroachPsycopgAsyncConfig()
    config.statement_config = None
    session_config = config.provide_session()._statement_config
    if callable(session_config):
        session_config = session_config()
    assert session_config is default_statement_config


def test_cockroach_psycopg_driver_features_typed_dict_accepts_retry_features() -> None:
    """TypedDict should accept all retry-related features."""
    features: CockroachPsycopgDriverFeatures = {
        "enable_auto_retry": True,
        "max_retries": 5,
        "retry_delay_base_ms": 50.0,
        "retry_delay_max_ms": 3000.0,
        "enable_retry_logging": True,
    }
    assert features["enable_auto_retry"] is True
    assert features["max_retries"] == 5


def test_cockroach_psycopg_driver_features_typed_dict_accepts_follower_read_features() -> None:
    """TypedDict should accept follower read features."""
    features: CockroachPsycopgDriverFeatures = {"enable_follower_reads": True, "default_staleness": "'-5s'"}
    assert features["enable_follower_reads"] is True
    assert features["default_staleness"] == "'-5s'"


def test_cockroach_psycopg_driver_features_typed_dict_accepts_json_features() -> None:
    """TypedDict should accept JSON codec features."""

    def serializer_fn(x: object) -> str:
        return str(x)

    def deserializer_fn(x: str) -> object:
        return x

    features: CockroachPsycopgDriverFeatures = {"json_serializer": serializer_fn, "json_deserializer": deserializer_fn}
    assert features["json_serializer"] is serializer_fn
    assert features["json_deserializer"] is deserializer_fn


def test_cockroach_psycopg_driver_features_typed_dict_accepts_event_features() -> None:
    """TypedDict should accept event backend features."""
    features: CockroachPsycopgDriverFeatures = {"enable_events": True, "events_backend": "poll_queue"}
    assert features["enable_events"] is True
    assert features["events_backend"] == "poll_queue"


def test_cockroach_psycopg_driver_features_rejects_unused_uuid_preference() -> None:
    """No unused CockroachDB-specific UUID preference should remain as a no-op."""
    assert "prefer_uuid_keys" not in CockroachPsycopgDriverFeatures.__optional_keys__


@pytest.mark.parametrize("config_type", [CockroachPsycopgSyncConfig, CockroachPsycopgAsyncConfig])
def test_cockroach_psycopg_config_rejects_unused_uuid_preference(
    config_type: type[CockroachPsycopgSyncConfig] | type[CockroachPsycopgAsyncConfig],
) -> None:
    """Raw driver feature mappings should not keep unused UUID preference silently."""
    with pytest.raises(ImproperConfigurationError, match="prefer_uuid_keys"):
        config_type(driver_features={"prefer_uuid_keys": True})


def test_cockroach_psycopg_cluster_option_is_folded_into_libpq_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_pool(_conninfo: str, **kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("sqlspec.adapters.cockroach_psycopg.config.ConnectionPool", _fake_pool)
    config = CockroachPsycopgSyncConfig(connection_config={"host": "localhost", "cluster": "brave-hippo-42"})

    config.create_pool()

    assert captured["kwargs"]["options"] == "--cluster=brave-hippo-42"
    assert "cluster" not in captured["kwargs"]


def test_cockroach_psycopg_cluster_option_appends_to_existing_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_pool(_conninfo: str, **kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("sqlspec.adapters.cockroach_psycopg.config.ConnectionPool", _fake_pool)
    config = CockroachPsycopgSyncConfig(
        connection_config={"host": "localhost", "options": "-c statement_timeout=5s", "cluster": "brave-hippo-42"}
    )

    config.create_pool()

    assert captured["kwargs"]["options"] == "-c statement_timeout=5s --cluster=brave-hippo-42"


def test_cockroach_psycopg_create_connection_does_not_create_a_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = MagicMock()
    connect = MagicMock(return_value=sentinel)
    monkeypatch.setattr("psycopg.crdb.CrdbConnection.connect", connect)
    config = CockroachPsycopgSyncConfig(connection_config={"host": "localhost", "min_size": 1, "max_size": 1})

    connection = config.create_connection()

    assert connection is sentinel
    assert config.connection_instance is None
    assert "min_size" not in connect.call_args.kwargs
    assert "max_size" not in connect.call_args.kwargs


def test_cockroach_psycopg_create_connection_applies_the_connection_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[object] = []
    sentinel = MagicMock()
    monkeypatch.setattr("psycopg.crdb.CrdbConnection.connect", MagicMock(return_value=sentinel))
    config = CockroachPsycopgSyncConfig(
        connection_config={"host": "localhost"}, driver_features={"on_connection_create": seen.append}
    )

    config.create_connection()

    assert seen == [sentinel]


@pytest.mark.parametrize("staleness", ["'-10s'", "follower_read_timestamp()", "with_max_staleness('10s')"])
def test_cockroach_psycopg_accepts_valid_follower_read_staleness(staleness: str) -> None:
    config = CockroachPsycopgSyncConfig(driver_features={"enable_follower_reads": True, "default_staleness": staleness})

    assert config.driver_features["default_staleness"] == staleness


@pytest.mark.parametrize("staleness", ["-10s", "now(); DROP TABLE users", "'-10s'; SELECT 1", 42])
def test_cockroach_psycopg_rejects_invalid_follower_read_staleness(staleness: Any) -> None:
    with pytest.raises(ImproperConfigurationError, match="default_staleness"):
        CockroachPsycopgSyncConfig(driver_features={"enable_follower_reads": True, "default_staleness": staleness})


def test_cockroach_psycopg_provide_session_rejects_invalid_staleness() -> None:
    config = CockroachPsycopgSyncConfig()

    with pytest.raises(ImproperConfigurationError, match="default_staleness"):
        config.provide_session(follower_reads=True, staleness="now(); DROP TABLE users")


def test_cockroach_psycopg_follower_reads_apply_once_at_transaction_start(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock()
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "_connection_in_transaction", lambda _self: False)
    driver = CockroachPsycopgSyncDriver(
        connection=connection, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )

    driver.begin()

    assert connection.execute.call_count == 1
    assert connection.execute.call_args.args[0] == "SET TRANSACTION AS OF SYSTEM TIME '-10s'"


def test_cockroach_psycopg_follower_reads_stay_off_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock()
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "_connection_in_transaction", lambda _self: False)
    driver = CockroachPsycopgSyncDriver(connection=connection, driver_features={"default_staleness": "'-10s'"})

    driver.begin()

    assert connection.execute.call_count == 0


def test_build_connection_config_resolves_aliases() -> None:
    """build_connection_config should map dsn/url to conninfo, database/db to dbname, and username to user."""
    cfg = build_connection_config({
        "url": "postgresql://usr:pwd@host.internal:26257/main",
        "database": "override_db",
        "username": "override_usr",
    })
    assert cfg["conninfo"] == "postgresql://usr:pwd@host.internal:26257/main"
    assert cfg["dbname"] == "override_db"
    assert cfg["user"] == "override_usr"
    assert "url" not in cfg
    assert "database" not in cfg
    assert "username" not in cfg


def test_cockroach_sync_config_normalizes_aliases() -> None:
    """CockroachPsycopgSyncConfig should normalize aliases in connection_config."""
    config = CockroachPsycopgSyncConfig(
        connection_config={"dsn": "postgresql://localhost:26257/initial", "database": "override"}
    )
    assert config.connection_config["conninfo"] == "postgresql://localhost:26257/initial"
    assert config.connection_config["dbname"] == "override"
    assert "dsn" not in config.connection_config
    assert "database" not in config.connection_config


def test_cockroach_async_config_normalizes_aliases() -> None:
    """CockroachPsycopgAsyncConfig should normalize aliases in connection_config."""
    config = CockroachPsycopgAsyncConfig(
        connection_config={"connection_string": "postgresql://localhost:26257/initial", "db": "override"}
    )
    assert config.connection_config["conninfo"] == "postgresql://localhost:26257/initial"
    assert config.connection_config["dbname"] == "override"
    assert "connection_string" not in config.connection_config
    assert "db" not in config.connection_config


def test_cockroach_psycopg_create_pool_merges_extra_and_preserves_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    """CockroachPsycopgSyncConfig should merge extra into pool kwargs and keep on_connection_create in driver_features."""
    captured: dict[str, Any] = {}

    def _fake_pool(_conninfo: str, **kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    def _hook(_conn: Any) -> None:
        return None

    monkeypatch.setattr("sqlspec.adapters.cockroach_psycopg.config.ConnectionPool", _fake_pool)
    config = CockroachPsycopgSyncConfig(
        connection_config={"host": "localhost", "extra": {"application_name": "sqlspec"}},
        driver_features={"on_connection_create": _hook},
    )
    assert config.driver_features["on_connection_create"] is _hook
    config.create_pool()
    assert captured["kwargs"]["application_name"] == "sqlspec"
    assert "extra" not in captured["kwargs"]


def test_cockroach_psycopg_native_export_escapes_literal_percent() -> None:
    """build_native_export must escape literal % when parameters are bound."""
    cmd, bound = build_native_export(
        "SELECT id FROM items WHERE code LIKE 'A%' AND tenant = %s", ["t1"], "s3://bucket/export", "parquet", {}
    )
    assert "LIKE 'A%%'" in cmd
    assert "tenant = %s" in cmd
    assert bound == ["s3://bucket/export", "t1"]


def test_cockroach_psycopg_follower_read_auto_transaction_commits_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto-started follower-read transactions in CockroachPsycopgSyncDriver must commit or rollback."""
    in_tx = False

    def _in_tx(_self: Any) -> bool:
        return in_tx

    def _begin(_self: Any) -> None:
        nonlocal in_tx
        in_tx = True

    def _commit(_self: Any) -> None:
        nonlocal in_tx
        in_tx = False

    def _rollback(_self: Any) -> None:
        nonlocal in_tx
        in_tx = False

    monkeypatch.setattr(CockroachPsycopgSyncDriver, "_connection_in_transaction", _in_tx)
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "begin", _begin)
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "commit", _commit)
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "rollback", _rollback)

    conn = MagicMock()
    cursor = MagicMock()
    col = MagicMock()
    col.name = "val"
    col.type_code = 23
    cursor.description = [col]
    cursor.fetchall.return_value = [{"val": 1}]
    driver = CockroachPsycopgSyncDriver(
        connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )
    stmt = SQL("SELECT 1")
    driver.dispatch_execute(cursor, stmt)
    assert in_tx is False

    cursor.execute.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        driver.dispatch_execute(cursor, stmt)
    assert in_tx is False


async def test_cockroach_psycopg_adk_stores_commit_ddl_and_validate_locality() -> None:
    """CockroachPsycopg ADK and memory stores must commit DDL and validate LOCALITY clauses."""
    commits: list[str] = []

    class _FakeSyncDriver:
        def execute_script(self, _sql: str) -> None:
            return None

        def commit(self) -> None:
            commits.append("sync")

    class _FakeSyncSession:
        def __enter__(self) -> _FakeSyncDriver:
            return _FakeSyncDriver()

        def __exit__(self, *_args: Any) -> None:
            return None

    class _FakeAsyncDriver:
        async def execute_script(self, _sql: str) -> None:
            return None

        async def commit(self) -> None:
            commits.append("async")

    class _FakeAsyncSession:
        async def __aenter__(self) -> _FakeAsyncDriver:
            return _FakeAsyncDriver()

        async def __aexit__(self, *_args: Any) -> None:
            return None

    sync_cfg = CockroachPsycopgSyncConfig(extension_config={"adk": {"enable_memory": True}})
    object.__setattr__(sync_cfg, "provide_session", lambda *_a, **_kw: _FakeSyncSession())
    CockroachPsycopgSyncADKStore(sync_cfg).create_tables()
    CockroachPsycopgSyncADKMemoryStore(sync_cfg).create_tables()

    async_cfg = CockroachPsycopgAsyncConfig(extension_config={"adk": {"enable_memory": True}})
    object.__setattr__(async_cfg, "provide_session", lambda *_a, **_kw: _FakeAsyncSession())
    await CockroachPsycopgAsyncADKStore(async_cfg).create_tables()
    await CockroachPsycopgAsyncADKMemoryStore(async_cfg).create_tables()

    assert commits == ["sync", "sync", "async", "async"]

    bad_cfg = CockroachPsycopgSyncConfig(extension_config={"adk": {"table_locality": "GLOBAL; DROP TABLE x"}})
    with pytest.raises(ImproperConfigurationError, match="LOCALITY"):
        CockroachPsycopgSyncADKStore(bad_cfg)._sessions_table_ddl()

    good_cfg = CockroachPsycopgSyncConfig(extension_config={"adk": {"table_locality": "GLOBAL"}})
    adk_store = CockroachPsycopgSyncADKStore(good_cfg)
    adk_store._session_table = "tenant_a.adk_sessions"
    assert "idx_tenant_a_adk_sessions_app_user" in adk_store._sessions_table_ddl()
    litestar_store = CockroachPsycopgSyncStore(good_cfg)
    litestar_store._table_name = "tenant_a.litestar_sessions"
    assert "idx_tenant_a_litestar_sessions_expires_at" in litestar_store._table_ddl()


def test_cockroach_psycopg_follower_read_only_for_readonly_statements(monkeypatch: pytest.MonkeyPatch) -> None:
    """CockroachPsycopgSyncDriver should not begin follower-read transactions for modifying operations."""
    in_tx = False

    def _in_tx(_self: Any) -> bool:
        return in_tx

    def _begin(_self: Any) -> None:
        nonlocal in_tx
        in_tx = True

    monkeypatch.setattr(CockroachPsycopgSyncDriver, "_connection_in_transaction", _in_tx)
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "begin", _begin)

    conn = MagicMock()
    cursor = MagicMock()
    col = MagicMock()
    col.name = "id"
    col.type_code = 23
    cursor.description = [col]
    cursor.fetchall.return_value = [{"id": 1}]
    driver = CockroachPsycopgSyncDriver(
        connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )
    modifying_stmt = SQL("INSERT INTO users (name) VALUES ('alice') RETURNING id")
    driver.dispatch_execute(cursor, modifying_stmt)
    assert in_tx is False


def test_cockroach_psycopg_begin_follower_read_failure_handling() -> None:
    """Failing SET TRANSACTION in begin() must rollback, reset transaction active, and raise mapped exception."""
    conn = MagicMock()
    conn.autocommit = True
    conn.info.transaction_status = 0
    conn.execute.side_effect = RuntimeError("syntax error in staleness")

    driver = CockroachPsycopgSyncDriver(
        connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )
    with pytest.raises(Exception):
        driver.begin()
    assert driver._transaction_active is False
    conn.rollback.assert_called_once()


def test_cockroach_psycopg_select_stream_follower_reads() -> None:
    """select_stream on a SELECT query with follower reads emits the staleness clause and closes."""
    executed_statements: list[str] = []
    conn = MagicMock()
    conn.autocommit = True
    conn.info.transaction_status = 0

    def fake_conn_execute(query: str, *args: Any, **kwargs: Any) -> Any:
        executed_statements.append(query)
        return MagicMock()

    conn.execute.side_effect = fake_conn_execute

    tx = MagicMock()
    conn.transaction.return_value = tx

    stream_cursor = MagicMock()
    col = MagicMock()
    col.name = "id"
    stream_cursor.description = [col]
    stream_cursor.fetchmany.side_effect = [[(1,)], []]
    conn.cursor.return_value = stream_cursor

    driver = CockroachPsycopgSyncDriver(
        connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )
    stream = driver.dispatch_select_stream(SQL("SELECT id FROM users"), chunk_size=10)
    assert stream is not None
    rows = list(stream)
    assert rows == [{"id": 1}]
    assert any("AS OF SYSTEM TIME '-10s'" in s for s in executed_statements)
    conn.commit.assert_called()


def test_cockroach_psycopg_sync_user_configure_runs_after_internal_setup() -> None:
    """User-supplied configure callback runs after internal connection setup in sync config."""
    call_order: list[str] = []
    fake_conn = MagicMock()
    fake_conn.autocommit = False

    def user_cb(conn: Any) -> None:
        assert conn.autocommit is True
        call_order.append("user_cb")

    config = CockroachPsycopgSyncConfig(connection_config={"autocommit": True, "configure": user_cb})

    with patch("sqlspec.adapters.cockroach_psycopg.config.psycopg_crdb.CrdbConnection.connect", return_value=fake_conn):
        conn = config.create_connection()
        assert conn is fake_conn
        assert call_order == ["user_cb"]

    pool = config.create_pool()
    assert pool is not None


async def test_cockroach_psycopg_async_user_configure_runs_after_internal_setup() -> None:
    """User-supplied async configure callback runs after internal connection setup in async config."""
    call_order: list[str] = []
    fake_conn = MagicMock()
    autocommit_val = False

    async def fake_set_autocommit(val: bool) -> None:
        nonlocal autocommit_val
        autocommit_val = val
        call_order.append("internal_autocommit")

    fake_conn.set_autocommit = AsyncMock(side_effect=fake_set_autocommit)

    async def async_user_cb(conn: Any) -> None:
        assert autocommit_val is True
        call_order.append("user_cb")

    config = CockroachPsycopgAsyncConfig(connection_config={"autocommit": True, "configure": async_user_cb})

    with patch(
        "sqlspec.adapters.cockroach_psycopg.config.psycopg_crdb.AsyncCrdbConnection.connect",
        AsyncMock(return_value=fake_conn),
    ):
        conn = await config.create_connection()
        assert conn is fake_conn
        assert call_order == ["internal_autocommit", "user_cb"]


def test_cockroach_psycopg_adk_hash_shard_bucket_count_validation() -> None:
    """Invalid hash_shard_bucket_count must raise ValueError in cockroach_psycopg adk store."""
    from sqlspec.adapters.cockroach_psycopg.adk.store import _cockroach_hash_shard_clause

    bad_config = CockroachPsycopgSyncConfig(
        extension_config={"adk": {"enable_hash_sharded_indexes": True, "hash_shard_bucket_count": -1}}
    )
    with pytest.raises(ValueError, match="hash_shard_bucket_count"):
        _cockroach_hash_shard_clause(bad_config.extension_config["adk"])


async def test_cockroach_psycopg_adk_memory_search_with_embedding() -> None:
    """search_entries with embedding executes vector cosine search in cockroach_psycopg."""
    executed_queries: list[str] = []

    class _FakeCursor:
        description = [
            ("id",),
            ("session_id",),
            ("app_name",),
            ("user_id",),
            ("scope",),
            ("event_id",),
            ("author",),
            ("timestamp",),
            ("content_json",),
            ("content_text",),
            ("metadata_json",),
            ("inserted_at",),
            ("embedding",),
        ]

        async def execute(self, sql: bytes, params: Any) -> None:
            executed_queries.append(sql.decode())

        async def fetchall(self) -> list[Any]:
            return []

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *_a: Any) -> None:
            pass

    class _FakeConn:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    class _FakeCtx:
        async def __aenter__(self) -> _FakeConn:
            return _FakeConn()

        async def __aexit__(self, *_a: Any) -> None:
            pass

    config = CockroachPsycopgAsyncConfig(extension_config={"adk": {"enable_memory": True}})
    object.__setattr__(config, "provide_connection", lambda: _FakeCtx())
    store = CockroachPsycopgAsyncADKMemoryStore(config)

    await store.search_entries(query="hello", app_name="app", user_id="u1", embedding=[0.1, 0.2, 0.3])
    assert len(executed_queries) == 1
    assert "embedding <=>" in executed_queries[0]


async def test_cockroach_psycopg_adk_memory_insert_batch_unnest() -> None:
    """insert_memory_entries executes a single UNNEST query rather than an N+1 loop."""
    executed_queries: list[str] = []

    class _FakeCursor:
        rowcount = 2

        async def execute(self, sql: Any, params: Any) -> None:
            executed_queries.append(str(sql))

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *_a: Any) -> None:
            pass

    class _FakeConn:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        async def commit(self) -> None:
            pass

    class _FakeCtx:
        async def __aenter__(self) -> _FakeConn:
            return _FakeConn()

        async def __aexit__(self, *_a: Any) -> None:
            pass

    config = CockroachPsycopgAsyncConfig(extension_config={"adk": {"enable_memory": True}})
    object.__setattr__(config, "provide_connection", lambda: _FakeCtx())
    store = CockroachPsycopgAsyncADKMemoryStore(config)

    entry1: Any = {
        "id": "1",
        "session_id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "e1",
        "author": "user",
        "timestamp": "2026-01-01T00:00:00Z",
        "content_json": {},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": "2026-01-01T00:00:00Z",
    }
    entry2: Any = {
        "id": "2",
        "session_id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "e2",
        "author": "user",
        "timestamp": "2026-01-01T00:00:00Z",
        "content_json": {},
        "content_text": "world",
        "metadata_json": None,
        "inserted_at": "2026-01-01T00:00:00Z",
    }

    count = await store.insert_memory_entries([entry1, entry2])
    assert count == 2
    assert len(executed_queries) == 1
    assert "unnest" in executed_queries[0].lower()


def test_cockroach_psycopg_sync_get_columns_and_indexes_by_table() -> None:
    """CockroachPsycopgSyncDataDictionary executes by_table query when table is specified."""
    from sqlspec.adapters.cockroach_psycopg.data_dictionary import CockroachPsycopgSyncDataDictionary

    calls: list[tuple[Any, dict[str, Any]]] = []
    driver = MagicMock()

    def fake_select(query: Any, **kwargs: Any) -> list[Any]:
        calls.append((query, kwargs))
        return []

    driver.select = fake_select
    dd = CockroachPsycopgSyncDataDictionary()

    dd.get_columns(driver, table="users", schema=None)
    assert len(calls) == 1
    assert calls[0][1]["table_name"] == "users"
    assert calls[0][1]["schema_name"] is None

    calls.clear()
    dd.get_indexes(driver, table="users", schema=None)
    assert len(calls) == 1
    assert calls[0][1]["table_name"] == "users"
    assert calls[0][1]["schema_name"] is None


async def test_cockroach_psycopg_async_get_columns_and_indexes_by_table() -> None:
    """CockroachPsycopgAsyncDataDictionary executes by_table query when table is specified."""
    from sqlspec.adapters.cockroach_psycopg.data_dictionary import CockroachPsycopgAsyncDataDictionary

    calls: list[tuple[Any, dict[str, Any]]] = []
    driver = MagicMock()

    async def fake_select(query: Any, **kwargs: Any) -> list[Any]:
        calls.append((query, kwargs))
        return []

    driver.select = AsyncMock(side_effect=fake_select)
    dd = CockroachPsycopgAsyncDataDictionary()

    await dd.get_columns(driver, table="users", schema=None)
    assert len(calls) == 1
    assert calls[0][1]["table_name"] == "users"
    assert calls[0][1]["schema_name"] is None

    calls.clear()
    await dd.get_indexes(driver, table="users", schema=None)
    assert len(calls) == 1
    assert calls[0][1]["table_name"] == "users"
    assert calls[0][1]["schema_name"] is None
