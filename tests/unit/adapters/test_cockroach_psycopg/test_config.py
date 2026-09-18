# pyright: reportAttributeAccessIssue=false
"""Unit tests for CockroachDB psycopg configuration.

Tests cover:
- CockroachPsycopgSyncConfig initialization and defaults
- CockroachPsycopgAsyncConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncSessionContext,
    CockroachPsycopgDriverFeatures,
    CockroachPsycopgRetryConfig,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
    CockroachPsycopgSyncSessionContext,
)
from sqlspec.adapters.cockroach_psycopg.config import default_statement_config
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
