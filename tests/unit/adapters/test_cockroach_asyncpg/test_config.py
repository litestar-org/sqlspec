"""Unit tests for CockroachDB AsyncPG configuration.

Tests cover:
- CockroachAsyncpgConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

from typing import Any, cast, get_args, get_origin
from unittest.mock import AsyncMock, patch

import pytest
from typing_extensions import NotRequired

from sqlspec.adapters.cockroach_asyncpg import (
    CockroachAsyncpgConfig,
    CockroachAsyncpgConnectionConfig,
    CockroachAsyncpgDriverFeatures,
    CockroachAsyncpgPoolConfig,
    CockroachAsyncpgRetryConfig,
)
from sqlspec.core import StatementConfig


def test_cockroach_asyncpg_config_default_initialization() -> None:
    """Config should initialize with sensible defaults."""
    config = CockroachAsyncpgConfig()
    assert config.connection_config is not None
    assert config.statement_config is not None
    assert config.driver_features is not None


def test_cockroach_asyncpg_config_auto_retry_enabled_by_default() -> None:
    """Auto retry should be enabled by default."""
    config = CockroachAsyncpgConfig()
    assert config.driver_features.get("enable_auto_retry") is True


def test_cockroach_asyncpg_config_retry_config_extraction() -> None:
    """Retry config should be extractable from driver features."""
    config = CockroachAsyncpgConfig(driver_features={"max_retries": 5, "retry_delay_base_ms": 100.0})
    retry_config = CockroachAsyncpgRetryConfig.from_features(config.driver_features)
    assert retry_config.max_retries == 5
    assert retry_config.base_delay_ms == 100.0


def test_cockroach_asyncpg_config_disable_auto_retry() -> None:
    """Auto retry can be explicitly disabled."""
    config = CockroachAsyncpgConfig(driver_features={"enable_auto_retry": False})
    assert config.driver_features.get("enable_auto_retry") is False


def test_cockroach_asyncpg_config_follower_reads_configuration() -> None:
    """Follower reads settings should be stored in driver features."""
    config = CockroachAsyncpgConfig(driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"})
    assert config.driver_features.get("enable_follower_reads") is True
    assert config.driver_features.get("default_staleness") == "'-10s'"


def test_cockroach_asyncpg_config_json_serializer_propagation() -> None:
    """JSON serializers should propagate to statement config."""

    def custom_serializer(obj: object) -> str:
        return f"custom:{obj}"

    def custom_deserializer(s: str) -> object:
        return {"parsed": s}

    config = CockroachAsyncpgConfig(
        driver_features={"json_serializer": custom_serializer, "json_deserializer": custom_deserializer}
    )
    param_config = config.statement_config.parameter_config
    assert param_config.json_serializer is custom_serializer
    assert param_config.json_deserializer is custom_deserializer


def test_cockroach_asyncpg_config_connection_config_dict_normalization() -> None:
    """Connection config dict should be normalized."""
    config = CockroachAsyncpgConfig(connection_config={"host": "localhost", "port": 26257, "database": "testdb"})
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 26257


def test_cockroach_asyncpg_config_dsn_in_connection_config() -> None:
    """DSN string should be accepted in connection config."""
    config = CockroachAsyncpgConfig(connection_config={"dsn": "postgresql://user:pass@localhost:26257/testdb"})
    assert "dsn" in config.connection_config


def test_cockroach_asyncpg_config_bind_key_configuration() -> None:
    """Bind key should be stored for multi-database setups."""
    config = CockroachAsyncpgConfig(bind_key="cockroach_primary")
    assert config.bind_key == "cockroach_primary"


def test_cockroach_asyncpg_connection_config_describes_current_asyncpg_connect_keys() -> None:
    """Typed config should include current asyncpg connection keywords."""
    annotations = CockroachAsyncpgConnectionConfig.__annotations__

    assert annotations["service"] is not None
    assert annotations["servicefile"] is not None
    assert annotations["timeout"] is not None
    assert annotations["target_session_attrs"] is not None
    assert annotations["krbsrvname"] is not None
    assert annotations["gsslib"] is not None

    target_session_attrs_annotation = cast("Any", annotations["target_session_attrs"])
    assert get_origin(target_session_attrs_annotation) is NotRequired
    assert set(get_args(get_args(target_session_attrs_annotation)[0])) == {
        "any",
        "primary",
        "standby",
        "read-write",
        "read-only",
        "prefer-standby",
    }

    gsslib_annotation = cast("Any", annotations["gsslib"])
    assert get_origin(gsslib_annotation) is NotRequired
    assert set(get_args(get_args(gsslib_annotation)[0])) == {"gssapi", "sspi"}


def test_cockroach_asyncpg_pool_config_describes_current_asyncpg_pool_keys() -> None:
    """Typed pool config should include asyncpg pool-only keywords."""
    annotations = CockroachAsyncpgPoolConfig.__annotations__

    assert annotations["connect"] is not None
    assert annotations["reset"] is not None
    assert annotations["loop"] is not None
    assert annotations["connection_class"] is not None
    assert annotations["record_class"] is not None


@pytest.mark.anyio
async def test_cockroach_asyncpg_create_pool_forwards_current_asyncpg_config_keys() -> None:
    """Pool creation should pass through current asyncpg connection and pool keywords."""

    async def reset(connection: object) -> None:
        _ = connection

    async def connect_factory(**_: Any) -> object:
        return object()

    loop = object()
    connection_class = object()
    record_class = object()
    pool = object()
    config = CockroachAsyncpgConfig(
        connection_config={
            "connect": connect_factory,
            "host": "localhost",
            "port": 26257,
            "database": "defaultdb",
            "service": "cockroach",
            "servicefile": "/tmp/pg_service.conf",
            "timeout": 3.5,
            "target_session_attrs": "read-write",
            "krbsrvname": "postgres",
            "gsslib": "gssapi",
            "reset": reset,
            "loop": loop,
            "connection_class": connection_class,
            "record_class": record_class,
        }
    )

    with patch(
        "sqlspec.adapters.cockroach_asyncpg.config.asyncpg_create_pool", new=AsyncMock(return_value=pool)
    ) as mock:
        result = await config._create_pool()

    assert result is pool
    await_args = mock.await_args
    assert await_args is not None
    kwargs = await_args.kwargs
    assert kwargs["service"] == "cockroach"
    assert kwargs["servicefile"] == "/tmp/pg_service.conf"
    assert kwargs["timeout"] == 3.5
    assert kwargs["target_session_attrs"] == "read-write"
    assert kwargs["krbsrvname"] == "postgres"
    assert kwargs["gsslib"] == "gssapi"
    assert kwargs["connect"] is connect_factory
    assert kwargs["reset"] is reset
    assert kwargs["loop"] is loop
    assert kwargs["connection_class"] is connection_class
    assert kwargs["record_class"] is record_class
    assert kwargs["init"] == config._init_connection


async def test_cockroach_asyncpg_config_init_connection_registers_json_codecs_before_user_hook() -> None:
    """Connection init should install JSON codecs before user callbacks."""
    events: list[str] = []

    async def user_hook(connection: object) -> None:
        events.append("user")

    async def register_json(connection: object, **_: object) -> None:
        events.append("json")

    config = CockroachAsyncpgConfig(driver_features={"on_connection_create": user_hook})
    connection = AsyncMock()
    with patch(
        "sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs",
        new=AsyncMock(side_effect=register_json),
        create=True,
    ) as register_mock:
        await config._init_connection(connection)
    register_mock.assert_awaited_once()
    assert events == ["json", "user"]


async def test_cockroach_asyncpg_config_init_connection_skips_json_codecs_when_disabled() -> None:
    """Disabling JSON codecs should preserve the user callback."""
    events: list[str] = []

    async def user_hook(connection: object) -> None:
        events.append("user")

    config = CockroachAsyncpgConfig(driver_features={"enable_json_codecs": False, "on_connection_create": user_hook})
    connection = AsyncMock()
    with patch(
        "sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs", new_callable=AsyncMock, create=True
    ) as register_mock:
        await config._init_connection(connection)
    register_mock.assert_not_awaited()
    assert events == ["user"]


@pytest.mark.anyio
async def test_cockroach_asyncpg_init_connection_registers_pgvector_when_enabled() -> None:
    config = CockroachAsyncpgConfig(driver_features={"enable_pgvector": True})
    connection = AsyncMock()
    with (
        patch("sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs", new_callable=AsyncMock, create=True),
        patch(
            "sqlspec.adapters.cockroach_asyncpg.config.register_pgvector_support", new_callable=AsyncMock, create=True
        ) as register_pgvector,
    ):
        await config._init_connection(connection)
    register_pgvector.assert_awaited_once_with(connection)


@pytest.mark.anyio
async def test_cockroach_asyncpg_init_connection_skips_pgvector_by_default() -> None:
    config = CockroachAsyncpgConfig()
    connection = AsyncMock()
    with (
        patch("sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs", new_callable=AsyncMock, create=True),
        patch(
            "sqlspec.adapters.cockroach_asyncpg.config.register_pgvector_support", new_callable=AsyncMock, create=True
        ) as register_pgvector,
    ):
        await config._init_connection(connection)
    register_pgvector.assert_not_awaited()


def test_cockroach_asyncpg_provide_session_uses_lazy_lambda() -> None:
    config = CockroachAsyncpgConfig()
    session_config = config.provide_session()._statement_config
    assert callable(session_config)


def test_cockroach_asyncpg_provide_session_reflects_post_construction_dialect_change() -> None:
    config = CockroachAsyncpgConfig()
    context = config.provide_session()
    config.statement_config = StatementConfig(dialect="pgvector")
    session_config = context._statement_config
    assert callable(session_config)
    assert session_config().dialect == "pgvector"


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_retry_features() -> None:
    """TypedDict should accept all retry-related features."""
    features: CockroachAsyncpgDriverFeatures = {
        "enable_auto_retry": True,
        "max_retries": 5,
        "retry_delay_base_ms": 50.0,
        "retry_delay_max_ms": 3000.0,
        "enable_retry_logging": True,
    }
    assert features["enable_auto_retry"] is True
    assert features["max_retries"] == 5


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_follower_read_features() -> None:
    """TypedDict should accept follower read features."""
    features: CockroachAsyncpgDriverFeatures = {"enable_follower_reads": True, "default_staleness": "'-5s'"}
    assert features["enable_follower_reads"] is True
    assert features["default_staleness"] == "'-5s'"


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_json_features() -> None:
    """TypedDict should accept JSON codec features."""

    def serializer_fn(x: object) -> str:
        return str(x)

    def deserializer_fn(x: str) -> object:
        return x

    features: CockroachAsyncpgDriverFeatures = {
        "json_serializer": serializer_fn,
        "json_deserializer": deserializer_fn,
        "enable_json_codecs": True,
    }
    assert features["enable_json_codecs"] is True


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_event_features() -> None:
    """TypedDict should accept event backend features."""
    features: CockroachAsyncpgDriverFeatures = {"enable_events": True, "events_backend": "poll_queue"}
    assert features["enable_events"] is True
    assert features["events_backend"] == "poll_queue"
