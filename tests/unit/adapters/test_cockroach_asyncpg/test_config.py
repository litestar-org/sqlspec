"""Unit tests for CockroachDB AsyncPG configuration.

Tests cover:
- CockroachAsyncpgConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

from unittest.mock import AsyncMock, patch

import pytest

from sqlspec.adapters.cockroach_asyncpg import (
    CockroachAsyncpgConfig,
    CockroachAsyncpgDriverFeatures,
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
    features: CockroachAsyncpgDriverFeatures = {"enable_events": True, "events_backend": "table_queue"}
    assert features["enable_events"] is True
    assert features["events_backend"] == "table_queue"
