# pyright: reportAttributeAccessIssue=false
"""Unit tests for CockroachDB psycopg configuration.

Tests cover:
- CockroachPsycopgSyncConfig initialization and defaults
- CockroachPsycopgAsyncConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgDriverFeatures,
    CockroachPsycopgRetryConfig,
    CockroachPsycopgSyncConfig,
)
from sqlspec.adapters.cockroach_psycopg.config import default_statement_config


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


def test_cockroach_psycopg_sync_config_bind_key_configuration() -> None:
    """Bind key should be stored for multi-database setups."""
    config = CockroachPsycopgSyncConfig(bind_key="cockroach_primary")
    assert config.bind_key == "cockroach_primary"


def test_cockroach_psycopg_sync_config_class_attributes() -> None:
    """Config should have correct class attributes."""
    assert CockroachPsycopgSyncConfig.supports_transactional_ddl is True
    assert CockroachPsycopgSyncConfig.supports_native_arrow_export is True
    assert CockroachPsycopgSyncConfig.supports_native_arrow_import is True


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


def test_cockroach_psycopg_async_config_class_attributes() -> None:
    """Config should have correct class attributes."""
    assert CockroachPsycopgAsyncConfig.supports_transactional_ddl is True
    assert CockroachPsycopgAsyncConfig.supports_native_arrow_export is True
    assert CockroachPsycopgAsyncConfig.supports_native_arrow_import is True


def test_cockroach_psycopg_async_config_provide_session_uses_default_statement_config_constant_when_config_missing() -> (
    None
):
    """Async session fallback should reuse the module-level default config."""
    config = CockroachPsycopgAsyncConfig()
    config.statement_config = None
    session_config = config.provide_session()._statement_config
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
    features: CockroachPsycopgDriverFeatures = {"enable_events": True, "events_backend": "table_queue"}
    assert features["enable_events"] is True
    assert features["events_backend"] == "table_queue"


def test_cockroach_psycopg_driver_features_typed_dict_accepts_uuid_preference() -> None:
    """TypedDict should accept CockroachDB-specific UUID preference."""
    features: CockroachPsycopgDriverFeatures = {"prefer_uuid_keys": True}
    assert features["prefer_uuid_keys"] is True
