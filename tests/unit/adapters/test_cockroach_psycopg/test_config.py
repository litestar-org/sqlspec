"""Unit tests for CockroachDB psycopg configuration.

Tests cover:
- CockroachPsycopgSyncConfig initialization and defaults
- CockroachPsycopgAsyncConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

import pytest

from sqlspec.adapters.cockroach_psycopg.config import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgDriverFeatures,
    CockroachPsycopgSyncConfig,
)
from sqlspec.adapters.cockroach_psycopg.core import CockroachPsycopgRetryConfig


@pytest.mark.xdist_group("cockroachdb")
class TestCockroachPsycopgSyncConfig:
    """Tests for CockroachPsycopgSyncConfig class."""

    def test_default_initialization(self) -> None:
        """Config should initialize with sensible defaults."""
        config = CockroachPsycopgSyncConfig()
        assert config.connection_config is not None
        assert config.statement_config is not None
        assert config.driver_features is not None

    def test_auto_retry_enabled_by_default(self) -> None:
        """Auto retry should be enabled by default."""
        config = CockroachPsycopgSyncConfig()
        assert config.driver_features.get("enable_auto_retry") is True

    def test_retry_config_extraction(self) -> None:
        """Retry config should be extractable from driver features."""
        config = CockroachPsycopgSyncConfig(driver_features={"max_retries": 5, "retry_delay_base_ms": 100.0})
        retry_config = CockroachPsycopgRetryConfig.from_features(config.driver_features)
        assert retry_config.max_retries == 5
        assert retry_config.base_delay_ms == 100.0

    def test_disable_auto_retry(self) -> None:
        """Auto retry can be explicitly disabled."""
        config = CockroachPsycopgSyncConfig(driver_features={"enable_auto_retry": False})
        assert config.driver_features.get("enable_auto_retry") is False

    def test_follower_reads_configuration(self) -> None:
        """Follower reads settings should be stored in driver features."""
        config = CockroachPsycopgSyncConfig(
            driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
        )
        assert config.driver_features.get("enable_follower_reads") is True
        assert config.driver_features.get("default_staleness") == "'-10s'"

    def test_json_serializer_propagation(self) -> None:
        """JSON serializer should propagate to statement config.

        Note: psycopg only uses json_serializer for parameter encoding.
        JSON deserialization is handled by psycopg's built-in type adapters.
        """

        def custom_serializer(obj: object) -> str:
            return f"custom:{obj}"

        config = CockroachPsycopgSyncConfig(driver_features={"json_serializer": custom_serializer})

        param_config = config.statement_config.parameter_config
        assert param_config.json_serializer is custom_serializer

    def test_connection_config_dict_normalization(self) -> None:
        """Connection config dict should be normalized."""
        config = CockroachPsycopgSyncConfig(connection_config={"host": "localhost", "port": 26257, "dbname": "testdb"})
        assert config.connection_config["host"] == "localhost"
        assert config.connection_config["port"] == 26257

    def test_conninfo_in_connection_config(self) -> None:
        """Conninfo string should be accepted in connection config."""
        config = CockroachPsycopgSyncConfig(
            connection_config={"conninfo": "postgresql://user:pass@localhost:26257/testdb"}
        )
        assert "conninfo" in config.connection_config

    def test_bind_key_configuration(self) -> None:
        """Bind key should be stored for multi-database setups."""
        config = CockroachPsycopgSyncConfig(bind_key="cockroach_primary")
        assert config.bind_key == "cockroach_primary"

    def test_class_attributes(self) -> None:
        """Config should have correct class attributes."""
        assert CockroachPsycopgSyncConfig.supports_transactional_ddl is True
        assert CockroachPsycopgSyncConfig.supports_native_arrow_export is True
        assert CockroachPsycopgSyncConfig.supports_native_arrow_import is True


@pytest.mark.xdist_group("cockroachdb")
class TestCockroachPsycopgAsyncConfig:
    """Tests for CockroachPsycopgAsyncConfig class."""

    def test_default_initialization(self) -> None:
        """Config should initialize with sensible defaults."""
        config = CockroachPsycopgAsyncConfig()
        assert config.connection_config is not None
        assert config.statement_config is not None
        assert config.driver_features is not None

    def test_auto_retry_enabled_by_default(self) -> None:
        """Auto retry should be enabled by default."""
        config = CockroachPsycopgAsyncConfig()
        assert config.driver_features.get("enable_auto_retry") is True

    def test_retry_config_extraction(self) -> None:
        """Retry config should be extractable from driver features."""
        config = CockroachPsycopgAsyncConfig(driver_features={"max_retries": 7, "retry_delay_base_ms": 75.0})
        retry_config = CockroachPsycopgRetryConfig.from_features(config.driver_features)
        assert retry_config.max_retries == 7
        assert retry_config.base_delay_ms == 75.0

    def test_disable_auto_retry(self) -> None:
        """Auto retry can be explicitly disabled."""
        config = CockroachPsycopgAsyncConfig(driver_features={"enable_auto_retry": False})
        assert config.driver_features.get("enable_auto_retry") is False

    def test_follower_reads_configuration(self) -> None:
        """Follower reads settings should be stored in driver features."""
        config = CockroachPsycopgAsyncConfig(
            driver_features={"enable_follower_reads": True, "default_staleness": "'-5s'"}
        )
        assert config.driver_features.get("enable_follower_reads") is True
        assert config.driver_features.get("default_staleness") == "'-5s'"

    def test_json_serializer_propagation(self) -> None:
        """JSON serializer should propagate to statement config.

        Note: psycopg only uses json_serializer for parameter encoding.
        JSON deserialization is handled by psycopg's built-in type adapters.
        """

        def custom_serializer(obj: object) -> str:
            return f"async:{obj}"

        config = CockroachPsycopgAsyncConfig(driver_features={"json_serializer": custom_serializer})

        param_config = config.statement_config.parameter_config
        assert param_config.json_serializer is custom_serializer

    def test_connection_config_dict_normalization(self) -> None:
        """Connection config dict should be normalized."""
        config = CockroachPsycopgAsyncConfig(
            connection_config={"host": "cockroach-node", "port": 26258, "dbname": "asyncdb"}
        )
        assert config.connection_config["host"] == "cockroach-node"
        assert config.connection_config["port"] == 26258

    def test_bind_key_configuration(self) -> None:
        """Bind key should be stored for multi-database setups."""
        config = CockroachPsycopgAsyncConfig(bind_key="cockroach_async")
        assert config.bind_key == "cockroach_async"

    def test_class_attributes(self) -> None:
        """Config should have correct class attributes."""
        assert CockroachPsycopgAsyncConfig.supports_transactional_ddl is True
        assert CockroachPsycopgAsyncConfig.supports_native_arrow_export is True
        assert CockroachPsycopgAsyncConfig.supports_native_arrow_import is True


@pytest.mark.xdist_group("cockroachdb")
class TestCockroachPsycopgDriverFeatures:
    """Tests for CockroachPsycopgDriverFeatures TypedDict structure."""

    def test_typed_dict_accepts_retry_features(self) -> None:
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

    def test_typed_dict_accepts_follower_read_features(self) -> None:
        """TypedDict should accept follower read features."""
        features: CockroachPsycopgDriverFeatures = {"enable_follower_reads": True, "default_staleness": "'-5s'"}
        assert features["enable_follower_reads"] is True
        assert features["default_staleness"] == "'-5s'"

    def test_typed_dict_accepts_json_features(self) -> None:
        """TypedDict should accept JSON codec features."""

        def serializer_fn(x: object) -> str:
            return str(x)

        def deserializer_fn(x: str) -> object:
            return x

        features: CockroachPsycopgDriverFeatures = {
            "json_serializer": serializer_fn,
            "json_deserializer": deserializer_fn,
        }
        assert features["json_serializer"] is serializer_fn
        assert features["json_deserializer"] is deserializer_fn

    def test_typed_dict_accepts_event_features(self) -> None:
        """TypedDict should accept event backend features."""
        features: CockroachPsycopgDriverFeatures = {"enable_events": True, "events_backend": "table_queue"}
        assert features["enable_events"] is True
        assert features["events_backend"] == "table_queue"

    def test_typed_dict_accepts_uuid_preference(self) -> None:
        """TypedDict should accept CockroachDB-specific UUID preference."""
        features: CockroachPsycopgDriverFeatures = {"prefer_uuid_keys": True}
        assert features["prefer_uuid_keys"] is True
