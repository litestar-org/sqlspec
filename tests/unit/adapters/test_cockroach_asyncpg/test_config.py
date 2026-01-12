"""Unit tests for CockroachDB AsyncPG configuration.

Tests cover:
- CockroachAsyncpgConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

import pytest

from sqlspec.adapters.cockroach_asyncpg.config import CockroachAsyncpgConfig, CockroachAsyncpgDriverFeatures
from sqlspec.adapters.cockroach_asyncpg.core import CockroachAsyncpgRetryConfig


@pytest.mark.xdist_group("cockroachdb")
class TestCockroachAsyncpgConfig:
    """Tests for CockroachAsyncpgConfig class."""

    def test_default_initialization(self) -> None:
        """Config should initialize with sensible defaults."""
        config = CockroachAsyncpgConfig()
        assert config.connection_config is not None
        assert config.statement_config is not None
        assert config.driver_features is not None

    def test_auto_retry_enabled_by_default(self) -> None:
        """Auto retry should be enabled by default."""
        config = CockroachAsyncpgConfig()
        assert config.driver_features.get("enable_auto_retry") is True

    def test_retry_config_extraction(self) -> None:
        """Retry config should be extractable from driver features."""
        config = CockroachAsyncpgConfig(driver_features={"max_retries": 5, "retry_delay_base_ms": 100.0})
        retry_config = CockroachAsyncpgRetryConfig.from_features(config.driver_features)
        assert retry_config.max_retries == 5
        assert retry_config.base_delay_ms == 100.0

    def test_disable_auto_retry(self) -> None:
        """Auto retry can be explicitly disabled."""
        config = CockroachAsyncpgConfig(driver_features={"enable_auto_retry": False})
        assert config.driver_features.get("enable_auto_retry") is False

    def test_follower_reads_configuration(self) -> None:
        """Follower reads settings should be stored in driver features."""
        config = CockroachAsyncpgConfig(driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"})
        assert config.driver_features.get("enable_follower_reads") is True
        assert config.driver_features.get("default_staleness") == "'-10s'"

    def test_json_serializer_propagation(self) -> None:
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

    def test_connection_config_dict_normalization(self) -> None:
        """Connection config dict should be normalized."""
        config = CockroachAsyncpgConfig(connection_config={"host": "localhost", "port": 26257, "database": "testdb"})
        assert config.connection_config["host"] == "localhost"
        assert config.connection_config["port"] == 26257

    def test_dsn_in_connection_config(self) -> None:
        """DSN string should be accepted in connection config."""
        config = CockroachAsyncpgConfig(connection_config={"dsn": "postgresql://user:pass@localhost:26257/testdb"})
        assert "dsn" in config.connection_config

    def test_bind_key_configuration(self) -> None:
        """Bind key should be stored for multi-database setups."""
        config = CockroachAsyncpgConfig(bind_key="cockroach_primary")
        assert config.bind_key == "cockroach_primary"


@pytest.mark.xdist_group("cockroachdb")
class TestCockroachAsyncpgDriverFeatures:
    """Tests for CockroachAsyncpgDriverFeatures TypedDict structure."""

    def test_typed_dict_accepts_retry_features(self) -> None:
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

    def test_typed_dict_accepts_follower_read_features(self) -> None:
        """TypedDict should accept follower read features."""
        features: CockroachAsyncpgDriverFeatures = {"enable_follower_reads": True, "default_staleness": "'-5s'"}
        assert features["enable_follower_reads"] is True
        assert features["default_staleness"] == "'-5s'"

    def test_typed_dict_accepts_json_features(self) -> None:
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

    def test_typed_dict_accepts_event_features(self) -> None:
        """TypedDict should accept event backend features."""
        features: CockroachAsyncpgDriverFeatures = {"enable_events": True, "events_backend": "table_queue"}
        assert features["enable_events"] is True
        assert features["events_backend"] == "table_queue"
