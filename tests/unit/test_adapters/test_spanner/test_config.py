import pytest

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.exceptions import ImproperConfigurationError


def test_config_initialization() -> None:
    """Test basic configuration initialization."""
    config = SpannerSyncConfig(
        pool_config={"project": "my-project", "instance_id": "my-instance", "database_id": "my-database"}
    )
    assert config.pool_config is not None
    assert config.pool_config["project"] == "my-project"
    assert config.pool_config["instance_id"] == "my-instance"
    assert config.pool_config["database_id"] == "my-database"


def test_config_defaults() -> None:
    """Test default values."""
    config = SpannerSyncConfig(pool_config={"project": "p", "instance_id": "i", "database_id": "d"})
    assert config.pool_config is not None
    assert config.pool_config["min_sessions"] == 1
    assert config.pool_config["max_sessions"] == 10


def test_improper_configuration() -> None:
    """Test validation of required fields."""
    config = SpannerSyncConfig()
    with pytest.raises(ImproperConfigurationError):
        config.provide_pool()


def test_driver_features_defaults() -> None:
    """Test driver features defaults."""
    config = SpannerSyncConfig(pool_config={"project": "p", "instance_id": "i", "database_id": "d"})
    assert config.driver_features["enable_uuid_conversion"] is True
    assert config.driver_features["json_serializer"] is not None
