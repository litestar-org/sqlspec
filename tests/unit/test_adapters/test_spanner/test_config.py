import pytest

from sqlspec.adapters.spanner.config import SpannerConfig
from sqlspec.exceptions import ImproperConfigurationError


def test_config_initialization() -> None:
    """Test basic configuration initialization."""
    config = SpannerConfig(
        connection_config={
            "project": "my-project",
            "instance_id": "my-instance",
            "database_id": "my-database",
        }
    )
    assert config.project == "my-project"
    assert config.instance_id == "my-instance"
    assert config.database_id == "my-database"
    assert config.pool_config is not None


def test_config_defaults() -> None:
    """Test default values."""
    config = SpannerConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"}
    )
    assert config.pool_config is not None
    assert config.pool_config["min_sessions"] == 1
    assert config.pool_config["max_sessions"] == 10


def test_improper_configuration() -> None:
    """Test validation of required fields."""
    config = SpannerConfig()
    # _create_pool checks for instance_id/database_id
    with pytest.raises(ImproperConfigurationError):
        config._create_pool()


def test_driver_features_defaults() -> None:
    """Test driver features defaults."""
    config = SpannerConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"}
    )
    assert config.driver_features["enable_uuid_conversion"] is True
    assert config.driver_features["json_serializer"] is not None