"""Integration tests for SQLite driver features configuration."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.mark.sqlite
def test_custom_adapters_disabled_by_default() -> None:
    """Test that process-global custom adapters are opt-in."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    assert config.driver_features.get("enable_custom_adapters") is False


@pytest.mark.sqlite
def test_enable_custom_adapters_feature() -> None:
    """Test enabling custom type adapters feature."""
    config = SqliteConfig(connection_config={"database": ":memory:"}, driver_features={"enable_custom_adapters": True})

    assert config.driver_features["enable_custom_adapters"] is True
