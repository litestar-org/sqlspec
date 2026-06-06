"""Integration tests for DuckDB driver features configuration."""

import json

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_config() -> DuckDBConfig:
    """Create a basic DuckDB configuration."""
    return DuckDBConfig(connection_config={"database": ":memory:"})


def test_driver_features_passed_to_driver() -> None:
    """Test that driver_features are properly passed to the driver instance."""
    custom_json = json.dumps

    config = DuckDBConfig(
        connection_config={"database": ":memory:"},
        driver_features={"json_serializer": custom_json, "enable_uuid_conversion": False},
    )
    try:
        with config.provide_session() as session:
            assert session.driver_features is not None
            assert "json_serializer" in session.driver_features
            assert "enable_uuid_conversion" in session.driver_features
            assert session.driver_features["enable_uuid_conversion"] is False
    finally:
        config.close_pool()
