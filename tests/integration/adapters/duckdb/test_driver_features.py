"""Integration tests for DuckDB driver features configuration."""

import json
import uuid

import msgspec
import pytest

from sqlspec.adapters.duckdb import DuckDBConfig

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_config() -> DuckDBConfig:
    """Create a basic DuckDB configuration."""
    return DuckDBConfig(connection_config={"database": ":memory:"})


def test_default_uuid_conversion_enabled(duckdb_config: DuckDBConfig) -> None:
    """Test that UUID conversion is enabled by default."""
    try:
        with duckdb_config.provide_session() as session:
            session.execute("CREATE TABLE test (id UUID)")
            uuid_str = "550e8400-e29b-41d4-a716-446655440000"
            session.execute("INSERT INTO test (id) VALUES (?)", (uuid_str,))

            result = session.select_one("SELECT id FROM test")
            assert result is not None
            assert isinstance(result["id"], uuid.UUID)
            assert str(result["id"]) == uuid_str
    finally:
        duckdb_config.close_pool()


def test_uuid_conversion_can_be_disabled() -> None:
    """Test that UUID conversion can be disabled via driver_features.

    When disabled, UUID strings are passed as-is to DuckDB without conversion.
    DuckDB still returns UUID objects from UUID columns (native behavior).
    """
    config = DuckDBConfig(connection_config={"database": ":memory:"}, driver_features={"enable_uuid_conversion": False})
    try:
        with config.provide_session() as session:
            session.execute("DROP TABLE IF EXISTS test")
            session.execute("CREATE TABLE test (id UUID, value TEXT)")
            uuid_str = "550e8400-e29b-41d4-a716-446655440000"

            session.execute("INSERT INTO test (id, value) VALUES (?, ?)", (uuid_str, "test"))

            result = session.select_one("SELECT id, value FROM test")
            assert result is not None
            assert isinstance(result["id"], uuid.UUID)
            assert result["value"] == "test"
    finally:
        config.close_pool()


def test_custom_json_serializer_for_dict() -> None:
    """Test custom JSON serializer for dict parameters."""

    def custom_json(obj: dict) -> str:
        return msgspec.json.encode(obj).decode("utf-8")

    config = DuckDBConfig(connection_config={"database": ":memory:"}, driver_features={"json_serializer": custom_json})
    try:
        with config.provide_session() as session:
            session.execute("DROP TABLE IF EXISTS test")
            session.execute("CREATE TABLE test (data JSON)")
            test_data = {"key": "value", "number": 42}
            session.execute("INSERT INTO test (data) VALUES (?)", (test_data,))

            result = session.select_one("SELECT data FROM test")
            assert result is not None
    finally:
        config.close_pool()


def test_custom_json_serializer_for_list() -> None:
    """Test custom JSON serializer for list parameters."""

    def custom_json(obj: list) -> str:
        return msgspec.json.encode(obj).decode("utf-8")

    config = DuckDBConfig(connection_config={"database": ":memory:"}, driver_features={"json_serializer": custom_json})
    try:
        with config.provide_session() as session:
            session.execute("DROP TABLE IF EXISTS test")
            session.execute("CREATE TABLE test (data JSON)")
            test_data = [1, 2, 3, 4, 5]
            session.execute("INSERT INTO test (data) VALUES (?)", (test_data,))

            result = session.select_one("SELECT data FROM test")
            assert result is not None
            assert result["data"] == "[1,2,3,4,5]"
    finally:
        config.close_pool()


def test_backward_compatibility_default_json_serializer(duckdb_config: DuckDBConfig) -> None:
    """Test backward compatibility - default JSON serializer still works."""
    try:
        with duckdb_config.provide_session() as session:
            session.execute("DROP TABLE IF EXISTS test")
            session.execute("CREATE TABLE test (data JSON)")
            test_data = {"key": "value", "nested": {"data": [1, 2, 3]}}
            session.execute("INSERT INTO test (data) VALUES (?)", (test_data,))

            result = session.select_one("SELECT data FROM test")
            assert result is not None
    finally:
        duckdb_config.close_pool()


def test_combined_features_json_and_uuid() -> None:
    """Test combining custom JSON serializer with disabled UUID conversion."""

    def custom_json(obj: dict | list) -> str:
        return json.dumps(obj, separators=(",", ":"))

    config = DuckDBConfig(
        connection_config={"database": ":memory:"},
        driver_features={"json_serializer": custom_json, "enable_uuid_conversion": False},
    )
    try:
        with config.provide_session() as session:
            session.execute("DROP TABLE IF EXISTS test")
            session.execute("CREATE TABLE test (id UUID, data JSON)")
            uuid_str = "550e8400-e29b-41d4-a716-446655440000"
            test_data = {"uuid_field": uuid_str, "other": "data"}

            session.execute("INSERT INTO test (id, data) VALUES (?, ?)", (uuid_str, test_data))

            result = session.select_one("SELECT id, data FROM test")
            assert result is not None
            assert isinstance(result["id"], uuid.UUID)
    finally:
        config.close_pool()


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
