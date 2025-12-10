"""Integration tests for SQLite driver features configuration."""

import json
import sqlite3
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.mark.sqlite
def test_driver_features_enabled_by_default() -> None:
    """Test that driver features are enabled by default for stdlib types."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    assert config.driver_features.get("enable_custom_adapters") is True


@pytest.mark.sqlite
def test_enable_custom_adapters_feature() -> None:
    """Test enabling custom type adapters feature."""
    config = SqliteConfig(connection_config={"database": ":memory:"}, driver_features={"enable_custom_adapters": True})

    assert config.driver_features["enable_custom_adapters"] is True


@pytest.mark.sqlite
def test_json_serialization_with_custom_adapters() -> None:
    """Test JSON dict/list serialization with custom adapters enabled."""
    config = SqliteConfig(
        connection_config={"database": ":memory:", "detect_types": sqlite3.PARSE_DECLTYPES},
        driver_features={"enable_custom_adapters": True},
    )

    sql = SQLSpec()
    sql.add_config(config)

    with sql.provide_session(config) as session:
        session.execute("CREATE TABLE test_json (id INTEGER, data JSON, items JSON)")

        test_dict = {"key": "value", "count": 42}
        test_list = [1, 2, 3, "four"]

        session.execute(
            "INSERT INTO test_json (id, data, items) VALUES (?, ?, ?)",
            (1, json.dumps(test_dict), json.dumps(test_list)),
        )

        result = session.select_one("SELECT data, items FROM test_json WHERE id = 1")

        assert result is not None
        assert result["data"] == test_dict
        assert result["items"] == test_list


@pytest.mark.sqlite
def test_custom_json_serializer() -> None:
    """Test using custom JSON serializer function."""

    def custom_serializer(obj: Any) -> str:
        return json.dumps(obj, separators=(",", ":"))

    def custom_deserializer(text: str) -> Any:
        return json.loads(text)

    config = SqliteConfig(
        connection_config={"database": ":memory:", "detect_types": sqlite3.PARSE_DECLTYPES},
        driver_features={
            "enable_custom_adapters": True,
            "json_serializer": custom_serializer,
            "json_deserializer": custom_deserializer,
        },
    )

    sql = SQLSpec()
    sql.add_config(config)

    with sql.provide_session(config) as session:
        session.execute("CREATE TABLE test_custom (id INTEGER, data JSON)")

        test_data = {"compact": True, "separator": "no_space"}
        session.execute("INSERT INTO test_custom (id, data) VALUES (?, ?)", (1, json.dumps(test_data)))

        result = session.select_one("SELECT data FROM test_custom WHERE id = 1")

        assert result is not None
        assert result["data"] == test_data


@pytest.mark.sqlite
def test_backward_compatibility_without_custom_adapters() -> None:
    """Test backward compatibility when custom adapters are not enabled."""
    config = SqliteConfig(connection_config={"database": ":memory:"})

    sql = SQLSpec()
    sql.add_config(config)

    with sql.provide_session(config) as session:
        session.execute("CREATE TABLE test_compat (id INTEGER, data TEXT)")

        test_dict = {"key": "value"}
        json_text = json.dumps(test_dict)

        session.execute("INSERT INTO test_compat (id, data) VALUES (?, ?)", (1, json_text))

        result = session.select_one("SELECT data FROM test_compat WHERE id = 1")

        assert result is not None
        assert isinstance(result["data"], str)
        assert json.loads(result["data"]) == test_dict
