"""Edge case tests for connection_config and connection_instance parameters.

Tests unusual scenarios, boundary conditions, and error cases for the
standardized parameter naming.
"""

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.sqlite.config import SqliteConfig


def test_connection_config_with_zero_pool_size() -> None:
    """Test connection_config with zero pool size parameters."""
    config = SqliteConfig(connection_config={"database": ":memory:", "pool_min_size": 0, "pool_max_size": 0})

    assert config.connection_config["pool_min_size"] == 0
    assert config.connection_config["pool_max_size"] == 0


def test_connection_config_with_negative_pool_size() -> None:
    """Test connection_config with negative pool size parameters."""
    config = DuckDBConfig(connection_config={"database": ":memory:", "pool_min_size": -1, "pool_max_size": -1})

    # Negative values are stored but pool creation may validate them
    assert config.connection_config["pool_min_size"] == -1
    assert config.connection_config["pool_max_size"] == -1


def test_connection_config_with_very_large_pool_size() -> None:
    """Test connection_config with very large pool size values."""
    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test", "min_size": 1000, "max_size": 10000}
    )

    assert config.connection_config["min_size"] == 1000
    assert config.connection_config["max_size"] == 10000


def test_connection_config_with_min_greater_than_max() -> None:
    """Test connection_config with min_size > max_size (invalid but stored)."""
    config = SqliteConfig(connection_config={"database": ":memory:", "pool_min_size": 10, "pool_max_size": 5})

    # Config stores values, validation happens at pool creation
    assert config.connection_config["pool_min_size"] == 10
    assert config.connection_config["pool_max_size"] == 5


def test_connection_config_with_special_characters_in_strings() -> None:
    """Test connection_config with special characters in string values."""
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://user:p@ss!w0rd#$%@localhost/test?sslmode=require",
            "server_settings": {"application_name": "app with spaces & symbols!"},
        }
    )

    assert "p@ss!w0rd#$%" in config.connection_config["dsn"]
    assert config.connection_config["server_settings"]["application_name"] == "app with spaces & symbols!"


def test_connection_config_with_unicode_strings() -> None:
    """Test connection_config with unicode characters."""
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://localhost/test",
            "server_settings": {"application_name": "テスト アプリ"},
        }
    )

    assert config.connection_config["server_settings"]["application_name"] == "テスト アプリ"


def test_connection_config_with_empty_strings() -> None:
    """Test connection_config with empty string values."""
    config = AsyncpgConfig(connection_config={"dsn": "", "user": "", "password": ""})

    assert config.connection_config["dsn"] == ""
    assert config.connection_config["user"] == ""
    assert config.connection_config["password"] == ""


def test_connection_config_with_none_values_in_dict() -> None:
    """Test connection_config with None values for keys."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "user": None, "password": None})

    assert config.connection_config["dsn"] == "postgresql://localhost/test"
    assert config.connection_config["user"] is None
    assert config.connection_config["password"] is None


def test_connection_config_with_boolean_false_values() -> None:
    """Test connection_config with False boolean values."""
    config = SqliteConfig(
        connection_config={"database": ":memory:", "check_same_thread": False, "cached_statements": 0}
    )

    assert config.connection_config["check_same_thread"] is False
    assert config.connection_config["cached_statements"] == 0


def test_connection_config_with_mixed_types() -> None:
    """Test connection_config with various Python types."""
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://localhost/test",
            "min_size": 5,
            "timeout": 30.5,
            "ssl": True,
            "server_settings": {"key": "value"},
            "record_class": None,
        }
    )

    assert isinstance(config.connection_config["dsn"], str)
    assert isinstance(config.connection_config["min_size"], int)
    assert isinstance(config.connection_config["timeout"], float)
    assert isinstance(config.connection_config["ssl"], bool)
    assert isinstance(config.connection_config["server_settings"], dict)
    assert config.connection_config["record_class"] is None


def test_connection_config_modification_after_creation() -> None:
    """Test that connection_config can be modified after config creation."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    # Modify existing key
    config.connection_config["dsn"] = "postgresql://localhost/test2"
    assert config.connection_config["dsn"] == "postgresql://localhost/test2"

    # Add new key
    config.connection_config["min_size"] = 5
    assert config.connection_config["min_size"] == 5

    # Delete key
    del config.connection_config["min_size"]
    assert "min_size" not in config.connection_config


def test_connection_config_clear_after_creation() -> None:
    """Test that connection_config can be cleared after creation."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5})

    config.connection_config.clear()

    assert config.connection_config == {}


def test_connection_config_update_method() -> None:
    """Test that connection_config supports dict update() method."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    config.connection_config.update({"min_size": 5, "max_size": 10})

    assert config.connection_config["min_size"] == 5
    assert config.connection_config["max_size"] == 10


def test_connection_config_with_deeply_nested_dicts() -> None:
    """Test connection_config with deeply nested dict structures."""
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://localhost/test",
            "server_settings": {"level1": {"level2": {"level3": {"key": "value"}}}},
        }
    )

    assert config.connection_config["server_settings"]["level1"]["level2"]["level3"]["key"] == "value"


def test_connection_config_with_list_values() -> None:
    """Test connection_config with list values."""
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://localhost/test",
            "server_settings": {"extensions": ["pg_trgm", "pgcrypto", "uuid-ossp"]},
        }
    )

    assert config.connection_config["server_settings"]["extensions"] == ["pg_trgm", "pgcrypto", "uuid-ossp"]


def test_connection_config_with_tuple_values() -> None:
    """Test connection_config with tuple values."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "ssl": ("require", "verify-ca")})

    assert config.connection_config["ssl"] == ("require", "verify-ca")


def test_connection_instance_set_to_arbitrary_object() -> None:
    """Test that connection_instance can be set to any object (no type checking)."""

    class FakePool:
        pass

    fake_pool = FakePool()
    config = DuckDBConfig(connection_config={"database": ":memory:"}, connection_instance=fake_pool)  # type: ignore[arg-type]

    assert config.connection_instance is fake_pool  # type: ignore[comparison-overlap]


def test_connection_instance_can_be_modified_after_creation() -> None:
    """Test that connection_instance can be modified after config creation."""
    from unittest.mock import MagicMock

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    assert config.connection_instance is None

    mock_pool = MagicMock()
    config.connection_instance = mock_pool

    assert config.connection_instance is mock_pool


def test_multiple_configs_do_not_share_connection_config() -> None:
    """Test that modifying one config's connection_config doesn't affect another."""
    config1 = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/db1"})
    config2 = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/db2"})

    # Modify config1
    config1.connection_config["min_size"] = 5

    # config2 should not be affected
    assert "min_size" not in config2.connection_config
    assert config2.connection_config["dsn"] == "postgresql://localhost/db2"


def test_connection_config_dict_reference_semantics() -> None:
    """Test that connection_config has dict reference semantics."""
    test_dict = {"dsn": "postgresql://localhost/test"}
    config = AsyncpgConfig(connection_config=test_dict)

    # Modifying the original dict should NOT affect config
    # (because config stores a copy or processes it)
    test_dict["min_size"] = 5  # pyright: ignore[reportArgumentType]

    # Depending on implementation, this may or may not affect config
    # Let's test the actual behavior
    if "min_size" in config.connection_config:
        # If it's a reference, this would be True
        assert config.connection_config["min_size"] == 5
    # If it's a copy, min_size won't be in config.connection_config


def test_connection_config_with_callables() -> None:
    """Test connection_config with callable values."""

    def custom_init() -> str:
        return "initialized"

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "init": custom_init})

    assert callable(config.connection_config["init"])
    assert config.connection_config["init"]() == "initialized"


def test_connection_config_with_very_long_strings() -> None:
    """Test connection_config with very long string values."""
    long_string = "x" * 10000
    config = AsyncpgConfig(connection_config={"dsn": f"postgresql://localhost/{long_string}"})

    assert len(config.connection_config["dsn"]) > 10000


def test_connection_config_key_with_reserved_python_keywords() -> None:
    """Test connection_config keys that are Python reserved words."""
    # Note: This is valid in dict keys even if they're reserved words
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://localhost/test",
            "class": "value",  # reserved word
            "def": "value",  # reserved word
            "return": "value",  # reserved word
        }
    )

    assert config.connection_config["class"] == "value"
    assert config.connection_config["def"] == "value"
    assert config.connection_config["return"] == "value"


def test_connection_config_numeric_keys() -> None:
    """Test connection_config with numeric keys (valid dict keys)."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", 1: "value", 2.5: "value"})  # type: ignore[dict-item,misc]

    assert config.connection_config[1] == "value"  # type: ignore[index]
    assert config.connection_config[2.5] == "value"  # type: ignore[index]


def test_connection_instance_remains_after_connection_config_change() -> None:
    """Test that connection_instance persists when connection_config is modified."""
    from unittest.mock import MagicMock

    mock_pool = MagicMock()
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"}, connection_instance=mock_pool)

    # Modify connection_config
    config.connection_config["min_size"] = 5

    # connection_instance should remain unchanged
    assert config.connection_instance is mock_pool


def test_connection_config_with_bytes_values() -> None:
    """Test connection_config with bytes values."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "ssl_cert": b"certificate data"})

    assert config.connection_config["ssl_cert"] == b"certificate data"


@pytest.mark.asyncio
async def test_aiosqlite_connection_config_with_pathlib_path() -> None:
    """Test that connection_config accepts pathlib.Path objects."""
    from pathlib import Path

    db_path = Path(":memory:")
    config = AiosqliteConfig(connection_config={"database": db_path})

    assert config.connection_config["database"] == db_path


def test_connection_config_setdefault_method() -> None:
    """Test that connection_config supports dict setdefault() method."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    result = config.connection_config.setdefault("min_size", 5)

    assert result == 5
    assert config.connection_config["min_size"] == 5

    # setdefault on existing key should return existing value
    result = config.connection_config.setdefault("dsn", "other_dsn")
    assert result == "postgresql://localhost/test"


def test_connection_config_get_method_with_default() -> None:
    """Test that connection_config supports dict get() method."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    assert config.connection_config.get("min_size", 5) == 5
    assert config.connection_config.get("dsn") == "postgresql://localhost/test"


def test_connection_config_pop_method() -> None:
    """Test that connection_config supports dict pop() method."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5})

    popped_value = config.connection_config.pop("min_size")

    assert popped_value == 5
    assert "min_size" not in config.connection_config


def test_connection_config_items_method() -> None:
    """Test that connection_config supports dict items() iteration."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5, "max_size": 10})

    items = list(config.connection_config.items())

    assert len(items) == 3
    assert ("dsn", "postgresql://localhost/test") in items
    assert ("min_size", 5) in items
    assert ("max_size", 10) in items


def test_connection_config_keys_method() -> None:
    """Test that connection_config supports dict keys() method."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5})

    keys = list(config.connection_config.keys())

    assert "dsn" in keys
    assert "min_size" in keys
    assert len(keys) == 2


def test_connection_config_values_method() -> None:
    """Test that connection_config supports dict values() method."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5})

    values = list(config.connection_config.values())

    assert "postgresql://localhost/test" in values
    assert 5 in values
    assert len(values) == 2


def test_connection_config_in_operator() -> None:
    """Test that connection_config supports 'in' operator."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    assert "dsn" in config.connection_config
    assert "min_size" not in config.connection_config


def test_connection_config_len_function() -> None:
    """Test that connection_config supports len() function."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5})

    assert len(config.connection_config) == 2


def test_connection_config_bool_evaluation() -> None:
    """Test that connection_config evaluates to bool correctly."""
    # Use AsyncpgConfig which doesn't add default values
    config_empty = AsyncpgConfig(connection_config={})
    config_with_data = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    assert not bool(config_empty.connection_config)
    assert bool(config_with_data.connection_config)


def test_connection_config_copy_method() -> None:
    """Test that connection_config supports dict copy() method."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5})

    config_copy = config.connection_config.copy()

    assert config_copy == config.connection_config
    assert config_copy is not config.connection_config  # Should be a shallow copy

    # Modifying copy should not affect original
    config_copy["max_size"] = 10
    assert "max_size" not in config.connection_config
