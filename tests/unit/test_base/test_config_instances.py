"""Unit tests for type-safe config instance registration.

Tests the instance-based config registration system that enables type-safe database handles.
Registry now uses id(config) as key, allowing multiple configs of the same adapter type.

Key changes tested:
1. Registry key changed from type(config) to id(config)
2. add_config returns the config instance (not type[Config])
3. All methods only accept config instances (removed type[Config] from signatures)
4. Validation added - methods raise ValueError if config not registered
"""

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec

pytestmark = pytest.mark.xdist_group("base")


def test_multiple_same_type_configs() -> None:
    """Test that multiple configs of same adapter type are stored separately."""
    manager = SQLSpec()
    config1 = DuckDBConfig(pool_config={"database": ":memory:"})
    config2 = DuckDBConfig(pool_config={"database": ":memory:"})

    handle1 = manager.add_config(config1)
    handle2 = manager.add_config(config2)

    assert len(manager.configs) == 2
    assert handle1 is config1
    assert handle2 is config2
    assert id(config1) != id(config2)


def test_add_config_returns_same_instance() -> None:
    """Test that add_config returns the same config instance."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    result = manager.add_config(config)

    assert result is config


def test_provide_session_rejects_unregistered_config() -> None:
    """Test that provide_session raises ValueError for unregistered configs."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with pytest.raises(ValueError, match="Config not registered"):
        manager.provide_session(config)


def test_provide_connection_rejects_unregistered_config() -> None:
    """Test that provide_connection raises ValueError for unregistered configs."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with pytest.raises(ValueError, match="Config not registered"):
        manager.provide_connection(config)


def test_get_connection_rejects_unregistered_config() -> None:
    """Test that get_connection raises ValueError for unregistered configs."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with pytest.raises(ValueError, match="Config not registered"):
        manager.get_connection(config)


def test_get_session_rejects_unregistered_config() -> None:
    """Test that get_session raises ValueError for unregistered configs."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with pytest.raises(ValueError, match="Config not registered"):
        manager.get_session(config)


def test_get_pool_rejects_unregistered_config() -> None:
    """Test that get_pool raises ValueError for unregistered configs."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with pytest.raises(ValueError, match="Config not registered"):
        manager.get_pool(config)


def test_close_pool_rejects_unregistered_config() -> None:
    """Test that close_pool raises ValueError for unregistered configs."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with pytest.raises(ValueError, match="Config not registered"):
        manager.close_pool(config)


def test_registry_uses_id_as_key() -> None:
    """Test that registry uses id(config) as key."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)

    assert id(config) in manager.configs


def test_mixed_adapter_types_stored_separately() -> None:
    """Test that different adapter types are stored separately."""
    manager = SQLSpec()
    duckdb_config = DuckDBConfig(pool_config={"database": ":memory:"})
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})

    manager.add_config(duckdb_config)
    manager.add_config(sqlite_config)

    assert len(manager.configs) == 2
    assert id(duckdb_config) in manager.configs
    assert id(sqlite_config) in manager.configs


def test_config_overwrite_warning_on_duplicate_id() -> None:
    """Test that adding same config instance twice overwrites."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)
    manager.add_config(config)

    assert len(manager.configs) == 1
    assert id(config) in manager.configs


def test_registered_config_works_with_provide_session() -> None:
    """Test that registered configs work with provide_session."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)

    with manager.provide_session(config) as session:
        assert hasattr(session, "execute")


def test_registered_config_works_with_provide_connection() -> None:
    """Test that registered configs work with provide_connection."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)

    with manager.provide_connection(config) as connection:
        assert connection is not None


def test_registered_config_works_with_get_connection() -> None:
    """Test that registered configs work with get_connection."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)

    connection = manager.get_connection(config)
    assert connection is not None


def test_registered_config_works_with_get_session() -> None:
    """Test that registered configs work with get_session."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)

    session = manager.get_session(config)
    assert hasattr(session, "execute")


def test_registered_config_works_with_get_pool() -> None:
    """Test that registered configs work with get_pool."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)

    pool = manager.get_pool(config)
    assert pool is not None


def test_registered_config_works_with_close_pool() -> None:
    """Test that registered configs work with close_pool."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)

    result = manager.close_pool(config)
    assert result is None


def test_multiple_configs_same_type_provide_session_independently() -> None:
    """Test that multiple configs of same type work independently with provide_session."""
    manager = SQLSpec()
    config1 = DuckDBConfig(pool_config={"database": ":memory:"})
    config2 = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config1)
    manager.add_config(config2)

    with manager.provide_session(config1) as session1:
        assert hasattr(session1, "execute")

    with manager.provide_session(config2) as session2:
        assert hasattr(session2, "execute")


def test_instance_identity_preserved_through_add_config() -> None:
    """Test that config instance identity is preserved through add_config."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})
    original_id = id(config)

    returned_config = manager.add_config(config)

    assert id(returned_config) == original_id
    assert returned_config is config


def test_configs_property_returns_dict_with_id_keys() -> None:
    """Test that configs property returns dict mapping id to config."""
    manager = SQLSpec()
    config1 = DuckDBConfig(pool_config={"database": ":memory:"})
    config2 = SqliteConfig(pool_config={"database": ":memory:"})

    manager.add_config(config1)
    manager.add_config(config2)

    configs_dict = manager.configs
    assert isinstance(configs_dict, dict)
    assert len(configs_dict) == 2
    assert configs_dict[id(config1)] is config1
    assert configs_dict[id(config2)] is config2


def test_multiple_managers_have_independent_registries() -> None:
    """Test that multiple SQLSpec instances have independent config registries."""
    manager1 = SQLSpec()
    manager2 = SQLSpec()

    config1 = DuckDBConfig(pool_config={"database": ":memory:"})
    config2 = DuckDBConfig(pool_config={"database": ":memory:"})

    manager1.add_config(config1)
    manager2.add_config(config2)

    assert len(manager1.configs) == 1
    assert len(manager2.configs) == 1
    assert id(config1) in manager1.configs
    assert id(config1) not in manager2.configs
    assert id(config2) in manager2.configs
    assert id(config2) not in manager1.configs


def test_config_can_be_registered_with_multiple_managers() -> None:
    """Test that same config instance can be registered with multiple managers."""
    manager1 = SQLSpec()
    manager2 = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager1.add_config(config)
    manager2.add_config(config)

    assert id(config) in manager1.configs
    assert id(config) in manager2.configs
    assert manager1.configs[id(config)] is manager2.configs[id(config)]


def test_unregistered_after_manager_recreation() -> None:
    """Test that configs are unregistered when manager is recreated."""
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager1 = SQLSpec()
    manager1.add_config(config)
    assert id(config) in manager1.configs

    manager2 = SQLSpec()
    assert id(config) not in manager2.configs


def test_registry_survives_config_modifications() -> None:
    """Test that registry lookup works after config attributes are modified."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config)
    original_id = id(config)

    config.pool_config["database"] = "test.db"

    assert id(config) == original_id
    assert id(config) in manager.configs


def test_error_message_clarity_for_unregistered_config() -> None:
    """Test that error message for unregistered config is clear and actionable."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with pytest.raises(ValueError) as exc_info:
        manager.provide_session(config)

    error_message = str(exc_info.value)
    assert "Config not registered" in error_message
    assert "add_config" in error_message


def test_three_configs_same_type_all_stored() -> None:
    """Test that more than two configs of same type are all stored."""
    manager = SQLSpec()
    config1 = DuckDBConfig(pool_config={"database": ":memory:"})
    config2 = DuckDBConfig(pool_config={"database": ":memory:"})
    config3 = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config1)
    manager.add_config(config2)
    manager.add_config(config3)

    assert len(manager.configs) == 3
    assert id(config1) in manager.configs
    assert id(config2) in manager.configs
    assert id(config3) in manager.configs


def test_registry_clear_on_fresh_instance() -> None:
    """Test that fresh SQLSpec instance has empty registry."""
    manager = SQLSpec()
    assert len(manager.configs) == 0
    assert isinstance(manager.configs, dict)


def test_config_instance_is_handle_pattern() -> None:
    """Test the 'config instance IS the handle' pattern."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    handle = manager.add_config(config)

    assert handle is config
    assert isinstance(handle, DuckDBConfig)

    with manager.provide_session(handle) as session:
        assert hasattr(session, "execute")


def test_multiple_sqlite_configs_stored_separately() -> None:
    """Test that multiple SQLite configs are stored separately."""
    manager = SQLSpec()
    config1 = SqliteConfig(pool_config={"database": ":memory:"})
    config2 = SqliteConfig(pool_config={"database": ":memory:"})

    manager.add_config(config1)
    manager.add_config(config2)

    assert len(manager.configs) == 2
    assert id(config1) in manager.configs
    assert id(config2) in manager.configs


def test_mixed_sqlite_duckdb_configs() -> None:
    """Test that mixed SQLite and DuckDB configs coexist."""
    manager = SQLSpec()
    sqlite1 = SqliteConfig(pool_config={"database": ":memory:"})
    duckdb1 = DuckDBConfig(pool_config={"database": ":memory:"})
    sqlite2 = SqliteConfig(pool_config={"database": ":memory:"})
    duckdb2 = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(sqlite1)
    manager.add_config(duckdb1)
    manager.add_config(sqlite2)
    manager.add_config(duckdb2)

    assert len(manager.configs) == 4
    assert id(sqlite1) in manager.configs
    assert id(duckdb1) in manager.configs
    assert id(sqlite2) in manager.configs
    assert id(duckdb2) in manager.configs


def test_config_not_in_registry_after_no_add() -> None:
    """Test that simply creating a config doesn't add it to registry."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    assert id(config) not in manager.configs


def test_provide_session_with_correct_config_after_multiple_adds() -> None:
    """Test provide_session works with correct config after adding multiple."""
    manager = SQLSpec()
    config1 = DuckDBConfig(pool_config={"database": ":memory:"})
    config2 = DuckDBConfig(pool_config={"database": ":memory:"})
    config3 = DuckDBConfig(pool_config={"database": ":memory:"})

    manager.add_config(config1)
    manager.add_config(config2)
    manager.add_config(config3)

    with manager.provide_session(config2) as session:
        assert hasattr(session, "execute")


def test_unregistered_config_error_all_methods() -> None:
    """Test that all methods reject unregistered configs with same error."""
    manager = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    methods_to_test = [
        lambda: manager.provide_session(config),
        lambda: manager.provide_connection(config),
        lambda: manager.get_connection(config),
        lambda: manager.get_session(config),
        lambda: manager.get_pool(config),
        lambda: manager.close_pool(config),
    ]

    for method in methods_to_test:
        with pytest.raises(ValueError, match="Config not registered"):
            method()
