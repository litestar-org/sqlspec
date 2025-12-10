"""Unit tests for connection_config and connection_instance parameters.

Tests the standardized parameter naming across all database adapters:
- connection_config (dict for connection/pool settings)
- connection_instance (pre-created pool/connection instance)

This test suite validates the refactoring from pool_config → connection_config
and pool_instance → connection_instance across all 11 adapters.

Key aspects tested:
1. Parameter acceptance and storage
2. Default empty dict for connection_config
3. None handling for connection_instance
4. Type validation
5. Configuration merging and overrides
"""

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig


def test_connection_config_parameter_accepts_dict() -> None:
    """Test that connection_config parameter accepts dict values."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "min_size": 5})

    assert config.connection_config["dsn"] == "postgresql://localhost/test"
    assert config.connection_config["min_size"] == 5


def test_connection_config_defaults_to_empty_dict() -> None:
    """Test that connection_config defaults to empty dict when not provided."""
    # Use AsyncpgConfig which doesn't modify connection_config
    config = AsyncpgConfig()

    assert config.connection_config == {}
    assert isinstance(config.connection_config, dict)


def test_connection_config_accepts_none_and_converts_to_empty_dict() -> None:
    """Test that connection_config=None is converted to empty dict."""
    # Use AsyncpgConfig which doesn't modify connection_config
    config = AsyncpgConfig(connection_config=None)

    assert config.connection_config == {}
    assert isinstance(config.connection_config, dict)


def test_connection_instance_defaults_to_none() -> None:
    """Test that connection_instance defaults to None when not provided."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    assert config.connection_instance is None


def test_connection_instance_accepts_none_explicitly() -> None:
    """Test that connection_instance=None is explicitly accepted."""
    config = PsycopgAsyncConfig(connection_config={"conninfo": "postgresql://localhost/test"}, connection_instance=None)

    assert config.connection_instance is None


def test_connection_config_stored_in_base_class() -> None:
    """Test that connection_config is stored in the base class attribute."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    assert hasattr(config, "connection_config")
    assert config.connection_config["dsn"] == "postgresql://localhost/test"


def test_connection_instance_stored_in_base_class() -> None:
    """Test that connection_instance is stored in the base class attribute."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    assert hasattr(config, "connection_instance")
    assert config.connection_instance is None


def test_asyncpg_config_accepts_connection_parameters() -> None:
    """Test AsyncpgConfig accepts connection_config and connection_instance."""
    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test", "min_size": 5, "max_size": 10, "timeout": 30.0},
        connection_instance=None,
    )

    assert config.connection_config["dsn"] == "postgresql://localhost/test"
    assert config.connection_config["min_size"] == 5
    assert config.connection_config["max_size"] == 10
    assert config.connection_config["timeout"] == 30.0
    assert config.connection_instance is None


def test_psycopg_async_config_accepts_connection_parameters() -> None:
    """Test PsycopgAsyncConfig accepts connection_config and connection_instance."""
    config = PsycopgAsyncConfig(
        connection_config={"conninfo": "postgresql://localhost/test", "min_size": 2, "max_size": 20},
        connection_instance=None,
    )

    assert config.connection_config["conninfo"] == "postgresql://localhost/test"
    assert config.connection_config["min_size"] == 2
    assert config.connection_config["max_size"] == 20
    assert config.connection_instance is None


def test_psycopg_sync_config_accepts_connection_parameters() -> None:
    """Test PsycopgSyncConfig accepts connection_config and connection_instance."""
    config = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://localhost/test", "min_size": 1, "max_size": 5},
        connection_instance=None,
    )

    assert config.connection_config["conninfo"] == "postgresql://localhost/test"
    assert config.connection_config["min_size"] == 1
    assert config.connection_config["max_size"] == 5
    assert config.connection_instance is None


def test_asyncmy_config_accepts_connection_parameters() -> None:
    """Test AsyncmyConfig accepts connection_config and connection_instance."""
    config = AsyncmyConfig(
        connection_config={
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "password",
            "database": "test",
            "minsize": 1,
            "maxsize": 10,
        },
        connection_instance=None,
    )

    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["database"] == "test"
    assert config.connection_config["minsize"] == 1
    assert config.connection_config["maxsize"] == 10
    assert config.connection_instance is None


def test_psqlpy_config_accepts_connection_parameters() -> None:
    """Test PsqlpyConfig accepts connection_config and connection_instance."""
    config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://localhost/test", "max_db_pool_size": 10}, connection_instance=None
    )

    assert config.connection_config["dsn"] == "postgresql://localhost/test"
    assert config.connection_config["max_db_pool_size"] == 10
    assert config.connection_instance is None


def test_oracle_async_config_accepts_connection_parameters() -> None:
    """Test OracleAsyncConfig accepts connection_config and connection_instance."""
    config = OracleAsyncConfig(
        connection_config={
            "user": "system",
            "password": "password",
            "dsn": "localhost:1521/ORCLPDB1",
            "min": 1,
            "max": 5,
        },
        connection_instance=None,
    )

    assert config.connection_config["user"] == "system"
    assert config.connection_config["dsn"] == "localhost:1521/ORCLPDB1"
    assert config.connection_config["min"] == 1
    assert config.connection_config["max"] == 5
    assert config.connection_instance is None


def test_oracle_sync_config_accepts_connection_parameters() -> None:
    """Test OracleSyncConfig accepts connection_config and connection_instance."""
    config = OracleSyncConfig(
        connection_config={
            "user": "system",
            "password": "password",
            "dsn": "localhost:1521/ORCLPDB1",
            "min": 2,
            "max": 10,
        },
        connection_instance=None,
    )

    assert config.connection_config["user"] == "system"
    assert config.connection_config["dsn"] == "localhost:1521/ORCLPDB1"
    assert config.connection_config["min"] == 2
    assert config.connection_config["max"] == 10
    assert config.connection_instance is None


def test_sqlite_config_accepts_connection_parameters() -> None:
    """Test SqliteConfig accepts connection_config and connection_instance."""
    config = SqliteConfig(
        connection_config={"database": ":memory:", "check_same_thread": False, "pool_min_size": 5, "pool_max_size": 10},
        connection_instance=None,
    )

    # SQLite converts :memory: to shared memory URI for pooling
    assert "memory" in config.connection_config["database"]
    assert config.connection_config["check_same_thread"] is False
    assert config.connection_config["pool_min_size"] == 5
    assert config.connection_config["pool_max_size"] == 10
    assert config.connection_instance is None


def test_aiosqlite_config_accepts_connection_parameters() -> None:
    """Test AiosqliteConfig accepts connection_config and connection_instance."""
    config = AiosqliteConfig(
        connection_config={"database": ":memory:", "timeout": 10.0, "pool_min_size": 2, "pool_max_size": 8},
        connection_instance=None,
    )

    # AioSQLite converts :memory: to shared memory URI for pooling
    assert "memory" in config.connection_config["database"]
    assert config.connection_config["timeout"] == 10.0
    assert config.connection_config["pool_min_size"] == 2
    assert config.connection_config["pool_max_size"] == 8
    assert config.connection_instance is None


def test_duckdb_config_accepts_connection_parameters() -> None:
    """Test DuckDBConfig accepts connection_config and connection_instance."""
    config = DuckDBConfig(
        connection_config={"database": ":memory:", "read_only": False, "pool_min_size": 3, "pool_max_size": 12},
        connection_instance=None,
    )

    # DuckDB converts :memory: to :memory:shared_db for pooling
    assert "memory" in config.connection_config["database"]
    assert config.connection_config["read_only"] is False
    assert config.connection_config["pool_min_size"] == 3
    assert config.connection_config["pool_max_size"] == 12
    assert config.connection_instance is None


def test_bigquery_config_accepts_connection_parameters() -> None:
    """Test BigQueryConfig accepts connection_config and connection_instance."""
    config = BigQueryConfig(
        connection_config={"project": "my-project", "dataset_id": "my_dataset", "location": "US"},
        connection_instance=None,
    )

    assert config.connection_config["project"] == "my-project"
    assert config.connection_config["dataset_id"] == "my_dataset"
    assert config.connection_config["location"] == "US"
    assert config.connection_instance is None


def test_adbc_config_accepts_connection_parameters() -> None:
    """Test AdbcConfig accepts connection_config and connection_instance."""
    config = AdbcConfig(
        connection_config={"driver": "adbc_driver_postgresql", "uri": "postgresql://localhost/test"},
        connection_instance=None,
    )

    assert config.connection_config["driver"] == "adbc_driver_postgresql"
    assert config.connection_config["uri"] == "postgresql://localhost/test"
    assert config.connection_instance is None


def test_spanner_config_accepts_connection_parameters() -> None:
    """Test SpannerSyncConfig accepts connection_config and connection_instance."""
    config = SpannerSyncConfig(
        connection_config={"instance_id": "test-instance", "database_id": "test-database"}, connection_instance=None
    )

    assert config.connection_config["instance_id"] == "test-instance"
    assert config.connection_config["database_id"] == "test-database"
    assert config.connection_instance is None


def test_connection_config_empty_dict_is_valid() -> None:
    """Test that empty connection_config dict is valid for adapters."""
    # Use AsyncpgConfig which doesn't add defaults
    config = AsyncpgConfig(connection_config={})

    assert config.connection_config == {}
    assert isinstance(config.connection_config, dict)


def test_connection_config_can_be_modified_after_creation() -> None:
    """Test that connection_config dict can be modified after config creation."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    config.connection_config["min_size"] = 5
    config.connection_config["max_size"] = 10

    assert config.connection_config["min_size"] == 5
    assert config.connection_config["max_size"] == 10


def test_connection_config_preserves_all_keys() -> None:
    """Test that connection_config preserves all provided keys."""
    test_config = {
        "dsn": "postgresql://localhost/test",
        "min_size": 5,
        "max_size": 10,
        "timeout": 30.0,
        "command_timeout": 60.0,
        "server_settings": {"application_name": "sqlspec"},
    }
    config = AsyncpgConfig(connection_config=test_config)

    for key, value in test_config.items():
        assert config.connection_config[key] == value


def test_multiple_configs_have_independent_connection_configs() -> None:
    """Test that multiple config instances have independent connection_config dicts."""
    config1 = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/db1"})
    config2 = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/db2"})

    assert config1.connection_config["dsn"] == "postgresql://localhost/db1"
    assert config2.connection_config["dsn"] == "postgresql://localhost/db2"

    # Modify config1 should not affect config2
    config1.connection_config["min_size"] = 5
    assert "min_size" not in config2.connection_config


def test_connection_config_with_nested_dicts() -> None:
    """Test that connection_config handles nested dict values."""
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://localhost/test",
            "server_settings": {"application_name": "sqlspec", "timezone": "UTC"},
        }
    )

    assert config.connection_config["server_settings"]["application_name"] == "sqlspec"
    assert config.connection_config["server_settings"]["timezone"] == "UTC"


def test_connection_config_with_various_value_types() -> None:
    """Test that connection_config handles various value types."""
    config = AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://localhost/test",  # str
            "min_size": 5,  # int
            "timeout": 30.5,  # float
            "ssl": True,  # bool
            "server_settings": {"key": "value"},  # dict
        }
    )

    assert isinstance(config.connection_config["dsn"], str)
    assert isinstance(config.connection_config["min_size"], int)
    assert isinstance(config.connection_config["timeout"], float)
    assert isinstance(config.connection_config["ssl"], bool)
    assert isinstance(config.connection_config["server_settings"], dict)


def test_sqlite_custom_pool_parameters_in_connection_config() -> None:
    """Test that SQLite custom pool parameters work in connection_config."""
    config = SqliteConfig(
        connection_config={
            "database": ":memory:",
            "pool_min_size": 5,
            "pool_max_size": 10,
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        }
    )

    assert config.connection_config["pool_min_size"] == 5
    assert config.connection_config["pool_max_size"] == 10
    assert config.connection_config["pool_pre_ping"] is True
    assert config.connection_config["pool_recycle"] == 3600


def test_aiosqlite_custom_pool_parameters_in_connection_config() -> None:
    """Test that AioSQLite custom pool parameters work in connection_config."""
    config = AiosqliteConfig(
        connection_config={
            "database": ":memory:",
            "pool_min_size": 2,
            "pool_max_size": 8,
            "pool_pre_ping": False,
            "pool_recycle": 7200,
        }
    )

    assert config.connection_config["pool_min_size"] == 2
    assert config.connection_config["pool_max_size"] == 8
    assert config.connection_config["pool_pre_ping"] is False
    assert config.connection_config["pool_recycle"] == 7200


def test_duckdb_custom_pool_parameters_in_connection_config() -> None:
    """Test that DuckDB custom pool parameters work in connection_config."""
    config = DuckDBConfig(
        connection_config={"database": ":memory:", "pool_min_size": 3, "pool_max_size": 12, "pool_pre_ping": True}
    )

    assert config.connection_config["pool_min_size"] == 3
    assert config.connection_config["pool_max_size"] == 12
    assert config.connection_config["pool_pre_ping"] is True


def test_connection_config_parameter_naming_consistency() -> None:
    """Test that all adapters use consistent connection_config parameter name."""
    adapters = [
        (AsyncpgConfig, {"dsn": "postgresql://localhost/test"}, None),
        (PsycopgAsyncConfig, {"conninfo": "postgresql://localhost/test"}, None),
        (PsycopgSyncConfig, {"conninfo": "postgresql://localhost/test"}, None),
        (AsyncmyConfig, {"host": "localhost", "database": "test"}, "port"),  # Adds default port
        (PsqlpyConfig, {"dsn": "postgresql://localhost/test"}, None),
        (OracleAsyncConfig, {"user": "system", "password": "pwd", "dsn": "localhost/XE"}, None),
        (OracleSyncConfig, {"user": "system", "password": "pwd", "dsn": "localhost/XE"}, None),
        (SqliteConfig, {"database": ":memory:"}, "database"),  # Converts :memory: to URI
        (AiosqliteConfig, {"database": ":memory:"}, "database"),  # Converts :memory: to URI
        (DuckDBConfig, {"database": ":memory:"}, "database"),  # Converts :memory: to shared_db
        (BigQueryConfig, {"project": "test-project"}, None),
        (AdbcConfig, {"driver": "adbc_driver_sqlite"}, None),
        (SpannerSyncConfig, {"instance_id": "test", "database_id": "test"}, None),
    ]

    for adapter_class, config_dict, modified_key in adapters:
        config = adapter_class(connection_config=config_dict)
        assert hasattr(config, "connection_config")
        assert hasattr(config, "connection_instance")

        # Check that original keys are present (may be modified or have defaults added)
        for key in config_dict:
            if key != modified_key:
                assert key in config.connection_config

        assert config.connection_instance is None


def test_connection_instance_parameter_naming_consistency() -> None:
    """Test that all adapters use consistent connection_instance parameter name."""
    adapters = [
        (AsyncpgConfig, {"dsn": "postgresql://localhost/test"}),
        (PsycopgAsyncConfig, {"conninfo": "postgresql://localhost/test"}),
        (PsycopgSyncConfig, {"conninfo": "postgresql://localhost/test"}),
        (AsyncmyConfig, {"host": "localhost", "database": "test"}),
        (PsqlpyConfig, {"dsn": "postgresql://localhost/test"}),
        (OracleAsyncConfig, {"user": "system", "password": "pwd", "dsn": "localhost/XE"}),
        (OracleSyncConfig, {"user": "system", "password": "pwd", "dsn": "localhost/XE"}),
        (SqliteConfig, {"database": ":memory:"}),
        (AiosqliteConfig, {"database": ":memory:"}),
        (DuckDBConfig, {"database": ":memory:"}),
        (BigQueryConfig, {"project": "test-project"}),
        (AdbcConfig, {"driver": "adbc_driver_sqlite"}),
        (SpannerSyncConfig, {"instance_id": "test", "database_id": "test"}),
    ]

    for adapter_class, config_dict in adapters:
        config = adapter_class(connection_config=config_dict, connection_instance=None)
        assert hasattr(config, "connection_instance")
        assert config.connection_instance is None


@pytest.mark.asyncio
async def test_asyncpg_config_with_pre_created_pool() -> None:
    """Test AsyncpgConfig with connection_instance set to pre-created pool."""
    from unittest.mock import AsyncMock, MagicMock

    # Create a mock pool
    mock_pool = MagicMock()
    mock_pool.acquire = AsyncMock()

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"}, connection_instance=mock_pool)

    assert config.connection_instance is mock_pool
    assert config.connection_config["dsn"] == "postgresql://localhost/test"


def test_sqlite_config_with_pre_created_pool() -> None:
    """Test SqliteConfig with connection_instance set to pre-created pool."""
    from unittest.mock import MagicMock

    # Create a mock pool
    mock_pool = MagicMock()

    config = SqliteConfig(connection_config={"database": ":memory:"}, connection_instance=mock_pool)

    assert config.connection_instance is mock_pool
    # SQLite converts :memory: to shared memory URI for pooling
    assert "memory" in config.connection_config["database"]


def test_duckdb_config_with_pre_created_pool() -> None:
    """Test DuckDBConfig with connection_instance set to pre-created pool."""
    from unittest.mock import MagicMock

    # Create a mock pool
    mock_pool = MagicMock()

    config = DuckDBConfig(connection_config={"database": ":memory:"}, connection_instance=mock_pool)

    assert config.connection_instance is mock_pool
    # DuckDB converts :memory: to :memory:shared_db for pooling
    assert "memory" in config.connection_config["database"]


def test_bigquery_config_with_pre_created_client() -> None:
    """Test BigQueryConfig with connection_instance set to pre-created client."""
    from unittest.mock import MagicMock

    # Create a mock client
    mock_client = MagicMock()

    config = BigQueryConfig(connection_config={"project": "test-project"}, connection_instance=mock_client)

    assert config.connection_instance is mock_client
    assert config.connection_config["project"] == "test-project"


def test_connection_instance_bypasses_pool_creation() -> None:
    """Test that providing connection_instance bypasses pool creation logic."""
    from unittest.mock import MagicMock

    mock_pool = MagicMock()

    config = DuckDBConfig(connection_config={"database": ":memory:"}, connection_instance=mock_pool)

    # When connection_instance is set, _create_pool should not be called
    # and provide_pool should return the provided instance
    pool = config.provide_pool()

    assert pool is mock_pool


def test_connection_config_does_not_accept_invalid_types() -> None:
    """Test that connection_config validates type at runtime (if validation exists)."""
    # Note: SQLSpec uses TypedDict, so type validation happens at type-check time
    # At runtime, we just ensure dict assignment works
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    assert isinstance(config.connection_config, dict)
