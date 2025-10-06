"""Test extension_config parameter support across all adapters."""

from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig


def test_sqlite_extension_config() -> None:
    """Test SqliteConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"session_key": "custom_session", "commit_mode": "manual"}}

    config = SqliteConfig(pool_config={"database": ":memory:"}, extension_config=extension_config)

    assert config.extension_config == extension_config
    assert config.extension_config["litestar"]["session_key"] == "custom_session"


def test_aiosqlite_extension_config() -> None:
    """Test AiosqliteConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"pool_key": "db_pool", "enable_correlation_middleware": False}}

    config = AiosqliteConfig(pool_config={"database": ":memory:"}, extension_config=extension_config)

    assert config.extension_config == extension_config
    assert config.extension_config["litestar"]["pool_key"] == "db_pool"


def test_duckdb_extension_config() -> None:
    """Test DuckDBConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"connection_key": "duckdb_conn"}}

    config = DuckDBConfig(pool_config={"database": ":memory:"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_asyncpg_extension_config() -> None:
    """Test AsyncpgConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"commit_mode": "autocommit"}}

    config = AsyncpgConfig(pool_config={"host": "localhost", "database": "test"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_psycopg_sync_extension_config() -> None:
    """Test PsycopgSyncConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"session_key": "psycopg_session"}}

    config = PsycopgSyncConfig(pool_config={"host": "localhost", "dbname": "test"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_psycopg_async_extension_config() -> None:
    """Test PsycopgAsyncConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"extra_commit_statuses": {201, 202}}}

    config = PsycopgAsyncConfig(pool_config={"host": "localhost", "dbname": "test"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_asyncmy_extension_config() -> None:
    """Test AsyncmyConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"commit_mode": "autocommit_include_redirect"}}

    config = AsyncmyConfig(pool_config={"host": "localhost", "database": "test"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_psqlpy_extension_config() -> None:
    """Test PsqlpyConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"extra_rollback_statuses": {400, 500}}}

    config = PsqlpyConfig(pool_config={"host": "localhost", "db_name": "test"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_oracle_sync_extension_config() -> None:
    """Test OracleSyncConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"enable_correlation_middleware": True}}

    config = OracleSyncConfig(pool_config={"user": "test", "password": "test"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_oracle_async_extension_config() -> None:
    """Test OracleAsyncConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"connection_key": "oracle_async"}}

    config = OracleAsyncConfig(pool_config={"user": "test", "password": "test"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_adbc_extension_config() -> None:
    """Test AdbcConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"session_key": "adbc_session"}}

    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": "sqlite://:memory:"}, extension_config=extension_config
    )

    assert config.extension_config == extension_config


def test_bigquery_extension_config() -> None:
    """Test BigQueryConfig accepts and stores extension_config."""
    extension_config = {"litestar": {"pool_key": "bigquery_pool"}}

    config = BigQueryConfig(connection_config={"project": "test-project"}, extension_config=extension_config)

    assert config.extension_config == extension_config


def test_extension_config_defaults_to_empty_dict() -> None:
    """Test that extension_config defaults to empty dict when not provided."""
    configs = [
        SqliteConfig(pool_config={"database": ":memory:"}),
        DuckDBConfig(pool_config={"database": ":memory:"}),
        AiosqliteConfig(pool_config={"database": ":memory:"}),
        AsyncpgConfig(pool_config={"host": "localhost"}),
        PsycopgSyncConfig(pool_config={"host": "localhost"}),
        PsycopgAsyncConfig(pool_config={"host": "localhost"}),
        AsyncmyConfig(pool_config={"host": "localhost"}),
        PsqlpyConfig(pool_config={"host": "localhost"}),
        OracleSyncConfig(pool_config={"user": "test", "password": "test"}),
        OracleAsyncConfig(pool_config={"user": "test", "password": "test"}),
        AdbcConfig(connection_config={"driver_name": "sqlite", "uri": "sqlite://:memory:"}),
        BigQueryConfig(connection_config={"project": "test"}),
    ]

    for config in configs:
        assert hasattr(config, "extension_config")
        assert config.extension_config == {}


def test_extension_config_with_multiple_extensions() -> None:
    """Test extension_config can hold multiple extension configurations."""
    extension_config: dict[str, dict[str, Any]] = {
        "litestar": {"session_key": "db_session", "commit_mode": "manual"},
        "custom_extension": {"setting1": "value1", "setting2": 42},
        "another_ext": {"enabled": True},
    }

    config = SqliteConfig(pool_config={"database": ":memory:"}, extension_config=extension_config)

    assert config.extension_config == extension_config
    assert len(config.extension_config) == 3
    assert "litestar" in config.extension_config
    assert "custom_extension" in config.extension_config
    assert "another_ext" in config.extension_config


@pytest.mark.parametrize(
    "config_class,init_kwargs",
    [
        (SqliteConfig, {"pool_config": {"database": ":memory:"}}),
        (AiosqliteConfig, {"pool_config": {"database": ":memory:"}}),
        (DuckDBConfig, {"pool_config": {"database": ":memory:"}}),
        (AsyncpgConfig, {"pool_config": {"host": "localhost"}}),
        (PsycopgSyncConfig, {"pool_config": {"host": "localhost"}}),
        (PsycopgAsyncConfig, {"pool_config": {"host": "localhost"}}),
        (AsyncmyConfig, {"pool_config": {"host": "localhost"}}),
        (PsqlpyConfig, {"pool_config": {"host": "localhost"}}),
        (OracleSyncConfig, {"pool_config": {"user": "test", "password": "test"}}),
        (OracleAsyncConfig, {"pool_config": {"user": "test", "password": "test"}}),
        (AdbcConfig, {"connection_config": {"driver_name": "sqlite", "uri": "sqlite://:memory:"}}),
        (BigQueryConfig, {"connection_config": {"project": "test"}}),
    ],
)
def test_all_adapters_accept_extension_config(config_class: type, init_kwargs: dict) -> None:
    """Parameterized test ensuring all adapters accept extension_config."""
    extension_config = {"test_extension": {"test_key": "test_value"}}

    config = config_class(**init_kwargs, extension_config=extension_config)

    assert hasattr(config, "extension_config")
    assert config.extension_config == extension_config
