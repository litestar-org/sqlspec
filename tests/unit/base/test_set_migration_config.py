"""Tests for set_migration_config() method on database config classes."""

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig

pytestmark = pytest.mark.xdist_group("base")


def test_set_migration_config_after_construction() -> None:
    """set_migration_config attaches config to an already-created instance."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    assert config.migration_config == {}

    config.set_migration_config({"script_location": "db/migrations", "version_table_name": "my_versions"})

    assert config.migration_config["script_location"] == "db/migrations"
    assert config.migration_config["version_table_name"] == "my_versions"


def test_set_migration_config_equivalent_to_constructor() -> None:
    """set_migration_config produces the same result as passing config at construction."""
    migration = {"script_location": "migrations", "version_table_name": "schema_version"}

    config_at_init = SqliteConfig(connection_config={"database": ":memory:"}, migration_config=migration)

    config_post_init = SqliteConfig(connection_config={"database": ":memory:"})
    config_post_init.set_migration_config(migration)

    assert config_at_init.migration_config == config_post_init.migration_config


def test_set_migration_config_overwrites_previous() -> None:
    """Calling set_migration_config replaces any previously set config."""
    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": "old_migrations"}
    )
    assert config.migration_config["script_location"] == "old_migrations"

    config.set_migration_config({"script_location": "new_migrations"})

    assert config.migration_config["script_location"] == "new_migrations"
    assert "version_table_name" not in config.migration_config


def test_set_migration_config_with_typed_dict() -> None:
    """set_migration_config works with MigrationConfig TypedDict values."""
    from sqlspec.config import MigrationConfig

    mc: MigrationConfig = {"script_location": "alembic", "enabled": True}
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.set_migration_config(mc)

    assert config.migration_config["script_location"] == "alembic"
    assert config.migration_config["enabled"] is True
