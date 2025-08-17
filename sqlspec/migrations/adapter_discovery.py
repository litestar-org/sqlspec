"""Adapter-specific migration discovery and loading.

This module provides functionality to discover and load adapter-specific
migration implementations when available.
"""

import importlib
from typing import TYPE_CHECKING, Any, cast

from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.migrations.base import BaseMigrationTracker

logger = get_logger("migrations.adapter_discovery")

__all__ = ("discover_migration_tracker",)


def discover_migration_tracker(config: Any, sync: bool = True) -> "type[BaseMigrationTracker[Any]]":
    """Discover and return adapter-specific migration tracker if available.

    Args:
        config: The SQLSpec configuration object.
        sync: Whether to discover sync (True) or async (False) tracker.

    Returns:
        Adapter-specific tracker class or default tracker class.
    """
    # Extract adapter name from config class
    config_class_name = type(config).__name__

    # Map config class names to adapter module names
    adapter_mapping = {
        "SqliteConfig": "sqlite",
        "DuckDBConfig": "duckdb",
        "PsycopgSyncConfig": "psycopg",
        "PsycopgAsyncConfig": "psycopg",
        "AsyncpgConfig": "asyncpg",
        "PsqlpyConfig": "psqlpy",
        "AsyncmyConfig": "asyncmy",
        "AiosqliteConfig": "aiosqlite",
        "OracleSyncConfig": "oracledb",
        "OracleAsyncConfig": "oracledb",
        "ADBCConfig": "adbc",
        "BigQueryConfig": "bigquery",
    }

    adapter_name = adapter_mapping.get(config_class_name)

    if not adapter_name:
        logger.debug("No adapter mapping found for config %s, using default tracker", config_class_name)
        return SyncMigrationTracker if sync else AsyncMigrationTracker

    # Try to import adapter-specific migrations module
    try:
        module_path = f"sqlspec.adapters.{adapter_name}.migrations"
        migrations_module = importlib.import_module(module_path)

        # Look for adapter-specific tracker classes
        if sync:
            tracker_class_names = [
                "OracleSyncMigrationTracker"
                if adapter_name == "oracledb"
                else f"{adapter_name.title()}SyncMigrationTracker",
                f"{adapter_name.upper()}SyncMigrationTracker",
                "SyncMigrationTracker",
            ]
        else:
            tracker_class_names = [
                "OracleAsyncMigrationTracker"
                if adapter_name == "oracledb"
                else f"{adapter_name.title()}AsyncMigrationTracker",
                f"{adapter_name.upper()}AsyncMigrationTracker",
                "AsyncMigrationTracker",
            ]

        for class_name in tracker_class_names:
            if hasattr(migrations_module, class_name):
                tracker_class = getattr(migrations_module, class_name)
                logger.debug("Using adapter-specific tracker: %s.%s", module_path, class_name)
                return cast("type[BaseMigrationTracker[Any]]", tracker_class)

        logger.debug("No suitable tracker class found in %s, using default", module_path)

    except ImportError:
        logger.debug("No adapter-specific migrations module found for %s, using default tracker", adapter_name)
    except Exception as e:
        logger.warning("Error loading adapter-specific migrations for %s: %s", adapter_name, e)

    # Fall back to default tracker
    return SyncMigrationTracker if sync else AsyncMigrationTracker
