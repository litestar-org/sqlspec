"""Migration version tracking for SQLSpec.

This module provides functionality to track applied migrations in the database.
"""

import os
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.migrations.base import BaseMigrationTracker
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("AsyncMigrationTracker", "SyncMigrationTracker")

logger = get_logger("migrations.tracker")


class SyncMigrationTracker(BaseMigrationTracker["SyncDriverAdapterBase"]):
    """Synchronous migration version tracker."""

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Args:
            driver: The database driver to use.
        """
        driver.execute(self._get_create_table_sql())
        self._safe_commit(driver)

    def get_current_version(self, driver: "SyncDriverAdapterBase") -> Optional[str]:
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current version number or None if no migrations applied.
        """
        result = driver.execute(self._get_current_version_sql())
        return result.data[0]["version_num"] if result.data else None

    def get_applied_migrations(self, driver: "SyncDriverAdapterBase") -> "list[dict[str, Any]]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records.
        """
        result = driver.execute(self._get_applied_migrations_sql())
        return result.data or []

    def record_migration(
        self, driver: "SyncDriverAdapterBase", version: str, description: str, execution_time_ms: int, checksum: str
    ) -> None:
        """Record a successfully applied migration.

        Args:
            driver: The database driver to use.
            version: Version number of the migration.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: MD5 checksum of the migration content.
        """
        driver.execute(
            self._get_record_migration_sql(
                version, description, execution_time_ms, checksum, os.environ.get("USER", "unknown")
            )
        )
        self._safe_commit(driver)

    def remove_migration(self, driver: "SyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record (used during downgrade).

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        driver.execute(self._get_remove_migration_sql(version))
        self._safe_commit(driver)

    def _safe_commit(self, driver: "SyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        try:
            connection = getattr(driver, "connection", None)
            if connection and hasattr(connection, "autocommit") and getattr(connection, "autocommit", False):
                return

            driver_features = getattr(driver, "driver_features", {})
            if driver_features and driver_features.get("autocommit", False):
                return

            driver.commit()
        except Exception:
            logger.debug("Failed to commit transaction, likely due to autocommit being enabled")


class AsyncMigrationTracker(BaseMigrationTracker["AsyncDriverAdapterBase"]):
    """Asynchronous migration version tracker."""

    async def ensure_tracking_table(self, driver: "AsyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Args:
            driver: The database driver to use.
        """
        await driver.execute(self._get_create_table_sql())
        await self._safe_commit_async(driver)

    async def get_current_version(self, driver: "AsyncDriverAdapterBase") -> Optional[str]:
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current version number or None if no migrations applied.
        """
        result = await driver.execute(self._get_current_version_sql())
        return result.data[0]["version_num"] if result.data else None

    async def get_applied_migrations(self, driver: "AsyncDriverAdapterBase") -> "list[dict[str, Any]]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records.
        """
        result = await driver.execute(self._get_applied_migrations_sql())
        return result.data or []

    async def record_migration(
        self, driver: "AsyncDriverAdapterBase", version: str, description: str, execution_time_ms: int, checksum: str
    ) -> None:
        """Record a successfully applied migration.

        Args:
            driver: The database driver to use.
            version: Version number of the migration.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: MD5 checksum of the migration content.
        """
        await driver.execute(
            self._get_record_migration_sql(
                version, description, execution_time_ms, checksum, os.environ.get("USER", "unknown")
            )
        )
        await self._safe_commit_async(driver)

    async def remove_migration(self, driver: "AsyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record (used during downgrade).

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        await driver.execute(self._get_remove_migration_sql(version))
        await self._safe_commit_async(driver)

    async def _safe_commit_async(self, driver: "AsyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        try:
            connection = getattr(driver, "connection", None)
            if connection and hasattr(connection, "autocommit") and getattr(connection, "autocommit", False):
                return

            driver_features = getattr(driver, "driver_features", {})
            if driver_features and driver_features.get("autocommit", False):
                return

            await driver.commit()
        except Exception:
            logger.debug("Failed to commit transaction, likely due to autocommit being enabled")
