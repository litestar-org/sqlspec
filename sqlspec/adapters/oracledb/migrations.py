"""Oracle-specific migration implementations.

This module provides Oracle Database-specific overrides for migration functionality
to handle Oracle's unique SQL syntax requirements.
"""

import getpass
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlspec._sql import sql
from sqlspec.builder._ddl import CreateTable
from sqlspec.migrations.base import BaseMigrationTracker
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("OracleAsyncMigrationTracker", "OracleSyncMigrationTracker")

logger = get_logger("migrations.oracle")


class OracleMigrationTrackerMixin:
    """Mixin providing Oracle-specific migration table creation."""

    __slots__ = ()

    version_table: str

    def _get_create_table_sql(self) -> CreateTable:
        """Get Oracle-specific SQL builder for creating the tracking table.

        Oracle doesn't support:
        - CREATE TABLE IF NOT EXISTS (need try/catch logic)
        - TEXT type (use VARCHAR2)
        - DEFAULT before NOT NULL is required

        Returns:
            SQL builder object for Oracle table creation.
        """
        return (
            sql.create_table(self.version_table)
            .column("version_num", "VARCHAR2(32)", primary_key=True)
            .column("description", "VARCHAR2(2000)")
            .column("applied_at", "TIMESTAMP", default="CURRENT_TIMESTAMP")
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR2(64)")
            .column("applied_by", "VARCHAR2(255)")
        )


class OracleSyncMigrationTracker(OracleMigrationTrackerMixin, BaseMigrationTracker["SyncDriverAdapterBase"]):
    """Oracle-specific sync migration tracker."""

    __slots__ = ()

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Oracle doesn't support IF NOT EXISTS, so we check for table existence first.

        Args:
            driver: The database driver to use.
        """

        check_sql = (
            sql.select(sql.count().as_("table_count"))
            .from_("user_tables")
            .where(sql.column("table_name") == self.version_table.upper())
        )
        result = driver.execute(check_sql)

        if result.data[0]["TABLE_COUNT"] == 0:
            driver.execute(self._get_create_table_sql())
            self._safe_commit(driver)

    def get_current_version(self, driver: "SyncDriverAdapterBase") -> "Optional[str]":
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current migration version or None if no migrations applied.
        """
        result = driver.execute(self._get_current_version_sql())
        return result.data[0]["VERSION_NUM"] if result.data else None

    def get_applied_migrations(self, driver: "SyncDriverAdapterBase") -> "list[dict[str, Any]]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records as dictionaries.
        """
        result = driver.execute(self._get_applied_migrations_sql())
        if not result.data:
            return []

        normalized_data = [{key.lower(): value for key, value in row.items()} for row in result.data]

        return cast("list[dict[str, Any]]", normalized_data)

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

        applied_by = getpass.getuser()

        record_sql = self._get_record_migration_sql(version, description, execution_time_ms, checksum, applied_by)
        driver.execute(record_sql)
        self._safe_commit(driver)

    def remove_migration(self, driver: "SyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record.

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        remove_sql = self._get_remove_migration_sql(version)
        driver.execute(remove_sql)
        self._safe_commit(driver)

    def _safe_commit(self, driver: "SyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        try:
            # Check driver features first (preferred approach)
            if driver.driver_features.get("autocommit", False):
                return

            # Fallback to connection-level autocommit check
            if driver.connection and driver.connection.autocommit:
                return

            driver.commit()
        except Exception:
            logger.debug("Failed to commit transaction, likely due to autocommit being enabled")


class OracleAsyncMigrationTracker(OracleMigrationTrackerMixin, BaseMigrationTracker["AsyncDriverAdapterBase"]):
    """Oracle-specific async migration tracker."""

    __slots__ = ()

    async def ensure_tracking_table(self, driver: "AsyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Oracle doesn't support IF NOT EXISTS, so we check for table existence first.

        Args:
            driver: The database driver to use.
        """

        check_sql = (
            sql.select(sql.count().as_("table_count"))
            .from_("user_tables")
            .where(sql.column("table_name") == self.version_table.upper())
        )
        result = await driver.execute(check_sql)

        if result.data[0]["TABLE_COUNT"] == 0:
            await driver.execute(self._get_create_table_sql())
            await self._safe_commit_async(driver)

    async def get_current_version(self, driver: "AsyncDriverAdapterBase") -> "Optional[str]":
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current migration version or None if no migrations applied.
        """
        result = await driver.execute(self._get_current_version_sql())
        return result.data[0]["VERSION_NUM"] if result.data else None

    async def get_applied_migrations(self, driver: "AsyncDriverAdapterBase") -> "list[dict[str, Any]]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records as dictionaries.
        """
        result = await driver.execute(self._get_applied_migrations_sql())
        if not result.data:
            return []

        normalized_data = [{key.lower(): value for key, value in row.items()} for row in result.data]

        return cast("list[dict[str, Any]]", normalized_data)

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

        applied_by = getpass.getuser()

        record_sql = self._get_record_migration_sql(version, description, execution_time_ms, checksum, applied_by)
        await driver.execute(record_sql)
        await self._safe_commit_async(driver)

    async def remove_migration(self, driver: "AsyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record.

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        remove_sql = self._get_remove_migration_sql(version)
        await driver.execute(remove_sql)
        await self._safe_commit_async(driver)

    async def _safe_commit_async(self, driver: "AsyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        try:
            # Check driver features first (preferred approach)
            if driver.driver_features.get("autocommit", False):
                return

            # Fallback to connection-level autocommit check
            if driver.connection and driver.connection.autocommit:
                return

            await driver.commit()
        except Exception:
            logger.debug("Failed to commit transaction, likely due to autocommit being enabled")
