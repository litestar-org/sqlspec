"""Migration version tracking for SQLSpec.

This module provides functionality to track applied migrations in the database.
"""

import os
from typing import TYPE_CHECKING, Any

from rich.console import Console

from sqlspec.builder import sql
from sqlspec.migrations.base import BaseMigrationTracker
from sqlspec.utils.logging import get_logger
from sqlspec.utils.version import parse_version

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("AsyncMigrationTracker", "SyncMigrationTracker")

logger = get_logger("migrations.tracker")


class SyncMigrationTracker(BaseMigrationTracker["SyncDriverAdapterBase"]):
    """Synchronous migration version tracker."""

    def _migrate_schema_if_needed(self, driver: "SyncDriverAdapterBase") -> None:
        """Check for and add any missing columns to the tracking table.

        Uses the adapter's data_dictionary to query existing columns,
        then compares to the target schema and adds missing columns one by one.

        Args:
            driver: The database driver to use.
        """
        try:
            columns_data = driver.data_dictionary.get_columns(driver, self.version_table)
            if not columns_data:
                logger.debug("Migration tracking table does not exist yet")
                return

            existing_columns = {col["column_name"] for col in columns_data}
            missing_columns = self._detect_missing_columns(existing_columns)

            if not missing_columns:
                logger.debug("Migration tracking table schema is up-to-date")
                return

            console = Console()
            console.print(
                f"[cyan]Migrating tracking table schema, adding columns: {', '.join(sorted(missing_columns))}[/]"
            )

            for col_name in sorted(missing_columns):
                self._add_column(driver, col_name)

            driver.commit()
            console.print("[green]Migration tracking table schema updated successfully[/]")

        except Exception:
            logger.exception("Could not check or migrate tracking table schema")

    def _add_column(self, driver: "SyncDriverAdapterBase", column_name: str) -> None:
        """Add a single column to the tracking table.

        Args:
            driver: The database driver to use.
            column_name: Name of the column to add (lowercase).
        """
        target_create = self._get_create_table_sql()
        column_def = next((col for col in target_create.columns if col.name.lower() == column_name), None)

        if not column_def:
            return

        alter_sql = sql.alter_table(self.version_table).add_column(
            name=column_def.name, dtype=column_def.dtype, default=column_def.default, not_null=column_def.not_null
        )
        driver.execute(alter_sql)
        logger.debug("Added column %s to tracking table", column_name)

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Also checks for and adds any missing columns to support schema migrations.

        Args:
            driver: The database driver to use.
        """
        driver.execute(self._get_create_table_sql())
        self._safe_commit(driver)

        self._migrate_schema_if_needed(driver)

    def get_current_version(self, driver: "SyncDriverAdapterBase") -> str | None:
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

        Parses version to determine type (sequential or timestamp) and
        auto-increments execution_sequence for application order tracking.

        Args:
            driver: The database driver to use.
            version: Version number of the migration.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: MD5 checksum of the migration content.
        """
        parsed_version = parse_version(version)
        version_type = parsed_version.type.value

        result = driver.execute(self._get_next_execution_sequence_sql())
        next_sequence = result.data[0]["next_seq"] if result.data else 1

        driver.execute(
            self._get_record_migration_sql(
                version,
                version_type,
                next_sequence,
                description,
                execution_time_ms,
                checksum,
                os.environ.get("USER", "unknown"),
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

    def update_version_record(self, driver: "SyncDriverAdapterBase", old_version: str, new_version: str) -> None:
        """Update migration version record from timestamp to sequential.

        Updates version_num and version_type while preserving execution_sequence,
        applied_at, and other tracking metadata. Used during fix command.

        Idempotent: If the version is already updated, logs and continues without error.
        This allows fix command to be safely re-run after pulling changes.

        Args:
            driver: The database driver to use.
            old_version: Current timestamp version string.
            new_version: New sequential version string.

        Raises:
            ValueError: If neither old_version nor new_version found in database.
        """
        parsed_new_version = parse_version(new_version)
        new_version_type = parsed_new_version.type.value

        result = driver.execute(self._get_update_version_sql(old_version, new_version, new_version_type))

        if result.rows_affected == 0:
            check_result = driver.execute(self._get_applied_migrations_sql())
            applied_versions = {row["version_num"] for row in check_result.data} if check_result.data else set()

            if new_version in applied_versions:
                logger.debug("Version already updated: %s -> %s", old_version, new_version)
                return

            msg = f"Migration version {old_version} not found in database"
            raise ValueError(msg)

        self._safe_commit(driver)
        logger.debug("Updated version record: %s -> %s", old_version, new_version)

    def _safe_commit(self, driver: "SyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        if driver.driver_features.get("autocommit", False):
            return

        try:
            driver.commit()
        except Exception:
            logger.debug("Failed to commit transaction, likely due to autocommit being enabled")


class AsyncMigrationTracker(BaseMigrationTracker["AsyncDriverAdapterBase"]):
    """Asynchronous migration version tracker."""

    async def _migrate_schema_if_needed(self, driver: "AsyncDriverAdapterBase") -> None:
        """Check for and add any missing columns to the tracking table.

        Uses the driver's data_dictionary to query existing columns,
        then compares to the target schema and adds missing columns one by one.

        Args:
            driver: The database driver to use.
        """
        try:
            columns_data = await driver.data_dictionary.get_columns(driver, self.version_table)
            if not columns_data:
                logger.debug("Migration tracking table does not exist yet")
                return

            existing_columns = {col["column_name"] for col in columns_data}
            missing_columns = self._detect_missing_columns(existing_columns)

            if not missing_columns:
                logger.debug("Migration tracking table schema is up-to-date")
                return

            console = Console()
            console.print(
                f"[cyan]Migrating tracking table schema, adding columns: {', '.join(sorted(missing_columns))}[/]"
            )

            for col_name in sorted(missing_columns):
                await self._add_column(driver, col_name)

            await driver.commit()
            console.print("[green]Migration tracking table schema updated successfully[/]")

        except Exception:
            logger.exception("Could not check or migrate tracking table schema")

    async def _add_column(self, driver: "AsyncDriverAdapterBase", column_name: str) -> None:
        """Add a single column to the tracking table.

        Args:
            driver: The database driver to use.
            column_name: Name of the column to add (lowercase).
        """
        target_create = self._get_create_table_sql()
        column_def = next((col for col in target_create.columns if col.name.lower() == column_name), None)

        if not column_def:
            return

        alter_sql = sql.alter_table(self.version_table).add_column(
            name=column_def.name, dtype=column_def.dtype, default=column_def.default, not_null=column_def.not_null
        )
        await driver.execute(alter_sql)
        logger.debug("Added column %s to tracking table", column_name)

    async def ensure_tracking_table(self, driver: "AsyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Also checks for and adds any missing columns to support schema migrations.

        Args:
            driver: The database driver to use.
        """
        await driver.execute(self._get_create_table_sql())
        await self._safe_commit_async(driver)

        await self._migrate_schema_if_needed(driver)

    async def get_current_version(self, driver: "AsyncDriverAdapterBase") -> str | None:
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

        Parses version to determine type (sequential or timestamp) and
        auto-increments execution_sequence for application order tracking.

        Args:
            driver: The database driver to use.
            version: Version number of the migration.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: MD5 checksum of the migration content.
        """
        parsed_version = parse_version(version)
        version_type = parsed_version.type.value

        result = await driver.execute(self._get_next_execution_sequence_sql())
        next_sequence = result.data[0]["next_seq"] if result.data else 1

        await driver.execute(
            self._get_record_migration_sql(
                version,
                version_type,
                next_sequence,
                description,
                execution_time_ms,
                checksum,
                os.environ.get("USER", "unknown"),
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

    async def update_version_record(self, driver: "AsyncDriverAdapterBase", old_version: str, new_version: str) -> None:
        """Update migration version record from timestamp to sequential.

        Updates version_num and version_type while preserving execution_sequence,
        applied_at, and other tracking metadata. Used during fix command.

        Idempotent: If the version is already updated, logs and continues without error.
        This allows fix command to be safely re-run after pulling changes.

        Args:
            driver: The database driver to use.
            old_version: Current timestamp version string.
            new_version: New sequential version string.

        Raises:
            ValueError: If neither old_version nor new_version found in database.
        """
        parsed_new_version = parse_version(new_version)
        new_version_type = parsed_new_version.type.value

        result = await driver.execute(self._get_update_version_sql(old_version, new_version, new_version_type))

        if result.rows_affected == 0:
            check_result = await driver.execute(self._get_applied_migrations_sql())
            applied_versions = {row["version_num"] for row in check_result.data} if check_result.data else set()

            if new_version in applied_versions:
                logger.debug("Version already updated: %s -> %s", old_version, new_version)
                return

            msg = f"Migration version {old_version} not found in database"
            raise ValueError(msg)

        await self._safe_commit_async(driver)
        logger.debug("Updated version record: %s -> %s", old_version, new_version)

    async def _safe_commit_async(self, driver: "AsyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        if driver.driver_features.get("autocommit", False):
            return

        try:
            await driver.commit()
        except Exception:
            logger.debug("Failed to commit transaction, likely due to autocommit being enabled")
