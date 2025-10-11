"""Oracle-specific migration implementations.

This module provides Oracle Database-specific overrides for migration functionality
to handle Oracle's unique SQL syntax requirements.
"""

import getpass
from typing import TYPE_CHECKING, Any, cast

from sqlspec.builder import CreateTable, sql
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
            .column("version_type", "VARCHAR2(16)")
            .column("execution_sequence", "INTEGER")
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

        Uses a PL/SQL block to make the operation atomic and prevent race conditions.

        Args:
            driver: The database driver to use.
        """
        create_script = f"""
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE {self.version_table} (
                version_num VARCHAR2(32) PRIMARY KEY,
                description VARCHAR2(2000),
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                execution_time_ms INTEGER,
                checksum VARCHAR2(64),
                applied_by VARCHAR2(255)
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -955 THEN
                    NULL; -- Table already exists
                ELSE
                    RAISE;
                END IF;
        END;
        """
        driver.execute_script(create_script)
        driver.commit()

    def get_current_version(self, driver: "SyncDriverAdapterBase") -> "str | None":
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
        driver.commit()

    def remove_migration(self, driver: "SyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record.

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        remove_sql = self._get_remove_migration_sql(version)
        driver.execute(remove_sql)
        driver.commit()


class OracleAsyncMigrationTracker(OracleMigrationTrackerMixin, BaseMigrationTracker["AsyncDriverAdapterBase"]):
    """Oracle-specific async migration tracker."""

    __slots__ = ()

    async def ensure_tracking_table(self, driver: "AsyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Uses a PL/SQL block to make the operation atomic and prevent race conditions.

        Args:
            driver: The database driver to use.
        """
        create_script = f"""
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE {self.version_table} (
                version_num VARCHAR2(32) PRIMARY KEY,
                description VARCHAR2(2000),
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                execution_time_ms INTEGER,
                checksum VARCHAR2(64),
                applied_by VARCHAR2(255)
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -955 THEN
                    NULL; -- Table already exists
                ELSE
                    RAISE;
                END IF;
        END;
        """
        await driver.execute_script(create_script)
        await driver.commit()

    async def get_current_version(self, driver: "AsyncDriverAdapterBase") -> "str | None":
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
        await driver.commit()

    async def remove_migration(self, driver: "AsyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record.

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        remove_sql = self._get_remove_migration_sql(version)
        await driver.execute(remove_sql)
        await driver.commit()
