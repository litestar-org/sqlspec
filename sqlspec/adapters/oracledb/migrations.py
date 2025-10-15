"""Oracle-specific migration implementations.

This module provides Oracle Database-specific overrides for migration functionality
to handle Oracle's unique SQL syntax requirements.
"""

import getpass
from typing import TYPE_CHECKING, Any

from sqlspec.builder import CreateTable, Select, sql
from sqlspec.migrations.base import BaseMigrationTracker
from sqlspec.utils.logging import get_logger
from sqlspec.utils.version import parse_version

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("OracleAsyncMigrationTracker", "OracleSyncMigrationTracker")

logger = get_logger("migrations.oracle")


class OracleMigrationTrackerMixin:
    """Mixin providing Oracle-specific migration table creation and querying.

    Oracle has unique identifier handling rules:
    - Unquoted identifiers are case-insensitive and stored as UPPERCASE
    - Quoted identifiers are case-sensitive and stored exactly as written

    This mixin overrides SQL builder methods to add quoted identifiers for
    all column references, ensuring they match the lowercase column names
    created by the migration table.
    """

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

    def _get_current_version_sql(self) -> Select:
        """Get Oracle-specific SQL for retrieving current version.

        Uses unquoted identifiers that Oracle will automatically convert to uppercase.

        Returns:
            SQL builder object for version query.
        """
        return sql.select("VERSION_NUM").from_(self.version_table).order_by("EXECUTION_SEQUENCE DESC").limit(1)

    def _get_applied_migrations_sql(self) -> Select:
        """Get Oracle-specific SQL for retrieving all applied migrations.

        Uses unquoted identifiers that Oracle will automatically convert to uppercase.

        Returns:
            SQL builder object for migrations query.
        """
        return sql.select("*").from_(self.version_table).order_by("EXECUTION_SEQUENCE")

    def _get_next_execution_sequence_sql(self) -> Select:
        """Get Oracle-specific SQL for retrieving next execution sequence.

        Uses unquoted identifiers that Oracle will automatically convert to uppercase.

        Returns:
            SQL builder object for sequence query.
        """
        return sql.select("COALESCE(MAX(EXECUTION_SEQUENCE), 0) + 1 AS NEXT_SEQ").from_(self.version_table)


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
                version_type VARCHAR2(16),
                execution_sequence INTEGER,
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
            List of migration records as dictionaries with lowercase keys.
        """
        result = driver.execute(self._get_applied_migrations_sql())
        if not result.data:
            return []

        return [{key.lower(): value for key, value in row.items()} for row in result.data]

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
        parsed_version = parse_version(version)
        version_type = parsed_version.type.value

        next_seq_result = driver.execute(self._get_next_execution_sequence_sql())
        execution_sequence = next_seq_result.data[0]["NEXT_SEQ"] if next_seq_result.data else 1

        record_sql = self._get_record_migration_sql(
            version, version_type, execution_sequence, description, execution_time_ms, checksum, applied_by
        )
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
                version_type VARCHAR2(16),
                execution_sequence INTEGER,
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
            List of migration records as dictionaries with lowercase keys.
        """
        result = await driver.execute(self._get_applied_migrations_sql())
        if not result.data:
            return []

        return [{key.lower(): value for key, value in row.items()} for row in result.data]

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
        parsed_version = parse_version(version)
        version_type = parsed_version.type.value

        next_seq_result = await driver.execute(self._get_next_execution_sequence_sql())
        execution_sequence = next_seq_result.data[0]["NEXT_SEQ"] if next_seq_result.data else 1

        record_sql = self._get_record_migration_sql(
            version, version_type, execution_sequence, description, execution_time_ms, checksum, applied_by
        )
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
