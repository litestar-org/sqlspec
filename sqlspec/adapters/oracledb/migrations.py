"""Oracle-specific migration implementations.

This module provides Oracle Database-specific overrides for migration functionality
to handle Oracle's unique SQL syntax requirements.
"""

import getpass
from typing import TYPE_CHECKING, Any

from rich.console import Console

from sqlspec.builder import CreateTable, Select, sql
from sqlspec.migrations.base import BaseMigrationTracker
from sqlspec.migrations.version import parse_version
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("OracleAsyncMigrationTracker", "OracleSyncMigrationTracker")

logger = get_logger("sqlspec.migrations.oracle")
console = Console()


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
            sql
            .create_table(self.version_table)
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

        Uses uppercase column names with lowercase aliases to match Python expectations.
        Oracle stores unquoted identifiers as UPPERCASE, so we query UPPERCASE columns
        and alias them as quoted "lowercase" for result consistency.

        Returns:
            SQL builder object for version query.
        """
        return (
            sql
            .select('VERSION_NUM AS "version_num"')
            .from_(self.version_table)
            .order_by("EXECUTION_SEQUENCE DESC")
            .limit(1)
        )

    def _get_applied_migrations_sql(self) -> Select:
        """Get Oracle-specific SQL for retrieving all applied migrations.

        Uses uppercase column names with lowercase aliases to match Python expectations.
        Oracle stores unquoted identifiers as UPPERCASE, so we query UPPERCASE columns
        and alias them as quoted "lowercase" for result consistency.

        Returns:
            SQL builder object for migrations query.
        """
        return (
            sql
            .select(
                'VERSION_NUM AS "version_num"',
                'VERSION_TYPE AS "version_type"',
                'EXECUTION_SEQUENCE AS "execution_sequence"',
                'DESCRIPTION AS "description"',
                'APPLIED_AT AS "applied_at"',
                'EXECUTION_TIME_MS AS "execution_time_ms"',
                'CHECKSUM AS "checksum"',
                'APPLIED_BY AS "applied_by"',
            )
            .from_(self.version_table)
            .order_by("EXECUTION_SEQUENCE")
        )

    def _get_next_execution_sequence_sql(self) -> Select:
        """Get Oracle-specific SQL for retrieving next execution sequence.

        Uses uppercase column names with lowercase alias to match Python expectations.
        Oracle stores unquoted identifiers as UPPERCASE, so we query UPPERCASE columns
        and alias them as quoted "lowercase" for result consistency.

        Returns:
            SQL builder object for sequence query.
        """
        return sql.select('COALESCE(MAX(EXECUTION_SEQUENCE), 0) + 1 AS "next_seq"').from_(self.version_table)

    def _get_existing_columns_sql(self) -> str:
        """Get SQL to query existing columns in the tracking table.

        Returns:
            Raw SQL string for Oracle's USER_TAB_COLUMNS query.
        """
        return f"""
            SELECT column_name
            FROM user_tab_columns
            WHERE table_name = '{self.version_table.upper()}'
        """

    def _detect_missing_columns(self, existing_columns: "set[str]") -> "set[str]":
        """Detect which columns are missing from the current schema.

        Args:
            existing_columns: Set of existing column names (uppercase).

        Returns:
            Set of missing column names (lowercase).
        """
        target_create = self._get_create_table_sql()
        target_columns = {col.name.lower() for col in target_create.columns}
        existing_lower = {col.lower() for col in existing_columns}
        return target_columns - existing_lower


class OracleSyncMigrationTracker(OracleMigrationTrackerMixin, BaseMigrationTracker["SyncDriverAdapterBase"]):
    """Oracle-specific sync migration tracker."""

    __slots__ = ()

    def _migrate_schema_if_needed(self, driver: "SyncDriverAdapterBase") -> None:
        """Check for and add any missing columns to the tracking table.

        Uses the driver's data dictionary to query existing columns from Oracle's
        USER_TAB_COLUMNS metadata table.

        Args:
            driver: The database driver to use.
        """
        try:
            columns_data = driver.data_dictionary.get_columns(driver, self.version_table)
            existing_columns = {str(row["column_name"]).upper() for row in columns_data}
            missing_columns = self._detect_missing_columns(existing_columns)

            if not missing_columns:
                logger.debug("Migration tracking table schema is up-to-date")
                return

            console.print(
                f"[cyan]Migrating tracking table schema, adding columns: {', '.join(sorted(missing_columns))}[/]"
            )

            for col_name in sorted(missing_columns):
                self._add_column(driver, col_name)

            driver.commit()
            console.print("[green]Migration tracking table schema updated successfully[/]")

        except Exception as e:
            logger.warning("Could not check or migrate tracking table schema: %s", e)

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

        default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
        not_null_clause = " NOT NULL" if column_def.not_null else ""

        alter_sql = f"""
            ALTER TABLE {self.version_table}
            ADD {column_def.name} {column_def.dtype}{default_clause}{not_null_clause}
        """

        driver.execute(alter_sql)
        logger.debug("Added column %s to tracking table", column_name)

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Uses a PL/SQL block to make the operation atomic and prevent race conditions.
        Also checks for and adds missing columns to support schema migrations.

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

        self._migrate_schema_if_needed(driver)

    def get_current_version(self, driver: "SyncDriverAdapterBase") -> "str | None":
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current migration version or None if no migrations applied.
        """
        result = driver.execute(self._get_current_version_sql())
        data = result.get_data()
        return data[0]["version_num"] if data else None

    def get_applied_migrations(self, driver: "SyncDriverAdapterBase") -> "list[dict[str, Any]]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records as dictionaries with lowercase keys.
        """
        result = driver.execute(self._get_applied_migrations_sql())
        return result.get_data()

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
        seq_data = next_seq_result.get_data()
        execution_sequence = seq_data[0]["next_seq"] if seq_data else 1

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

            msg = f"Migration {old_version} not found in database for update to {new_version}"
            raise ValueError(msg)

        driver.commit()


class OracleAsyncMigrationTracker(OracleMigrationTrackerMixin, BaseMigrationTracker["AsyncDriverAdapterBase"]):
    """Oracle-specific async migration tracker."""

    __slots__ = ()

    async def _migrate_schema_if_needed(self, driver: "AsyncDriverAdapterBase") -> None:
        """Check for and add any missing columns to the tracking table.

        Uses the driver's data dictionary to query existing columns from Oracle's
        USER_TAB_COLUMNS metadata table.

        Args:
            driver: The database driver to use.
        """
        try:
            columns_data = await driver.data_dictionary.get_columns(driver, self.version_table)
            existing_columns = {str(row["column_name"]).upper() for row in columns_data}
            missing_columns = self._detect_missing_columns(existing_columns)

            if not missing_columns:
                logger.debug("Migration tracking table schema is up-to-date")
                return

            console.print(
                f"[cyan]Migrating tracking table schema, adding columns: {', '.join(sorted(missing_columns))}[/]"
            )

            for col_name in sorted(missing_columns):
                await self._add_column(driver, col_name)

            await driver.commit()
            console.print("[green]Migration tracking table schema updated successfully[/]")

        except Exception as e:
            logger.warning("Could not check or migrate tracking table schema: %s", e)

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

        default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
        not_null_clause = " NOT NULL" if column_def.not_null else ""

        alter_sql = f"""
            ALTER TABLE {self.version_table}
            ADD {column_def.name} {column_def.dtype}{default_clause}{not_null_clause}
        """

        await driver.execute(alter_sql)
        logger.debug("Added column %s to tracking table", column_name)

    async def ensure_tracking_table(self, driver: "AsyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Uses a PL/SQL block to make the operation atomic and prevent race conditions.
        Also checks for and adds missing columns to support schema migrations.

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

        await self._migrate_schema_if_needed(driver)

    async def get_current_version(self, driver: "AsyncDriverAdapterBase") -> "str | None":
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current migration version or None if no migrations applied.
        """
        result = await driver.execute(self._get_current_version_sql())
        data = result.get_data()
        return data[0]["version_num"] if data else None

    async def get_applied_migrations(self, driver: "AsyncDriverAdapterBase") -> "list[dict[str, Any]]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records as dictionaries with lowercase keys.
        """
        result = await driver.execute(self._get_applied_migrations_sql())
        return result.get_data()

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
        seq_data = next_seq_result.get_data()
        execution_sequence = seq_data[0]["next_seq"] if seq_data else 1

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

            msg = f"Migration {old_version} not found in database for update to {new_version}"
            raise ValueError(msg)

        await driver.commit()
