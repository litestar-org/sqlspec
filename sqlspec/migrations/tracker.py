"""Migration version tracking for SQLSpec.

This module provides functionality to track applied migrations in the database.
"""

import logging
from collections.abc import Mapping
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

from rich.console import Console

from sqlspec.migrations.base import BaseMigrationTracker
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.migrations.base import AppliedMigrationRecord

__all__ = ("AsyncMigrationTracker", "SyncMigrationTracker")

logger = get_logger("sqlspec.migrations.tracker")
_console = Console()


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
            if self.version_table_schema:
                columns_data = driver.data_dictionary.get_columns(
                    driver, self.version_table_name, schema=self.version_table_schema
                )
            else:
                columns_data = driver.data_dictionary.get_columns(driver, self.version_table_name)
            if not columns_data:
                _log_tracking_table_missing(driver, self.version_table)
                columns_data = []

            missing_columns = self._detect_missing_columns(_extract_existing_columns(columns_data))
            if not missing_columns:
                _log_schema_current(driver, self.version_table)
                return

            if self._should_echo():
                _console.print(
                    f"[cyan]Migrating tracking table schema, adding columns: {', '.join(sorted(missing_columns))}[/]"
                )

            for col_name, statement in self._add_column_statements(missing_columns):
                driver.execute(statement)
                _log_column_added(driver, self.version_table, col_name)

            driver.commit()
            if self._should_echo():
                _console.print("[green]Migration tracking table schema updated successfully[/]")

        except Exception as exc:
            with suppress(Exception):
                driver.rollback()
            _log_schema_check_failed(driver, self.version_table, exc)

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Also checks for and adds any missing columns to support schema migrations.

        Args:
            driver: The database driver to use.
        """
        driver.execute(self._tracking_table_ddl())
        self._safe_commit(driver)

        self._migrate_schema_if_needed(driver)

    def get_current_version(self, driver: "SyncDriverAdapterBase") -> str | None:
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current version number or None if no migrations applied.
        """
        result = driver.execute(self._current_version_query())
        return _finalize_current_version(driver, result)

    def get_applied_migrations(self, driver: "SyncDriverAdapterBase") -> "list[AppliedMigrationRecord]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records.
        """
        result = driver.execute(self._applied_migrations_query())
        return _finalize_applied_migrations(driver, result)

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
        version_type = self._derive_version_type(version)

        seq_result = driver.execute(self._next_execution_sequence_query())
        next_sequence = self._extract_next_sequence(seq_result)

        driver.execute(
            self._record_migration_statement(
                version, version_type, next_sequence, description, execution_time_ms, checksum, self._applied_by()
            )
        )
        self._safe_commit(driver)
        _log_migration_recorded(driver, version)

    def remove_migration(self, driver: "SyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record (used during downgrade).

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        driver.execute(self._remove_migration_statement(version))
        self._safe_commit(driver)
        _log_migration_removed(driver, version)

    def update_version_record(
        self,
        driver: "SyncDriverAdapterBase",
        old_version: str,
        new_version: str,
        applied_versions: "set[str] | None" = None,
    ) -> None:
        """Update migration version record from timestamp to sequential.

        Updates version_num and version_type while preserving execution_sequence,
        applied_at, and other tracking metadata. Used during fix command.

        Idempotent: If the version is already updated, logs and continues without error.
        This allows fix command to be safely re-run after pulling changes.

        Args:
            driver: The database driver to use.
            old_version: Current timestamp version string.
            new_version: New sequential version string.
            applied_versions: Previously loaded version set for batch reuse.

        Raises:
            ValueError: If neither old_version nor new_version found in database.
        """
        new_version_type = self._derive_version_type(new_version)

        result = driver.execute(self._update_version_statement(old_version, new_version, new_version_type))

        if result.rows_affected == 0:
            if applied_versions is None:
                check_result = driver.execute(self._applied_migrations_query())
                applied_versions = self._extract_applied_versions(check_result)
            if _resolve_version_update_miss(driver, old_version, new_version, applied_versions):
                return

        self._safe_commit(driver)
        _log_version_updated(driver, old_version, new_version)

    def replace_with_squash(
        self,
        driver: "SyncDriverAdapterBase",
        squashed_version: str,
        replaced_versions: "list[str]",
        description: str,
        checksum: str,
    ) -> None:
        """Replace multiple migration records with a single squashed record.

        Deletes all replaced version records and inserts a new record for the
        squashed migration with metadata about which versions it replaces.

        Args:
            driver: The database driver to use.
            squashed_version: Version number of the squashed migration.
            replaced_versions: List of version strings being replaced.
            description: Description of the squashed migration.
            checksum: MD5 checksum of the squashed migration content.
        """
        driver.execute(self._delete_versions_statement(replaced_versions))

        seq_result = driver.execute(self._next_execution_sequence_query())
        next_sequence = self._extract_next_sequence(seq_result)

        version_type = self._derive_version_type(squashed_version)
        replaces_str = ",".join(replaced_versions)
        driver.execute(
            self._record_squashed_migration_statement(
                squashed_version,
                version_type,
                next_sequence,
                description,
                0,
                checksum,
                self._applied_by(),
                replaces_str,
            )
        )

        self._safe_commit(driver)
        _log_squash_recorded(driver, squashed_version, len(replaced_versions))

    def is_squash_already_applied(
        self, driver: "SyncDriverAdapterBase", squashed_version: str, replaced_versions: "list[str]"
    ) -> bool:
        """Check if a squash operation has already been applied.

        Determines if any of the replaced versions exist in the database,
        indicating that the original migrations were applied before the squash.

        Args:
            driver: The database driver to use.
            squashed_version: Version number of the squashed migration (unused but kept for API consistency).
            replaced_versions: List of version strings that would be replaced.

        Returns:
            True if any replaced version exists (squash already applied), False otherwise.
        """
        result = driver.execute(self._check_versions_query(replaced_versions))
        return bool(result.data)

    def _safe_commit(self, driver: "SyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        if driver.driver_features.get("autocommit", False):
            return

        try:
            driver.commit()
        except Exception as exc:
            if self._is_autocommit_error(exc):
                _log_commit_skipped(driver, exc)
            else:
                raise


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
            if self.version_table_schema:
                columns_data = await driver.data_dictionary.get_columns(
                    driver, self.version_table_name, schema=self.version_table_schema
                )
            else:
                columns_data = await driver.data_dictionary.get_columns(driver, self.version_table_name)
            if not columns_data:
                _log_tracking_table_missing(driver, self.version_table)
                columns_data = []

            missing_columns = self._detect_missing_columns(_extract_existing_columns(columns_data))
            if not missing_columns:
                _log_schema_current(driver, self.version_table)
                return

            if self._should_echo():
                _console.print(
                    f"[cyan]Migrating tracking table schema, adding columns: {', '.join(sorted(missing_columns))}[/]"
                )

            for col_name, statement in self._add_column_statements(missing_columns):
                await driver.execute(statement)
                _log_column_added(driver, self.version_table, col_name)

            await driver.commit()
            if self._should_echo():
                _console.print("[green]Migration tracking table schema updated successfully[/]")

        except Exception as exc:
            with suppress(Exception):
                await driver.rollback()
            _log_schema_check_failed(driver, self.version_table, exc)

    async def ensure_tracking_table(self, driver: "AsyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it doesn't exist.

        Also checks for and adds any missing columns to support schema migrations.

        Args:
            driver: The database driver to use.
        """
        await driver.execute(self._tracking_table_ddl())
        await self._safe_commit(driver)

        await self._migrate_schema_if_needed(driver)

    async def get_current_version(self, driver: "AsyncDriverAdapterBase") -> str | None:
        """Get the latest applied migration version.

        Args:
            driver: The database driver to use.

        Returns:
            The current version number or None if no migrations applied.
        """
        result = await driver.execute(self._current_version_query())
        return _finalize_current_version(driver, result)

    async def get_applied_migrations(self, driver: "AsyncDriverAdapterBase") -> "list[AppliedMigrationRecord]":
        """Get all applied migrations in order.

        Args:
            driver: The database driver to use.

        Returns:
            List of migration records.
        """
        result = await driver.execute(self._applied_migrations_query())
        return _finalize_applied_migrations(driver, result)

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
        version_type = self._derive_version_type(version)

        seq_result = await driver.execute(self._next_execution_sequence_query())
        next_sequence = self._extract_next_sequence(seq_result)

        await driver.execute(
            self._record_migration_statement(
                version, version_type, next_sequence, description, execution_time_ms, checksum, self._applied_by()
            )
        )
        await self._safe_commit(driver)
        _log_migration_recorded(driver, version)

    async def remove_migration(self, driver: "AsyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record (used during downgrade).

        Args:
            driver: The database driver to use.
            version: Version number to remove.
        """
        await driver.execute(self._remove_migration_statement(version))
        await self._safe_commit(driver)
        _log_migration_removed(driver, version)

    async def update_version_record(
        self,
        driver: "AsyncDriverAdapterBase",
        old_version: str,
        new_version: str,
        applied_versions: "set[str] | None" = None,
    ) -> None:
        """Update migration version record from timestamp to sequential.

        Updates version_num and version_type while preserving execution_sequence,
        applied_at, and other tracking metadata. Used during fix command.

        Idempotent: If the version is already updated, logs and continues without error.
        This allows fix command to be safely re-run after pulling changes.

        Args:
            driver: The database driver to use.
            old_version: Current timestamp version string.
            new_version: New sequential version string.
            applied_versions: Previously loaded version set for batch reuse.

        Raises:
            ValueError: If neither old_version nor new_version found in database.
        """
        new_version_type = self._derive_version_type(new_version)

        result = await driver.execute(self._update_version_statement(old_version, new_version, new_version_type))

        if result.rows_affected == 0:
            if applied_versions is None:
                check_result = await driver.execute(self._applied_migrations_query())
                applied_versions = self._extract_applied_versions(check_result)
            if _resolve_version_update_miss(driver, old_version, new_version, applied_versions):
                return

        await self._safe_commit(driver)
        _log_version_updated(driver, old_version, new_version)

    async def replace_with_squash(
        self,
        driver: "AsyncDriverAdapterBase",
        squashed_version: str,
        replaced_versions: "list[str]",
        description: str,
        checksum: str,
    ) -> None:
        """Replace multiple migration records with a single squashed record.

        Deletes all replaced version records and inserts a new record for the
        squashed migration with metadata about which versions it replaces.

        Args:
            driver: The database driver to use.
            squashed_version: Version number of the squashed migration.
            replaced_versions: List of version strings being replaced.
            description: Description of the squashed migration.
            checksum: MD5 checksum of the squashed migration content.
        """
        await driver.execute(self._delete_versions_statement(replaced_versions))

        seq_result = await driver.execute(self._next_execution_sequence_query())
        next_sequence = self._extract_next_sequence(seq_result)

        version_type = self._derive_version_type(squashed_version)
        replaces_str = ",".join(replaced_versions)
        await driver.execute(
            self._record_squashed_migration_statement(
                squashed_version,
                version_type,
                next_sequence,
                description,
                0,
                checksum,
                self._applied_by(),
                replaces_str,
            )
        )

        await self._safe_commit(driver)
        _log_squash_recorded(driver, squashed_version, len(replaced_versions))

    async def is_squash_already_applied(
        self, driver: "AsyncDriverAdapterBase", squashed_version: str, replaced_versions: "list[str]"
    ) -> bool:
        """Check if a squash operation has already been applied.

        Determines if any of the replaced versions exist in the database,
        indicating that the original migrations were applied before the squash.

        Args:
            driver: The database driver to use.
            squashed_version: Version number of the squashed migration (unused but kept for API consistency).
            replaced_versions: List of version strings that would be replaced.

        Returns:
            True if any replaced version exists (squash already applied), False otherwise.
        """
        result = await driver.execute(self._check_versions_query(replaced_versions))
        return bool(result.data)

    async def _safe_commit(self, driver: "AsyncDriverAdapterBase") -> None:
        """Safely commit a transaction only if autocommit is disabled.

        Args:
            driver: The database driver to use.
        """
        if driver.driver_features.get("autocommit", False):
            return

        try:
            await driver.commit()
        except Exception as exc:
            if self._is_autocommit_error(exc):
                _log_commit_skipped(driver, exc)
            else:
                raise


def _extract_column_name(metadata: Any) -> "str | None":
    """Extract column name from a metadata entry."""
    if isinstance(metadata, Mapping):
        value = metadata.get("column_name")
        if value is None:
            value = metadata.get("COLUMN_NAME")
        return str(value).lower() if value is not None else None
    value = getattr(metadata, "column_name", None)
    if value is not None:
        return str(value).lower()
    return None


def _extract_existing_columns(columns_data: "list[Any]") -> "set[str]":
    """Return the set of existing tracking-table column names."""
    return {name for col in columns_data if (name := _extract_column_name(col)) is not None}


def _log_tracking_table_missing(driver: Any, version_table: str) -> None:
    """Log that the tracking table has no columns to inspect."""
    log_with_context(
        logger,
        logging.DEBUG,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        table=version_table,
        operation="table_check",
        status="missing",
    )


def _log_schema_current(driver: Any, version_table: str) -> None:
    """Log that the tracking table schema already has all columns."""
    log_with_context(
        logger,
        logging.DEBUG,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        table=version_table,
        operation="schema_check",
        status="current",
    )


def _log_schema_check_failed(driver: Any, version_table: str, exc: Exception) -> None:
    """Log that a tracking-table schema check failed."""
    log_with_context(
        logger,
        logging.ERROR,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        table=version_table,
        operation="schema_check",
        status="failed",
        error_type=type(exc).__name__,
    )


def _log_column_added(driver: Any, version_table: str, column_name: str) -> None:
    """Log that a column was added to the tracking table."""
    log_with_context(
        logger,
        logging.INFO,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        table=version_table,
        column_name=column_name,
        operation="schema_update",
        status="column_added",
    )


def _finalize_current_version(driver: Any, result: Any) -> "str | None":
    """Extract, log, and return the current migration version from a query result."""
    current = result.get_data()[0]["version_num"] if result.data else None
    log_with_context(
        logger,
        logging.DEBUG,
        "migration.history",
        db_system=resolve_db_system(type(driver).__name__),
        current_version=current,
        status="current",
    )
    return current


def _finalize_applied_migrations(driver: Any, result: Any) -> "list[AppliedMigrationRecord]":
    """Extract, log, and return applied migration records from a query result."""
    applied = cast("list[AppliedMigrationRecord]", result.get_data())
    log_with_context(
        logger,
        logging.DEBUG,
        "migration.history",
        db_system=resolve_db_system(type(driver).__name__),
        applied_count=len(applied),
        status="listed",
    )
    return applied


def _log_migration_recorded(driver: Any, version: str) -> None:
    """Log that a migration was recorded."""
    log_with_context(
        logger,
        logging.DEBUG,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        version=version,
        operation="record",
        status="recorded",
    )


def _log_migration_removed(driver: Any, version: str) -> None:
    """Log that a migration record was removed."""
    log_with_context(
        logger,
        logging.DEBUG,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        version=version,
        operation="remove",
        status="removed",
    )


def _resolve_version_update_miss(driver: Any, old_version: str, new_version: str, applied_versions: "set[str]") -> bool:
    """Resolve a zero-row version update.

    Args:
        driver: The database driver in use.
        old_version: Current timestamp version string.
        new_version: New sequential version string.
        applied_versions: Version numbers already recorded in the database.

    Returns:
        True when the update is a no-op re-run and should be skipped.

    Raises:
        ValueError: If neither old_version nor new_version found in database.
    """
    if new_version in applied_versions:
        log_with_context(
            logger,
            logging.DEBUG,
            "migration.track",
            db_system=resolve_db_system(type(driver).__name__),
            old_version=old_version,
            new_version=new_version,
            operation="version_update",
            status="skipped",
        )
        return True

    msg = f"Migration version {old_version} not found in database"
    raise ValueError(msg)


def _log_version_updated(driver: Any, old_version: str, new_version: str) -> None:
    """Log that a migration version record was updated."""
    log_with_context(
        logger,
        logging.INFO,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        old_version=old_version,
        new_version=new_version,
        operation="version_update",
        status="updated",
    )


def _log_squash_recorded(driver: Any, squashed_version: str, replaced_count: int) -> None:
    """Log that a squashed migration record was written."""
    log_with_context(
        logger,
        logging.INFO,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        squashed_version=squashed_version,
        replaced_count=replaced_count,
        operation="squash",
        status="recorded",
    )


def _log_commit_skipped(driver: Any, exc: Exception) -> None:
    """Log that a commit was skipped because the driver manages autocommit."""
    log_with_context(
        logger,
        logging.DEBUG,
        "migration.track",
        db_system=resolve_db_system(type(driver).__name__),
        operation="commit",
        status="skipped",
        reason="autocommit",
        error_type=type(exc).__name__,
    )
