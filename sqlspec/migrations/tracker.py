"""Migration version tracking for SQLSpec.

This module provides functionality to track applied migrations in the database.
"""

import importlib
import logging
from contextlib import asynccontextmanager, contextmanager, suppress
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import MigrationError, SQLSpecError
from sqlspec.migrations.base import BaseMigrationTracker
from sqlspec.migrations.schema import ensure_schema_async, ensure_schema_sync
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Sequence

    from rich.console import Console

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.migrations.base import AppliedMigrationRecord

__all__ = ("AsyncMigrationTracker", "SyncMigrationTracker")

_POSTGRES_LOCK_DIALECTS: Final[frozenset[str]] = frozenset({
    "postgres",
    "postgresql",
    "pgvector",
    "paradedb",
    "pg_textsearch",
})
_MYSQL_LOCK_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "mariadb"})
_MSSQL_LOCK_DIALECTS: Final[frozenset[str]] = frozenset({"mssql", "tsql"})

logger = get_logger("sqlspec.migrations.tracker")
_console: "Console | None" = None


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncMigrationTracker(BaseMigrationTracker["SyncDriverAdapterBase"]):
    """Synchronous migration version tracker."""

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
        self,
        driver: "SyncDriverAdapterBase",
        version: str,
        description: str,
        execution_time_ms: int,
        checksum: str,
        *,
        commit: bool = True,
        replaces: "Sequence[str] | None" = None,
    ) -> None:
        """Record a successfully applied migration.

        Parses version to determine type (sequential or timestamp) and
        auto-increments execution_sequence for application order tracking.

        Args:
            driver: The database driver to use.
            version: Version number of the migration.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: SHA-256 checksum hex string of the migration content.
            commit: Whether to commit immediately after recording.
            replaces: Optional sequence of replaced migration versions for squashed migrations.
        """
        version_type = self._derive_version_type(version)
        replaces_csv = ",".join(replaces) if replaces else None
        if str(getattr(driver, "dialect", "") or "").lower() in self._SEQUENCED_INSERT_DIALECTS:
            statement: Any = self._record_migration_sequenced_statement(
                version, version_type, description, execution_time_ms, checksum, self._applied_by(), replaces_csv
            )
        else:
            seq_result = driver.execute(self._next_execution_sequence_query())
            next_sequence = self._extract_next_sequence(seq_result)

            statement = self._record_statement(
                version,
                version_type,
                next_sequence,
                description,
                execution_time_ms,
                checksum,
                self._applied_by(),
                replaces,
            )
        driver.execute(statement)
        if commit:
            self._safe_commit(driver)
        _log_migration_recorded(driver, version)

    def remove_migration(self, driver: "SyncDriverAdapterBase", version: str, *, commit: bool = True) -> None:
        """Remove a migration record (used during downgrade).

        Args:
            driver: The database driver to use.
            version: Version number to remove.
            commit: Whether to commit immediately after removing.
        """
        driver.execute(self._remove_migration_statement(version))
        if commit:
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
            checksum: SHA-256 checksum hex string of the squashed migration content.
        """
        driver.execute(self._delete_versions_statement(replaced_versions))

        version_type = self._derive_version_type(squashed_version)
        replaces_csv = ",".join(replaced_versions) if replaced_versions else None
        if str(getattr(driver, "dialect", "") or "").lower() in self._SEQUENCED_INSERT_DIALECTS:
            statement: Any = self._record_migration_sequenced_statement(
                squashed_version, version_type, description, 0, checksum, self._applied_by(), replaces_csv
            )
        else:
            seq_result = driver.execute(self._next_execution_sequence_query())
            next_sequence = self._extract_next_sequence(seq_result)
            statement = self._record_statement(
                squashed_version,
                version_type,
                next_sequence,
                description,
                0,
                checksum,
                self._applied_by(),
                replaced_versions,
            )
        driver.execute(statement)

        self._safe_commit(driver)
        _log_squash_recorded(driver, squashed_version, len(replaced_versions))

    def is_squash_already_applied(
        self, driver: "SyncDriverAdapterBase", squashed_version: str, replaced_versions: "list[str]"
    ) -> bool:
        """Check if a squash operation has already been applied.

        Determines if all of the replaced versions exist in the database,
        indicating that the original migrations were applied before the squash.

        Args:
            driver: The database driver to use.
            squashed_version: Version number of the squashed migration.
            replaced_versions: List of version strings that would be replaced.

        Returns:
            True if all replaced versions exist (squash already applied), False if none exist.

        Raises:
            MigrationError: If only a strict subset of replaced_versions exists in the database.
        """
        result = driver.execute(self._check_versions_query(replaced_versions))
        return _validate_squash_applied_state(result, squashed_version, replaced_versions)

    @contextmanager
    def acquire_lock(self, driver: "SyncDriverAdapterBase", *, timeout_seconds: int = 30) -> "Iterator[None]":
        """Serialize mutating migration sessions using a dialect-appropriate lock.

        PostgreSQL uses session advisory locks (skipped for CockroachDB drivers), MySQL and
        MariaDB use ``GET_LOCK`` / ``RELEASE_LOCK``, SQL Server uses ``sp_getapplock`` /
        ``sp_releaseapplock``, and SQLite takes an immediate transaction lock when no
        transaction is already open. Unsupported backends yield directly.

        Args:
            driver: Active session driver.
            timeout_seconds: Lock acquisition timeout for timed lock backends.

        Yields:
            Control to the caller while the migration lock is held.

        Raises:
            MigrationError: When MySQL or SQL Server lock acquisition fails or times out.
        """
        raw_dialect = getattr(type(driver), "dialect", None)
        dialect = raw_dialect.lower() if isinstance(raw_dialect, str) else ""
        if not dialect or type(driver).__module__.startswith("sqlspec.adapters.cockroach_"):
            yield
            return

        lock_name, lock_id = self._migration_lock_key()
        if dialect in _POSTGRES_LOCK_DIALECTS:
            driver.execute("SELECT pg_advisory_lock(:lock_id)", {"lock_id": lock_id})
            try:
                yield
            finally:
                driver.execute("SELECT pg_advisory_unlock(:lock_id)", {"lock_id": lock_id})
            return

        if dialect in _MYSQL_LOCK_DIALECTS:
            lock_result = driver.select_value(
                "SELECT GET_LOCK(:lock_name, :timeout)", {"lock_name": lock_name, "timeout": max(timeout_seconds, 0)}
            )
            if lock_result != 1:
                msg = f"Failed to acquire MySQL migration lock {lock_name!r} within {timeout_seconds}s"
                raise MigrationError(msg)
            try:
                yield
            finally:
                driver.execute("SELECT RELEASE_LOCK(:lock_name)", {"lock_name": lock_name})
            return

        if dialect in _MSSQL_LOCK_DIALECTS:
            timeout_ms = max(timeout_seconds, 0) * 1000
            try:
                ms_result = driver.execute(
                    "DECLARE @sqlspec_res INT; "
                    "EXEC @sqlspec_res = sp_getapplock :lock_name, 'Exclusive', 'Session', :timeout_ms; "
                    "IF @sqlspec_res < 0 THROW 51000, 'Migration lock acquisition failed', 1; "
                    "SELECT @sqlspec_res",
                    {"lock_name": lock_name, "timeout_ms": timeout_ms},
                )
            except SQLSpecError as exc:
                if "Migration lock acquisition failed" in str(exc):
                    msg = f"Failed to acquire SQL Server migration lock {lock_name!r} within {timeout_seconds}s"
                    raise MigrationError(msg) from exc
                raise
            if ms_result.data:
                lock_code = ms_result.scalar()
                if isinstance(lock_code, int) and lock_code < 0:
                    msg = f"Failed to acquire SQL Server migration lock {lock_name!r} (code {lock_code})"
                    raise MigrationError(msg)
            try:
                yield
            finally:
                driver.execute("EXEC sp_releaseapplock :lock_name, 'Session'", {"lock_name": lock_name})
            return

        if dialect == "sqlite":
            if driver._connection_in_transaction():  # pyright: ignore[reportPrivateUsage]
                yield
                return
            driver.execute("BEGIN IMMEDIATE")
            lock_error: BaseException | None = None
            try:
                yield
            except BaseException as exc:
                lock_error = exc
            if lock_error is not None:
                if driver._connection_in_transaction():  # pyright: ignore[reportPrivateUsage]
                    driver.rollback()
                raise lock_error
            if driver._connection_in_transaction():  # pyright: ignore[reportPrivateUsage]
                driver.commit()
            return

        yield

    def _migrate_schema_if_needed(self, driver: "SyncDriverAdapterBase") -> None:
        """Add missing tracking-table columns through the shared schema engine.

        Args:
            driver: The database driver to use.
        """
        try:
            target = self._tracking_schema_target()
            result = ensure_schema_sync(driver, [target], manage_schema=True, create_schema=False, assume_existing=True)
            added_columns = result.added_columns.get(target.identity, [])
            if not added_columns:
                _log_schema_current(driver, self.version_table)
                return
            if self._should_echo():
                _get_console().print(
                    f"[cyan]Migrating tracking table schema, adding columns: {', '.join(added_columns)}[/]"
                )
            for column_name in added_columns:
                _log_column_added(driver, self.version_table, column_name)
            if self._should_echo():
                _get_console().print("[green]Migration tracking table schema updated successfully[/]")
        except Exception as exc:
            with suppress(Exception):
                driver.rollback()
            _log_schema_check_failed(driver, self.version_table, exc)

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

    def _record_statement(
        self,
        version: str,
        version_type: str,
        execution_sequence: int,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
        replaces: "Sequence[str] | None" = None,
    ) -> Any:
        """Build an INSERT statement for a standard or squashed migration record."""
        return _build_record_statement(
            self,
            version,
            version_type,
            execution_sequence,
            description,
            execution_time_ms,
            checksum,
            applied_by,
            replaces,
        )


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncMigrationTracker(BaseMigrationTracker["AsyncDriverAdapterBase"]):
    """Asynchronous migration version tracker."""

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
        self,
        driver: "AsyncDriverAdapterBase",
        version: str,
        description: str,
        execution_time_ms: int,
        checksum: str,
        *,
        commit: bool = True,
        replaces: "Sequence[str] | None" = None,
    ) -> None:
        """Record a successfully applied migration.

        Parses version to determine type (sequential or timestamp) and
        auto-increments execution_sequence for application order tracking.

        Args:
            driver: The database driver to use.
            version: Version number of the migration.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: SHA-256 checksum hex string of the migration content.
            commit: Whether to commit immediately after recording.
            replaces: Optional sequence of replaced migration versions for squashed migrations.
        """
        version_type = self._derive_version_type(version)
        replaces_csv = ",".join(replaces) if replaces else None
        if str(getattr(driver, "dialect", "") or "").lower() in self._SEQUENCED_INSERT_DIALECTS:
            statement: Any = self._record_migration_sequenced_statement(
                version, version_type, description, execution_time_ms, checksum, self._applied_by(), replaces_csv
            )
        else:
            seq_result = await driver.execute(self._next_execution_sequence_query())
            next_sequence = self._extract_next_sequence(seq_result)

            statement = self._record_statement(
                version,
                version_type,
                next_sequence,
                description,
                execution_time_ms,
                checksum,
                self._applied_by(),
                replaces,
            )
        await driver.execute(statement)
        if commit:
            await self._safe_commit(driver)
        _log_migration_recorded(driver, version)

    async def remove_migration(self, driver: "AsyncDriverAdapterBase", version: str, *, commit: bool = True) -> None:
        """Remove a migration record (used during downgrade).

        Args:
            driver: The database driver to use.
            version: Version number to remove.
            commit: Whether to commit immediately after removing.
        """
        await driver.execute(self._remove_migration_statement(version))
        if commit:
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
            checksum: SHA-256 checksum hex string of the squashed migration content.
        """
        await driver.execute(self._delete_versions_statement(replaced_versions))

        version_type = self._derive_version_type(squashed_version)
        replaces_csv = ",".join(replaced_versions) if replaced_versions else None
        if str(getattr(driver, "dialect", "") or "").lower() in self._SEQUENCED_INSERT_DIALECTS:
            statement: Any = self._record_migration_sequenced_statement(
                squashed_version, version_type, description, 0, checksum, self._applied_by(), replaces_csv
            )
        else:
            seq_result = await driver.execute(self._next_execution_sequence_query())
            next_sequence = self._extract_next_sequence(seq_result)
            statement = self._record_statement(
                squashed_version,
                version_type,
                next_sequence,
                description,
                0,
                checksum,
                self._applied_by(),
                replaced_versions,
            )
        await driver.execute(statement)

        await self._safe_commit(driver)
        _log_squash_recorded(driver, squashed_version, len(replaced_versions))

    async def is_squash_already_applied(
        self, driver: "AsyncDriverAdapterBase", squashed_version: str, replaced_versions: "list[str]"
    ) -> bool:
        """Check if a squash operation has already been applied.

        Determines if all of the replaced versions exist in the database,
        indicating that the original migrations were applied before the squash.

        Args:
            driver: The database driver to use.
            squashed_version: Version number of the squashed migration.
            replaced_versions: List of version strings that would be replaced.

        Returns:
            True if all replaced versions exist (squash already applied), False if none exist.

        Raises:
            MigrationError: If only a strict subset of replaced_versions exists in the database.
        """
        result = await driver.execute(self._check_versions_query(replaced_versions))
        return _validate_squash_applied_state(result, squashed_version, replaced_versions)

    @asynccontextmanager
    async def acquire_lock(
        self, driver: "AsyncDriverAdapterBase", *, timeout_seconds: int = 30
    ) -> "AsyncIterator[None]":
        """Serialize mutating migration sessions using a dialect-appropriate lock.

        PostgreSQL uses session advisory locks (skipped for CockroachDB drivers), MySQL and
        MariaDB use ``GET_LOCK`` / ``RELEASE_LOCK``, SQL Server uses ``sp_getapplock`` /
        ``sp_releaseapplock``, and SQLite takes an immediate transaction lock when no
        transaction is already open. Unsupported backends yield directly.

        Args:
            driver: Active async session driver.
            timeout_seconds: Lock acquisition timeout for timed lock backends.

        Yields:
            Control to the caller while the migration lock is held.

        Raises:
            MigrationError: When MySQL or SQL Server lock acquisition fails or times out.
        """
        raw_dialect = getattr(type(driver), "dialect", None)
        dialect = raw_dialect.lower() if isinstance(raw_dialect, str) else ""
        if not dialect or type(driver).__module__.startswith("sqlspec.adapters.cockroach_"):
            yield
            return

        lock_name, lock_id = self._migration_lock_key()
        if dialect in _POSTGRES_LOCK_DIALECTS:
            await driver.execute("SELECT pg_advisory_lock(:lock_id)", {"lock_id": lock_id})
            try:
                yield
            finally:
                await driver.execute("SELECT pg_advisory_unlock(:lock_id)", {"lock_id": lock_id})
            return

        if dialect in _MYSQL_LOCK_DIALECTS:
            lock_result = await driver.select_value(
                "SELECT GET_LOCK(:lock_name, :timeout)", {"lock_name": lock_name, "timeout": max(timeout_seconds, 0)}
            )
            if lock_result != 1:
                msg = f"Failed to acquire MySQL migration lock {lock_name!r} within {timeout_seconds}s"
                raise MigrationError(msg)
            try:
                yield
            finally:
                await driver.execute("SELECT RELEASE_LOCK(:lock_name)", {"lock_name": lock_name})
            return

        if dialect in _MSSQL_LOCK_DIALECTS:
            timeout_ms = max(timeout_seconds, 0) * 1000
            try:
                ms_result = await driver.execute(
                    "DECLARE @sqlspec_res INT; "
                    "EXEC @sqlspec_res = sp_getapplock :lock_name, 'Exclusive', 'Session', :timeout_ms; "
                    "IF @sqlspec_res < 0 THROW 51000, 'Migration lock acquisition failed', 1; "
                    "SELECT @sqlspec_res",
                    {"lock_name": lock_name, "timeout_ms": timeout_ms},
                )
            except SQLSpecError as exc:
                if "Migration lock acquisition failed" in str(exc):
                    msg = f"Failed to acquire SQL Server migration lock {lock_name!r} within {timeout_seconds}s"
                    raise MigrationError(msg) from exc
                raise
            if ms_result.data:
                lock_code = ms_result.scalar()
                if isinstance(lock_code, int) and lock_code < 0:
                    msg = f"Failed to acquire SQL Server migration lock {lock_name!r} (code {lock_code})"
                    raise MigrationError(msg)
            try:
                yield
            finally:
                await driver.execute("EXEC sp_releaseapplock :lock_name, 'Session'", {"lock_name": lock_name})
            return

        if dialect == "sqlite":
            if driver._connection_in_transaction():  # pyright: ignore[reportPrivateUsage]
                yield
                return
            await driver.execute("BEGIN IMMEDIATE")
            lock_error: BaseException | None = None
            try:
                yield
            except BaseException as exc:
                lock_error = exc
            if lock_error is not None:
                if driver._connection_in_transaction():  # pyright: ignore[reportPrivateUsage]
                    await driver.rollback()
                raise lock_error
            if driver._connection_in_transaction():  # pyright: ignore[reportPrivateUsage]
                await driver.commit()
            return

        yield

    async def _migrate_schema_if_needed(self, driver: "AsyncDriverAdapterBase") -> None:
        """Add missing tracking-table columns through the shared schema engine.

        Args:
            driver: The database driver to use.
        """
        try:
            target = self._tracking_schema_target()
            result = await ensure_schema_async(
                driver, [target], manage_schema=True, create_schema=False, assume_existing=True
            )
            added_columns = result.added_columns.get(target.identity, [])
            if not added_columns:
                _log_schema_current(driver, self.version_table)
                return
            if self._should_echo():
                _get_console().print(
                    f"[cyan]Migrating tracking table schema, adding columns: {', '.join(added_columns)}[/]"
                )
            for column_name in added_columns:
                _log_column_added(driver, self.version_table, column_name)
            if self._should_echo():
                _get_console().print("[green]Migration tracking table schema updated successfully[/]")
        except Exception as exc:
            with suppress(Exception):
                await driver.rollback()
            _log_schema_check_failed(driver, self.version_table, exc)

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

    def _record_statement(
        self,
        version: str,
        version_type: str,
        execution_sequence: int,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
        replaces: "Sequence[str] | None" = None,
    ) -> Any:
        """Build an INSERT statement for a standard or squashed migration record."""
        return _build_record_statement(
            self,
            version,
            version_type,
            execution_sequence,
            description,
            execution_time_ms,
            checksum,
            applied_by,
            replaces,
        )


def _build_record_statement(
    tracker: BaseMigrationTracker[Any],
    version: str,
    version_type: str,
    execution_sequence: int,
    description: str,
    execution_time_ms: int,
    checksum: str,
    applied_by: str,
    replaces: "Sequence[str] | None" = None,
) -> Any:
    """Return an INSERT builder for either a standard or squashed migration record."""
    if replaces:
        return tracker._record_squashed_migration_statement(
            version,
            version_type,
            execution_sequence,
            description,
            execution_time_ms,
            checksum,
            applied_by,
            ",".join(replaces),
        )
    return tracker._record_migration_statement(
        version, version_type, execution_sequence, description, execution_time_ms, checksum, applied_by
    )


with suppress(TypeError):
    setattr(BaseMigrationTracker, "_record_statement", _build_record_statement)


def _validate_squash_applied_state(result: Any, squashed_version: str, replaced_versions: "list[str]") -> bool:
    """Validate that either all or none of the replaced versions are applied."""
    if not result.data:
        return False
    data = result.get_data() if callable(getattr(result, "get_data", None)) else result.data
    if isinstance(data, list) and data and isinstance(data[0], dict) and "version_num" in data[0]:
        found_versions = {row["version_num"] for row in data if "version_num" in row}
        expected_versions = set(replaced_versions)
        if 0 < len(found_versions) < len(expected_versions):
            missing = sorted(expected_versions - found_versions)
            msg = (
                f"Partial squash state detected for {squashed_version}: "
                f"found {sorted(found_versions)}, missing {missing}"
            )
            raise MigrationError(msg)
    return True


def _get_console() -> "Console":
    global _console
    if _console is None:
        _console = cast("Console", importlib.import_module("rich.console").Console())
    return _console


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
    current: str | None = None
    if result.data:
        first_row = result.get_data()[0]
        raw_version = first_row.get("version_num") if "version_num" in first_row else first_row.get("VERSION_NUM")
        current = str(raw_version) if raw_version is not None else None
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
    rows = result.get_data()
    if rows and isinstance(rows[0], dict) and "version_num" not in rows[0] and "VERSION_NUM" in rows[0]:
        rows = [{str(key).lower(): value for key, value in row.items()} for row in rows]
    applied = cast("list[AppliedMigrationRecord]", rows)
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
