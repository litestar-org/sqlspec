"""Migration execution engine for SQLSpec.

This module handles migration file loading and execution using SQLFileLoader.
"""

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.migrations.base import BaseMigrationRunner
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
    from sqlspec.statement.sql import SQL

__all__ = ("AsyncMigrationRunner", "SyncMigrationRunner")

logger = get_logger("migrations.runner")


class SyncMigrationRunner(BaseMigrationRunner["SyncDriverAdapterProtocol[Any]"]):
    """Sync version - executes migrations using SQLFileLoader."""

    def get_migration_files(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of (version, path) tuples sorted by version.
        """
        return self._get_migration_files_sync()

    def load_migration(self, file_path: Path) -> "dict[str, Any]":
        """Load a migration file and extract its components.

        Args:
            file_path: Path to the migration file.

        Returns:
            Dictionary containing migration metadata and queries.
        """
        return self._load_migration_metadata(file_path)

    def execute_upgrade(
        self, driver: "SyncDriverAdapterProtocol[Any]", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute an upgrade migration.

        Args:
            driver: The database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        upgrade_sql = self._get_migration_sql(migration, "up")
        if upgrade_sql is None:
            return None, 0

        start_time = time.time()

        # Execute migration
        driver.execute(upgrade_sql)

        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    def execute_downgrade(
        self, driver: "SyncDriverAdapterProtocol[Any]", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute a downgrade migration.

        Args:
            driver: The database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        downgrade_sql = self._get_migration_sql(migration, "down")
        if downgrade_sql is None:
            return None, 0

        start_time = time.time()

        # Execute migration
        driver.execute(downgrade_sql)

        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    def load_all_migrations(self) -> "dict[str, SQL]":
        """Load all migrations into a single namespace for bulk operations.

        Returns a dictionary mapping query names to SQL objects.
        Useful for:
        - Migration analysis tools
        - Documentation generation
        - Validation and linting
        - Migration squashing

        Returns:
            Dictionary mapping query names to SQL objects.
        """
        all_queries = {}
        migrations = self.get_migration_files()

        for _version, file_path in migrations:
            self.loader.load_sql(file_path)

            # Get all queries from this file
            for query_name in self.loader.list_queries():
                # Store with full query name for uniqueness
                all_queries[query_name] = self.loader.get_sql(query_name)

        return all_queries


class AsyncMigrationRunner(BaseMigrationRunner["AsyncDriverAdapterProtocol[Any]"]):
    """Async version - executes migrations using SQLFileLoader."""

    async def get_migration_files(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of tuples containing (version, file_path).
        """
        # For async, we still use the sync file operations since Path.glob is sync
        return self._get_migration_files_sync()

    async def load_migration(self, file_path: Path) -> "dict[str, Any]":
        """Load a migration file and extract its components.

        Args:
            file_path: Path to the migration file.

        Returns:
            Dictionary containing migration metadata.
        """
        # File loading is still sync, so we use the base implementation
        return self._load_migration_metadata(file_path)

    async def execute_upgrade(
        self, driver: "AsyncDriverAdapterProtocol[Any]", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute an upgrade migration.

        Args:
            driver: The async database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        upgrade_sql = self._get_migration_sql(migration, "up")
        if upgrade_sql is None:
            return None, 0

        start_time = time.time()

        # Execute migration
        await driver.execute(upgrade_sql)

        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    async def execute_downgrade(
        self, driver: "AsyncDriverAdapterProtocol[Any]", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute a downgrade migration.

        Args:
            driver: The async database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        downgrade_sql = self._get_migration_sql(migration, "down")
        if downgrade_sql is None:
            return None, 0

        start_time = time.time()

        # Execute migration
        await driver.execute(downgrade_sql)

        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    async def load_all_migrations(self) -> "dict[str, SQL]":
        """Load all migrations into a single namespace for bulk operations.

        Returns a dictionary mapping query names to SQL objects.
        Useful for:
        - Migration analysis tools
        - Documentation generation
        - Validation and linting
        - Migration squashing

        Returns:
            Dictionary mapping query names to SQL objects.
        """
        all_queries = {}
        migrations = await self.get_migration_files()

        for _version, file_path in migrations:
            self.loader.load_sql(file_path)

            # Get all queries from this file
            for query_name in self.loader.list_queries():
                # Store with full query name for uniqueness
                all_queries[query_name] = self.loader.get_sql(query_name)

        return all_queries
