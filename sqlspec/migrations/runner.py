"""Migration execution engine for SQLSpec.

This module handles migration file loading and execution using SQLFileLoader.
"""

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.core.statement import SQL
from sqlspec.migrations.base import BaseMigrationRunner
from sqlspec.migrations.loaders import get_migration_loader
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import run_

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("AsyncMigrationRunner", "SyncMigrationRunner")

logger = get_logger("migrations.runner")


class SyncMigrationRunner(BaseMigrationRunner["SyncDriverAdapterBase"]):
    """Executes migrations using SQLFileLoader."""

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
        self, driver: "SyncDriverAdapterBase", migration: "dict[str, Any]"
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
        driver.execute(upgrade_sql)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    def execute_downgrade(
        self, driver: "SyncDriverAdapterBase", migration: "dict[str, Any]"
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
        driver.execute(downgrade_sql)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    def load_all_migrations(self) -> "dict[str, SQL]":
        """Load all migrations into a single namespace for bulk operations.

        Returns:
            Dictionary mapping query names to SQL objects.
        """
        all_queries = {}
        migrations = self.get_migration_files()

        for version, file_path in migrations:
            if file_path.suffix == ".sql":
                self.loader.load_sql(file_path)
                for query_name in self.loader.list_queries():
                    all_queries[query_name] = self.loader.get_sql(query_name)
            else:
                loader = get_migration_loader(file_path, self.migrations_path, self.project_root)

                try:
                    up_sql = run_(loader.get_up_sql)(file_path)
                    down_sql = run_(loader.get_down_sql)(file_path)

                    if up_sql:
                        all_queries[f"migrate-{version}-up"] = SQL(up_sql[0])
                    if down_sql:
                        all_queries[f"migrate-{version}-down"] = SQL(down_sql[0])

                except Exception as e:
                    logger.debug("Failed to load Python migration %s: %s", file_path, e)

        return all_queries


class AsyncMigrationRunner(BaseMigrationRunner["AsyncDriverAdapterBase"]):
    """Executes migrations using SQLFileLoader."""

    async def get_migration_files(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of tuples containing (version, file_path).
        """
        return self._get_migration_files_sync()

    async def load_migration(self, file_path: Path) -> "dict[str, Any]":
        """Load a migration file and extract its components.

        Args:
            file_path: Path to the migration file.

        Returns:
            Dictionary containing migration metadata.
        """
        return self._load_migration_metadata(file_path)

    async def execute_upgrade(
        self, driver: "AsyncDriverAdapterBase", migration: "dict[str, Any]"
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
        await driver.execute(upgrade_sql)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    async def execute_downgrade(
        self, driver: "AsyncDriverAdapterBase", migration: "dict[str, Any]"
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
        await driver.execute(downgrade_sql)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    async def load_all_migrations(self) -> "dict[str, SQL]":
        """Load all migrations into a single namespace for bulk operations.

        Returns:
            Dictionary mapping query names to SQL objects.
        """
        all_queries = {}
        migrations = await self.get_migration_files()

        for version, file_path in migrations:
            if file_path.suffix == ".sql":
                self.loader.load_sql(file_path)
                for query_name in self.loader.list_queries():
                    all_queries[query_name] = self.loader.get_sql(query_name)
            else:
                loader = get_migration_loader(file_path, self.migrations_path, self.project_root)

                try:
                    up_sql = await loader.get_up_sql(file_path)
                    down_sql = await loader.get_down_sql(file_path)

                    if up_sql:
                        all_queries[f"migrate-{version}-up"] = SQL(up_sql[0])
                    if down_sql:
                        all_queries[f"migrate-{version}-down"] = SQL(down_sql[0])

                except Exception as e:
                    logger.debug("Failed to load Python migration %s: %s", file_path, e)

        return all_queries
