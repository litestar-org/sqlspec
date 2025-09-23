"""Migration execution engine for SQLSpec.

This module provides a unified migration runner that handles both synchronous
and asynchronous migration execution seamlessly.
"""

import inspect
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.core.statement import SQL
from sqlspec.migrations.base import BaseMigrationRunner
from sqlspec.migrations.loaders import get_migration_loader
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import await_

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("MigrationRunner",)

logger = get_logger("migrations.runner")


class MigrationRunner(BaseMigrationRunner["Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]"]):
    """Unified migration executor that handles both sync and async operations seamlessly."""

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

    async def load_migration_async(self, file_path: Path) -> "dict[str, Any]":
        """Load a migration file and extract its components (async version).

        Args:
            file_path: Path to the migration file.

        Returns:
            Dictionary containing migration metadata.
        """
        return await self._load_migration_metadata_async(file_path)

    async def _load_migration_metadata_async(self, file_path: Path) -> "dict[str, Any]":
        """Load migration metadata from file (async version).

        Args:
            file_path: Path to the migration file.

        Returns:
            Migration metadata dictionary.
        """
        context_to_use = self.context
        if context_to_use and file_path.name.startswith("ext_"):
            version = self._extract_version(file_path.name)
            if version and version.startswith("ext_"):
                min_extension_version_parts = 3
                parts = version.split("_", 2)
                if len(parts) >= min_extension_version_parts:
                    ext_name = parts[1]
                    if ext_name in self.extension_configs:
                        from sqlspec.migrations.context import MigrationContext

                        context_to_use = MigrationContext(
                            dialect=self.context.dialect if self.context else None,
                            config=self.context.config if self.context else None,
                            driver=self.context.driver if self.context else None,
                            metadata=self.context.metadata.copy() if self.context and self.context.metadata else {},
                            extension_config=self.extension_configs[ext_name],
                        )

        for ext_name, ext_path in self.extension_migrations.items():
            if file_path.parent == ext_path:
                if ext_name in self.extension_configs and self.context:
                    from sqlspec.migrations.context import MigrationContext

                    context_to_use = MigrationContext(
                        config=self.context.config,
                        dialect=self.context.dialect,
                        driver=self.context.driver,
                        metadata=self.context.metadata.copy() if self.context.metadata else {},
                        extension_config=self.extension_configs[ext_name],
                    )
                break

        loader = get_migration_loader(file_path, self.migrations_path, self.project_root, context_to_use)
        loader.validate_migration_file(file_path)
        content = file_path.read_text(encoding="utf-8")
        checksum = self._calculate_checksum(content)
        version = self._extract_version(file_path.name)
        description = file_path.stem.split("_", 1)[1] if "_" in file_path.stem else ""

        has_upgrade, has_downgrade = True, False

        if file_path.suffix == ".sql":
            up_query, down_query = f"migrate-{version}-up", f"migrate-{version}-down"
            self.loader.clear_cache()
            self.loader.load_sql(file_path)
            has_upgrade, has_downgrade = self.loader.has_query(up_query), self.loader.has_query(down_query)
        else:
            try:
                has_downgrade = bool(await loader.get_down_sql(file_path))
            except Exception:
                has_downgrade = False

        return {
            "version": version,
            "description": description,
            "file_path": file_path,
            "checksum": checksum,
            "has_upgrade": has_upgrade,
            "has_downgrade": has_downgrade,
            "loader": loader,
        }

    def execute_upgrade(
        self, driver: "Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute an upgrade migration.

        Args:
            driver: The database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        if self._is_async_driver(driver):
            return await_(self._execute_upgrade_async, raise_sync_error=False)(
                cast("AsyncDriverAdapterBase", driver), migration
            )
        return self._execute_upgrade_sync(cast("SyncDriverAdapterBase", driver), migration)

    def _execute_upgrade_sync(
        self, driver: "SyncDriverAdapterBase", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute an upgrade migration synchronously.

        Args:
            driver: The sync database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        upgrade_sql_list = self._get_migration_sql(migration, "up")
        if upgrade_sql_list is None:
            return None, 0

        start_time = time.time()

        for sql_statement in upgrade_sql_list:
            if sql_statement.strip():
                driver.execute_script(sql_statement)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    async def _execute_upgrade_async(
        self, driver: "AsyncDriverAdapterBase", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute an upgrade migration asynchronously.

        Args:
            driver: The async database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        upgrade_sql_list = await self._get_migration_sql_async(migration, "up")
        if upgrade_sql_list is None:
            return None, 0

        start_time = time.time()

        for sql_statement in upgrade_sql_list:
            if sql_statement.strip():
                await driver.execute_script(sql_statement)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    def execute_downgrade(
        self, driver: "Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute a downgrade migration.

        Args:
            driver: The database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        if self._is_async_driver(driver):
            return await_(self._execute_downgrade_async, raise_sync_error=False)(
                cast("AsyncDriverAdapterBase", driver), migration
            )
        return self._execute_downgrade_sync(cast("SyncDriverAdapterBase", driver), migration)

    def _execute_downgrade_sync(
        self, driver: "SyncDriverAdapterBase", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute a downgrade migration synchronously.

        Args:
            driver: The sync database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        downgrade_sql_list = self._get_migration_sql(migration, "down")
        if downgrade_sql_list is None:
            return None, 0

        start_time = time.time()

        for sql_statement in downgrade_sql_list:
            if sql_statement.strip():
                driver.execute_script(sql_statement)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    async def _execute_downgrade_async(
        self, driver: "AsyncDriverAdapterBase", migration: "dict[str, Any]"
    ) -> "tuple[Optional[str], int]":
        """Execute a downgrade migration asynchronously.

        Args:
            driver: The async database driver to use.
            migration: Migration metadata dictionary.

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        downgrade_sql_list = await self._get_migration_sql_async(migration, "down")
        if downgrade_sql_list is None:
            return None, 0

        start_time = time.time()

        for sql_statement in downgrade_sql_list:
            if sql_statement.strip():
                await driver.execute_script(sql_statement)
        execution_time = int((time.time() - start_time) * 1000)
        return None, execution_time

    async def _get_migration_sql_async(self, migration: "dict[str, Any]", direction: str) -> "Optional[list[str]]":
        """Get migration SQL for given direction (async version).

        Args:
            migration: Migration metadata.
            direction: Either 'up' or 'down'.

        Returns:
            SQL statements for the migration.
        """
        if not migration.get(f"has_{direction}grade"):
            if direction == "down":
                logger.warning("Migration %s has no downgrade query", migration["version"])
                return None
            msg = f"Migration {migration['version']} has no upgrade query"
            raise ValueError(msg)

        file_path, loader = migration["file_path"], migration["loader"]

        try:
            method = loader.get_up_sql if direction == "up" else loader.get_down_sql
            sql_statements = await method(file_path)

        except Exception as e:
            if direction == "down":
                logger.warning("Failed to load downgrade for migration %s: %s", migration["version"], e)
                return None
            msg = f"Failed to load upgrade for migration {migration['version']}: {e}"
            raise ValueError(msg) from e
        else:
            if sql_statements:
                return cast("list[str]", sql_statements)
            return None

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
                loader = get_migration_loader(file_path, self.migrations_path, self.project_root, self.context)

                try:
                    up_sql = await_(loader.get_up_sql, raise_sync_error=False)(file_path)
                    down_sql = await_(loader.get_down_sql, raise_sync_error=False)(file_path)

                    if up_sql:
                        all_queries[f"migrate-{version}-up"] = SQL(up_sql[0])
                    if down_sql:
                        all_queries[f"migrate-{version}-down"] = SQL(down_sql[0])

                except Exception as e:
                    logger.debug("Failed to load Python migration %s: %s", file_path, e)

        return all_queries

    async def load_all_migrations_async(self) -> "dict[str, SQL]":
        """Load all migrations into a single namespace for bulk operations (async version).

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
                loader = get_migration_loader(file_path, self.migrations_path, self.project_root, self.context)

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

    def _is_async_driver(self, driver: "Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]") -> bool:
        """Check if the driver is async by examining its methods.

        Args:
            driver: The driver to check.

        Returns:
            True if driver is async, False otherwise.
        """
        execute_method = getattr(driver, "execute_script", None)
        if execute_method is None:
            return False

        return inspect.iscoroutinefunction(execute_method)
