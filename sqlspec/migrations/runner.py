"""Migration execution engine for SQLSpec.

This module provides separate sync and async migration runners with clean separation
of concerns and proper type safety.
"""

import inspect
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Union, cast, overload

from sqlspec.core import SQL
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.loaders import get_migration_loader
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import async_, await_

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("AsyncMigrationRunner", "SyncMigrationRunner", "create_migration_runner")

logger = get_logger("migrations.runner")


class BaseMigrationRunner(ABC):
    """Base migration runner with common functionality shared between sync and async implementations."""

    def __init__(
        self,
        migrations_path: Path,
        extension_migrations: "dict[str, Path] | None" = None,
        context: "MigrationContext | None" = None,
        extension_configs: "dict[str, dict[str, Any]] | None" = None,
    ) -> None:
        """Initialize the migration runner.

        Args:
            migrations_path: Path to the directory containing migration files.
            extension_migrations: Optional mapping of extension names to their migration paths.
            context: Optional migration context for Python migrations.
            extension_configs: Optional mapping of extension names to their configurations.
        """
        self.migrations_path = migrations_path
        self.extension_migrations = extension_migrations or {}
        from sqlspec.loader import SQLFileLoader

        self.loader = SQLFileLoader()
        self.project_root: Path | None = None
        self.context = context
        self.extension_configs = extension_configs or {}

    def _extract_version(self, filename: str) -> "str | None":
        """Extract version from filename.

        Supports sequential (0001), timestamp (20251011120000), and extension-prefixed
        (ext_litestar_0001) version formats.

        Args:
            filename: The migration filename.

        Returns:
            The extracted version string or None.
        """
        extension_version_parts = 3
        timestamp_min_length = 4

        name_without_ext = filename.rsplit(".", 1)[0]

        if name_without_ext.startswith("ext_"):
            parts = name_without_ext.split("_", 3)
            if len(parts) >= extension_version_parts:
                return f"{parts[0]}_{parts[1]}_{parts[2]}"
            return None

        parts = name_without_ext.split("_", 1)
        if parts and parts[0].isdigit():
            return parts[0] if len(parts[0]) > timestamp_min_length else parts[0].zfill(4)

        return None

    def _calculate_checksum(self, content: str) -> str:
        """Calculate MD5 checksum of migration content.

        Canonicalizes content by excluding query name headers that change during
        fix command (migrate-{version}-up/down). This ensures checksums remain
        stable when converting timestamp versions to sequential format.

        Args:
            content: The migration file content.

        Returns:
            MD5 checksum hex string.
        """
        import hashlib
        import re

        canonical_content = re.sub(r"^--\s*name:\s*migrate-[^-]+-(?:up|down)\s*$", "", content, flags=re.MULTILINE)

        return hashlib.md5(canonical_content.encode()).hexdigest()  # noqa: S324

    @abstractmethod
    def load_migration(self, file_path: Path) -> Union["dict[str, Any]", "Coroutine[Any, Any, dict[str, Any]]"]:
        """Load a migration file and extract its components.

        Args:
            file_path: Path to the migration file.

        Returns:
            Dictionary containing migration metadata and queries.
            For async implementations, returns a coroutine.
        """

    def _get_migration_files_sync(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of tuples containing (version, file_path).
        """

        migrations = []

        # Scan primary migration path
        if self.migrations_path.exists():
            for pattern in ("*.sql", "*.py"):
                for file_path in self.migrations_path.glob(pattern):
                    if file_path.name.startswith("."):
                        continue
                    version = self._extract_version(file_path.name)
                    if version:
                        migrations.append((version, file_path))

        # Scan extension migration paths
        for ext_name, ext_path in self.extension_migrations.items():
            if ext_path.exists():
                for pattern in ("*.sql", "*.py"):
                    for file_path in ext_path.glob(pattern):
                        if file_path.name.startswith("."):
                            continue
                        # Prefix extension migrations to avoid version conflicts
                        version = self._extract_version(file_path.name)
                        if version:
                            # Use ext_ prefix to distinguish extension migrations
                            prefixed_version = f"ext_{ext_name}_{version}"
                            migrations.append((prefixed_version, file_path))

        from sqlspec.utils.version import parse_version

        def version_sort_key(migration_tuple: "tuple[str, Path]") -> "Any":
            version_str = migration_tuple[0]
            try:
                return parse_version(version_str)
            except ValueError:
                return version_str

        return sorted(migrations, key=version_sort_key)

    def get_migration_files(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of (version, path) tuples sorted by version.
        """
        return self._get_migration_files_sync()

    def _load_migration_metadata_common(self, file_path: Path, version: "str | None" = None) -> "dict[str, Any]":
        """Load common migration metadata that doesn't require async operations.

        Args:
            file_path: Path to the migration file.
            version: Optional pre-extracted version (preserves prefixes like ext_adk_0001).

        Returns:
            Partial migration metadata dictionary.
        """
        import re

        content = file_path.read_text(encoding="utf-8")
        checksum = self._calculate_checksum(content)
        if version is None:
            version = self._extract_version(file_path.name)
        description = file_path.stem.split("_", 1)[1] if "_" in file_path.stem else ""

        transactional_match = re.search(
            r"^--\s*transactional:\s*(true|false)\s*$", content, re.MULTILINE | re.IGNORECASE
        )
        transactional = None
        if transactional_match:
            transactional = transactional_match.group(1).lower() == "true"

        return {
            "version": version,
            "description": description,
            "file_path": file_path,
            "checksum": checksum,
            "content": content,
            "transactional": transactional,
        }

    def _get_context_for_migration(self, file_path: Path) -> "MigrationContext | None":
        """Get the appropriate context for a migration file.

        Args:
            file_path: Path to the migration file.

        Returns:
            Migration context to use, or None to use default.
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
                    context_to_use = MigrationContext(
                        config=self.context.config,
                        dialect=self.context.dialect,
                        driver=self.context.driver,
                        metadata=self.context.metadata.copy() if self.context.metadata else {},
                        extension_config=self.extension_configs[ext_name],
                    )
                break

        return context_to_use

    def should_use_transaction(self, migration: "dict[str, Any]", config: Any) -> bool:
        """Determine if migration should run in a transaction.

        Args:
            migration: Migration metadata dictionary.
            config: The database configuration instance.

        Returns:
            True if migration should be wrapped in a transaction.
        """
        if not config.supports_transactional_ddl:
            return False

        if migration.get("transactional") is not None:
            return bool(migration["transactional"])

        migration_config = getattr(config, "migration_config", {}) or {}
        return bool(migration_config.get("transactional", True))


class SyncMigrationRunner(BaseMigrationRunner):
    """Synchronous migration runner with pure sync methods."""

    def load_migration(self, file_path: Path, version: "str | None" = None) -> "dict[str, Any]":
        """Load a migration file and extract its components.

        Args:
            file_path: Path to the migration file.
            version: Optional pre-extracted version (preserves prefixes like ext_adk_0001).

        Returns:
            Dictionary containing migration metadata and queries.
        """
        metadata = self._load_migration_metadata_common(file_path, version)
        context_to_use = self._get_context_for_migration(file_path)

        loader = get_migration_loader(file_path, self.migrations_path, self.project_root, context_to_use, self.loader)
        loader.validate_migration_file(file_path)

        has_upgrade, has_downgrade = True, False

        if file_path.suffix == ".sql":
            version = metadata["version"]
            up_query, down_query = f"migrate-{version}-up", f"migrate-{version}-down"
            has_upgrade, has_downgrade = self.loader.has_query(up_query), self.loader.has_query(down_query)
        else:
            try:
                has_downgrade = bool(self._get_migration_sql_sync({"loader": loader, "file_path": file_path}, "down"))
            except Exception:
                has_downgrade = False

        metadata.update({"has_upgrade": has_upgrade, "has_downgrade": has_downgrade, "loader": loader})
        return metadata

    def execute_upgrade(
        self,
        driver: "SyncDriverAdapterBase",
        migration: "dict[str, Any]",
        *,
        use_transaction: "bool | None" = None,
        on_success: "Callable[[int], None] | None" = None,
    ) -> "tuple[str | None, int]":
        """Execute an upgrade migration.

        Args:
            driver: The sync database driver to use.
            migration: Migration metadata dictionary.
            use_transaction: Override transaction behavior. If None, uses should_use_transaction logic.
            on_success: Callback invoked with execution_time_ms before commit (for version tracking).

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        upgrade_sql_list = self._get_migration_sql_sync(migration, "up")
        if upgrade_sql_list is None:
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        start_time = time.time()

        if use_transaction:
            try:
                driver.begin()
                for sql_statement in upgrade_sql_list:
                    if sql_statement.strip():
                        driver.execute_script(sql_statement)
                execution_time = int((time.time() - start_time) * 1000)
                if on_success:
                    on_success(execution_time)
                driver.commit()
            except Exception:
                driver.rollback()
                raise
        else:
            for sql_statement in upgrade_sql_list:
                if sql_statement.strip():
                    driver.execute_script(sql_statement)
            execution_time = int((time.time() - start_time) * 1000)
            if on_success:
                on_success(execution_time)

        return None, execution_time

    def execute_downgrade(
        self,
        driver: "SyncDriverAdapterBase",
        migration: "dict[str, Any]",
        *,
        use_transaction: "bool | None" = None,
        on_success: "Callable[[int], None] | None" = None,
    ) -> "tuple[str | None, int]":
        """Execute a downgrade migration.

        Args:
            driver: The sync database driver to use.
            migration: Migration metadata dictionary.
            use_transaction: Override transaction behavior. If None, uses should_use_transaction logic.
            on_success: Callback invoked with execution_time_ms before commit (for version tracking).

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        downgrade_sql_list = self._get_migration_sql_sync(migration, "down")
        if downgrade_sql_list is None:
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        start_time = time.time()

        if use_transaction:
            try:
                driver.begin()
                for sql_statement in downgrade_sql_list:
                    if sql_statement.strip():
                        driver.execute_script(sql_statement)
                execution_time = int((time.time() - start_time) * 1000)
                if on_success:
                    on_success(execution_time)
                driver.commit()
            except Exception:
                driver.rollback()
                raise
        else:
            for sql_statement in downgrade_sql_list:
                if sql_statement.strip():
                    driver.execute_script(sql_statement)
            execution_time = int((time.time() - start_time) * 1000)
            if on_success:
                on_success(execution_time)

        return None, execution_time

    def _get_migration_sql_sync(self, migration: "dict[str, Any]", direction: str) -> "list[str] | None":
        """Get migration SQL for given direction (sync version).

        Args:
            migration: Migration metadata.
            direction: Either 'up' or 'down'.

        Returns:
            SQL statements for the migration.
        """
        # If this is being called during migration loading (no has_*grade field yet),
        # don't raise/warn - just proceed to check if the method exists
        if f"has_{direction}grade" in migration and not migration.get(f"has_{direction}grade"):
            if direction == "down":
                logger.warning("Migration %s has no downgrade query", migration.get("version"))
                return None
            msg = f"Migration {migration.get('version')} has no upgrade query"
            raise ValueError(msg)

        file_path, loader = migration["file_path"], migration["loader"]

        try:
            method = loader.get_up_sql if direction == "up" else loader.get_down_sql
            sql_statements = (
                await_(method, raise_sync_error=False)(file_path)
                if inspect.iscoroutinefunction(method)
                else method(file_path)
            )

        except Exception as e:
            if direction == "down":
                logger.warning("Failed to load downgrade for migration %s: %s", migration.get("version"), e)
                return None
            msg = f"Failed to load upgrade for migration {migration.get('version')}: {e}"
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
                loader = get_migration_loader(
                    file_path, self.migrations_path, self.project_root, self.context, self.loader
                )

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


class AsyncMigrationRunner(BaseMigrationRunner):
    """Asynchronous migration runner with pure async methods."""

    async def get_migration_files(self) -> "list[tuple[str, Path]]":  # type: ignore[override]
        """Get all migration files sorted by version.

        Returns:
            List of (version, path) tuples sorted by version.
        """
        return self._get_migration_files_sync()

    async def load_migration(self, file_path: Path, version: "str | None" = None) -> "dict[str, Any]":
        """Load a migration file and extract its components.

        Args:
            file_path: Path to the migration file.
            version: Optional pre-extracted version (preserves prefixes like ext_adk_0001).

        Returns:
            Dictionary containing migration metadata and queries.
        """
        metadata = self._load_migration_metadata_common(file_path, version)
        context_to_use = self._get_context_for_migration(file_path)

        loader = get_migration_loader(file_path, self.migrations_path, self.project_root, context_to_use, self.loader)
        loader.validate_migration_file(file_path)

        has_upgrade, has_downgrade = True, False

        if file_path.suffix == ".sql":
            version = metadata["version"]
            up_query, down_query = f"migrate-{version}-up", f"migrate-{version}-down"
            has_upgrade, has_downgrade = self.loader.has_query(up_query), self.loader.has_query(down_query)
        else:
            try:
                has_downgrade = bool(
                    await self._get_migration_sql_async({"loader": loader, "file_path": file_path}, "down")
                )
            except Exception:
                has_downgrade = False

        metadata.update({"has_upgrade": has_upgrade, "has_downgrade": has_downgrade, "loader": loader})
        return metadata

    async def execute_upgrade(
        self,
        driver: "AsyncDriverAdapterBase",
        migration: "dict[str, Any]",
        *,
        use_transaction: "bool | None" = None,
        on_success: "Callable[[int], Awaitable[None]] | None" = None,
    ) -> "tuple[str | None, int]":
        """Execute an upgrade migration.

        Args:
            driver: The async database driver to use.
            migration: Migration metadata dictionary.
            use_transaction: Override transaction behavior. If None, uses should_use_transaction logic.
            on_success: Async callback invoked with execution_time_ms before commit (for version tracking).

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        upgrade_sql_list = await self._get_migration_sql_async(migration, "up")
        if upgrade_sql_list is None:
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        start_time = time.time()

        if use_transaction:
            try:
                await driver.begin()
                for sql_statement in upgrade_sql_list:
                    if sql_statement.strip():
                        await driver.execute_script(sql_statement)
                execution_time = int((time.time() - start_time) * 1000)
                if on_success:
                    await on_success(execution_time)
                await driver.commit()
            except Exception:
                await driver.rollback()
                raise
        else:
            for sql_statement in upgrade_sql_list:
                if sql_statement.strip():
                    await driver.execute_script(sql_statement)
            execution_time = int((time.time() - start_time) * 1000)
            if on_success:
                await on_success(execution_time)

        return None, execution_time

    async def execute_downgrade(
        self,
        driver: "AsyncDriverAdapterBase",
        migration: "dict[str, Any]",
        *,
        use_transaction: "bool | None" = None,
        on_success: "Callable[[int], Awaitable[None]] | None" = None,
    ) -> "tuple[str | None, int]":
        """Execute a downgrade migration.

        Args:
            driver: The async database driver to use.
            migration: Migration metadata dictionary.
            use_transaction: Override transaction behavior. If None, uses should_use_transaction logic.
            on_success: Async callback invoked with execution_time_ms before commit (for version tracking).

        Returns:
            Tuple of (sql_content, execution_time_ms).
        """
        downgrade_sql_list = await self._get_migration_sql_async(migration, "down")
        if downgrade_sql_list is None:
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        start_time = time.time()

        if use_transaction:
            try:
                await driver.begin()
                for sql_statement in downgrade_sql_list:
                    if sql_statement.strip():
                        await driver.execute_script(sql_statement)
                execution_time = int((time.time() - start_time) * 1000)
                if on_success:
                    await on_success(execution_time)
                await driver.commit()
            except Exception:
                await driver.rollback()
                raise
        else:
            for sql_statement in downgrade_sql_list:
                if sql_statement.strip():
                    await driver.execute_script(sql_statement)
            execution_time = int((time.time() - start_time) * 1000)
            if on_success:
                await on_success(execution_time)

        return None, execution_time

    async def _get_migration_sql_async(self, migration: "dict[str, Any]", direction: str) -> "list[str] | None":
        """Get migration SQL for given direction (async version).

        Args:
            migration: Migration metadata.
            direction: Either 'up' or 'down'.

        Returns:
            SQL statements for the migration.
        """
        # If this is being called during migration loading (no has_*grade field yet),
        # don't raise/warn - just proceed to check if the method exists
        if f"has_{direction}grade" in migration and not migration.get(f"has_{direction}grade"):
            if direction == "down":
                logger.warning("Migration %s has no downgrade query", migration.get("version"))
                return None
            msg = f"Migration {migration.get('version')} has no upgrade query"
            raise ValueError(msg)

        file_path, loader = migration["file_path"], migration["loader"]

        try:
            method = loader.get_up_sql if direction == "up" else loader.get_down_sql
            sql_statements = await method(file_path)

        except Exception as e:
            if direction == "down":
                logger.warning("Failed to load downgrade for migration %s: %s", migration.get("version"), e)
                return None
            msg = f"Failed to load upgrade for migration {migration.get('version')}: {e}"
            raise ValueError(msg) from e
        else:
            if sql_statements:
                return cast("list[str]", sql_statements)
            return None

    async def load_all_migrations(self) -> "dict[str, SQL]":
        """Load all migrations into a single namespace for bulk operations.

        Returns:
            Dictionary mapping query names to SQL objects.
        """
        all_queries = {}
        migrations = await self.get_migration_files()

        for version, file_path in migrations:
            if file_path.suffix == ".sql":
                await async_(self.loader.load_sql)(file_path)
                for query_name in self.loader.list_queries():
                    all_queries[query_name] = self.loader.get_sql(query_name)
            else:
                loader = get_migration_loader(
                    file_path, self.migrations_path, self.project_root, self.context, self.loader
                )

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


@overload
def create_migration_runner(
    migrations_path: Path,
    extension_migrations: "dict[str, Path]",
    context: "MigrationContext | None",
    extension_configs: "dict[str, Any]",
    is_async: "Literal[False]" = False,
) -> SyncMigrationRunner: ...


@overload
def create_migration_runner(
    migrations_path: Path,
    extension_migrations: "dict[str, Path]",
    context: "MigrationContext | None",
    extension_configs: "dict[str, Any]",
    is_async: "Literal[True]",
) -> AsyncMigrationRunner: ...


def create_migration_runner(
    migrations_path: Path,
    extension_migrations: "dict[str, Path]",
    context: "MigrationContext | None",
    extension_configs: "dict[str, Any]",
    is_async: bool = False,
) -> "SyncMigrationRunner | AsyncMigrationRunner":
    """Factory function to create the appropriate migration runner.

    Args:
        migrations_path: Path to migrations directory.
        extension_migrations: Extension migration paths.
        context: Migration context.
        extension_configs: Extension configurations.
        is_async: Whether to create async or sync runner.

    Returns:
        Appropriate migration runner instance.
    """
    if is_async:
        return AsyncMigrationRunner(migrations_path, extension_migrations, context, extension_configs)
    return SyncMigrationRunner(migrations_path, extension_migrations, context, extension_configs)
