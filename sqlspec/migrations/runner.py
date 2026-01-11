"""Migration execution engine for SQLSpec."""

import ast
import hashlib
import inspect
import logging
import re
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Union, cast, overload

from sqlspec.core import SQL
from sqlspec.loader import SQLFileLoader
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.loaders import get_migration_loader
from sqlspec.migrations.templates import TemplateDescriptionHints
from sqlspec.migrations.version import parse_version
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.sync_tools import async_, await_

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.observability import ObservabilityRuntime

__all__ = ("AsyncMigrationRunner", "SyncMigrationRunner", "create_migration_runner")

logger = get_logger("sqlspec.migrations.runner")


class _CachedMigrationMetadata:
    """Cached migration metadata keyed by file path."""

    __slots__ = ("metadata", "mtime_ns", "size")

    def __init__(self, metadata: "dict[str, Any]", mtime_ns: int, size: int) -> None:
        self.metadata = metadata
        self.mtime_ns = mtime_ns
        self.size = size

    def clone(self) -> "dict[str, Any]":
        return dict(self.metadata)


class _MigrationFileEntry:
    """Represents a migration file discovered during directory scanning."""

    __slots__ = ("extension_name", "path")

    def __init__(self, path: Path, extension_name: "str | None") -> None:
        self.path = path
        self.extension_name = extension_name


class BaseMigrationRunner(ABC):
    """Base migration runner with common functionality shared between sync and async implementations."""

    def __init__(
        self,
        migrations_path: Path,
        extension_migrations: "dict[str, Path] | None" = None,
        context: "MigrationContext | None" = None,
        extension_configs: "dict[str, dict[str, Any]] | None" = None,
        runtime: "ObservabilityRuntime | None" = None,
        description_hints: "TemplateDescriptionHints | None" = None,
    ) -> None:
        """Initialize the migration runner.

        Args:
            migrations_path: Path to the directory containing migration files.
            extension_migrations: Optional mapping of extension names to their migration paths.
            context: Optional migration context for Python migrations.
            extension_configs: Optional mapping of extension names to their configurations.
            runtime: Observability runtime shared with command/context consumers.
            description_hints: Hints for extracting migration descriptions.
        """
        self.migrations_path = migrations_path
        self.extension_migrations = extension_migrations or {}
        self.runtime = runtime
        self.loader = SQLFileLoader(runtime=runtime)
        self.project_root: Path | None = None
        self.context = context
        self.extension_configs = extension_configs or {}
        self._listing_digest: str | None = None
        self._listing_cache: list[tuple[str, Path]] | None = None
        self._listing_signatures: dict[str, tuple[int, int]] = {}
        self._metadata_cache: dict[str, _CachedMigrationMetadata] = {}
        self.description_hints = description_hints or TemplateDescriptionHints()

    def _metric(self, name: str, amount: float = 1.0) -> None:
        if self.runtime is None:
            return
        self.runtime.increment_metric(name, amount)

    def _iter_directory_entries(self, base_path: Path, extension_name: "str | None") -> "list[_MigrationFileEntry]":
        """Collect migration files discovered under a base path."""

        if not base_path.exists():
            return []

        entries: list[_MigrationFileEntry] = []
        for pattern in ("*.sql", "*.py"):
            for file_path in sorted(base_path.glob(pattern)):
                if file_path.name.startswith("."):
                    continue
                entries.append(_MigrationFileEntry(path=file_path, extension_name=extension_name))
        return entries

    def _collect_listing_entries(self) -> "tuple[list[_MigrationFileEntry], dict[str, tuple[int, int]], str]":
        """Gather migration files, stat signatures, and digest for cache validation."""

        entries: list[_MigrationFileEntry] = []
        signatures: dict[str, tuple[int, int]] = {}
        digest_source = hashlib.md5(usedforsecurity=False)

        for entry in self._iter_directory_entries(self.migrations_path, None):
            self._record_entry(entry, entries, signatures, digest_source)

        for ext_name, ext_path in self.extension_migrations.items():
            for entry in self._iter_directory_entries(ext_path, ext_name):
                self._record_entry(entry, entries, signatures, digest_source)

        return entries, signatures, digest_source.hexdigest()

    def _record_entry(
        self,
        entry: _MigrationFileEntry,
        entries: "list[_MigrationFileEntry]",
        signatures: "dict[str, tuple[int, int]]",
        digest_source: Any,
    ) -> None:
        """Record entry metadata for cache decisions."""

        try:
            stat_result = entry.path.stat()
        except FileNotFoundError:
            return

        path_str = str(entry.path)
        token = (stat_result.st_mtime_ns, stat_result.st_size)
        signatures[path_str] = token
        digest_source.update(path_str.encode("utf-8"))
        digest_source.update(f"{token[0]}:{token[1]}".encode())
        entries.append(entry)

    def _build_sorted_listing(self, entries: "list[_MigrationFileEntry]") -> "list[tuple[str, Path]]":
        """Construct sorted migration listing from directory entries."""

        migrations: list[tuple[str, Path]] = []

        for entry in entries:
            version = self._extract_version(entry.path.name)
            if not version:
                continue
            if entry.extension_name:
                version = f"ext_{entry.extension_name}_{version}"
            migrations.append((version, entry.path))

        def version_sort_key(migration_tuple: "tuple[str, Path]") -> "Any":
            version_str = migration_tuple[0]
            try:
                return parse_version(version_str)
            except ValueError:
                return version_str

        return sorted(migrations, key=version_sort_key)

    def _log_listing_invalidation(
        self, previous: "dict[str, tuple[int, int]]", current: "dict[str, tuple[int, int]]"
    ) -> None:
        """Log cache invalidation details at INFO level."""

        prev_keys = set(previous)
        curr_keys = set(current)
        added = curr_keys - prev_keys
        removed = prev_keys - curr_keys
        modified = {key for key in prev_keys & curr_keys if previous[key] != current[key]}
        logger.info(
            "Migration listing cache invalidated (added=%d, removed=%d, modified=%d)",
            len(added),
            len(removed),
            len(modified),
        )
        self._metric("migrations.listing.cache_invalidations")
        if added:
            self._metric("migrations.listing.added", float(len(added)))
        if removed:
            self._metric("migrations.listing.removed", float(len(removed)))
        if modified:
            self._metric("migrations.listing.modified", float(len(modified)))

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

    def _load_migration_listing(self) -> "list[tuple[str, Path]]":
        """Build the cached migration listing shared by sync/async runners."""
        entries, signatures, digest = self._collect_listing_entries()
        cached_listing = self._listing_cache

        if cached_listing is not None and self._listing_digest == digest:
            self._metric("migrations.listing.cache_hit")
            self._metric("migrations.listing.files_cached", float(len(cached_listing)))
            logger.debug("Migration listing cache hit (%d files)", len(cached_listing))
            return cached_listing

        files = self._build_sorted_listing(entries)
        previous_digest = self._listing_digest
        previous_signatures = self._listing_signatures

        self._metric("migrations.listing.cache_miss")
        self._metric("migrations.listing.files_scanned", float(len(files)))

        self._listing_cache = files
        self._listing_signatures = signatures
        self._listing_digest = digest

        if previous_digest is None:
            logger.debug("Primed migration listing cache with %d files", len(files))
        else:
            self._log_listing_invalidation(previous_signatures, signatures)

        return files

    @abstractmethod
    def get_migration_files(self) -> "list[tuple[str, Path]] | Awaitable[list[tuple[str, Path]]]":
        """Get all migration files sorted by version."""

    def _load_migration_metadata_common(self, file_path: Path, version: "str | None" = None) -> "dict[str, Any]":
        """Load common migration metadata that doesn't require async operations.

        Args:
            file_path: Path to the migration file.
            version: Optional pre-extracted version (preserves prefixes like ext_adk_0001).

        Returns:
            Partial migration metadata dictionary.
        """
        cache_key = str(file_path)
        stat_result = file_path.stat()
        cached_metadata = self._metadata_cache.get(cache_key)
        if (
            cached_metadata
            and cached_metadata.mtime_ns == stat_result.st_mtime_ns
            and cached_metadata.size == stat_result.st_size
        ):
            self._metric("migrations.metadata.cache_hit")
            logger.debug("Migration metadata cache hit: %s", cache_key)
            metadata = cached_metadata.clone()
            metadata["file_path"] = file_path
            return metadata

        self._metric("migrations.metadata.cache_miss")
        self._metric("migrations.metadata.bytes", float(stat_result.st_size))

        content = file_path.read_text(encoding="utf-8")
        checksum = self._calculate_checksum(content)
        if version is None:
            version = self._extract_version(file_path.name)
        description = self._extract_description(content, file_path)
        if not description:
            description = file_path.stem.split("_", 1)[1] if "_" in file_path.stem else ""

        transactional_match = re.search(
            r"^--\s*transactional:\s*(true|false)\s*$", content, re.MULTILINE | re.IGNORECASE
        )
        transactional = None
        if transactional_match:
            transactional = transactional_match.group(1).lower() == "true"

        metadata = {
            "version": version,
            "description": description,
            "file_path": file_path,
            "checksum": checksum,
            "content": content,
            "transactional": transactional,
        }
        self._metadata_cache[cache_key] = _CachedMigrationMetadata(
            metadata=dict(metadata), mtime_ns=stat_result.st_mtime_ns, size=stat_result.st_size
        )
        if cached_metadata:
            logger.debug("Migration metadata cache invalidated: %s", cache_key)
        else:
            logger.debug("Cached migration metadata: %s", cache_key)
        return metadata

    def _extract_description(self, content: str, file_path: Path) -> str:
        if file_path.suffix == ".sql":
            return self._extract_sql_description(content)
        if file_path.suffix == ".py":
            return self._extract_python_description(content)
        return ""

    def _extract_sql_description(self, content: str) -> str:
        keys = self.description_hints.sql_keys
        for line in content.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("--"):
                body = stripped.lstrip("-").strip()
                if not body:
                    continue
                if ":" in body:
                    key, value = body.split(":", 1)
                    if key.strip() in keys:
                        return value.strip()
                continue
            break
        return ""

    def _extract_python_description(self, content: str) -> str:
        try:
            module = ast.parse(content)
        except SyntaxError:
            return ""
        docstring = ast.get_docstring(module) or ""
        keys = self.description_hints.python_keys
        for line in docstring.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if ":" in stripped:
                key, value = stripped.split(":", 1)
                if key.strip() in keys:
                    return value.strip()
            return stripped
        return ""

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

    def should_use_transaction(
        self, migration: "dict[str, Any]", config: "DatabaseConfigProtocol[Any, Any, Any]"
    ) -> bool:
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

        migration_config = cast("dict[str, Any]", config.migration_config) or {}
        return bool(migration_config.get("transactional", True))


class SyncMigrationRunner(BaseMigrationRunner):
    """Synchronous migration runner with pure sync methods."""

    def get_migration_files(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of (version, path) tuples sorted by version.
        """
        return self._load_migration_listing()

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
                has_downgrade = bool(self._get_migration_sql({"loader": loader, "file_path": file_path}, "down"))
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
        upgrade_sql_list = self._get_migration_sql(migration, "up")
        if upgrade_sql_list is None:
            self._metric("migrations.upgrade.skipped")
            log_with_context(
                logger,
                logging.WARNING,
                "migration.apply",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                status="missing",
            )
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        runtime = self.runtime
        span = None
        if runtime is not None:
            version = cast("str | None", migration.get("version"))
            span = runtime.start_migration_span("upgrade", version=version)
            runtime.increment_metric("migrations.upgrade.invocations")
        log_with_context(
            logger,
            logging.INFO,
            "migration.apply",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            use_transaction=use_transaction,
            status="start",
        )

        start_time = time.perf_counter()
        execution_time = 0

        try:
            if use_transaction:
                driver.begin()
                for sql_statement in upgrade_sql_list:
                    if sql_statement.strip():
                        driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    on_success(execution_time)
                driver.commit()
            else:
                for sql_statement in upgrade_sql_list:
                    if sql_statement.strip():
                        driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    on_success(execution_time)
        except Exception as exc:
            if use_transaction:
                driver.rollback()
            if runtime is not None:
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                runtime.increment_metric("migrations.upgrade.errors")
                runtime.end_migration_span(span, duration_ms=duration_ms, error=exc)
            log_with_context(
                logger,
                logging.ERROR,
                "migration.apply",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                duration_ms=int((time.perf_counter() - start_time) * 1000),
                error_type=type(exc).__name__,
                status="failed",
            )
            raise

        if runtime is not None:
            runtime.increment_metric("migrations.upgrade.applied")
            runtime.increment_metric("migrations.upgrade.duration_ms", float(execution_time))
            runtime.end_migration_span(span, duration_ms=execution_time)
        log_with_context(
            logger,
            logging.INFO,
            "migration.apply",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            duration_ms=execution_time,
            status="complete",
        )

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
        downgrade_sql_list = self._get_migration_sql(migration, "down")
        if downgrade_sql_list is None:
            self._metric("migrations.downgrade.skipped")
            log_with_context(
                logger,
                logging.WARNING,
                "migration.rollback",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                status="missing",
            )
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        runtime = self.runtime
        span = None
        if runtime is not None:
            version = cast("str | None", migration.get("version"))
            span = runtime.start_migration_span("downgrade", version=version)
            runtime.increment_metric("migrations.downgrade.invocations")
        log_with_context(
            logger,
            logging.INFO,
            "migration.rollback",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            use_transaction=use_transaction,
            status="start",
        )

        start_time = time.perf_counter()
        execution_time = 0

        try:
            if use_transaction:
                driver.begin()
                for sql_statement in downgrade_sql_list:
                    if sql_statement.strip():
                        driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    on_success(execution_time)
                driver.commit()
            else:
                for sql_statement in downgrade_sql_list:
                    if sql_statement.strip():
                        driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    on_success(execution_time)
        except Exception as exc:
            if use_transaction:
                driver.rollback()
            if runtime is not None:
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                runtime.increment_metric("migrations.downgrade.errors")
                runtime.end_migration_span(span, duration_ms=duration_ms, error=exc)
            log_with_context(
                logger,
                logging.ERROR,
                "migration.rollback",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                duration_ms=int((time.perf_counter() - start_time) * 1000),
                error_type=type(exc).__name__,
                status="failed",
            )
            raise

        if runtime is not None:
            runtime.increment_metric("migrations.downgrade.applied")
            runtime.increment_metric("migrations.downgrade.duration_ms", float(execution_time))
            runtime.end_migration_span(span, duration_ms=execution_time)
        log_with_context(
            logger,
            logging.INFO,
            "migration.rollback",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            duration_ms=execution_time,
            status="complete",
        )

        return None, execution_time

    def _get_migration_sql(self, migration: "dict[str, Any]", direction: str) -> "list[str] | None":
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

    async def get_migration_files(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of (version, path) tuples sorted by version.
        """
        return await async_(self._load_migration_listing)()

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
            self._metric("migrations.upgrade.skipped")
            log_with_context(
                logger,
                logging.WARNING,
                "migration.apply",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                status="missing",
            )
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        runtime = self.runtime
        span = None
        if runtime is not None:
            version = cast("str | None", migration.get("version"))
            span = runtime.start_migration_span("upgrade", version=version)
            runtime.increment_metric("migrations.upgrade.invocations")
        log_with_context(
            logger,
            logging.INFO,
            "migration.apply",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            use_transaction=use_transaction,
            status="start",
        )

        start_time = time.perf_counter()
        execution_time = 0

        try:
            if use_transaction:
                await driver.begin()
                for sql_statement in upgrade_sql_list:
                    if sql_statement.strip():
                        await driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    await on_success(execution_time)
                await driver.commit()
            else:
                for sql_statement in upgrade_sql_list:
                    if sql_statement.strip():
                        await driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    await on_success(execution_time)
        except Exception as exc:
            if use_transaction:
                await driver.rollback()
            if runtime is not None:
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                runtime.increment_metric("migrations.upgrade.errors")
                runtime.end_migration_span(span, duration_ms=duration_ms, error=exc)
            log_with_context(
                logger,
                logging.ERROR,
                "migration.apply",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                duration_ms=int((time.perf_counter() - start_time) * 1000),
                error_type=type(exc).__name__,
                status="failed",
            )
            raise

        if runtime is not None:
            runtime.increment_metric("migrations.upgrade.applied")
            runtime.increment_metric("migrations.upgrade.duration_ms", float(execution_time))
            runtime.end_migration_span(span, duration_ms=execution_time)
        log_with_context(
            logger,
            logging.INFO,
            "migration.apply",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            duration_ms=execution_time,
            status="complete",
        )

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
            self._metric("migrations.downgrade.skipped")
            log_with_context(
                logger,
                logging.WARNING,
                "migration.rollback",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                status="missing",
            )
            return None, 0

        if use_transaction is None:
            config = self.context.config if self.context else None
            use_transaction = self.should_use_transaction(migration, config) if config else False

        runtime = self.runtime
        span = None
        if runtime is not None:
            version = cast("str | None", migration.get("version"))
            span = runtime.start_migration_span("downgrade", version=version)
            runtime.increment_metric("migrations.downgrade.invocations")
        log_with_context(
            logger,
            logging.INFO,
            "migration.rollback",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            use_transaction=use_transaction,
            status="start",
        )

        start_time = time.perf_counter()
        execution_time = 0

        try:
            if use_transaction:
                await driver.begin()
                for sql_statement in downgrade_sql_list:
                    if sql_statement.strip():
                        await driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    await on_success(execution_time)
                await driver.commit()
            else:
                for sql_statement in downgrade_sql_list:
                    if sql_statement.strip():
                        await driver.execute_script(sql_statement)
                execution_time = int((time.perf_counter() - start_time) * 1000)
                if on_success:
                    await on_success(execution_time)
        except Exception as exc:
            if use_transaction:
                await driver.rollback()
            if runtime is not None:
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                runtime.increment_metric("migrations.downgrade.errors")
                runtime.end_migration_span(span, duration_ms=duration_ms, error=exc)
            log_with_context(
                logger,
                logging.ERROR,
                "migration.rollback",
                db_system=resolve_db_system(type(driver).__name__),
                version=migration.get("version"),
                duration_ms=int((time.perf_counter() - start_time) * 1000),
                error_type=type(exc).__name__,
                status="failed",
            )
            raise

        if runtime is not None:
            runtime.increment_metric("migrations.downgrade.applied")
            runtime.increment_metric("migrations.downgrade.duration_ms", float(execution_time))
            runtime.end_migration_span(span, duration_ms=execution_time)
        log_with_context(
            logger,
            logging.INFO,
            "migration.rollback",
            db_system=resolve_db_system(type(driver).__name__),
            version=migration.get("version"),
            duration_ms=execution_time,
            status="complete",
        )

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
    runtime: "ObservabilityRuntime | None" = None,
    description_hints: "TemplateDescriptionHints | None" = None,
) -> SyncMigrationRunner: ...


@overload
def create_migration_runner(
    migrations_path: Path,
    extension_migrations: "dict[str, Path]",
    context: "MigrationContext | None",
    extension_configs: "dict[str, Any]",
    is_async: "Literal[True]",
    runtime: "ObservabilityRuntime | None" = None,
    description_hints: "TemplateDescriptionHints | None" = None,
) -> AsyncMigrationRunner: ...


def create_migration_runner(
    migrations_path: Path,
    extension_migrations: "dict[str, Path]",
    context: "MigrationContext | None",
    extension_configs: "dict[str, Any]",
    is_async: bool = False,
    runtime: "ObservabilityRuntime | None" = None,
    description_hints: "TemplateDescriptionHints | None" = None,
) -> "SyncMigrationRunner | AsyncMigrationRunner":
    """Factory function to create the appropriate migration runner.

    Args:
        migrations_path: Path to migrations directory.
        extension_migrations: Extension migration paths.
        context: Migration context.
        extension_configs: Extension configurations.
        is_async: Whether to create async or sync runner.
        runtime: Observability runtime shared with loaders and execution steps.
        description_hints: Optional description extraction hints from template profiles.

    Returns:
        Appropriate migration runner instance.
    """
    if is_async:
        return AsyncMigrationRunner(
            migrations_path,
            extension_migrations,
            context,
            extension_configs,
            runtime=runtime,
            description_hints=description_hints,
        )
    return SyncMigrationRunner(
        migrations_path,
        extension_migrations,
        context,
        extension_configs,
        runtime=runtime,
        description_hints=description_hints,
    )
