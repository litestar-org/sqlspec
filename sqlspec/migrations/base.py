"""Base classes for SQLSpec migrations."""

import ast
import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from rich.console import Console

from sqlspec.builder import CreateTable, Delete, Insert, Select, Update, sql
from sqlspec.loader import SQLFileLoader
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.loaders import get_migration_loader
from sqlspec.migrations.templates import MigrationTemplateSettings, TemplateDescriptionHints, build_template_settings
from sqlspec.migrations.version import parse_version
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import module_to_os_path
from sqlspec.utils.sync_tools import await_

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.observability import ObservabilityRuntime

__all__ = ("BaseMigrationCommands", "BaseMigrationRunner", "BaseMigrationTracker")

DriverT = TypeVar("DriverT")
ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.migrations.base")


class BaseMigrationTracker(ABC, Generic[DriverT]):
    """Base class for migration version tracking."""

    __slots__ = ("version_table",)

    def __init__(self, version_table_name: str = "ddl_migrations") -> None:
        """Initialize the migration tracker.

        Args:
            version_table_name: Name of the table to track migrations.
        """
        self.version_table = version_table_name

    def _get_create_table_sql(self) -> CreateTable:
        """Get SQL builder for creating the tracking table.

        Schema includes both legacy and new versioning columns:
        - version_num: Migration version (sequential or timestamp format)
        - version_type: Format indicator ('sequential' or 'timestamp')
        - execution_sequence: Auto-incrementing application order
        - description: Human-readable migration description
        - applied_at: Timestamp when migration was applied
        - execution_time_ms: Migration execution duration
        - checksum: MD5 hash for content verification
        - applied_by: User who applied the migration

        Returns:
            SQL builder object for table creation.
        """
        return (
            sql
            .create_table(self.version_table)
            .if_not_exists()
            .column("version_num", "VARCHAR(32)", primary_key=True)
            .column("version_type", "VARCHAR(16)")
            .column("execution_sequence", "INTEGER")
            .column("description", "TEXT")
            .column("applied_at", "TIMESTAMP", default="CURRENT_TIMESTAMP", not_null=True)
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR(64)")
            .column("applied_by", "VARCHAR(255)")
        )

    def _get_current_version_sql(self) -> Select:
        """Get SQL builder for retrieving current version.

        Uses execution_sequence to get the last applied migration,
        which may differ from version_num order due to out-of-order migrations.

        Returns:
            SQL builder object for version query.
        """
        return sql.select("version_num").from_(self.version_table).order_by("execution_sequence DESC").limit(1)

    def _get_applied_migrations_sql(self) -> Select:
        """Get SQL builder for retrieving all applied migrations.

        Orders by execution_sequence to show migrations in application order,
        which preserves the actual execution history for out-of-order migrations.

        Returns:
            SQL builder object for migrations query.
        """
        return sql.select("*").from_(self.version_table).order_by("execution_sequence")

    def _get_next_execution_sequence_sql(self) -> Select:
        """Get SQL builder for retrieving next execution sequence.

        Returns:
            SQL builder object for sequence query.
        """
        return sql.select("COALESCE(MAX(execution_sequence), 0) + 1 AS next_seq").from_(self.version_table)

    def _get_record_migration_sql(
        self,
        version: str,
        version_type: str,
        execution_sequence: int,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
    ) -> Insert:
        """Get SQL builder for recording a migration.

        Args:
            version: Version number of the migration.
            version_type: Version format type ('sequential' or 'timestamp').
            execution_sequence: Auto-incrementing application order.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: MD5 checksum of the migration content.
            applied_by: User who applied the migration.

        Returns:
            SQL builder object for insert.
        """
        return (
            sql
            .insert(self.version_table)
            .columns(
                "version_num",
                "version_type",
                "execution_sequence",
                "description",
                "execution_time_ms",
                "checksum",
                "applied_by",
            )
            .values(version, version_type, execution_sequence, description, execution_time_ms, checksum, applied_by)
        )

    def _get_remove_migration_sql(self, version: str) -> Delete:
        """Get SQL builder for removing a migration record.

        Args:
            version: Version number to remove.

        Returns:
            SQL builder object for delete.
        """
        return sql.delete().from_(self.version_table).where(sql.version_num == version)

    def _get_update_version_sql(self, old_version: str, new_version: str, new_version_type: str) -> Update:
        """Get SQL builder for updating version record.

        Updates version_num and version_type while preserving execution_sequence,
        applied_at, and other metadata. Used during fix command to convert
        timestamp versions to sequential format.

        Args:
            old_version: Current version string.
            new_version: New version string.
            new_version_type: New version type ('sequential' or 'timestamp').

        Returns:
            SQL builder object for update.
        """
        return (
            sql
            .update(self.version_table)
            .set("version_num", new_version)
            .set("version_type", new_version_type)
            .where(sql.version_num == old_version)
        )

    def _get_check_column_exists_sql(self) -> Select:
        """Get SQL to check what columns exist in the tracking table.

        Returns a query that will fail gracefully if the table doesn't exist,
        and returns column names if it does.

        Returns:
            SQL builder object for column check query.
        """
        return sql.select("*").from_(self.version_table).limit(0)

    def _get_add_missing_columns_sql(self, missing_columns: "set[str]") -> "list[str]":
        """Generate ALTER TABLE statements to add missing columns.

        Args:
            missing_columns: Set of column names that need to be added.

        Returns:
            List of SQL statements to execute.
        """

        statements = []
        target_create = self._get_create_table_sql()

        column_definitions = {col.name.lower(): col for col in target_create.columns}

        for col_name in sorted(missing_columns):
            if col_name in column_definitions:
                col_def = column_definitions[col_name]
                alter = sql.alter_table(self.version_table).add_column(
                    name=col_def.name,
                    dtype=col_def.dtype,
                    default=col_def.default,
                    not_null=col_def.not_null,
                    unique=col_def.unique,
                    comment=col_def.comment,
                )
                statements.append(str(alter))

        return statements

    def _detect_missing_columns(self, existing_columns: "set[str]") -> "set[str]":
        """Detect which columns are missing from the current schema.

        Args:
            existing_columns: Set of existing column names (may be uppercase/lowercase).

        Returns:
            Set of missing column names (lowercase).
        """
        target_create = self._get_create_table_sql()
        target_columns = {col.name.lower() for col in target_create.columns}
        existing_lower = {col.lower() for col in existing_columns}
        return target_columns - existing_lower

    @abstractmethod
    def ensure_tracking_table(self, driver: DriverT) -> Any:
        """Create the migration tracking table if it doesn't exist.

        Implementations should also check for and add any missing columns
        to support schema migrations from older versions.
        """
        ...

    @abstractmethod
    def get_current_version(self, driver: DriverT) -> Any:
        """Get the latest applied migration version."""
        ...

    @abstractmethod
    def get_applied_migrations(self, driver: DriverT) -> Any:
        """Get all applied migrations in order."""
        ...

    @abstractmethod
    def record_migration(
        self, driver: DriverT, version: str, description: str, execution_time_ms: int, checksum: str
    ) -> Any:
        """Record a successfully applied migration."""
        ...

    @abstractmethod
    def remove_migration(self, driver: DriverT, version: str) -> Any:
        """Remove a migration record."""
        ...


class BaseMigrationRunner(ABC, Generic[DriverT]):
    """Base class for migration execution."""

    extension_configs: "dict[str, dict[str, Any]]"

    def __init__(
        self,
        migrations_path: Path,
        extension_migrations: "dict[str, Path] | None" = None,
        context: "Any | None" = None,
        extension_configs: "dict[str, dict[str, Any]] | None" = None,
        description_hints: "TemplateDescriptionHints | None" = None,
    ) -> None:
        """Initialize the migration runner.

        Args:
            migrations_path: Path to the directory containing migration files.
            extension_migrations: Optional mapping of extension names to their migration paths.
            context: Optional migration context for Python migrations.
            extension_configs: Optional mapping of extension names to their configurations.
            description_hints: Preferred metadata keys for extracting human descriptions
                from SQL comments and Python docstrings.
        """
        self.migrations_path = migrations_path
        self.extension_migrations = extension_migrations or {}
        self.loader = SQLFileLoader()
        self.project_root: Path | None = None
        self.context = context
        self.extension_configs = extension_configs or {}
        self.description_hints = description_hints or TemplateDescriptionHints()

    def _extract_version(self, filename: str) -> str | None:
        """Extract version from filename.

        Args:
            filename: The migration filename.

        Returns:
            The extracted version string or None.
        """
        stem = Path(filename).stem

        if stem.startswith("ext_"):
            return stem

        parts = stem.split("_", 1)
        return parts[0].zfill(4) if parts and parts[0].isdigit() else None

    def _calculate_checksum(self, content: str) -> str:
        """Calculate MD5 checksum of migration content.

        Args:
            content: The migration file content.

        Returns:
            MD5 checksum hex string.
        """

        return hashlib.md5(content.encode()).hexdigest()  # noqa: S324

    def _get_migration_files_sync(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Uses version-aware sorting that handles both sequential and timestamp
        formats correctly, with extension migrations sorted by extension name.

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

        return sorted(migrations, key=_migration_sort_key)

    def _load_migration_metadata(self, file_path: Path, version: "str | None" = None) -> "dict[str, Any]":
        """Load migration metadata from file.

        Args:
            file_path: Path to the migration file.
            version: Optional pre-extracted version (preserves prefixes like ext_adk_0001).

        Returns:
            Migration metadata dictionary.
        """
        if version is None:
            version = self._extract_version(file_path.name)

        context_to_use = self.context

        for ext_name, ext_path in self.extension_migrations.items():
            if file_path.parent == ext_path:
                if ext_name in self.extension_configs and self.context:
                    context_to_use = MigrationContext(
                        dialect=self.context.dialect,
                        config=self.context.config,
                        driver=self.context.driver,
                        metadata=self.context.metadata.copy() if self.context.metadata else {},
                        extension_config=self.extension_configs[ext_name],
                    )
                break

        loader = get_migration_loader(file_path, self.migrations_path, self.project_root, context_to_use)
        loader.validate_migration_file(file_path)
        content = file_path.read_text(encoding="utf-8")
        checksum = self._calculate_checksum(content)
        description = self._extract_description(content, file_path)
        if not description:
            description = file_path.stem.split("_", 1)[1] if "_" in file_path.stem else ""

        has_upgrade, has_downgrade = True, False

        if file_path.suffix == ".sql":
            up_query, down_query = f"migrate-{version}-up", f"migrate-{version}-down"
            self.loader.clear_cache()
            self.loader.load_sql(file_path)
            has_upgrade, has_downgrade = self.loader.has_query(up_query), self.loader.has_query(down_query)
        else:
            try:
                has_downgrade = bool(await_(loader.get_down_sql, raise_sync_error=False)(file_path))
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

    def _get_migration_sql(self, migration: "dict[str, Any]", direction: str) -> "list[str] | None":
        """Get migration SQL for given direction.

        Args:
            migration: Migration metadata.
            direction: Either 'up' or 'down'.

        Returns:
            SQL object for the migration.
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
            sql_statements = await_(method, raise_sync_error=False)(file_path)

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

    @abstractmethod
    def get_migration_files(self) -> Any:
        """Get all migration files sorted by version."""
        ...

    @abstractmethod
    def load_migration(self, file_path: Path) -> Any:
        """Load a migration file and extract its components."""
        ...

    @abstractmethod
    def execute_upgrade(self, driver: DriverT, migration: "dict[str, Any]") -> Any:
        """Execute an upgrade migration."""
        ...

    @abstractmethod
    def execute_downgrade(self, driver: DriverT, migration: "dict[str, Any]") -> Any:
        """Execute a downgrade migration."""
        ...

    @abstractmethod
    def load_all_migrations(self) -> Any:
        """Load all migrations into a single namespace for bulk operations."""
        ...


def _migration_sort_key(item: "tuple[str, Path]") -> Any:
    return parse_version(item[0])


class BaseMigrationCommands(ABC, Generic[ConfigT, DriverT]):
    """Base class for migration commands."""

    extension_configs: "dict[str, dict[str, Any]]"

    def __init__(self, config: ConfigT) -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        self.config = config
        migration_config = cast("dict[str, Any]", self.config.migration_config) or {}

        self.version_table = migration_config.get("version_table_name", "ddl_migrations")
        self.migrations_path = Path(migration_config.get("script_location", "migrations"))
        self.project_root = Path(migration_config["project_root"]) if "project_root" in migration_config else None
        self.include_extensions = migration_config.get("include_extensions", [])
        self.extension_configs = self._parse_extension_configs()
        self._template_settings: MigrationTemplateSettings = build_template_settings(migration_config)
        self._runtime: ObservabilityRuntime | None = self.config.get_observability_runtime()
        self._last_command_error: Exception | None = None
        self._last_command_metrics: dict[str, float] | None = None

    def _parse_extension_configs(self) -> "dict[str, dict[str, Any]]":
        """Parse extension configurations from include_extensions.

        Reads extension configuration from config.extension_config for each
        extension listed in include_extensions.

        Returns:
            Dictionary mapping extension names to their configurations.
        """
        configs = {}

        for ext_config in self.include_extensions:
            if not isinstance(ext_config, str):
                logger.warning("Extension must be a string name, got: %s", ext_config)
                continue

            ext_name = ext_config
            ext_options = cast("dict[str, Any]", self.config.extension_config).get(ext_name, {})
            configs[ext_name] = ext_options

        return configs

    def _discover_extension_migrations(self) -> "dict[str, Path]":
        """Discover migration paths for configured extensions.

        Returns:
            Dictionary mapping extension names to their migration paths.
        """

        extension_migrations = {}

        for ext_name in self.extension_configs:
            module_name = "sqlspec.extensions.litestar" if ext_name == "litestar" else f"sqlspec.extensions.{ext_name}"

            try:
                module_path = module_to_os_path(module_name)
                migrations_dir = module_path / "migrations"

                if migrations_dir.exists():
                    extension_migrations[ext_name] = migrations_dir
                    logger.debug("Found migrations for extension %s at %s", ext_name, migrations_dir)
                else:
                    logger.warning("No migrations directory found for extension %s", ext_name)
            except TypeError:
                logger.warning("Extension %s not found", ext_name)

        return extension_migrations

    def _get_init_readme_content(self) -> str:
        """Get README content for migration directory initialization.

        Returns:
            README markdown content.
        """
        return """# SQLSpec Migrations

This directory contains database migration files.

## File Format

Migration files use SQLFileLoader's named query syntax with versioned names:

```sql
-- name: migrate-20251011120000-up
CREATE TABLE example (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-20251011120000-down
DROP TABLE example;
```

## Naming Conventions

### File Names

Format: `{version}_{description}.sql`

- Version: Timestamp in YYYYMMDDHHmmss format (UTC)
- Description: Brief description using underscores
- Example: `20251011120000_create_users_table.sql`

### Query Names

- Upgrade: `migrate-{version}-up`
- Downgrade: `migrate-{version}-down`

## Version Format

Migrations use **timestamp-based versioning** (YYYYMMDDHHmmss):

- **Format**: 14-digit UTC timestamp
- **Example**: `20251011120000` (October 11, 2025 at 12:00:00 UTC)
- **Benefits**: Eliminates merge conflicts when multiple developers create migrations concurrently

### Creating Migrations

Use the CLI to generate timestamped migrations:

```bash
sqlspec create-migration "add user table"
# Creates: 20251011120000_add_user_table.sql
```

The timestamp is automatically generated in UTC timezone.

## Migration Execution

Migrations are applied in chronological order based on their timestamps.
The database tracks both version and execution order separately to handle
out-of-order migrations gracefully (e.g., from late-merging branches).
"""

    def _get_init_init_content(self) -> str:
        """Get __init__.py content for migration directory initialization.

        Returns:
            Python module docstring content for the __init__.py file.
        """
        return """Migrations.
"""

    def init_directory(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        console = Console()

        migrations_dir = Path(directory)
        migrations_dir.mkdir(parents=True, exist_ok=True)

        if package:
            init = migrations_dir / "__init__.py"
            init.write_text(self._get_init_init_content())

        readme = migrations_dir / "README.md"
        readme.write_text(self._get_init_readme_content())

        console.print(f"[green]Initialized migrations in {directory}[/]")

    def _record_command_metric(self, name: str, value: float) -> None:
        """Accumulate per-command metrics for decorator flushing."""

        if self._last_command_metrics is None:
            self._last_command_metrics = {}
        self._last_command_metrics[name] = self._last_command_metrics.get(name, 0.0) + value

    @abstractmethod
    def init(self, directory: str, package: bool = True) -> Any:
        """Initialize migration directory structure."""
        ...

    @abstractmethod
    def current(self, verbose: bool = False) -> Any:
        """Show current migration version."""
        ...

    @abstractmethod
    def upgrade(self, revision: str = "head") -> Any:
        """Upgrade to a target revision."""
        ...

    @abstractmethod
    def downgrade(self, revision: str = "-1") -> Any:
        """Downgrade to a target revision."""
        ...

    @abstractmethod
    def stamp(self, revision: str) -> Any:
        """Mark database as being at a specific revision without running migrations."""
        ...

    @abstractmethod
    def revision(self, message: str, file_type: str = "sql") -> Any:
        """Create a new migration file."""
        ...
