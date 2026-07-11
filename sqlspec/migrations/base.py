"""Base classes for SQLSpec migrations."""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from mypy_extensions import mypyc_attr
from rich.console import Console
from typing_extensions import NotRequired, TypedDict

from sqlspec.builder import AlterTable, CreateTable, Delete, Insert, Select, Update, sql
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.templates import MigrationTemplateSettings, build_template_settings
from sqlspec.migrations.utils import resolve_default_schema as _resolve_default_schema
from sqlspec.migrations.utils import resolve_tracker_schema as _resolve_tracker_schema
from sqlspec.migrations.version import parse_version
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import module_to_os_path

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.observability import ObservabilityRuntime

__all__ = ("AppliedMigrationRecord", "BaseMigrationCommands", "BaseMigrationTracker", "LoadedMigrationMetadata")

DriverT = TypeVar("DriverT")
ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.migrations.base")


class LoadedMigrationMetadata(TypedDict):
    """Metadata for a migration loaded from a file.

    Keyed on ``version`` (file-derived). The ``has_upgrade``, ``has_downgrade``,
    and ``loader`` keys are added by ``load_migration`` after the base metadata
    is built by ``_load_metadata``.
    """

    version: "str | None"
    description: str
    file_path: "Path"
    checksum: str
    content: str
    transactional: "bool | None"
    has_upgrade: NotRequired[bool]
    has_downgrade: NotRequired[bool]
    loader: NotRequired[Any]


class AppliedMigrationRecord(TypedDict):
    """Row from the migration tracking table.

    Keyed on ``version_num`` (database column), unlike file-loaded metadata
    which keys on ``version``.
    """

    version_num: str
    version_type: str
    execution_sequence: int
    description: str
    applied_at: Any
    execution_time_ms: int
    checksum: str
    applied_by: "str | None"
    replaces: "str | None"


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseMigrationTracker(ABC, Generic[DriverT]):
    """Base class for migration version tracking."""

    __slots__ = ("_output_policy", "version_table", "version_table_name", "version_table_schema")

    def __init__(self, version_table_name: str = "ddl_migrations", version_table_schema: str | None = None) -> None:
        """Initialize the migration tracker.

        ``version_table_name`` may include a ``"schema.table"`` prefix for adapters
        that historically embedded the schema in the table name. When both that
        prefix and ``version_table_schema`` are supplied and agree, the prefix is
        stripped so the qualified table is not double-prefixed.

        Args:
            version_table_name: Name of the table to track migrations.
            version_table_schema: Optional schema that stores the tracking table.
        """
        bare_name, embedded_schema = self._split_version_table(version_table_name)
        resolved_schema = version_table_schema or embedded_schema
        self.version_table_name = bare_name
        self.version_table_schema = resolved_schema
        self.version_table = self._qualify_version_table(bare_name, resolved_schema)
        self._output_policy = {"use_logger": False, "echo": True, "summary_only": False}

    @staticmethod
    def _split_version_table(version_table_name: str) -> "tuple[str, str | None]":
        """Split a ``schema.table`` value into (table, schema)."""
        if "." not in version_table_name:
            return version_table_name, None
        schema, _, table = version_table_name.rpartition(".")
        return table, schema or None

    def _qualify_version_table(self, version_table_name: str, version_table_schema: str | None) -> str:
        """Return the tracker table name, qualified with schema when configured."""
        if version_table_schema:
            return f"{version_table_schema}.{version_table_name}"
        return version_table_name

    def _tracking_table_builder(self) -> CreateTable:
        """Return a CREATE TABLE builder for the tracker table."""
        builder = sql.create_table(self.version_table_name)
        if self.version_table_schema:
            builder.in_schema(self.version_table_schema)
        return builder

    def set_output_policy(self, *, use_logger: bool, echo: bool, summary_only: bool) -> None:
        """Set output policy for tracker console/logging behavior."""
        self._output_policy = {"use_logger": use_logger, "echo": echo, "summary_only": summary_only}

    def _should_echo(self) -> bool:
        """Return True when console output should be emitted."""
        return bool(self._output_policy.get("echo", True)) and not bool(self._output_policy.get("use_logger", False))

    def _tracking_table_ddl(self) -> CreateTable:
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
            self
            ._tracking_table_builder()
            .if_not_exists()
            .column("version_num", "VARCHAR(32)", primary_key=True)
            .column("version_type", "VARCHAR(16)")
            .column("execution_sequence", "INTEGER")
            .column("description", "TEXT")
            .column("applied_at", "TIMESTAMP", default="CURRENT_TIMESTAMP", not_null=True)
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR(64)")
            .column("applied_by", "VARCHAR(255)")
            .column("replaces", "TEXT")
        )

    def _current_version_query(self) -> Select:
        """Get SQL builder for retrieving current version.

        Uses execution_sequence to get the last applied migration,
        which may differ from version_num order due to out-of-order migrations.

        Returns:
            SQL builder object for version query.
        """
        return sql.select("version_num").from_(self.version_table).order_by("execution_sequence DESC").limit(1)

    def _applied_migrations_query(self) -> Select:
        """Get SQL builder for retrieving all applied migrations.

        Orders by execution_sequence to show migrations in application order,
        which preserves the actual execution history for out-of-order migrations.

        Returns:
            SQL builder object for migrations query.
        """
        return sql.select("*").from_(self.version_table).order_by("execution_sequence")

    def _next_execution_sequence_query(self) -> Select:
        """Get SQL builder for retrieving next execution sequence.

        Returns:
            SQL builder object for sequence query.
        """
        return sql.select("COALESCE(MAX(execution_sequence), 0) + 1 AS next_seq").from_(self.version_table)

    def _record_migration_statement(
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

    def _remove_migration_statement(self, version: str) -> Delete:
        """Get SQL builder for removing a migration record.

        Args:
            version: Version number to remove.

        Returns:
            SQL builder object for delete.
        """
        return sql.delete().from_(self.version_table).where(sql.version_num == version)

    def _update_version_statement(self, old_version: str, new_version: str, new_version_type: str) -> Update:
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

    def _delete_versions_statement(self, versions: "list[str]") -> Delete:
        """Get SQL builder for deleting multiple version records.

        Used by squash operations to remove replaced migration records.

        Args:
            versions: List of version strings to delete.

        Returns:
            SQL builder object for delete.
        """
        return sql.delete().from_(self.version_table).where(sql.version_num.in_(versions))

    def _check_versions_query(self, versions: "list[str]") -> Select:
        """Get SQL builder for checking whether any versions exist.

        Args:
            versions: List of version strings to check.

        Returns:
            SQL builder object for version existence query.
        """
        return sql.select("version_num").from_(self.version_table).where(sql.version_num.in_(versions))

    def _record_squashed_migration_statement(
        self,
        version: str,
        version_type: str,
        execution_sequence: int,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
        replaces: str,
    ) -> Insert:
        """Get SQL builder for recording a squashed migration.

        Args:
            version: Version number of the squashed migration.
            version_type: Version format type ('sequential' or 'timestamp').
            execution_sequence: Auto-incrementing application order.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: MD5 checksum of the migration content.
            applied_by: User who applied the migration.
            replaces: Comma-separated list of replaced versions.

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
                "replaces",
            )
            .values(
                version,
                version_type,
                execution_sequence,
                description,
                execution_time_ms,
                checksum,
                applied_by,
                replaces,
            )
        )

    def _column_exists_query(self) -> Select:
        """Get SQL to check what columns exist in the tracking table.

        Returns a query that will fail gracefully if the table doesn't exist,
        and returns column names if it does.

        Returns:
            SQL builder object for column check query.
        """
        return sql.select("*").from_(self.version_table).limit(0)

    def _detect_missing_columns(self, existing_columns: "set[str]") -> "set[str]":
        """Detect which columns are missing from the current schema.

        Args:
            existing_columns: Set of existing column names (may be uppercase/lowercase).

        Returns:
            Set of missing column names (lowercase).
        """
        target_create = self._tracking_table_ddl()
        target_columns = {col.name.lower() for col in target_create.columns}
        existing_lower = {col.lower() for col in existing_columns}
        return target_columns - existing_lower

    @abstractmethod
    def ensure_tracking_table(self, driver: DriverT) -> "None | Awaitable[None]":
        """Create the migration tracking table if it doesn't exist.

        Implementations should also check for and add any missing columns
        to support schema migrations from older versions.
        """
        ...

    @abstractmethod
    def get_current_version(self, driver: DriverT) -> "str | None | Awaitable[str | None]":
        """Get the latest applied migration version."""
        ...

    @abstractmethod
    def get_applied_migrations(
        self, driver: DriverT
    ) -> "list[AppliedMigrationRecord] | Awaitable[list[AppliedMigrationRecord]]":
        """Get all applied migrations in order."""
        ...

    @abstractmethod
    def record_migration(
        self, driver: DriverT, version: str, description: str, execution_time_ms: int, checksum: str
    ) -> "None | Awaitable[None]":
        """Record a successfully applied migration."""
        ...

    @abstractmethod
    def remove_migration(self, driver: DriverT, version: str) -> "None | Awaitable[None]":
        """Remove a migration record."""
        ...

    def _build_add_column_statement(self, column_name: str) -> "AlterTable | None":
        """Return an ALTER TABLE builder that adds ``column_name``.

        Args:
            column_name: Name of the tracking-table column to add (lowercase).

        Returns:
            SQL builder for the ALTER TABLE, or None when the column is unknown.
        """
        target_create = self._tracking_table_ddl()
        column_def = next((col for col in target_create.columns if col.name.lower() == column_name), None)
        if not column_def:
            return None
        return sql.alter_table(self.version_table).add_column(
            name=column_def.name, dtype=column_def.dtype, default=column_def.default, not_null=column_def.not_null
        )

    def _add_column_statements(self, missing_columns: "set[str]") -> "list[tuple[str, AlterTable]]":
        """Build all missing-column statements from one target-DDL snapshot."""
        target_columns = {column.name.lower(): column for column in self._tracking_table_ddl().columns}
        statements: list[tuple[str, AlterTable]] = []
        for column_name in sorted(missing_columns):
            column = target_columns.get(column_name)
            if column is not None:
                statements.append((
                    column_name,
                    sql.alter_table(self.version_table).add_column(
                        name=column.name, dtype=column.dtype, default=column.default, not_null=column.not_null
                    ),
                ))
        return statements

    def _derive_version_type(self, version: str) -> str:
        """Return the version-format type ('sequential' or 'timestamp') for a version."""
        return parse_version(version).type.value

    def _applied_by(self) -> str:
        """Return the user recorded as having applied a migration."""
        return os.environ.get("USER", "unknown")

    def _extract_next_sequence(self, result: Any) -> int:
        """Return the next execution sequence from a sequence-query result."""
        return result.get_data()[0]["next_seq"] if result.data else 1

    def _extract_applied_versions(self, result: Any) -> "set[str]":
        """Return the set of applied version numbers from a query result."""
        return {row["version_num"] for row in result.get_data()} if result.data else set()

    def _is_autocommit_error(self, exc: Exception) -> bool:
        """Return True when an exception indicates an autocommit-managed transaction."""
        exc_str = str(exc).lower()
        return "autocommit" in exc_str or "cannot commit" in exc_str


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseMigrationCommands(ABC, Generic[ConfigT, DriverT]):
    """Base class for migration commands."""

    extension_configs: "dict[str, dict[str, Any]]"

    def __init__(self, config: ConfigT) -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        self.config = config
        migration_config = self._get_migration_config()

        self.version_table = migration_config.get("version_table_name", "ddl_migrations")
        self.migrations_path = Path(migration_config.get("script_location", "migrations"))
        self.project_root = Path(migration_config["project_root"]) if "project_root" in migration_config else None
        self.include_extensions = migration_config.get("include_extensions", [])
        self.extension_configs = self._parse_extension_configs()
        self._template_settings: MigrationTemplateSettings = build_template_settings(migration_config)
        self._runtime: ObservabilityRuntime | None = self.config.get_observability_runtime()
        self._last_command_error: Exception | None = None
        self._last_command_metrics: dict[str, float] | None = None

    def _get_migration_config(self) -> "dict[str, Any]":
        """Return migration config as a plain dictionary."""
        return cast("dict[str, Any]", self.config.migration_config) or {}

    def _resolve_default_schema(self) -> str | None:
        """Return the configured default migration schema."""
        return _resolve_default_schema(self._get_migration_config())

    def _config_supports_schemas(self) -> bool:
        """Return whether the bound config opts into schema-aware migrations."""
        return bool(getattr(self.config, "supports_migration_schemas", False))

    def _require_schema_support(self, default_schema: str) -> None:
        """Raise when ``default_schema`` is configured for an unsupported adapter."""
        if self._config_supports_schemas():
            return
        adapter = type(self.config).__name__
        msg = (
            f"{adapter} does not support migration default schemas; "
            f"remove default_schema={default_schema!r} from migration_config."
        )
        raise MigrationError(msg)

    def _resolve_tracker_schema(self) -> str | None:
        """Return tracker schema only for adapters that support schema-qualified migration tables."""
        if not self._config_supports_schemas():
            return None
        return _resolve_tracker_schema(self._get_migration_config())

    def _create_tracker(self) -> Any:
        """Create the configured migration tracker without breaking legacy constructors."""
        tracker_schema = self._resolve_tracker_schema()
        if tracker_schema is None:
            return self.config.migration_tracker_type(self.version_table)
        return self.config.migration_tracker_type(self.version_table, version_table_schema=tracker_schema)

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
out-of-order migrations gracefully.
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

        use_logger, echo, summary_only = self._resolve_output_policy(False, None, None)
        if echo and not use_logger:
            console.print(f"[green]Initialized migrations in {directory}[/]")
        elif use_logger and not summary_only:
            logger.info("Initialized migrations in %s", directory)

    def _record_command_metric(self, name: str, value: float) -> None:
        """Accumulate per-command metrics for decorator flushing."""

        if self._last_command_metrics is None:
            self._last_command_metrics = {}
        self._last_command_metrics[name] = self._last_command_metrics.get(name, 0.0) + value

    def _collect_pending_migrations(
        self, all_migrations: "list[tuple[str, Path]]", applied_set: set[str], revision: str
    ) -> "list[tuple[str, Path]]":
        """Collect pending migrations that need to be applied."""
        pending = []
        for version, file_path in all_migrations:
            if version not in applied_set:
                if revision == "head":
                    pending.append((version, file_path))
                else:
                    parsed_version = parse_version(version)
                    parsed_revision = parse_version(revision)
                    if parsed_version <= parsed_revision:
                        pending.append((version, file_path))
        return pending

    def _collect_revert_migrations(
        self, applied: "list[AppliedMigrationRecord]", revision: str
    ) -> "list[AppliedMigrationRecord]":
        """Collect migrations to revert based on target revision."""
        if revision == "-1":
            return [applied[-1]]
        if revision == "base":
            return list(reversed(applied))
        parsed_revision = parse_version(revision)
        to_revert = []
        for migration in reversed(applied):
            parsed_migration_version = parse_version(migration["version_num"])
            if parsed_migration_version > parsed_revision:
                to_revert.append(migration)
        return to_revert

    def _resolve_use_logger(self, method_value: bool) -> bool:
        """Resolve effective use_logger setting.

        Method parameter takes precedence over config default. When the method
        parameter is True, logger output is used. When False, we check the config
        default.

        Args:
            method_value: The use_logger parameter passed to the method.

        Returns:
            True to use logger output, False for Rich console output.
        """
        if method_value:
            return True
        migration_config = self._get_migration_config()
        return bool(migration_config.get("use_logger", False))

    def _resolve_echo(self, method_value: "bool | None") -> bool:
        """Resolve effective echo setting."""
        if method_value is not None:
            return bool(method_value)
        migration_config = self._get_migration_config()
        return bool(migration_config.get("echo", True))

    def _resolve_summary_only(self, method_value: "bool | None") -> bool:
        """Resolve effective summary_only setting."""
        if method_value is not None:
            return bool(method_value)
        migration_config = self._get_migration_config()
        return bool(migration_config.get("summary_only", False))

    def _resolve_output_policy(
        self, use_logger: bool, echo: "bool | None", summary_only: "bool | None"
    ) -> "tuple[bool, bool, bool]":
        """Resolve output policy with method values overriding config defaults."""
        resolved_use_logger = self._resolve_use_logger(use_logger)
        resolved_echo = self._resolve_echo(echo)
        resolved_summary_only = self._resolve_summary_only(summary_only)
        return resolved_use_logger, resolved_echo, resolved_summary_only

    @abstractmethod
    def init(self, directory: str, package: bool = True) -> "None | Awaitable[None]":
        """Initialize migration directory structure."""
        ...

    @abstractmethod
    def current(self, verbose: bool = False) -> "str | None | Awaitable[str | None]":
        """Show current migration version."""
        ...

    @abstractmethod
    def upgrade(self, revision: str = "head") -> "None | Awaitable[None]":
        """Upgrade to a target revision."""
        ...

    @abstractmethod
    def downgrade(self, revision: str = "-1") -> "None | Awaitable[None]":
        """Downgrade to a target revision."""
        ...

    @abstractmethod
    def stamp(self, revision: str) -> "None | Awaitable[None]":
        """Mark database as being at a specific revision without running migrations."""
        ...

    @abstractmethod
    def revision(self, message: str, file_type: str | None = None) -> "None | Awaitable[None]":
        """Create a new migration file."""
        ...
