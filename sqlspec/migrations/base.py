"""Base classes for SQLSpec migrations."""

import hashlib
import importlib
import os
from abc import abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, NotRequired, TypeVar, cast

from mypy_extensions import mypyc_attr
from sqlglot import exp
from typing_extensions import TypedDict

from sqlspec.builder._column import Column
from sqlspec.builder._ddl import CreateTable, _parse_ddl_identifier, _parse_ddl_table
from sqlspec.builder._delete import Delete
from sqlspec.builder._insert import Insert
from sqlspec.builder._select import Select
from sqlspec.builder._update import Update
from sqlspec.exceptions import MigrationError, SQLSpecError
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.migrations.templates import MigrationTemplateSettings, build_template_settings
from sqlspec.migrations.utils import (
    resolve_default_schema,
    resolve_extension_migration_mode,
    resolve_extension_migrations_path,
    resolve_tracker_schema,
)
from sqlspec.migrations.version import parse_version
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import module_to_os_path

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.observability import ObservabilityRuntime

__all__ = (
    "AppliedMigrationRecord",
    "BaseMigrationCommands",
    "BaseMigrationTracker",
    "ExtensionReconcilerDescriptor",
    "LoadedMigrationMetadata",
)

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
    """Sequential or timestamp migration version parsed from the filename or header."""
    description: str
    """Human-readable migration summary read from the file metadata or filename."""
    file_path: "Path"
    """Filesystem path to the ``.sql`` or ``.py`` migration script."""
    checksum: str
    """Hexadecimal digest of the normalized migration source content."""
    content: str
    """Raw UTF-8 source text of the migration file."""
    transactional: "bool | None"
    """Explicit per-migration transaction override, or ``None`` to use adapter defaults."""
    schema: NotRequired["str | None"]
    """Optional session schema directive parsed from ``-- schema: <name>``."""
    replaces: NotRequired[list[str]]
    """Prior migration version identifiers superseded by this squashed revision."""
    has_upgrade: NotRequired[bool]
    """Whether the migration defines an executable upgrade step."""
    has_downgrade: NotRequired[bool]
    """Whether the migration defines a reversible downgrade step."""
    loader: NotRequired[Any]
    """Migration loader instance responsible for executing this script."""


class AppliedMigrationRecord(TypedDict):
    """Row from the migration tracking table.

    Keyed on ``version_num`` (database column), unlike file-loaded metadata
    which keys on ``version``.
    """

    version_num: str
    """Applied migration version identifier stored in ``ddl_migrations``."""
    version_type: str
    """Persisted migration version format (``sequential`` or ``timestamp``)."""
    execution_sequence: int
    """Monotonic ordinal recording the order in which migrations were applied."""
    description: str
    """Migration description persisted when the version was applied."""
    applied_at: Any
    """Timestamp recording when the migration completed."""
    execution_time_ms: int
    """Wall-clock execution duration of the upgrade step in milliseconds."""
    checksum: str
    """Content digest recorded at apply time for drift detection."""
    applied_by: "str | None"
    """Database user or OS/git actor recorded when applying the migration."""
    replaces: "str | None"
    """Comma-separated prior version identifiers superseded by a squashed migration."""


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseMigrationTracker(Generic[DriverT]):
    """Base class for migration version tracking."""

    _POSTGRES_LOCK_DIALECTS: "ClassVar[frozenset[str]]" = frozenset({
        "postgres",
        "postgresql",
        "pgvector",
        "paradedb",
        "pg_textsearch",
    })
    _MYSQL_LOCK_DIALECTS: "ClassVar[frozenset[str]]" = frozenset({"mysql", "mariadb"})
    _MSSQL_LOCK_DIALECTS: "ClassVar[frozenset[str]]" = frozenset({"mssql", "tsql"})
    _SEQUENCED_INSERT_DIALECTS: "ClassVar[frozenset[str]]" = frozenset({
        *_POSTGRES_LOCK_DIALECTS,
        *_MYSQL_LOCK_DIALECTS,
        "sqlite",
        "duckdb",
    })
    _MSSQL_ACQUIRE_LOCK_SQL: "ClassVar[str]" = (
        "DECLARE @sqlspec_res INT; "
        "EXEC @sqlspec_res = sp_getapplock :lock_name, 'Exclusive', 'Session', :timeout_ms; "
        "IF @sqlspec_res < 0 THROW 51000, 'Migration lock acquisition failed', 1; "
        "SELECT @sqlspec_res"
    )
    _MSSQL_RELEASE_LOCK_SQL: "ClassVar[str]" = "EXEC sp_releaseapplock :lock_name, 'Session'"

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
        parsed_table = _parse_ddl_table(version_table_name)
        table_identifier = (
            parsed_table.this
            if isinstance(parsed_table.this, exp.Identifier)
            else _parse_ddl_identifier(version_table_name)
        )
        embedded_db = parsed_table.args.get("db")
        embedded_schema_identifier = embedded_db if isinstance(embedded_db, exp.Identifier) else None

        explicit_schema_identifier = _parse_ddl_identifier(version_table_schema) if version_table_schema else None
        if explicit_schema_identifier is not None:
            if (
                embedded_schema_identifier is not None
                and explicit_schema_identifier.name == embedded_schema_identifier.name
                and embedded_schema_identifier.quoted
                and not explicit_schema_identifier.quoted
            ):
                resolved_schema_identifier: exp.Identifier | None = embedded_schema_identifier
            else:
                resolved_schema_identifier = explicit_schema_identifier
        else:
            resolved_schema_identifier = embedded_schema_identifier

        self.version_table_name = table_identifier.name
        self.version_table_schema = resolved_schema_identifier.name if resolved_schema_identifier is not None else None
        self.version_table = self._qualify_version_table(
            table_identifier.sql(copy=False),
            resolved_schema_identifier.sql(copy=False) if resolved_schema_identifier is not None else None,
        )
        self._output_policy = {"use_logger": False, "echo": True, "summary_only": False}
        self._sqlite_lock_owners: set[int] = set()

    def set_output_policy(self, *, use_logger: bool, echo: bool, summary_only: bool) -> None:
        """Set output policy for tracker console/logging behavior."""
        self._output_policy = {"use_logger": use_logger, "echo": echo, "summary_only": summary_only}

    def holds_transaction_lock(self, driver: Any) -> bool:
        """Return True while this tracker owns an outer SQLite transaction lock on ``driver``."""
        return id(driver) in self._sqlite_lock_owners

    @abstractmethod
    def ensure_tracking_table(self, driver: DriverT) -> "Awaitable[None] | None":
        """Create the migration tracking table if it doesn't exist.

        Implementations should also check for and add any missing columns
        to support schema migrations from older versions.
        """
        ...

    @abstractmethod
    def get_current_version(self, driver: DriverT) -> "str | Awaitable[str | None] | None":
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
        self,
        driver: DriverT,
        version: str,
        description: str,
        execution_time_ms: int,
        checksum: str,
        *,
        commit: bool = True,
    ) -> "Awaitable[None] | None":
        """Record a successfully applied migration."""
        ...

    @abstractmethod
    def remove_migration(self, driver: DriverT, version: str, *, commit: bool = True) -> "Awaitable[None] | None":
        """Remove a migration record."""
        ...

    @staticmethod
    def _split_version_table(version_table_name: str) -> "tuple[str, str | None]":
        """Split a ``schema.table`` value into unquoted ``(table, schema)`` names."""
        parsed_table = _parse_ddl_table(version_table_name)
        table_identifier = (
            parsed_table.this
            if isinstance(parsed_table.this, exp.Identifier)
            else _parse_ddl_identifier(version_table_name)
        )
        embedded_db = parsed_table.args.get("db")
        schema_name = embedded_db.name if isinstance(embedded_db, exp.Identifier) else None
        return table_identifier.name, schema_name or None

    def _qualify_version_table(self, version_table_name: str, version_table_schema: str | None) -> str:
        """Return the tracker table name, qualified with schema when configured."""
        if version_table_schema:
            return f"{version_table_schema}.{version_table_name}"
        return version_table_name

    def catalog_schema_name(self) -> "str | None":
        """Return the tracking table's schema in the form data dictionary getters accept.

        Returns:
            Schema name for metadata lookups, or None when the table is unqualified.
        """
        return self.version_table_schema

    def _tracking_schema_target(self) -> SchemaTarget:
        """Return the schema target that compares the tracking table's columns with its DDL.

        Returns:
            Target naming the tracking table for introspection and additive DDL.
        """
        return SchemaTarget(self.version_table, self._tracking_table_ddl())

    def _tracking_table_builder(self) -> "CreateTable":
        """Return a CREATE TABLE builder for the tracker table."""
        return CreateTable(self.version_table)

    def _should_echo(self) -> bool:
        """Return True when console output should be emitted."""
        return bool(self._output_policy.get("echo", True)) and not bool(self._output_policy.get("use_logger", False))

    def _tracking_table_ddl(self) -> "CreateTable":
        """Get SQL builder for creating the tracking table.

        Schema includes both legacy and new versioning columns:
            - version_num: Migration version (sequential or timestamp format)
            - version_type: Format indicator ('sequential' or 'timestamp')
            - execution_sequence: Auto-incrementing application order
            - description: Human-readable migration description
            - applied_at: Timestamp when migration was applied
            - execution_time_ms: Migration execution duration
            - checksum: SHA-256 hex digest for content verification
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

    def _current_version_query(self) -> "Select":
        """Get SQL builder for retrieving current version.

        Uses execution_sequence to get the last applied migration,
        which may differ from version_num order due to out-of-order migrations.

        Returns:
            SQL builder object for version query.
        """
        return Select("version_num").from_(self.version_table).order_by("execution_sequence DESC").limit(1)

    def _applied_migrations_query(self) -> "Select":
        """Get SQL builder for retrieving all applied migrations.

        Orders by execution_sequence to show migrations in application order,
        which preserves the actual execution history for out-of-order migrations.

        Returns:
            SQL builder object for migrations query.
        """
        return Select("*").from_(self.version_table).order_by("execution_sequence")

    def _next_execution_sequence_query(self) -> "Select":
        """Get SQL builder for retrieving next execution sequence.

        Returns:
            SQL builder object for sequence query.
        """
        return Select("COALESCE(MAX(execution_sequence), 0) + 1 AS next_seq").from_(self.version_table)

    def _record_migration_statement(
        self,
        version: str,
        version_type: str,
        execution_sequence: int,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
    ) -> "Insert":
        """Get SQL builder for recording a migration.

        Args:
            version: Version number of the migration.
            version_type: Version format type ('sequential' or 'timestamp').
            execution_sequence: Auto-incrementing application order.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: SHA-256 checksum hex string of the migration content.
            applied_by: User who applied the migration.

        Returns:
            SQL builder object for insert.
        """
        return (
            Insert(self.version_table)
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

    def _remove_migration_statement(self, version: str) -> "Delete":
        """Get SQL builder for removing a migration record.

        Args:
            version: Version number to remove.

        Returns:
            SQL builder object for delete.
        """
        return Delete().from_(self.version_table).where(Column("version_num") == version)

    def _update_version_statement(self, old_version: str, new_version: str, new_version_type: str) -> "Update":
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
            Update(self.version_table)
            .set("version_num", new_version)
            .set("version_type", new_version_type)
            .where(Column("version_num") == old_version)
        )

    def _delete_versions_statement(self, versions: "list[str]") -> "Delete":
        """Get SQL builder for deleting multiple version records.

        Used by squash operations to remove replaced migration records.

        Args:
            versions: List of version strings to delete.

        Returns:
            SQL builder object for delete.
        """
        return Delete().from_(self.version_table).where(Column("version_num").in_(versions))

    def _check_versions_query(self, versions: "list[str]") -> "Select":
        """Get SQL builder for checking whether any versions exist.

        Args:
            versions: List of version strings to check.

        Returns:
            SQL builder object for version existence query.
        """
        return Select("version_num").from_(self.version_table).where(Column("version_num").in_(versions))

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
    ) -> "Insert":
        """Get SQL builder for recording a squashed migration.

        Args:
            version: Version number of the squashed migration.
            version_type: Version format type ('sequential' or 'timestamp').
            execution_sequence: Auto-incrementing application order.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: SHA-256 checksum hex string of the migration content.
            applied_by: User who applied the migration.
            replaces: Comma-separated list of replaced versions.

        Returns:
            SQL builder object for insert.
        """
        return (
            Insert(self.version_table)
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

    def _record_migration_sequenced_statement(
        self,
        version: str,
        version_type: str,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
        replaces: str | None = None,
    ) -> "Insert":
        """Get SQL builder for atomically inserting a migration record with next sequence."""
        columns: list[str] = [
            "version_num",
            "version_type",
            "execution_sequence",
            "description",
            "execution_time_ms",
            "checksum",
            "applied_by",
        ]
        select_query = Select()
        _, version_param = select_query.add_parameter(version, name="version_num")
        _, type_param = select_query.add_parameter(version_type, name="version_type")
        _, desc_param = select_query.add_parameter(description, name="description")
        _, duration_param = select_query.add_parameter(execution_time_ms, name="execution_time_ms")
        _, checksum_param = select_query.add_parameter(checksum, name="checksum")
        _, actor_param = select_query.add_parameter(applied_by, name="applied_by")
        projections: list[exp.Expr] = [
            exp.Placeholder(this=version_param),
            exp.Placeholder(this=type_param),
            exp.column("next_seq", table="seq_src"),
            exp.Placeholder(this=desc_param),
            exp.Placeholder(this=duration_param),
            exp.Placeholder(this=checksum_param),
            exp.Placeholder(this=actor_param),
        ]
        if replaces is not None:
            columns.append("replaces")
            _, replaces_param = select_query.add_parameter(replaces, name="replaces")
            projections.append(exp.Placeholder(this=replaces_param))
        next_seq_subquery = Select("COALESCE(MAX(execution_sequence), 0) + 1 AS next_seq").from_(self.version_table)
        select_query = select_query.select(*projections).from_(next_seq_subquery, alias="seq_src")
        return Insert(self.version_table).columns(*columns).from_select(select_query)

    def _migration_lock_key(self) -> "tuple[str, int]":
        """Derive the bounded lock name and signed 64-bit integer advisory lock ID."""
        lock_name = f"sqlspec_mig_{self.version_table}"[:64]
        lock_id = int.from_bytes(hashlib.sha256(lock_name.encode("utf-8")).digest()[:8], "big", signed=True)
        return lock_name, lock_id

    @classmethod
    def _lock_backend_kind(cls, driver: Any) -> str:
        """Classify ``driver`` into its migration lock backend family or empty string."""
        raw_dialect = getattr(type(driver), "dialect", None)
        dialect = raw_dialect.lower() if isinstance(raw_dialect, str) else ""
        if not dialect or type(driver).__module__.startswith("sqlspec.adapters.cockroach_"):
            return ""
        if dialect in cls._POSTGRES_LOCK_DIALECTS:
            return "postgres"
        if dialect in cls._MYSQL_LOCK_DIALECTS:
            return "mysql"
        if dialect in cls._MSSQL_LOCK_DIALECTS:
            return "mssql"
        if dialect == "sqlite":
            return "sqlite"
        return ""

    def _uses_sequenced_insert(self, driver: Any) -> bool:
        """Return True when ``driver`` uses single-statement ``execution_sequence`` inserts."""
        return str(getattr(driver, "dialect", "") or "").lower() in self._SEQUENCED_INSERT_DIALECTS

    @staticmethod
    def _translate_mssql_lock_exception(
        lock_name: str, timeout_seconds: int, exc: SQLSpecError
    ) -> MigrationError | None:
        """Convert T-SQL lock acquisition errors into ``MigrationError`` when matched."""
        if "Migration lock acquisition failed" in str(exc):
            msg = f"Failed to acquire SQL Server migration lock {lock_name!r} within {timeout_seconds}s"
            return MigrationError(msg)
        return None

    @staticmethod
    def _validate_mssql_lock_result(lock_name: str, result: Any) -> None:
        """Raise ``MigrationError`` when ``sp_getapplock`` returns a negative status code."""
        if result.data:
            lock_code = result.scalar()
            if isinstance(lock_code, int) and lock_code < 0:
                msg = f"Failed to acquire SQL Server migration lock {lock_name!r} (code {lock_code})"
                raise MigrationError(msg)

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


class ExtensionReconcilerDescriptor:
    """Immutable descriptor locating an opted-in extension reconciler module."""

    __slots__ = ("module_path", "name")

    def __init__(self, name: str, module_path: Path) -> None:
        self.name = name
        self.module_path = module_path

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExtensionReconcilerDescriptor):
            return NotImplemented
        return self.name == other.name and self.module_path == other.module_path

    def __hash__(self) -> int:
        return hash((self.name, self.module_path))

    def __repr__(self) -> str:
        return f"ExtensionReconcilerDescriptor(name={self.name!r}, module_path={self.module_path!r})"


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseMigrationCommands(Generic[ConfigT, DriverT]):
    """Base class for migration commands."""

    extension_configs: "dict[str, dict[str, Any]]"
    extension_reconcilers: "tuple[ExtensionReconcilerDescriptor, ...]"
    _resolved_extension_dirs: "dict[str, Path | None]"

    def __init__(self, config: ConfigT) -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        raw_features = getattr(config, "driver_features", None)
        driver_features = raw_features if isinstance(raw_features, dict) else {}
        direct_dsn = driver_features.get("direct_dsn")
        derive_fn = getattr(config, "derive_direct_config", None)
        if isinstance(direct_dsn, str) and direct_dsn and callable(derive_fn):
            config = cast("ConfigT", derive_fn())
        self.config = config
        if self.config.driver_features.get("tenant_strategy") is not None:
            msg = "Migrations cannot use tenant_strategy; use a separate configuration without tenant_strategy"
            raise MigrationError(msg)
        migration_config = self._get_migration_config()

        self.version_table = migration_config.get("version_table_name", "ddl_migrations")
        self.migrations_path = Path(migration_config.get("script_location", "migrations"))
        raw_project_root = migration_config.get("project_root")
        self.project_root = Path(raw_project_root) if raw_project_root is not None else None
        self.include_extensions = migration_config.get("include_extensions", [])
        self.extension_configs = self._parse_extension_configs()
        self._resolved_extension_dirs = {}
        self.extension_reconcilers = self._discover_extension_reconcilers()
        self._template_settings: MigrationTemplateSettings = build_template_settings(migration_config)
        self._runtime: ObservabilityRuntime | None = self.config.get_observability_runtime()
        self._last_command_error: Exception | None = None
        self._last_command_metrics: dict[str, float] | None = None

    def init_directory(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        console = importlib.import_module("rich.console").Console()

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

    @abstractmethod
    def init(self, directory: str, package: bool = True) -> "Awaitable[None] | None":
        """Initialize migration directory structure."""
        ...

    @abstractmethod
    def current(self, verbose: bool = False) -> "str | Awaitable[str | None] | None":
        """Show current migration version."""
        ...

    @abstractmethod
    def upgrade(self, revision: str = "head") -> "Awaitable[None] | None":
        """Upgrade to a target revision."""
        ...

    @abstractmethod
    def downgrade(self, revision: str = "-1") -> "Awaitable[None] | None":
        """Downgrade to a target revision."""
        ...

    @abstractmethod
    def stamp(self, revision: str) -> "Awaitable[None] | None":
        """Mark database as being at a specific revision without running migrations."""
        ...

    @abstractmethod
    def revision(
        self,
        message: str,
        file_type: str | None = None,
        *,
        from_schema: bool = False,
        from_data: "str | Path | None" = None,
        schema: str | None = None,
        target_table: str | None = None,
    ) -> "Path | Awaitable[Path]":
        """Create a new migration file."""
        ...

    def _get_migration_config(self) -> "dict[str, Any]":
        """Return migration config as a plain dictionary."""
        return cast("dict[str, Any]", self.config.migration_config) or {}

    def _resolve_default_schema(self) -> str | None:
        """Return the configured default migration schema."""
        return resolve_default_schema(self._get_migration_config())

    def _config_supports_schemas(self) -> bool:
        """Return whether the bound config opts into schema-aware migrations."""
        return bool(getattr(self.config, "supports_migration_schemas", False))

    def _resolve_tracker_schema(self) -> str | None:
        """Return the tracker schema, rejecting schema options the adapter cannot honor.

        Returns:
            The tracker schema, or None when no schema applies.

        Raises:
            MigrationError: The adapter lacks schema support and a schema option is configured.
        """
        migration_config = self._get_migration_config()
        if self._config_supports_schemas():
            return resolve_tracker_schema(migration_config)
        self._reject_schema_options(migration_config)
        return None

    def _reject_schema_options(self, migration_config: "dict[str, Any]") -> None:
        adapter = type(self.config).__name__
        default_schema = resolve_default_schema(migration_config)
        if default_schema is not None:
            msg = (
                f"{adapter} does not support migration default schemas; "
                f"remove default_schema={default_schema!r} from migration_config."
            )
            raise MigrationError(msg)
        version_table_schema = migration_config.get("version_table_schema")
        if isinstance(version_table_schema, str) and version_table_schema:
            msg = (
                f"{adapter} does not support migration version table schemas; "
                f"remove version_table_schema={version_table_schema!r} from migration_config "
                "or qualify version_table_name with the schema."
            )
            raise MigrationError(msg)

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
            Dictionary mapping extension names to shallow copies of their configurations.
        """
        configs: dict[str, dict[str, Any]] = {}
        extension_config_map = cast("dict[str, Any]", self.config.extension_config)

        for ext_config in self.include_extensions:
            if not isinstance(ext_config, str):
                logger.warning("Extension must be a string name, got: %s", ext_config)
                continue

            ext_name = ext_config
            if ext_name in configs:
                continue
            raw_options = extension_config_map.get(ext_name, {})
            configs[ext_name] = dict(raw_options) if isinstance(raw_options, Mapping) else {}

        return configs

    def _resolve_extension_migrations_dir(self, ext_name: str, ext_options: "Mapping[str, Any]") -> "Path | None":
        """Resolve the filesystem directory containing migrations for an extension.

        Args:
            ext_name: Configured extension name.
            ext_options: Per-extension configuration mapping.

        Raises:
            MigrationError: Extension explicitly configures ``reconcile`` mode and its migrations directory cannot be resolved.

        Returns:
            Resolved directory path, or ``None`` when absent without explicit ``reconcile`` mode.
        """
        resolve_extension_migration_mode(None, ext_options, extension_name=ext_name)
        explicit_reconcile = ext_options.get("migration_mode") == "reconcile"
        if ext_name in self._resolved_extension_dirs and not explicit_reconcile:
            return self._resolved_extension_dirs[ext_name]
        spec = ext_options.get("migrations_path")
        if spec is not None:
            resolved = resolve_extension_migrations_path(ext_name, spec)
            if resolved is None:
                if explicit_reconcile:
                    msg = (
                        f"Extension '{ext_name}' configured migration_mode='reconcile', "
                        f"but migrations_path {spec!r} is not an existing directory."
                    )
                    raise MigrationError(msg)
                logger.warning(
                    "Extension '%s' migrations_path %r is not an existing directory; "
                    "no migrations were registered for it.",
                    ext_name,
                    spec,
                )
                self._resolved_extension_dirs[ext_name] = None
                return None
            self._resolved_extension_dirs[ext_name] = resolved
            return resolved

        module_name = f"sqlspec.extensions.{ext_name}"
        try:
            module_path = module_to_os_path(module_name)
        except ImportError as exc:
            if explicit_reconcile:
                msg = (
                    f"Extension '{ext_name}' configured migration_mode='reconcile', "
                    f"but '{module_name}' could not be imported ({exc}). "
                    f"Set extension_config['{ext_name}']['migrations_path'] to register "
                    "reconcilers shipped by another package."
                )
                raise MigrationError(msg) from exc
            logger.warning(
                "Extension '%s' is configured but '%s' is not importable; no migrations were "
                "registered for it. Set extension_config['%s']['migrations_path'] to register "
                "migrations shipped by another package.",
                ext_name,
                module_name,
                ext_name,
            )
            self._resolved_extension_dirs[ext_name] = None
            return None

        migrations_dir = module_path / "migrations"
        if not migrations_dir.is_dir():
            if explicit_reconcile:
                msg = (
                    f"Extension '{ext_name}' configured migration_mode='reconcile', "
                    f"but ships no migrations directory at '{migrations_dir}'."
                )
                raise MigrationError(msg)
            logger.debug("Extension %s ships no migrations directory", ext_name)
            self._resolved_extension_dirs[ext_name] = None
            return None

        self._resolved_extension_dirs[ext_name] = migrations_dir
        return migrations_dir

    def _discover_extension_reconcilers(self) -> "tuple[ExtensionReconcilerDescriptor, ...]":
        """Discover extension reconciler modules without importing them.

        Raises:
            MigrationError: An extension with explicit ``migration_mode='reconcile'`` lacks a ``reconcile.py`` file.

        Returns:
            Ordered tuple of reconciler descriptors matching ``include_extensions`` order.
        """
        reconcilers: list[ExtensionReconcilerDescriptor] = []
        for ext_name, ext_options in self.extension_configs.items():
            mode = resolve_extension_migration_mode(None, ext_options, extension_name=ext_name)
            if mode != "reconcile":
                continue
            explicit_reconcile = ext_options.get("migration_mode") == "reconcile"
            ext_dir = self._resolve_extension_migrations_dir(ext_name, ext_options)
            if ext_dir is None:
                continue
            reconcile_path = ext_dir / "reconcile.py"
            if not reconcile_path.is_file():
                if explicit_reconcile:
                    msg = (
                        f"Extension '{ext_name}' configured migration_mode='reconcile', "
                        f"but no reconcile.py file exists at '{reconcile_path}'."
                    )
                    raise MigrationError(msg)
                continue
            reconcilers.append(ExtensionReconcilerDescriptor(ext_name, reconcile_path))
        return tuple(reconcilers)

    def _retained_reconcile_versions(self, applied_migrations: "Sequence[AppliedMigrationRecord]") -> set[str]:
        """Return applied version_num values belonging to extensions currently configured in reconcile mode.

        Args:
            applied_migrations: Applied migration records from the tracking table.

        Returns:
            Set of retained ``version_num`` strings belonging to active ``reconcile`` extensions.
        """
        reconcile_names = {descriptor.name for descriptor in self.extension_reconcilers}
        if not reconcile_names:
            return set()
        retained: set[str] = set()
        for record in applied_migrations:
            version_num = record["version_num"]
            try:
                parsed = parse_version(version_num)
            except ValueError:
                continue
            if parsed.extension in reconcile_names:
                retained.add(version_num)
        return retained

    def _discover_extension_migrations(self) -> "dict[str, Path]":
        """Discover migration paths for configured versioned extensions.

        An extension may declare ``migrations_path`` in its ``extension_config`` entry to
        ship migrations from any package. Extensions without it resolve against the
        ``sqlspec.extensions.<name>`` namespace. Extensions resolved as reconcilers
        are omitted from numbered migration discovery.

        Returns:
            Dictionary mapping extension names to their migration paths.
        """
        extension_migrations: dict[str, Path] = {}
        reconcile_names = {descriptor.name for descriptor in self.extension_reconcilers}

        for ext_name, ext_options in self.extension_configs.items():
            if ext_name in reconcile_names:
                continue
            mode = resolve_extension_migration_mode(None, ext_options, extension_name=ext_name)
            if mode == "reconcile" and ext_options.get("migration_mode") == "reconcile":
                continue
            resolved = self._resolve_extension_migrations_dir(ext_name, ext_options)
            if resolved is not None:
                extension_migrations[ext_name] = resolved
                logger.debug("Found migrations for extension %s at %s", ext_name, resolved)

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

    def _record_command_metric(self, name: str, value: float) -> None:
        """Accumulate per-command metrics for decorator flushing."""

        if self._last_command_metrics is None:
            self._last_command_metrics = {}
        self._last_command_metrics[name] = self._last_command_metrics.get(name, 0.0) + value

    def _collect_pending_migrations(
        self, all_migrations: "list[tuple[str, Path]]", applied_set: set[str], revision: str
    ) -> "list[tuple[str, Path]]":
        """Collect pending migrations that need to be applied."""
        applied_parsed = set()
        for applied_version in applied_set:
            try:
                applied_parsed.add(parse_version(applied_version))
            except ValueError:
                continue
        pending = []
        parsed_revision = parse_version(revision) if revision != "head" else None
        for version, file_path in all_migrations:
            if version in applied_set:
                continue
            parsed_version = parse_version(version)
            if parsed_version in applied_parsed:
                continue
            if (
                parsed_revision is None
                or (parsed_revision.extension is None and parsed_version.extension is not None)
                or (parsed_version.extension == parsed_revision.extension and parsed_version <= parsed_revision)
            ):
                pending.append((version, file_path))
        return pending

    def _collect_revert_migrations(
        self, applied: "list[AppliedMigrationRecord]", revision: str
    ) -> "list[AppliedMigrationRecord]":
        """Collect migrations to revert based on target revision."""
        if revision == "-1":
            core_applied = [m for m in applied if not m["version_num"].startswith("ext_")]
            if core_applied:
                return [core_applied[-1]]
            return [applied[-1]]
        if revision == "base":
            return list(reversed(applied))
        parsed_revision = parse_version(revision)
        to_revert = []
        for migration in applied[::-1]:
            parsed_migration_version = parse_version(migration["version_num"])
            if (
                parsed_migration_version.extension == parsed_revision.extension
                and parsed_migration_version > parsed_revision
            ):
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
