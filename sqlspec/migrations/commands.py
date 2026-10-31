"""Migration command implementations for SQLSpec.

This module provides the main command interface for database migrations.
"""

import asyncio
import functools
import gzip
import importlib
import inspect
import logging
import os
import time
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Generic, NamedTuple, NoReturn, ParamSpec, TypeVar, cast, overload

import sqlglot
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table
from sqlglot import exp
from sqlglot.errors import SqlglotError

from sqlspec.builder import CreateTable, sql
from sqlspec.exceptions import MigrationError, SQLBuilderError, SQLSpecError, SquashValidationError
from sqlspec.migrations.base import BaseMigrationCommands
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner
from sqlspec.migrations.squash import MigrationSquasher, schema_ddl_statements
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.migrations.utils import create_migration_file
from sqlspec.migrations.validation import validate_migration_order, validate_squash_range
from sqlspec.migrations.version import generate_conversion_map, generate_timestamp_version, parse_version
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, SyncConfigT
    from sqlspec.data_dictionary import TableMetadata
    from sqlspec.migrations.base import AppliedMigrationRecord, LoadedMigrationMetadata
    from sqlspec.migrations.runner import BaseMigrationRunner
    from sqlspec.migrations.squash import SquashPlan

__all__ = (
    "SUPPORTED_DATA_FILE_SUFFIXES",
    "AsyncMigrationCommands",
    "MigrationCommandsCore",
    "SquashReconciliation",
    "SyncMigrationCommands",
    "VersionReconciliationPlan",
    "VersionRename",
    "create_migration_commands",
)

logger = get_logger("sqlspec.migrations.commands")
console = Console()
P = ParamSpec("P")
R = TypeVar("R")
CommandConfigT = TypeVar("CommandConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")
CommandRunnerT = TypeVar("CommandRunnerT", bound="BaseMigrationRunner")

SUPPORTED_DATA_FILE_SUFFIXES: Final[tuple[str, ...]] = (
    ".json",
    ".json.gz",
    ".jsonl",
    ".jsonl.gz",
    ".ndjson",
    ".ndjson.gz",
    ".parquet",
    ".arrow",
    ".ipc",
    ".feather",
)
_ARROW_FILE_SUFFIXES: Final[frozenset[str]] = frozenset({".arrow", ".ipc", ".feather"})
_LINE_DELIMITED_SUFFIXES: Final[frozenset[str]] = frozenset({".jsonl", ".jsonl.gz", ".ndjson", ".ndjson.gz"})
_OTHER_DATA_FILE_SUFFIXES: Final[tuple[str, ...]] = (
    ".csv",
    ".tsv",
    ".txt",
    ".xlsx",
    ".xls",
    ".xml",
    ".yaml",
    ".yml",
    ".avro",
    ".orc",
    ".sql",
    ".zip",
    ".gz",
    ".bz2",
    ".xz",
    ".zst",
    ".tar",
)
_GENERIC_DIALECTS: Final[frozenset[str]] = frozenset({"", "generic", "unknown"})
_AUTO_DUMP_ERRORS: Final[tuple[type[Exception], ...]] = (OSError, SQLSpecError, SqlglotError, ValueError)


MetadataBuilder = Callable[[dict[str, Any]], tuple[str | None, dict[str, Any]]]


def _with_command_span(
    event: str, metadata_fn: "MetadataBuilder | None" = None, *, dry_run_param: str | None = "dry_run"
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Attach span lifecycle and command metric management to command methods."""

    metric_prefix = f"migrations.command.{event}"

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        signature = inspect.signature(func)

        def _prepare(self: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, bool, Any]:
            runtime = self._runtime
            metadata_args = _bind_arguments(signature, args, kwargs)
            dry_run = False
            if dry_run_param is not None:
                dry_run = bool(metadata_args.get(dry_run_param, False))
            metadata: dict[str, Any] | None = None
            version: str | None = None
            span = None
            if runtime is not None:
                runtime.increment_metric(f"{metric_prefix}.invocations")
                if dry_run_param is not None and dry_run:
                    runtime.increment_metric(f"{metric_prefix}.dry_run")
                if metadata_fn is not None:
                    version, metadata = metadata_fn(metadata_args)
                span = runtime.start_migration_span(f"command.{event}", version=version, metadata=metadata)
            return runtime, dry_run, span

        def _finalize(
            self: Any,
            runtime: Any,
            span: Any,
            start: float,
            error: "Exception | None",
            recorded_error: bool,
            dry_run: bool,
        ) -> None:
            command_error = self._last_command_error
            self._last_command_error = None
            command_metrics = self._last_command_metrics
            self._last_command_metrics = None
            if runtime is None:
                return
            if command_error is not None and not recorded_error:
                runtime.increment_metric(f"{metric_prefix}.errors")
            if not dry_run and command_metrics:
                for metric, value in command_metrics.items():
                    runtime.increment_metric(f"{metric_prefix}.{metric}", value)
            duration_ms = int((time.perf_counter() - start) * 1000)
            runtime.end_migration_span(span, duration_ms=duration_ms, error=error or command_error)

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                self = args[0]
                runtime, dry_run, span = _prepare(self, args, kwargs)
                start = time.perf_counter()
                error: Exception | None = None
                error_recorded = False
                try:
                    async_func = cast("Callable[P, Awaitable[R]]", func)
                    return await async_func(*args, **kwargs)
                except Exception as exc:  # pragma: no cover
                    error = exc
                    if runtime is not None:
                        runtime.increment_metric(f"{metric_prefix}.errors")
                        error_recorded = True
                    raise
                finally:
                    _finalize(self, runtime, span, start, error, error_recorded, dry_run)

            return cast("Callable[P, R]", async_wrapper)

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            self = args[0]
            runtime, dry_run, span = _prepare(self, args, kwargs)
            start = time.perf_counter()
            error: Exception | None = None
            error_recorded = False
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # pragma: no cover
                error = exc
                if runtime is not None:
                    runtime.increment_metric(f"{metric_prefix}.errors")
                    error_recorded = True
                raise
            finally:
                _finalize(self, runtime, span, start, error, error_recorded, dry_run)

        return cast("Callable[P, R]", sync_wrapper)

    return decorator


def _command_metadata(args: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    """Build span metadata (revision, dry_run) shared by upgrade and downgrade commands."""
    revision = cast("str | None", args.get("revision"))
    metadata = {"dry_run": str(args.get("dry_run", False)).lower()}
    return revision, metadata


class SquashReconciliation(NamedTuple):
    """Tracking-table change that collapses applied replaced versions into one squashed migration record."""

    target_version: str
    replaced_versions: tuple[str, ...]
    description: str
    checksum: str


class VersionRename(NamedTuple):
    """Tracking-table change that renames an applied version to the version of its migration file on disk.

    ``claimed_versions`` is the set of versions already accounted for when the rename was planned; the tracker uses
    it to recognise a rename that another process already performed.
    """

    old_version: str
    new_version: str
    claimed_versions: frozenset[str]


class VersionReconciliationPlan(NamedTuple):
    """Tracking-table changes needed to match the applied records to the migration files on disk.

    ``applied_versions`` lists the applied versions as they will look once the changes are written, which lets a
    dry run compute pending migrations without touching the database.
    """

    squashes: tuple[SquashReconciliation, ...]
    renames: tuple[VersionRename, ...]
    checksum_mismatches: tuple[tuple[str, str], ...]
    applied_versions: tuple[str, ...]

    @property
    def change_count(self) -> int:
        """Return the number of tracking-table changes in the plan."""
        return len(self.squashes) + len(self.renames)


class MigrationCommandsCore(BaseMigrationCommands[CommandConfigT, Any], Generic[CommandConfigT, CommandRunnerT]):
    """Database-independent planning, rendering, and reporting shared by sync and async migration commands.

    Subclasses perform every database, tracker, and migration-loader call and pass the results to these methods, so
    the sync and async command classes stay thin I/O shells around one implementation.
    """

    tracker: Any
    runner: CommandRunnerT

    def _version_table_exclusion(self, include_migrations_table: bool = False) -> tuple[str, ...]:
        """Return the ``exclude_tables`` value for schema DDL introspection.

        The tracker exposes the bare, unquoted version table name with any schema qualifier removed, which is the
        form the data dictionary matches against.

        Args:
            include_migrations_table: Keep the tracking table in the introspected DDL.

        Returns:
            Table names to exclude.
        """
        if include_migrations_table:
            return ()
        return (str(self.tracker.version_table_name),)

    def _tracking_table_exists(self, tables: "Sequence[TableMetadata]") -> bool:
        """Return whether the migration tracking table appears in introspected table metadata.

        Names compare case-insensitively. When the tracker is schema-qualified, a table reported in another schema
        does not match.

        Args:
            tables: Table metadata returned by ``data_dictionary.get_tables``.

        Returns:
            True when the tracking table exists.
        """
        target_name = str(self.tracker.version_table_name).lower()
        target_schema = self.tracker.version_table_schema
        for table in tables:
            if str(table.get("table_name") or "").lower() != target_name:
                continue
            reported_schema = table.get("schema_name") or table.get("table_schema")
            if target_schema is None or reported_schema is None:
                return True
            if str(reported_schema).lower() == str(target_schema).lower():
                return True
        return False

    def _resolve_data_source(self, from_data: "str | Path") -> "Path | str":
        """Classify a ``from_data`` value as a data file path or a database table name.

        Classification is syntactic so the same argument never changes meaning based on what happens to exist in
        the working directory:

        - A ``Path`` instance, a value containing a path separator, or a value ending with a known data file
          suffix is a file path. It must be an existing regular file with a supported suffix
          (``SUPPORTED_DATA_FILE_SUFFIXES``); directories, missing files, and unsupported suffixes such as
          ``.csv`` raise ``MigrationError``.
        - Anything else is a table name, optionally schema-qualified (``public.users``). Without an explicit
          ``schema`` argument, a qualified name splits on its last dot.

        Args:
            from_data: Value passed as ``from_data`` / ``--from-data``.

        Returns:
            The validated file ``Path``, or the table name string.

        Raises:
            MigrationError: When a file path is a directory, missing, or has an unsupported suffix.
        """
        text = str(from_data)
        if not isinstance(from_data, Path) and not self._looks_like_data_path(text):
            return text
        path = Path(from_data)
        supported = ", ".join(SUPPORTED_DATA_FILE_SUFFIXES)
        if path.is_dir():
            msg = f"Data source '{text}' is a directory; pass a data file ({supported}) or a table name"
            raise MigrationError(msg)
        if self._data_file_suffix(path) is None:
            msg = f"Unsupported data file '{text}'; supported suffixes: {supported}"
            raise MigrationError(msg)
        if not path.is_file():
            msg = f"Data file not found: {text}"
            raise MigrationError(msg)
        return path

    def _prepare_revision_source(self, *, from_schema: bool, from_data: "str | Path | None") -> "Path | str | None":
        """Validate revision source flags and classify ``from_data`` as a file ``Path`` or table name."""
        if from_schema and from_data is not None:
            msg = "Cannot specify both from_schema and from_data"
            raise MigrationError(msg)
        if from_data is None:
            return None
        return self._resolve_data_source(from_data)

    def _data_file_revision_sql(
        self, data_path: Path, *, schema: str | None, target_table: str | None
    ) -> "tuple[list[str], list[str]]":
        """Infer CREATE TABLE and matching DROP TABLE statements from a validated data file.

        ``target_table`` and ``schema`` are literal names; they are quoted as needed rather than parsed as SQL.

        Args:
            data_path: Data file returned by ``_resolve_data_source``.
            schema: Optional schema for the created table.
            target_table: Optional table name. Defaults to the file name without its data suffix.

        Returns:
            Tuple of UP and DOWN statement lists.

        Raises:
            MigrationError: When the file cannot be read or no DDL can be inferred from it.
        """
        suffix = cast("str", self._data_file_suffix(data_path))
        table_name = target_table or data_path.name[: -len(suffix)]
        dialect = self._context_dialect()
        table_sql = exp.Table(this=exp.to_identifier(table_name), db=exp.to_identifier(schema) if schema else None).sql(
            dialect=dialect
        )
        try:
            if suffix == ".parquet":
                ensure_pyarrow()
                arrow_schema = importlib.import_module("pyarrow.parquet").read_schema(data_path)
                builder = CreateTable.from_arrow_schema(table_sql, arrow_schema, dialect=dialect)
            elif suffix in _ARROW_FILE_SUFFIXES:
                ensure_pyarrow()
                with importlib.import_module("pyarrow.ipc").open_file(data_path) as reader:
                    arrow_schema = reader.schema
                builder = CreateTable.from_arrow_schema(table_sql, arrow_schema, dialect=dialect)
            else:
                builder = CreateTable.from_records(
                    table_sql, self._read_data_file_records(data_path, suffix), dialect=dialect
                )
            up_statement = self._terminate_statement(builder.build(dialect=dialect).sql)
        except (OSError, ValueError, SQLBuilderError, SqlglotError) as exc:
            msg = f"Could not infer DDL from data file '{data_path}': {exc}"
            raise MigrationError(msg) from exc
        return [up_statement], [self._drop_statement_for(up_statement, dialect)]

    def _staging_table_revision_sql(
        self, ddl: "str | None", source_table: str, dialect: str | None
    ) -> "tuple[list[str], list[str]]":
        """Build UP and DOWN statements from DDL inferred by profiling a staging table.

        The DOWN statement drops exactly the table the UP statement creates, with the same identifier quoting.

        Args:
            ddl: CREATE TABLE statement returned by ``infer_table_ddl_from_data``.
            source_table: Staging table that was profiled.
            dialect: Dialect of the inferred DDL.

        Returns:
            Tuple of UP and DOWN statement lists.

        Raises:
            MigrationError: When no DDL was inferred.
        """
        if not ddl:
            msg = f"Could not infer DDL from table '{source_table}'"
            raise MigrationError(msg)
        up_statement = self._terminate_statement(ddl)
        return [up_statement], [self._drop_statement_for(up_statement, dialect)]

    def _write_revision(
        self,
        message: str,
        file_type: str | None,
        up_sql: "Sequence[str] | None" = None,
        down_sql: "Sequence[str] | None" = None,
    ) -> Path:
        """Create a timestamp-versioned migration file and report it.

        Args:
            message: Description for the migration.
            file_type: Migration file format ('sql' or 'py'); defaults to the template profile.
            up_sql: Optional statements for the UP section.
            down_sql: Optional statements for the DOWN section.

        Returns:
            Path to the created migration file.
        """
        version = generate_timestamp_version()
        selected_format = file_type or self._template_settings.default_format
        file_path = create_migration_file(
            self.migrations_path,
            version,
            message,
            selected_format,
            config=self.config,
            template_settings=self._template_settings,
            up_sql=up_sql,
            down_sql=down_sql,
        )
        log_with_context(
            logger,
            logging.DEBUG,
            "migration.create",
            db_system=resolve_db_system(type(self.config).__name__),
            version=version,
            file_path=str(file_path),
            file_type=selected_format,
            description=message,
        )
        console.print(f"[green]Created migration:[/] {file_path}")
        return file_path

    def _render_schema_dump(
        self, create_res: Any, *, dialect_label: str, schema: str | None, output_path: "str | Path | None", echo: bool
    ) -> str:
        """Render schema DDL to SQL text and write it to the requested or configured dump path.

        Args:
            create_res: ``get_schema_ddl(direction="create")`` result.
            dialect_label: Dialect name for the dump header.
            schema: Inspected schema, recorded in the header when set.
            output_path: Explicit output file. Falls back to ``migration_config["schema_dump_path"]``.
            echo: Report the written file on the console.

        Returns:
            The rendered schema SQL.
        """
        up_ddl, _ = schema_ddl_statements(create_res, ())
        lines: list[str] = ["-- SQLSpec Schema Dump", f"-- Dialect: {dialect_label}"]
        if schema:
            lines.append(f"-- Schema: {schema}")
        lines.append("")
        if up_ddl:
            lines.append("\n\n".join(stmt.rstrip() for stmt in up_ddl))
        rendered = "\n".join(lines).rstrip() + "\n"

        resolved_output: Path | None = None
        if output_path is not None:
            resolved_output = Path(output_path)
        else:
            configured_path = self._get_migration_config().get("schema_dump_path")
            if isinstance(configured_path, (str, Path)):
                resolved_output = Path(configured_path)

        if resolved_output is not None:
            resolved_output.parent.mkdir(parents=True, exist_ok=True)
            resolved_output.write_text(rendered, encoding="utf-8")
            if echo:
                console.print(f"[green]✓ Schema dumped to {resolved_output}[/]")
        return rendered

    def _auto_dump_path(self) -> "Path | None":
        """Return the schema dump path written after ``upgrade`` when ``auto_dump_schema`` is enabled."""
        migration_config = self._get_migration_config()
        auto_dump = migration_config.get("auto_dump_schema")
        if not auto_dump:
            return None
        if isinstance(auto_dump, (str, Path)):
            return Path(auto_dump)
        configured_path = migration_config.get("schema_dump_path")
        if isinstance(configured_path, (str, Path)):
            return Path(configured_path)
        return self.migrations_path / "schema.sql"

    def _report_auto_dump_failure(self, path: Path, error: Exception, *, use_logger: bool, echo: bool) -> None:
        """Report a failed post-upgrade schema dump without failing the already committed upgrade.

        Args:
            path: Dump path that could not be written.
            error: Failure raised while dumping the schema.
            use_logger: Whether logger output is enabled.
            echo: Whether console output is enabled.
        """
        logger.warning(
            "Migrations were applied, but the automatic schema dump to %s failed: %s", path, error, exc_info=error
        )
        if echo and not use_logger:
            console.print(
                f"[yellow]Warning: migrations were applied, but the automatic schema dump to {path} failed: {error}[/]"
            )

    def _verification_report(
        self,
        applied_migrations: "list[AppliedMigrationRecord]",
        loaded_files: "dict[str, LoadedMigrationMetadata]",
        *,
        strict: bool,
        fail_on_pending: bool,
        use_logger: bool,
        echo: bool,
        summary_only: bool,
    ) -> "dict[str, Any]":
        """Compare applied records against migration files, report the result, and enforce failure modes.

        Args:
            applied_migrations: Records from the tracking table (empty when it does not exist).
            loaded_files: Loaded migration metadata keyed by version.
            strict: Raise on checksum drift, orphaned records, or partially applied squashes.
            fail_on_pending: Raise when migrations are pending.
            use_logger: Whether logger output is enabled.
            echo: Whether console output is enabled.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Verification report with ``valid``, ``checksum_mismatches``, ``orphaned_records``,
            ``pending_versions``, and ``partial_squash_errors``.

        Raises:
            MigrationError: When ``strict`` and the report is invalid, or ``fail_on_pending`` and migrations are
                pending.
        """
        report = self._evaluate_verification(applied_migrations, loaded_files)
        problems: list[str] = []
        if report["checksum_mismatches"]:
            mismatched = ", ".join(item["version"] for item in report["checksum_mismatches"])
            problems.append(f"checksum mismatch in [{mismatched}]")
        if report["orphaned_records"]:
            problems.append(f"orphaned applied records [{', '.join(report['orphaned_records'])}]")
        if report["partial_squash_errors"]:
            partials = ", ".join(item["version"] for item in report["partial_squash_errors"])
            problems.append(f"partially applied squash in [{partials}]")
        pending_failure = fail_on_pending and bool(report["pending_versions"])
        if pending_failure:
            problems.append(f"pending migrations [{', '.join(report['pending_versions'])}]")

        if not problems:
            pending_count = len(report["pending_versions"])
            _output_info(
                use_logger,
                echo,
                summary_only,
                "Migration verification passed (%d pending)",
                pending_count,
                rich_message=f"[green]✓ Migration verification passed ({pending_count} pending)[/]",
            )
            return report

        summary_text = "; ".join(problems)
        _output_error(
            use_logger,
            echo,
            summary_only,
            "Verification failed: %s",
            summary_text,
            rich_message=f"[red]✗ Verification failed: {summary_text}[/]",
        )
        if (strict and not report["valid"]) or pending_failure:
            msg = f"Verification failed: {summary_text}"
            raise MigrationError(msg)
        return report

    def _plan_version_reconciliation(
        self,
        all_migrations: "list[tuple[str, Path]]",
        applied_migrations: "list[AppliedMigrationRecord]",
        loaded_files: "dict[str, LoadedMigrationMetadata]",
    ) -> VersionReconciliationPlan:
        """Plan the tracking-table changes that align applied records with migration files on disk.

        Squashed files whose replaced versions are all applied collapse into one record. Timestamp versions renamed
        by ``fix`` are renamed when checksums match, as are applied versions whose file was renamed to another
        unclaimed version with the same checksum.

        Args:
            all_migrations: Migration files on disk as ``(version, path)`` pairs.
            applied_migrations: Records from the tracking table.
            loaded_files: Loaded metadata for every migration file that could be loaded.

        Returns:
            The reconciliation plan. Nothing is written.

        Raises:
            SquashValidationError: When a squashed migration's replaced versions are only partially applied.
        """
        applied_map: dict[str, AppliedMigrationRecord] = {m["version_num"]: m for m in applied_migrations}
        applied_versions: list[str] = [m["version_num"] for m in applied_migrations]
        files_by_version = dict(all_migrations)
        claimed_versions = set(applied_map)
        reconciled_old_versions: set[str] = set()
        squashes: list[SquashReconciliation] = []
        renames: list[VersionRename] = []
        checksum_mismatches: list[tuple[str, str]] = []

        for target_version, _ in all_migrations:
            migration = loaded_files.get(target_version)
            if migration is None:
                continue
            state, applied_replaces, missing_replaces = self._classify_squash_state(
                target_version, migration, applied_map
            )
            if state == "already_reconciled":
                reconciled_old_versions.update(applied_replaces)
                continue
            if state == "partial":
                self._raise_partial_squash(target_version, applied_replaces, missing_replaces)
            if state != "replace":
                continue
            squashes.append(
                SquashReconciliation(
                    target_version=target_version,
                    replaced_versions=tuple(applied_replaces),
                    description=migration["description"],
                    checksum=migration["checksum"],
                )
            )
            for old_version in applied_replaces:
                applied_map.pop(old_version, None)
                claimed_versions.discard(old_version)
                reconciled_old_versions.add(old_version)
            applied_versions = [v for v in applied_versions if v not in applied_replaces]
            applied_versions.append(target_version)
            applied_map[target_version] = cast(
                "AppliedMigrationRecord",
                {
                    "version_num": target_version,
                    "description": migration["description"],
                    "checksum": migration["checksum"],
                    "replaces": ",".join(applied_replaces),
                },
            )
            claimed_versions.add(target_version)

        for old_version, new_version in generate_conversion_map(all_migrations).items():
            if old_version not in applied_map or new_version in claimed_versions:
                continue
            renamed_migration = loaded_files.get(new_version)
            if renamed_migration is None:
                continue
            reconciled_old_versions.add(old_version)
            if renamed_migration["checksum"] != applied_map[old_version]["checksum"]:
                checksum_mismatches.append((old_version, new_version))
                continue
            renames.append(VersionRename(old_version, new_version, frozenset(claimed_versions)))
            claimed_versions.discard(old_version)
            claimed_versions.add(new_version)
            applied_versions = [new_version if v == old_version else v for v in applied_versions]

        for applied_version, applied_record in list(applied_map.items()):
            if applied_version in files_by_version or applied_version in reconciled_old_versions:
                continue
            for file_version, _ in all_migrations:
                file_migration = loaded_files.get(file_version)
                if file_migration is None or file_version in claimed_versions:
                    continue
                if applied_record["checksum"] == file_migration["checksum"]:
                    renames.append(VersionRename(applied_version, file_version, frozenset(claimed_versions)))
                    claimed_versions.discard(applied_version)
                    claimed_versions.add(file_version)
                    applied_versions = [file_version if v == applied_version else v for v in applied_versions]
                    break

        return VersionReconciliationPlan(
            squashes=tuple(squashes),
            renames=tuple(renames),
            checksum_mismatches=tuple(checksum_mismatches),
            applied_versions=tuple(applied_versions),
        )

    def _report_version_reconciliation(
        self, plan: VersionReconciliationPlan, *, dry_run: bool, use_logger: bool, echo: bool, summary_only: bool
    ) -> None:
        """Report planned (dry run) or applied tracking-table reconciliation.

        Args:
            plan: Reconciliation plan.
            dry_run: Whether the changes were only planned.
            use_logger: Whether logger output is enabled.
            echo: Whether console output is enabled.
            summary_only: Whether summary-only logging is enabled.
        """
        verb = "Would reconcile" if dry_run else "Reconciled"
        for squash in plan.squashes:
            replaced = ", ".join(squash.replaced_versions)
            _output_info(
                use_logger,
                echo,
                summary_only,
                "%s squashed migration: %s -> %s",
                verb,
                replaced,
                squash.target_version,
                rich_message=f"  [dim]{verb} squashed migration:[/] {replaced} → {squash.target_version}",
            )
        for rename in plan.renames:
            _output_info(
                use_logger,
                echo,
                summary_only,
                "%s version: %s -> %s",
                verb,
                rename.old_version,
                rename.new_version,
                rich_message=f"  [dim]{verb} version:[/] {rename.old_version} → {rename.new_version}",
            )
        for old_version, new_version in plan.checksum_mismatches:
            if use_logger:
                logger.warning("Checksum mismatch for %s -> %s, skipping auto-sync", old_version, new_version)
            elif echo:
                console.print(
                    f"  [yellow]Warning: Checksum mismatch for {old_version} → {new_version}, skipping auto-sync[/]"
                )
        if plan.change_count:
            _output_info(
                use_logger,
                echo,
                summary_only,
                "%s %d version record(s)",
                verb,
                plan.change_count,
                rich_message=f"[cyan]{verb} {plan.change_count} version record(s)[/]",
            )

    def _create_squasher(self, runner: SyncMigrationRunner, strategy: str) -> MigrationSquasher:
        """Create a squasher bound to a synchronous migration runner and the configured version table."""
        return MigrationSquasher(
            self.migrations_path,
            runner,
            self._template_settings,
            strategy=strategy,
            version_table=str(self.tracker.version_table_name),
        )

    def _resolve_squash_range(
        self, all_migrations: "list[tuple[str, Path]]", start_version: str | None, end_version: str | None
    ) -> "tuple[str, str] | None":
        """Resolve an open squash range to the first and last core migrations.

        Args:
            all_migrations: Migration files on disk as ``(version, path)`` pairs.
            start_version: Requested first version, or None for the first core migration.
            end_version: Requested last version, or None for the last core migration.

        Returns:
            The resolved ``(start, end)`` range, or None when there are no core migrations to squash.
        """
        if start_version is not None and end_version is not None:
            return start_version, end_version
        core_versions = [version for version, _ in all_migrations if not version.startswith("ext_")]
        if not core_versions:
            console.print("[yellow]No sequential migrations found to squash[/]")
            return None
        resolved_start = start_version if start_version is not None else core_versions[0]
        resolved_end = end_version if end_version is not None else core_versions[-1]
        console.print(f"[cyan]Squashing range: {resolved_start} to {resolved_end}[/]")
        return resolved_start, resolved_end

    def _print_squash_plan(self, plans: "list[SquashPlan]") -> None:
        """Print the source-to-target table for a squash plan."""
        table = Table(title="Squash Plan")
        table.add_column("Version", style="cyan")
        table.add_column("File")
        table.add_column("Target", style="green")
        total_migrations = 0
        for plan in plans:
            for version, file_path in plan.source_migrations:
                table.add_row(version, file_path.name, plan.target_path.name)
                total_migrations += 1
        console.print(table)
        target_files = ", ".join(plan.target_path.name for plan in plans)
        console.print(
            f"\n[yellow]{total_migrations} migrations will be squashed into {len(plans)} file(s): {target_files}[/]"
        )

    def _print_squash_dry_run(self, squasher: MigrationSquasher, plans: "list[SquashPlan]") -> None:
        """Extract and print the planned UP and DOWN SQL for each squash plan in dry-run mode."""
        for plan in plans:
            up_sql, down_sql = squasher.extract_plan_sql(plan)
            console.print(f"\n[cyan]-- {plan.target_path.name} (up)[/]")
            for statement in up_sql:
                console.print(statement.rstrip())
            if down_sql:
                console.print(f"\n[cyan]-- {plan.target_path.name} (down)[/]")
                for statement in down_sql:
                    console.print(statement.rstrip())
        console.print("[yellow][Preview Mode - No changes made][/]")

    def _validate_schema_squash_applied(
        self,
        runner: SyncMigrationRunner,
        all_migrations: "list[tuple[str, Path]]",
        start_version: str,
        end_version: str,
        *,
        allow_gaps: bool,
        applied_migrations: "list[AppliedMigrationRecord]",
    ) -> None:
        """Ensure every migration in a ``schema`` squash range is already applied in the database."""
        source_migrations = validate_squash_range(all_migrations, start_version, end_version, allow_gaps=allow_gaps)
        covered_versions: set[str] = set()
        for record in applied_migrations:
            covered_versions.add(record["version_num"])
            raw_replaces = record.get("replaces")
            if isinstance(raw_replaces, str) and raw_replaces:
                covered_versions.update(part.strip() for part in raw_replaces.split(",") if part.strip())
        unapplied: list[str] = []
        for version, file_path in source_migrations:
            if version in covered_versions:
                continue
            migration = runner.load_migration(file_path, version)
            replaces = list(migration.get("replaces") or [])
            if replaces and all(rep in covered_versions for rep in replaces):
                continue
            unapplied.append(version)
        if unapplied:
            msg = (
                "Cannot squash with the schema strategy because the database has unapplied migrations "
                f"({', '.join(unapplied)}). Run 'upgrade' so all migrations through {end_version} are applied."
            )
            raise SquashValidationError(msg)

    def _apply_squash_files(self, squasher: MigrationSquasher, plans: "list[SquashPlan]") -> None:
        """Write squashed migration files, keeping the backup until the caller commits or rolls back."""
        squasher.apply_squash(plans, cleanup_backup=False)
        for plan in plans:
            console.print(f"[green]✓ Created squashed migration: {plan.target_path.name}[/]")

    def _squash_record_checksum(self, plan: "SquashPlan") -> str:
        """Return the checksum recorded for a squashed migration in the tracking table."""
        return self.runner.calculate_checksum(plan.target_path.read_text(encoding="utf-8"))

    def _finish_squash(self, squasher: MigrationSquasher) -> None:
        """Remove the squash backup, drop stale migration caches, and report completion."""
        squasher.cleanup_backup()
        self.runner.clear_caches()
        console.print("[green]✓ Squash complete![/]")

    def _context_dialect(self) -> str | None:
        """Return the configured migration dialect, or None for generic SQL."""
        dialect = MigrationContext.from_config(self.config).dialect
        return None if dialect in _GENERIC_DIALECTS else dialect

    def _log_reconciliation_load_failure(self, version: str, file_path: Path, error: Exception) -> None:
        """Log a migration file that could not be loaded for tracking-table reconciliation."""
        log_with_context(
            logger,
            logging.DEBUG,
            "migration.list",
            db_system=resolve_db_system(type(self.config).__name__),
            version=version,
            file_path=str(file_path),
            error_type=type(error).__name__,
            status="failed",
            operation="load_for_reconciliation",
        )

    @staticmethod
    def _evaluate_verification(
        applied_migrations: "list[AppliedMigrationRecord]", loaded_files: "dict[str, LoadedMigrationMetadata]"
    ) -> "dict[str, Any]":
        """Compare applied tracking records against loaded migration files."""
        applied_map = {m["version_num"]: m for m in applied_migrations}
        checksum_mismatches: list[dict[str, str]] = []
        orphaned_records: list[str] = []
        pending_versions: list[str] = []
        partial_squash_errors: list[dict[str, Any]] = []
        covered_applied_versions: set[str] = set()

        for version, migration in loaded_files.items():
            state, applied_replaces, missing_replaces = MigrationCommandsCore._classify_squash_state(
                version, migration, applied_map
            )
            if state in {"replace", "partial"}:
                covered_applied_versions.update(applied_replaces)
                if state == "partial":
                    partial_squash_errors.append({
                        "version": version,
                        "applied_replaces": applied_replaces,
                        "missing_replaces": missing_replaces,
                    })
                continue
            if state == "already_reconciled":
                covered_applied_versions.update(applied_replaces)
            elif version not in applied_map:
                pending_versions.append(version)
                continue
            covered_applied_versions.add(version)
            applied_checksum = str(applied_map[version].get("checksum") or "")
            file_checksum = str(migration.get("checksum") or "")
            if applied_checksum != file_checksum:
                checksum_mismatches.append({"version": version, "expected": applied_checksum, "actual": file_checksum})

        orphaned_records.extend(
            record["version_num"]
            for record in applied_migrations
            if record["version_num"] not in covered_applied_versions
        )
        return {
            "valid": not (checksum_mismatches or orphaned_records or partial_squash_errors),
            "checksum_mismatches": checksum_mismatches,
            "orphaned_records": orphaned_records,
            "pending_versions": pending_versions,
            "partial_squash_errors": partial_squash_errors,
        }

    @staticmethod
    def _classify_squash_state(
        target_version: str, migration: "LoadedMigrationMetadata", applied_map: "dict[str, AppliedMigrationRecord]"
    ) -> "tuple[str, list[str], list[str]]":
        """Classify a migration file against applied records for squash reconciliation.

        Args:
            target_version: Version of the migration file.
            migration: Loaded migration metadata.
            applied_map: Applied records keyed by ``version_num``.

        Returns:
            Tuple of ``(state, applied_replaces, missing_replaces)`` where ``state`` is one of ``"none"``,
            ``"already_reconciled"``, ``"fresh"``, ``"replace"``, or ``"partial"``.
        """
        replaces = list(migration.get("replaces") or [])
        if not replaces:
            return "none", [], []

        target_record = applied_map.get(target_version)
        other_replaced_applied = [v for v in replaces if v != target_version and v in applied_map]
        if (
            target_record is not None
            and not other_replaced_applied
            and (
                target_version not in replaces
                or bool(target_record.get("replaces"))
                or target_record.get("checksum") == migration.get("checksum")
                or len(replaces) == 1
            )
        ):
            return "already_reconciled", replaces, []

        applied_replaces = [v for v in replaces if v in applied_map]
        missing_replaces = [v for v in replaces if v not in applied_map]
        if not applied_replaces:
            return "fresh", [], missing_replaces
        if not missing_replaces:
            return "replace", applied_replaces, []
        return "partial", applied_replaces, missing_replaces

    @staticmethod
    def _raise_partial_squash(
        target_version: str, applied_replaces: "list[str]", missing_replaces: "list[str]"
    ) -> NoReturn:
        """Raise the error for a squashed migration whose replaced versions are only partially applied."""
        msg = (
            f"Cannot reconcile squashed migration {target_version}: "
            f"applied replaced versions {applied_replaces}, missing {missing_replaces}. "
            "Apply the missing versions with the pre-squash migration files, or restore the database, before "
            "upgrading with the squashed migration."
        )
        raise SquashValidationError(msg)

    @staticmethod
    def _driver_dialect(driver: Any) -> str | None:
        """Return the SQL dialect of a driver, or None for generic SQL."""
        dialect = getattr(driver.data_dictionary, "dialect", None) or getattr(driver, "dialect", None)
        if dialect is None or str(dialect) in _GENERIC_DIALECTS:
            return None
        return str(dialect)

    @staticmethod
    def _dump_dialect_label(driver: Any, target_dialect: str | None) -> str:
        """Return the dialect name written into a schema dump header."""
        return str(
            target_dialect
            or getattr(driver.data_dictionary, "dialect", None)
            or getattr(driver, "dialect", None)
            or "sql"
        )

    @staticmethod
    def _looks_like_data_path(text: str) -> bool:
        """Return whether a ``from_data`` string is written as a file path rather than a table name."""
        separators = {"/", os.sep} | ({os.altsep} if os.altsep else set())
        if any(separator in text for separator in separators):
            return True
        lowered = text.lower()
        return lowered.endswith(SUPPORTED_DATA_FILE_SUFFIXES + _OTHER_DATA_FILE_SUFFIXES)

    @staticmethod
    def _split_qualified_table(source: str, schema: str | None) -> "tuple[str, str | None]":
        """Split a ``schema.table`` staging table reference on its last dot unless a schema was given."""
        if schema is not None or "." not in source:
            return source, schema
        source_schema, _, table_name = source.rpartition(".")
        return table_name, source_schema

    @staticmethod
    def _data_file_suffix(path: Path) -> str | None:
        """Return the longest supported data suffix of a file name, or None when it is unsupported."""
        lowered = path.name.lower()
        matches = [suffix for suffix in SUPPORTED_DATA_FILE_SUFFIXES if lowered.endswith(suffix)]
        if not matches or len(lowered) == len(max(matches, key=len)):
            return None
        return max(matches, key=len)

    @staticmethod
    def _read_data_file_records(data_path: Path, suffix: str) -> "list[dict[str, Any]]":
        """Read JSON or line-delimited JSON records from a (optionally gzipped) data file.

        Raises:
            MigrationError: When the file does not contain a list of JSON objects.
        """
        if suffix.endswith(".gz"):
            with gzip.open(data_path, mode="rt", encoding="utf-8") as handle:
                content = handle.read()
        else:
            content = data_path.read_text(encoding="utf-8")
        decoded: Any
        if suffix in _LINE_DELIMITED_SUFFIXES:
            decoded = [from_json(line) for line in content.splitlines() if line.strip()]
        else:
            decoded = from_json(content)
        if not isinstance(decoded, list) or not all(isinstance(row, dict) for row in decoded):
            msg = f"Data file '{data_path}' must contain a list of JSON objects"
            raise MigrationError(msg)
        return cast("list[dict[str, Any]]", decoded)

    @staticmethod
    def _terminate_statement(statement: str) -> str:
        """Return a statement with exactly one trailing semicolon."""
        return statement.strip().rstrip(";").rstrip() + ";"

    @staticmethod
    def _drop_statement_for(create_statement: str, dialect: str | None) -> str:
        """Build ``DROP TABLE IF EXISTS`` for the exact table a CREATE TABLE statement creates.

        Raises:
            MigrationError: When the statement cannot be parsed as CREATE TABLE.
        """
        try:
            parsed = sqlglot.parse_one(create_statement, read=dialect)
        except SqlglotError as exc:
            msg = f"Could not parse inferred DDL to build its DROP statement: {exc}"
            raise MigrationError(msg) from exc
        target = parsed.this if isinstance(parsed, exp.Create) else None
        if isinstance(target, exp.Schema):
            target = target.this
        if not isinstance(target, exp.Table):
            msg = f"Inferred DDL is not a CREATE TABLE statement: {create_statement}"
            raise MigrationError(msg)
        drop = exp.Drop(kind="TABLE", tables=[target.copy()], exists=True)
        return MigrationCommandsCore._terminate_statement(drop.sql(dialect=dialect))


class SyncMigrationCommands(MigrationCommandsCore["SyncConfigT", SyncMigrationRunner]):
    """Synchronous migration commands."""

    def __init__(self, config: "SyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        super().__init__(config)
        self.tracker = self._create_tracker()
        context = MigrationContext.from_config(self.config)
        context.extension_config = self.extension_configs

        self.runner = SyncMigrationRunner(
            self.migrations_path,
            self._discover_extension_migrations(),
            context,
            self.extension_configs,
            runtime=self._runtime,
            description_hints=self._template_settings.description_hints,
        )

    def init(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        self.init_directory(directory, package)

    def current(self, verbose: bool = False) -> "str | None":
        """Show current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            The current migration version or None if no migrations applied.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)

            applied: list[AppliedMigrationRecord] = []
            if verbose:
                applied = self.tracker.get_applied_migrations(driver)
                current = applied[-1]["version_num"] if applied else None
            else:
                current = self.tracker.get_current_version(driver)
            if not current:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "migration.list",
                    db_system=resolve_db_system(type(driver).__name__),
                    current_version=None,
                    applied_count=0,
                    verbose=verbose,
                    status="empty",
                )
                console.print("[yellow]No migrations applied yet[/]")
                return None

            console.print(f"[green]Current version:[/] {current}")

            if verbose:
                table = Table(title="Applied Migrations")
                table.add_column("Version", style="cyan")
                table.add_column("Description")
                table.add_column("Applied At")
                table.add_column("Time (ms)", justify="right")
                table.add_column("Applied By")

                for migration in applied:
                    table.add_row(
                        migration["version_num"],
                        migration.get("description", ""),
                        str(migration.get("applied_at", "")),
                        str(migration.get("execution_time_ms", "")),
                        migration.get("applied_by", ""),
                    )

                console.print(table)

            applied_count = len(applied) if verbose else None
            log_with_context(
                logger,
                logging.DEBUG,
                "migration.list",
                db_system=resolve_db_system(type(driver).__name__),
                current_version=current,
                applied_count=applied_count,
                verbose=verbose,
                status="complete",
            )
            return cast("str | None", current)

    @_with_command_span("upgrade", metadata_fn=_command_metadata)
    def upgrade(
        self,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Upgrade to a target revision.

        Validates migration order and warns if out-of-order migrations are detected.
        Out-of-order migrations can occur when branches merge in different orders
        across environments.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: If True, allow out-of-order migrations even in strict mode.
                Defaults to False.
            auto_sync: If True, automatically reconcile renamed and squashed migrations in the tracking table.
                Defaults to True. Can be disabled via --no-auto-sync flag. A partially applied squash raises
                ``SquashValidationError`` either way.
            dry_run: If True, report the reconciliation and migrations that would run without creating the
                tracking table or writing any tracking rows.
            use_logger: If True, output to logger instead of Rich console.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        runtime = self._runtime
        applied_count = 0
        pending_count = 0
        db_system: str | None = None
        error: Exception | None = None
        ul, echo_value, summary_value = self._resolve_output_policy(use_logger, echo, summary_only)
        self.runner.set_use_logger(ul)
        self.runner.set_summary_only(summary_value)
        self.tracker.set_output_policy(use_logger=ul, echo=echo_value, summary_only=summary_value)
        output_info = functools.partial(_output_info, ul, echo_value, summary_value)
        start_time = time.perf_counter()

        try:
            if dry_run:
                output_info(
                    "DRY RUN MODE: No database changes will be applied",
                    rich_message="[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n",
                )

            with self.config.provide_session() as driver:
                db_system = resolve_db_system(type(driver).__name__)
                self.runner.set_driver(driver)
                self._validate_migration_schema(driver)
                applied_versions = self._synchronized_applied_versions(
                    driver,
                    auto_sync=auto_sync and bool(self._get_migration_config().get("auto_sync", True)),
                    dry_run=dry_run,
                    use_logger=ul,
                    echo=echo_value,
                    summary_only=summary_value,
                )
                applied_set = set(applied_versions)

                all_migrations = self.runner.get_migration_files()
                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.available", float(len(all_migrations)))

                pending = self._collect_pending_migrations(all_migrations, applied_set, revision)
                pending_count = len(pending)

                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.pending", float(len(pending)))

                if not pending:
                    _report_no_pending_migrations(ul, echo_value, summary_value, bool(all_migrations))
                    return

                migration_config = cast("dict[str, Any]", self.config.migration_config) or {}
                strict_ordering = migration_config.get("strict_ordering", False) and not allow_missing
                validate_migration_order(
                    [v for v, _ in pending],
                    applied_versions,
                    strict_ordering,
                    use_logger=ul,
                    echo=echo_value,
                    summary_only=summary_value,
                )

                output_info(
                    "Found %d pending migrations",
                    len(pending),
                    rich_message=f"[yellow]Found {len(pending)} pending migrations[/]",
                )

                for version, file_path in pending:
                    migration = self.runner.load_migration(file_path, version)
                    action_verb = "Would apply" if dry_run else "Applying"
                    output_info(
                        "%s %s: %s",
                        action_verb,
                        version,
                        migration["description"],
                        rich_message=f"\n[cyan]{action_verb} {version}:[/] {migration['description']}",
                    )

                    if dry_run:
                        output_info(
                            "Migration file: %s", file_path, rich_message=f"[dim]Migration file: {file_path}[/]"
                        )
                        continue

                    result = self._apply_single_migration(driver, migration, version, ul, echo_value, summary_value)
                    if result is None:
                        return
                    applied_count += 1

        except Exception as exc:  # pragma: no cover
            error = exc
            raise
        finally:
            effective_error = error or self._last_command_error
            if not dry_run and applied_count:
                self._record_command_metric("applied", float(applied_count))
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="upgrade",
                status="failed" if effective_error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=applied_count,
                reverted_count=None,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=effective_error,
                allow_missing=allow_missing,
                auto_sync=auto_sync,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )
        elif applied_count > 0 and self._last_command_error is None:
            auto_dump_path = self._auto_dump_path()
            if auto_dump_path is not None:
                try:
                    self.dump_schema(output_path=auto_dump_path, echo=echo_value)
                except _AUTO_DUMP_ERRORS as exc:
                    self._report_auto_dump_failure(auto_dump_path, exc, use_logger=ul, echo=echo_value)

    @_with_command_span("downgrade", metadata_fn=_command_metadata)
    def downgrade(
        self,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Downgrade to a target revision.

        Args:
            revision: Target revision or "-1" for one step back.
            dry_run: If True, show what would be done without making changes.
            use_logger: If True, output to logger instead of Rich console.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        runtime = self._runtime
        reverted_count = 0
        pending_count = 0
        db_system: str | None = None
        error: Exception | None = None
        ul, echo_value, summary_value = self._resolve_output_policy(use_logger, echo, summary_only)
        self.runner.set_use_logger(ul)
        self.runner.set_summary_only(summary_value)
        self.tracker.set_output_policy(use_logger=ul, echo=echo_value, summary_only=summary_value)
        output_info = functools.partial(_output_info, ul, echo_value, summary_value)
        output_error = functools.partial(_output_error, ul, echo_value, summary_value)
        start_time = time.perf_counter()

        try:
            if dry_run:
                output_info(
                    "DRY RUN MODE: No database changes will be applied",
                    rich_message="[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n",
                )

            with self.config.provide_session() as driver:
                db_system = resolve_db_system(type(driver).__name__)
                self.runner.set_driver(driver)
                self._validate_migration_schema(driver)
                self.tracker.ensure_tracking_table(driver)
                applied: list[AppliedMigrationRecord] = self.tracker.get_applied_migrations(driver)
                if runtime is not None:
                    runtime.increment_metric("migrations.command.downgrade.available", float(len(applied)))
                if not applied:
                    output_info("No migrations to downgrade", rich_message="[yellow]No migrations to downgrade[/]")
                    return

                to_revert = self._collect_revert_migrations(applied, revision)
                pending_count = len(to_revert)

                if runtime is not None:
                    runtime.increment_metric("migrations.command.downgrade.pending", float(len(to_revert)))

                if not to_revert:
                    output_info("Nothing to downgrade", rich_message="[yellow]Nothing to downgrade[/]")
                    return

                output_info(
                    "Reverting %d migrations",
                    len(to_revert),
                    rich_message=f"[yellow]Reverting {len(to_revert)} migrations[/]",
                )
                all_files = dict(self.runner.get_migration_files())

                for migration_record in to_revert:
                    version = migration_record["version_num"]
                    if version not in all_files:
                        output_error(
                            "Migration file not found for %s",
                            version,
                            rich_message=f"[red]Migration file not found for {version}[/]",
                        )
                        if runtime is not None:
                            runtime.increment_metric("migrations.command.downgrade.missing_files")
                        self._last_command_error = MigrationError(f"Migration file not found for {version}")
                        return

                    migration = self.runner.load_migration(all_files[version], version)
                    action_verb = "Would revert" if dry_run else "Reverting"
                    output_info(
                        "%s %s: %s",
                        action_verb,
                        version,
                        migration["description"],
                        rich_message=f"\n[cyan]{action_verb} {version}:[/] {migration['description']}",
                    )

                    if dry_run:
                        output_info(
                            "Migration file: %s",
                            all_files[version],
                            rich_message=f"[dim]Migration file: {all_files[version]}[/]",
                        )
                        continue

                    result = self._revert_single_migration(driver, migration, version, ul, echo_value, summary_value)
                    if result is None:
                        return
                    reverted_count += 1

        except Exception as exc:  # pragma: no cover
            error = exc
            raise
        finally:
            effective_error = error or self._last_command_error
            if not dry_run and reverted_count:
                self._record_command_metric("reverted", float(reverted_count))
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="downgrade",
                status="failed" if effective_error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=None,
                reverted_count=reverted_count,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=effective_error,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )

    def stamp(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)
            if revision == "base":
                clear_sql = sql.delete().from_(self.tracker.version_table)
                driver.execute(clear_sql)
                if isinstance(self.tracker, SyncMigrationTracker):
                    self.tracker._safe_commit(driver)
                console.print("[green]Database stamped at revision base[/]")
                return

            all_migrations_list = self.runner.get_migration_files()
            target_version: str | None = None
            try:
                parsed_target = parse_version(revision)
            except ValueError:
                parsed_target = None

            for v, _ in all_migrations_list:
                if v == revision:
                    target_version = v
                    break
                if parsed_target is not None:
                    try:
                        if parse_version(v) == parsed_target:
                            target_version = v
                            break
                    except ValueError:
                        continue

            if target_version is None:
                console.print(f"[red]Unknown revision: {revision}[/]")
                return

            target_parsed = parse_version(target_version) if parsed_target is not None else None
            clear_sql = sql.delete().from_(self.tracker.version_table)
            driver.execute(clear_sql)
            for v, path in all_migrations_list:
                should_record = False
                if target_parsed is not None:
                    try:
                        v_parsed = parse_version(v)
                        if v_parsed.extension == target_parsed.extension and v_parsed <= target_parsed:
                            should_record = True
                    except ValueError:
                        should_record = v == target_version
                elif v == target_version:
                    should_record = True
                if should_record:
                    if path.exists():
                        migration = self.runner.load_migration(path, v)
                        description = migration["description"] or f"Stamped to {v}"
                        checksum = migration["checksum"]
                    else:
                        description = f"Stamped to {v}"
                        checksum = "manual-stamp"
                    self.tracker.record_migration(driver, v, description, 0, checksum)
            console.print(f"[green]Database stamped at revision {target_version}[/]")

    def revision(
        self,
        message: str,
        file_type: str | None = None,
        *,
        from_schema: bool = False,
        from_data: "str | Path | None" = None,
        schema: str | None = None,
        target_table: str | None = None,
    ) -> "Path":
        """Create a new migration file with timestamp-based versioning.

        Generates a unique timestamp version (YYYYMMDDHHmmss format) to avoid
        conflicts when multiple developers create migrations concurrently.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
            from_schema: When True, populate the migration with the current database schema DDL.
            from_data: Data file path or staging table name to infer DDL from. See
                ``MigrationCommandsCore._resolve_data_source`` for the detection rules.
            schema: Optional database schema name.
            target_table: Optional target table name when bootstrapping from data.

        Returns:
            Path to the created migration file.

        Raises:
            MigrationError: When both sources are given or DDL cannot be inferred.
        """
        source = self._prepare_revision_source(from_schema=from_schema, from_data=from_data)
        up_sql: Sequence[str] | None = None
        down_sql: Sequence[str] | None = None
        if from_schema:
            excluded = self._version_table_exclusion()
            with self.config.provide_session() as driver:
                up_sql, down_sql = schema_ddl_statements(
                    driver.data_dictionary.get_schema_ddl(
                        driver, schema=schema, exclude_tables=excluded, direction="create"
                    ),
                    driver.data_dictionary.get_schema_ddl(
                        driver, schema=schema, exclude_tables=excluded, direction="drop"
                    ),
                )
        elif isinstance(source, Path):
            up_sql, down_sql = self._data_file_revision_sql(source, schema=schema, target_table=target_table)
        elif isinstance(source, str):
            source_table, source_schema = self._split_qualified_table(source, schema)
            with self.config.provide_session() as driver:
                try:
                    ddl_res = driver.data_dictionary.infer_table_ddl_from_data(
                        driver, source_table, schema=source_schema, target_table=target_table
                    )
                except (SqlglotError, SQLBuilderError) as exc:
                    msg = f"Could not infer DDL from table '{source}': {exc}"
                    raise MigrationError(msg) from exc
                up_sql, down_sql = self._staging_table_revision_sql(ddl_res.ddl, source, self._driver_dialect(driver))

        return self._write_revision(message, file_type, up_sql, down_sql)

    def dump_schema(
        self,
        output_path: "str | Path | None" = None,
        *,
        schema: str | None = None,
        include_migrations_table: bool = False,
        prefer_native: bool = True,
        target_dialect: str | None = None,
        echo: bool | None = None,
    ) -> str:
        """Dump the full current database schema DDL to a string and optional file path.

        Args:
            output_path: Optional file path to write the rendered schema SQL.
            schema: Optional database schema name to inspect.
            include_migrations_table: Whether to include the migration tracking table.
            prefer_native: Prefer engine-native catalog DDL when available.
            target_dialect: Optional target SQLGlot dialect for cross-dialect transpilation.
            echo: Echo output to the console.

        Returns:
            Rendered schema DDL string.
        """
        _, echo_value, _ = self._resolve_output_policy(False, echo, False)
        with self.config.provide_session() as driver:
            create_res = driver.data_dictionary.get_schema_ddl(
                driver,
                schema=schema,
                exclude_tables=self._version_table_exclusion(include_migrations_table),
                direction="create",
                prefer_native=prefer_native,
                target_dialect=target_dialect,
            )
            dialect_label = self._dump_dialect_label(driver, target_dialect)
        return self._render_schema_dump(
            create_res, dialect_label=dialect_label, schema=schema, output_path=output_path, echo=echo_value
        )

    def verify(
        self, *, strict: bool = False, fail_on_pending: bool = False, use_logger: bool = False, echo: bool | None = None
    ) -> "dict[str, Any]":
        """Verify database migration tracking records against migration files on disk.

        Verification is read-only: a missing tracking table is reported as every migration pending and is never
        created.

        Args:
            strict: Raise ``MigrationError`` when checksum drift, orphaned records, or partial squashes exist.
            fail_on_pending: Raise ``MigrationError`` when migrations are pending.
            use_logger: Output verification messages to logger instead of Rich console.
            echo: Echo output to the console.

        Returns:
            Verification report dictionary with ``valid``, ``checksum_mismatches``, ``orphaned_records``,
            ``pending_versions``, and ``partial_squash_errors``.
        """
        ul, echo_value, summary_value = self._resolve_output_policy(use_logger, echo, None)
        with self.config.provide_session() as driver:
            applied_migrations = self._read_applied_migrations(driver)

        self.runner.clear_caches()
        loaded_files: dict[str, LoadedMigrationMetadata] = {
            version: self.runner.load_migration(file_path, version)
            for version, file_path in self.runner.get_migration_files()
        }
        return self._verification_report(
            applied_migrations,
            loaded_files,
            strict=strict,
            fail_on_pending=fail_on_pending,
            use_logger=ul,
            echo=echo_value,
            summary_only=summary_value,
        )

    def squash(
        self,
        start_version: str | None = None,
        end_version: str | None = None,
        description: str | None = None,
        *,
        dry_run: bool = False,
        update_database: bool = True,
        yes: bool = False,
        allow_gaps: bool = False,
        output_format: str = "sql",
        strategy: str = "concat",
        single_file: bool | None = None,
    ) -> None:
        """Squash a range of migrations into a single file.

        Combines multiple sequential or timestamped migrations into a single release migration.

        Args:
            start_version: First version in the range to squash (inclusive).
                When None, defaults to the first core migration found.
            end_version: Last version in the range to squash (inclusive).
                When None, defaults to the last core migration found.
            description: Description for the squashed migration file.
                When None, defaults to 'initial_schema' if yes=True or prompts interactively.
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
            allow_gaps: Allow gaps in version sequence.
            output_format: Output format ("sql" or "py").
            strategy: Squash strategy: "concat" (default) keeps every statement in order, "ddl" folds DDL and
                refuses ranges it cannot fold faithfully, "schema" reflects the live database schema.
            single_file: Force a single squashed file when True, split by file type/directives when False or None.
        """
        resolved_strategy = strategy.lower()
        all_migrations = self.runner.get_migration_files()
        squasher = self._create_squasher(self.runner, resolved_strategy)
        version_range = self._resolve_squash_range(all_migrations, start_version, end_version)
        if version_range is None:
            return
        if description is None:
            description = (
                "initial_schema" if yes else Prompt.ask("Migration description", default="squashed_migrations")
            )

        schema_ddl: tuple[list[str], list[str]] | None = None
        if resolved_strategy == "schema":
            squasher.plan_squash(
                version_range[0],
                version_range[1],
                description,
                allow_gaps=allow_gaps,
                output_format=output_format,
                strategy=resolved_strategy,
                single_file=single_file,
            )
            excluded = self._version_table_exclusion()
            with self.config.provide_session() as driver:
                self._validate_schema_squash_applied(
                    self.runner,
                    all_migrations,
                    version_range[0],
                    version_range[1],
                    allow_gaps=allow_gaps,
                    applied_migrations=self._read_applied_migrations(driver),
                )
                schema_ddl = schema_ddl_statements(
                    driver.data_dictionary.get_schema_ddl(driver, exclude_tables=excluded, direction="create"),
                    driver.data_dictionary.get_schema_ddl(driver, exclude_tables=excluded, direction="drop"),
                )

        plans = squasher.plan_squash(
            version_range[0],
            version_range[1],
            description,
            allow_gaps=allow_gaps,
            output_format=output_format,
            strategy=resolved_strategy,
            single_file=single_file,
            schema_ddl=schema_ddl,
        )
        self._print_squash_plan(plans)
        if dry_run:
            self._print_squash_dry_run(squasher, plans)
            return
        if not yes and not Confirm.ask("Proceed with squash?", default=False):
            console.print("[yellow]Squash cancelled[/]")
            return

        try:
            if update_database:
                with self.config.provide_session() as driver:
                    self.tracker.ensure_tracking_table(driver)
                    applied_plans = [
                        plan
                        for plan in plans
                        if self.tracker.is_squash_already_applied(driver, plan.target_version, plan.source_versions)
                    ]
                    self._apply_squash_files(squasher, plans)
                    for plan in applied_plans:
                        self.tracker.replace_with_squash(
                            driver,
                            plan.target_version,
                            plan.source_versions,
                            description,
                            self._squash_record_checksum(plan),
                        )
                    console.print("[green]✓ Updated migration tracking table[/]")
            else:
                self._apply_squash_files(squasher, plans)
        except Exception:
            squasher.rollback_backup()
            raise
        self._finish_squash(squasher)

    def fix(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
        """
        all_migrations = self.runner.get_migration_files()

        conversion_map = generate_conversion_map(all_migrations)

        if not conversion_map:
            console.print("[yellow]No timestamp migrations found - nothing to convert[/]")
            return

        fixer = MigrationFixer(self.migrations_path)
        renames = fixer.plan_renames(conversion_map)

        table = Table(title="Migration Conversions")
        table.add_column("Current Version", style="cyan")
        table.add_column("New Version", style="green")
        table.add_column("File")

        for rename in renames:
            table.add_row(rename.old_version, rename.new_version, rename.old_path.name)

        console.print(table)
        console.print(f"\n[yellow]{len(renames)} migrations will be converted[/]")

        if dry_run:
            console.print("[yellow][Preview Mode - No changes made][/]")
            return

        if not yes and not Confirm.ask("Proceed with conversion?", default=False):
            console.print("[yellow]Conversion cancelled[/]")
            return

        try:
            backup_path = fixer.create_backup()
            console.print(f"[green]✓ Created backup in {backup_path.name}[/]")

            fixer.apply_renames(renames)
            for rename in renames:
                console.print(f"[green]✓ Renamed {rename.old_path.name} → {rename.new_path.name}[/]")

            if update_database:
                with self.config.provide_session() as driver:
                    self.tracker.ensure_tracking_table(driver)
                    applied_migrations: list[AppliedMigrationRecord] = self.tracker.get_applied_migrations(driver)
                    applied_versions = {m["version_num"] for m in applied_migrations}

                    updated_count = 0
                    for old_version, new_version in conversion_map.items():
                        if old_version in applied_versions:
                            self.tracker.update_version_record(
                                driver, old_version, new_version, applied_versions=applied_versions
                            )
                            updated_count += 1

                    if updated_count > 0:
                        console.print(
                            f"[green]✓ Updated {updated_count} version records in migration tracking table[/]"
                        )
                    else:
                        console.print("[green]✓ No applied migrations to update in tracking table[/]")

            fixer.cleanup()
            self.runner.clear_caches()
            console.print("[green]✓ Conversion complete![/]")

        except Exception as e:
            console.print(f"[red]✗ Error: {e}[/]")
            fixer.rollback()
            console.print("[yellow]Restored files from backup[/]")
            raise

    def _validate_migration_schema(self, driver: Any) -> None:
        """Validate the configured migration schema exists before issuing DDL."""
        default_schema = self._resolve_default_schema()
        if default_schema is None:
            return
        self._require_schema_support(default_schema)
        if not driver.has_schema(default_schema):
            msg = f"Configured schema '{default_schema}' does not exist"
            raise MigrationError(msg)

    def _read_applied_migrations(self, driver: Any) -> "list[AppliedMigrationRecord]":
        """Read applied migration records without creating the tracking table.

        Returns:
            Applied records, or an empty list when the tracking table does not exist.
        """
        tables = driver.data_dictionary.get_tables(driver, schema=self.tracker.version_table_schema)
        if not self._tracking_table_exists(tables):
            return []
        return cast("list[AppliedMigrationRecord]", self.tracker.get_applied_migrations(driver))

    def _load_reconcilable_migrations(
        self, all_migrations: "list[tuple[str, Path]]"
    ) -> "dict[str, LoadedMigrationMetadata]":
        """Load metadata for tracking-table reconciliation, skipping files that cannot be loaded.

        A skipped file is never reconciled; if it is pending, ``upgrade`` loads it again and reports the failure.
        """
        loaded: dict[str, LoadedMigrationMetadata] = {}
        for version, file_path in all_migrations:
            try:
                loaded[version] = self.runner.load_migration(file_path, version)
            except Exception as exc:
                self._log_reconciliation_load_failure(version, file_path, exc)
        return loaded

    def _synchronized_applied_versions(
        self, driver: Any, *, auto_sync: bool, dry_run: bool, use_logger: bool, echo: bool, summary_only: bool
    ) -> "list[str]":
        """Return applied versions after optional reconciliation, writing nothing in a dry run.

        A dry run neither creates the tracking table nor reconciles records; it reports the reconciliation it would
        perform and returns the applied versions as they would look afterwards. A partially applied squash raises
        even when ``auto_sync`` is disabled.

        Args:
            driver: Active database driver.
            auto_sync: Whether to reconcile renamed and squashed migrations.
            dry_run: Whether database writes are disabled.
            use_logger: Whether logger output is enabled.
            echo: Whether console output is enabled.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Applied version numbers.
        """
        if dry_run:
            applied_migrations = self._read_applied_migrations(driver)
        else:
            self.tracker.ensure_tracking_table(driver)
            applied_migrations = self.tracker.get_applied_migrations(driver)
        all_migrations = self.runner.get_migration_files()
        plan = self._plan_version_reconciliation(
            all_migrations, applied_migrations, self._load_reconcilable_migrations(all_migrations)
        )
        if not auto_sync or not plan.change_count:
            return [m["version_num"] for m in applied_migrations]
        self._report_version_reconciliation(
            plan, dry_run=dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only
        )
        if dry_run:
            return list(plan.applied_versions)
        self._apply_version_reconciliation(driver, plan)
        return [m["version_num"] for m in self.tracker.get_applied_migrations(driver)]

    def _apply_version_reconciliation(self, driver: Any, plan: VersionReconciliationPlan) -> None:
        """Write a reconciliation plan to the tracking table."""
        for squash in plan.squashes:
            self.tracker.replace_with_squash(
                driver, squash.target_version, list(squash.replaced_versions), squash.description, squash.checksum
            )
        for rename in plan.renames:
            self.tracker.update_version_record(
                driver, rename.old_version, rename.new_version, applied_versions=set(rename.claimed_versions)
            )

    def _apply_single_migration(
        self,
        driver: Any,
        migration: "LoadedMigrationMetadata",
        version: str,
        use_logger: bool,
        echo: bool,
        summary_only: bool,
    ) -> int | None:
        """Apply a single migration and record it.

        Args:
            driver: Database driver instance.
            migration: Migration dictionary with version, description, checksum.
            version: Version string.
            use_logger: Whether to output to logger instead of console.
            echo: Whether to echo output to the console.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Execution time in ms on success, None on failure.
        """
        use_txn = self.runner.should_use_transaction(migration, self.config)
        try:

            def record_version(
                exec_time: int,
                migration: "LoadedMigrationMetadata" = migration,
                version: str = version,
                use_txn: bool = use_txn,
            ) -> None:
                recorded_version = migration["version"] or version
                if isinstance(self.tracker, SyncMigrationTracker):
                    self.tracker.record_migration(
                        driver,
                        recorded_version,
                        migration["description"],
                        exec_time,
                        migration["checksum"],
                        commit=not use_txn,
                        replaces=migration.get("replaces"),
                    )
                else:
                    self.tracker.record_migration(
                        driver, recorded_version, migration["description"], exec_time, migration["checksum"]
                    )

            _, execution_time = self.runner.execute_upgrade(
                driver, migration, use_transaction=use_txn, on_success=record_version
            )
        except Exception as exc:
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_error(
                use_logger,
                echo,
                summary_only,
                "Migration %s failed%s",
                version,
                rollback_msg,
                rich_message=f"[red]✗ Failed{rollback_msg}: {exc}[/]",
            )
            self._last_command_error = exc
            return None
        else:
            _output_info(
                use_logger,
                echo,
                summary_only,
                "Applied migration %s in %dms",
                version,
                execution_time,
                rich_message=f"[green]✓ Applied in {execution_time}ms[/]",
            )
            return execution_time

    def _revert_single_migration(
        self,
        driver: Any,
        migration: "LoadedMigrationMetadata",
        version: str,
        use_logger: bool,
        echo: bool,
        summary_only: bool,
    ) -> int | None:
        """Revert a single migration.

        Args:
            driver: Database driver instance.
            migration: Migration dictionary.
            version: Version string.
            use_logger: Whether to output to logger instead of console.
            echo: Whether to echo output to the console.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Execution time in ms on success, None on failure.
        """
        use_txn = self.runner.should_use_transaction(migration, self.config)
        reverted = False
        try:

            def remove_version(exec_time: int, version: str = version, use_txn: bool = use_txn) -> None:
                nonlocal reverted
                reverted = True
                if isinstance(self.tracker, SyncMigrationTracker):
                    self.tracker.remove_migration(driver, version, commit=not use_txn)
                else:
                    self.tracker.remove_migration(driver, version)

            _, execution_time = self.runner.execute_downgrade(
                driver, migration, use_transaction=use_txn, on_success=remove_version
            )
        except Exception as exc:
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_error(
                use_logger,
                echo,
                summary_only,
                "Migration %s failed%s",
                version,
                rollback_msg,
                rich_message=f"[red]✗ Failed{rollback_msg}: {exc}[/]",
            )
            self._last_command_error = exc
            return None
        if not reverted:
            missing_exc = MigrationError(f"Migration {version} does not support downgrade")
            _output_error(
                use_logger,
                echo,
                summary_only,
                "Migration %s failed",
                version,
                rich_message=f"[red]✗ Failed: {missing_exc}[/]",
            )
            self._last_command_error = missing_exc
            return None
        _output_info(
            use_logger,
            echo,
            summary_only,
            "Reverted migration %s in %dms",
            version,
            execution_time,
            rich_message=f"[green]✓ Reverted in {execution_time}ms[/]",
        )
        return execution_time


class AsyncMigrationCommands(MigrationCommandsCore["AsyncConfigT", AsyncMigrationRunner]):
    """Asynchronous migration commands."""

    def __init__(self, config: "AsyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        super().__init__(config)
        self.tracker = self._create_tracker()
        context = MigrationContext.from_config(self.config)
        context.extension_config = self.extension_configs

        self.runner = AsyncMigrationRunner(
            self.migrations_path,
            self._discover_extension_migrations(),
            context,
            self.extension_configs,
            runtime=self._runtime,
            description_hints=self._template_settings.description_hints,
        )

    async def init(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory path for migrations.
            package: Whether to create __init__.py in the directory.
        """
        self.init_directory(directory, package)

    async def current(self, verbose: bool = False) -> "str | None":
        """Show current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            The current migration version or None if no migrations applied.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            applied: list[AppliedMigrationRecord] = []
            if verbose:
                applied = await self.tracker.get_applied_migrations(driver)
                current = applied[-1]["version_num"] if applied else None
            else:
                current = await self.tracker.get_current_version(driver)
            if not current:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "migration.list",
                    db_system=resolve_db_system(type(driver).__name__),
                    current_version=None,
                    applied_count=0,
                    verbose=verbose,
                    status="empty",
                )
                console.print("[yellow]No migrations applied yet[/]")
                return None

            console.print(f"[green]Current version:[/] {current}")
            if verbose:
                table = Table(title="Applied Migrations")
                table.add_column("Version", style="cyan")
                table.add_column("Description")
                table.add_column("Applied At")
                table.add_column("Time (ms)", justify="right")
                table.add_column("Applied By")
                for migration in applied:
                    table.add_row(
                        migration["version_num"],
                        migration.get("description", ""),
                        str(migration.get("applied_at", "")),
                        str(migration.get("execution_time_ms", "")),
                        migration.get("applied_by", ""),
                    )
                console.print(table)

            applied_count = len(applied) if verbose else None
            log_with_context(
                logger,
                logging.DEBUG,
                "migration.list",
                db_system=resolve_db_system(type(driver).__name__),
                current_version=current,
                applied_count=applied_count,
                verbose=verbose,
                status="complete",
            )
            return cast("str | None", current)

    @_with_command_span("upgrade", metadata_fn=_command_metadata)
    async def upgrade(
        self,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Upgrade to a target revision.

        Validates migration order and warns if out-of-order migrations are detected.
        Out-of-order migrations can occur when branches merge in different orders
        across environments.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: If True, allow out-of-order migrations even in strict mode.
                Defaults to False.
            auto_sync: If True, automatically reconcile renamed and squashed migrations in the tracking table.
                Defaults to True. Can be disabled via --no-auto-sync flag. A partially applied squash raises
                ``SquashValidationError`` either way.
            dry_run: If True, report the reconciliation and migrations that would run without creating the
                tracking table or writing any tracking rows.
            use_logger: If True, output to logger instead of Rich console.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        runtime = self._runtime
        applied_count = 0
        pending_count = 0
        db_system: str | None = None
        error: Exception | None = None
        ul, echo_value, summary_value = self._resolve_output_policy(use_logger, echo, summary_only)
        self.runner.set_use_logger(ul)
        self.runner.set_summary_only(summary_value)
        self.tracker.set_output_policy(use_logger=ul, echo=echo_value, summary_only=summary_value)
        output_info = functools.partial(_output_info, ul, echo_value, summary_value)
        start_time = time.perf_counter()

        try:
            if dry_run:
                output_info(
                    "DRY RUN MODE: No database changes will be applied",
                    rich_message="[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n",
                )

            async with self.config.provide_session() as driver:
                db_system = resolve_db_system(type(driver).__name__)
                self.runner.set_driver(driver)
                await self._validate_migration_schema(driver)
                applied_versions = await self._synchronized_applied_versions(
                    driver,
                    auto_sync=auto_sync and bool(self._get_migration_config().get("auto_sync", True)),
                    dry_run=dry_run,
                    use_logger=ul,
                    echo=echo_value,
                    summary_only=summary_value,
                )
                applied_set = set(applied_versions)

                all_migrations = await self.runner.get_migration_files()
                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.available", float(len(all_migrations)))

                pending = self._collect_pending_migrations(all_migrations, applied_set, revision)
                pending_count = len(pending)

                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.pending", float(len(pending)))

                if not pending:
                    _report_no_pending_migrations(ul, echo_value, summary_value, bool(all_migrations))
                    return

                migration_config = cast("dict[str, Any]", self.config.migration_config) or {}
                strict_ordering = migration_config.get("strict_ordering", False) and not allow_missing
                validate_migration_order(
                    [v for v, _ in pending],
                    applied_versions,
                    strict_ordering,
                    use_logger=ul,
                    echo=echo_value,
                    summary_only=summary_value,
                )

                output_info(
                    "Found %d pending migrations",
                    len(pending),
                    rich_message=f"[yellow]Found {len(pending)} pending migrations[/]",
                )

                for version, file_path in pending:
                    migration = await self.runner.load_migration(file_path, version)
                    action_verb = "Would apply" if dry_run else "Applying"
                    output_info(
                        "%s %s: %s",
                        action_verb,
                        version,
                        migration["description"],
                        rich_message=f"\n[cyan]{action_verb} {version}:[/] {migration['description']}",
                    )

                    if dry_run:
                        output_info(
                            "Migration file: %s", file_path, rich_message=f"[dim]Migration file: {file_path}[/]"
                        )
                        continue

                    result = await self._apply_single_migration(
                        driver, migration, version, ul, echo_value, summary_value
                    )
                    if result is None:
                        return
                    applied_count += 1

        except Exception as exc:  # pragma: no cover
            error = exc
            raise
        finally:
            effective_error = error or self._last_command_error
            if not dry_run and applied_count:
                self._record_command_metric("applied", float(applied_count))
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="upgrade",
                status="failed" if effective_error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=applied_count,
                reverted_count=None,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=effective_error,
                allow_missing=allow_missing,
                auto_sync=auto_sync,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )
        elif applied_count > 0 and self._last_command_error is None:
            auto_dump_path = self._auto_dump_path()
            if auto_dump_path is not None:
                try:
                    await self.dump_schema(output_path=auto_dump_path, echo=echo_value)
                except _AUTO_DUMP_ERRORS as exc:
                    self._report_auto_dump_failure(auto_dump_path, exc, use_logger=ul, echo=echo_value)

    @_with_command_span("downgrade", metadata_fn=_command_metadata)
    async def downgrade(
        self,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Downgrade to a target revision.

        Args:
            revision: Target revision or "-1" for one step back.
            dry_run: If True, show what would be done without making changes.
            use_logger: If True, output to logger instead of Rich console.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        runtime = self._runtime
        reverted_count = 0
        pending_count = 0
        db_system: str | None = None
        error: Exception | None = None
        ul, echo_value, summary_value = self._resolve_output_policy(use_logger, echo, summary_only)
        self.runner.set_use_logger(ul)
        self.runner.set_summary_only(summary_value)
        self.tracker.set_output_policy(use_logger=ul, echo=echo_value, summary_only=summary_value)
        output_info = functools.partial(_output_info, ul, echo_value, summary_value)
        output_error = functools.partial(_output_error, ul, echo_value, summary_value)
        start_time = time.perf_counter()

        try:
            if dry_run:
                output_info(
                    "DRY RUN MODE: No database changes will be applied",
                    rich_message="[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n",
                )

            async with self.config.provide_session() as driver:
                db_system = resolve_db_system(type(driver).__name__)
                self.runner.set_driver(driver)
                await self._validate_migration_schema(driver)
                await self.tracker.ensure_tracking_table(driver)

                applied: list[AppliedMigrationRecord] = await self.tracker.get_applied_migrations(driver)
                if runtime is not None:
                    runtime.increment_metric("migrations.command.downgrade.available", float(len(applied)))
                if not applied:
                    output_info("No migrations to downgrade", rich_message="[yellow]No migrations to downgrade[/]")
                    return

                to_revert = self._collect_revert_migrations(applied, revision)
                pending_count = len(to_revert)

                if runtime is not None:
                    runtime.increment_metric("migrations.command.downgrade.pending", float(len(to_revert)))

                if not to_revert:
                    output_info("Nothing to downgrade", rich_message="[yellow]Nothing to downgrade[/]")
                    return

                output_info(
                    "Reverting %d migrations",
                    len(to_revert),
                    rich_message=f"[yellow]Reverting {len(to_revert)} migrations[/]",
                )
                all_files = dict(await self.runner.get_migration_files())

                for migration_record in to_revert:
                    version = migration_record["version_num"]
                    if version not in all_files:
                        output_error(
                            "Migration file not found for %s",
                            version,
                            rich_message=f"[red]Migration file not found for {version}[/]",
                        )
                        if runtime is not None:
                            runtime.increment_metric("migrations.command.downgrade.missing_files")
                        self._last_command_error = MigrationError(f"Migration file not found for {version}")
                        return

                    migration = await self.runner.load_migration(all_files[version], version)
                    action_verb = "Would revert" if dry_run else "Reverting"
                    output_info(
                        "%s %s: %s",
                        action_verb,
                        version,
                        migration["description"],
                        rich_message=f"\n[cyan]{action_verb} {version}:[/] {migration['description']}",
                    )

                    if dry_run:
                        output_info(
                            "Migration file: %s",
                            all_files[version],
                            rich_message=f"[dim]Migration file: {all_files[version]}[/]",
                        )
                        continue

                    result = await self._revert_single_migration(
                        driver, migration, version, ul, echo_value, summary_value
                    )
                    if result is None:
                        return
                    reverted_count += 1

        except Exception as exc:  # pragma: no cover
            error = exc
            raise
        finally:
            effective_error = error or self._last_command_error
            if not dry_run and reverted_count:
                self._record_command_metric("reverted", float(reverted_count))
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="downgrade",
                status="failed" if effective_error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=None,
                reverted_count=reverted_count,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=effective_error,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )

    async def stamp(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            if revision == "base":
                clear_sql = sql.delete().from_(self.tracker.version_table)
                await driver.execute(clear_sql)
                if isinstance(self.tracker, AsyncMigrationTracker):
                    await self.tracker._safe_commit(driver)
                console.print("[green]Database stamped at revision base[/]")
                return

            all_migrations_list = await self.runner.get_migration_files()
            target_version: str | None = None
            try:
                parsed_target = parse_version(revision)
            except ValueError:
                parsed_target = None

            for v, _ in all_migrations_list:
                if v == revision:
                    target_version = v
                    break
                if parsed_target is not None:
                    try:
                        if parse_version(v) == parsed_target:
                            target_version = v
                            break
                    except ValueError:
                        continue

            if target_version is None:
                console.print(f"[red]Unknown revision: {revision}[/]")
                return

            target_parsed = parse_version(target_version) if parsed_target is not None else None
            clear_sql = sql.delete().from_(self.tracker.version_table)
            await driver.execute(clear_sql)
            for v, path in all_migrations_list:
                should_record = False
                if target_parsed is not None:
                    try:
                        v_parsed = parse_version(v)
                        if v_parsed.extension == target_parsed.extension and v_parsed <= target_parsed:
                            should_record = True
                    except ValueError:
                        should_record = v == target_version
                elif v == target_version:
                    should_record = True
                if should_record:
                    if path.exists():
                        migration = await self.runner.load_migration(path, v)
                        description = migration["description"] or f"Stamped to {v}"
                        checksum = migration["checksum"]
                    else:
                        description = f"Stamped to {v}"
                        checksum = "manual-stamp"
                    await self.tracker.record_migration(driver, v, description, 0, checksum)
            console.print(f"[green]Database stamped at revision {target_version}[/]")

    async def revision(
        self,
        message: str,
        file_type: str | None = None,
        *,
        from_schema: bool = False,
        from_data: "str | Path | None" = None,
        schema: str | None = None,
        target_table: str | None = None,
    ) -> "Path":
        """Create a new migration file with timestamp-based versioning.

        Generates a unique timestamp version (YYYYMMDDHHmmss format) to avoid
        conflicts when multiple developers create migrations concurrently.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
            from_schema: When True, populate the migration with the current database schema DDL.
            from_data: Data file path or staging table name to infer DDL from. See
                ``MigrationCommandsCore._resolve_data_source`` for the detection rules.
            schema: Optional database schema name.
            target_table: Optional target table name when bootstrapping from data.

        Returns:
            Path to the created migration file.

        Raises:
            MigrationError: When both sources are given or DDL cannot be inferred.
        """
        source = self._prepare_revision_source(from_schema=from_schema, from_data=from_data)
        up_sql: Sequence[str] | None = None
        down_sql: Sequence[str] | None = None
        if from_schema:
            excluded = self._version_table_exclusion()
            async with self.config.provide_session() as driver:
                up_sql, down_sql = schema_ddl_statements(
                    await driver.data_dictionary.get_schema_ddl(
                        driver, schema=schema, exclude_tables=excluded, direction="create"
                    ),
                    await driver.data_dictionary.get_schema_ddl(
                        driver, schema=schema, exclude_tables=excluded, direction="drop"
                    ),
                )
        elif isinstance(source, Path):
            up_sql, down_sql = self._data_file_revision_sql(source, schema=schema, target_table=target_table)
        elif isinstance(source, str):
            source_table, source_schema = self._split_qualified_table(source, schema)
            async with self.config.provide_session() as driver:
                try:
                    ddl_res = await driver.data_dictionary.infer_table_ddl_from_data(
                        driver, source_table, schema=source_schema, target_table=target_table
                    )
                except (SqlglotError, SQLBuilderError) as exc:
                    msg = f"Could not infer DDL from table '{source}': {exc}"
                    raise MigrationError(msg) from exc
                up_sql, down_sql = self._staging_table_revision_sql(ddl_res.ddl, source, self._driver_dialect(driver))

        return self._write_revision(message, file_type, up_sql, down_sql)

    async def dump_schema(
        self,
        output_path: "str | Path | None" = None,
        *,
        schema: str | None = None,
        include_migrations_table: bool = False,
        prefer_native: bool = True,
        target_dialect: str | None = None,
        echo: bool | None = None,
    ) -> str:
        """Dump the full current database schema DDL to a string and optional file path.

        Args:
            output_path: Optional file path to write the rendered schema SQL.
            schema: Optional database schema name to inspect.
            include_migrations_table: Whether to include the migration tracking table.
            prefer_native: Prefer engine-native catalog DDL when available.
            target_dialect: Optional target SQLGlot dialect for cross-dialect transpilation.
            echo: Echo output to the console.

        Returns:
            Rendered schema DDL string.
        """
        _, echo_value, _ = self._resolve_output_policy(False, echo, False)
        async with self.config.provide_session() as driver:
            create_res = await driver.data_dictionary.get_schema_ddl(
                driver,
                schema=schema,
                exclude_tables=self._version_table_exclusion(include_migrations_table),
                direction="create",
                prefer_native=prefer_native,
                target_dialect=target_dialect,
            )
            dialect_label = self._dump_dialect_label(driver, target_dialect)
        return self._render_schema_dump(
            create_res, dialect_label=dialect_label, schema=schema, output_path=output_path, echo=echo_value
        )

    async def verify(
        self, *, strict: bool = False, fail_on_pending: bool = False, use_logger: bool = False, echo: bool | None = None
    ) -> "dict[str, Any]":
        """Verify database migration tracking records against migration files on disk.

        Verification is read-only: a missing tracking table is reported as every migration pending and is never
        created.

        Args:
            strict: Raise ``MigrationError`` when checksum drift, orphaned records, or partial squashes exist.
            fail_on_pending: Raise ``MigrationError`` when migrations are pending.
            use_logger: Output verification messages to logger instead of Rich console.
            echo: Echo output to the console.

        Returns:
            Verification report dictionary with ``valid``, ``checksum_mismatches``, ``orphaned_records``,
            ``pending_versions``, and ``partial_squash_errors``.
        """
        ul, echo_value, summary_value = self._resolve_output_policy(use_logger, echo, None)
        async with self.config.provide_session() as driver:
            applied_migrations = await self._read_applied_migrations(driver)

        self.runner.clear_caches()
        loaded_files: dict[str, LoadedMigrationMetadata] = {
            version: await self.runner.load_migration(file_path, version)
            for version, file_path in await self.runner.get_migration_files()
        }
        return self._verification_report(
            applied_migrations,
            loaded_files,
            strict=strict,
            fail_on_pending=fail_on_pending,
            use_logger=ul,
            echo=echo_value,
            summary_only=summary_value,
        )

    async def squash(
        self,
        start_version: str | None = None,
        end_version: str | None = None,
        description: str | None = None,
        *,
        dry_run: bool = False,
        update_database: bool = True,
        yes: bool = False,
        allow_gaps: bool = False,
        output_format: str = "sql",
        strategy: str = "concat",
        single_file: bool | None = None,
    ) -> None:
        """Squash a range of migrations into a single file.

        Combines multiple sequential or timestamped migrations into a single release migration.

        Args:
            start_version: First version in the range to squash (inclusive).
                When None, defaults to the first core migration found.
            end_version: Last version in the range to squash (inclusive).
                When None, defaults to the last core migration found.
            description: Description for the squashed migration file.
                When None, defaults to 'initial_schema' if yes=True or prompts interactively.
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
            allow_gaps: Allow gaps in version sequence.
            output_format: Output format ("sql" or "py").
            strategy: Squash strategy: "concat" (default) keeps every statement in order, "ddl" folds DDL and
                refuses ranges it cannot fold faithfully, "schema" reflects the live database schema.
            single_file: Force a single squashed file when True, split by file type/directives when False or None.
        """
        resolved_strategy = strategy.lower()
        sync_runner = self._create_squash_runner()
        all_migrations = sync_runner.get_migration_files()
        squasher = self._create_squasher(sync_runner, resolved_strategy)
        version_range = self._resolve_squash_range(all_migrations, start_version, end_version)
        if version_range is None:
            return
        if description is None:
            description = (
                "initial_schema"
                if yes
                else await asyncio.to_thread(Prompt.ask, "Migration description", default="squashed_migrations")
            )

        schema_ddl: tuple[list[str], list[str]] | None = None
        if resolved_strategy == "schema":
            squasher.plan_squash(
                version_range[0],
                version_range[1],
                description,
                allow_gaps=allow_gaps,
                output_format=output_format,
                strategy=resolved_strategy,
                single_file=single_file,
            )
            excluded = self._version_table_exclusion()
            async with self.config.provide_session() as driver:
                self._validate_schema_squash_applied(
                    sync_runner,
                    all_migrations,
                    version_range[0],
                    version_range[1],
                    allow_gaps=allow_gaps,
                    applied_migrations=await self._read_applied_migrations(driver),
                )
                schema_ddl = schema_ddl_statements(
                    await driver.data_dictionary.get_schema_ddl(driver, exclude_tables=excluded, direction="create"),
                    await driver.data_dictionary.get_schema_ddl(driver, exclude_tables=excluded, direction="drop"),
                )

        plans = squasher.plan_squash(
            version_range[0],
            version_range[1],
            description,
            allow_gaps=allow_gaps,
            output_format=output_format,
            strategy=resolved_strategy,
            single_file=single_file,
            schema_ddl=schema_ddl,
        )
        self._print_squash_plan(plans)
        if dry_run:
            self._print_squash_dry_run(squasher, plans)
            return
        if not yes and not await asyncio.to_thread(Confirm.ask, "Proceed with squash?", default=False):
            console.print("[yellow]Squash cancelled[/]")
            return

        try:
            if update_database:
                async with self.config.provide_session() as driver:
                    await self.tracker.ensure_tracking_table(driver)
                    applied_plans = [
                        plan
                        for plan in plans
                        if await self.tracker.is_squash_already_applied(
                            driver, plan.target_version, plan.source_versions
                        )
                    ]
                    self._apply_squash_files(squasher, plans)
                    for plan in applied_plans:
                        await self.tracker.replace_with_squash(
                            driver,
                            plan.target_version,
                            plan.source_versions,
                            description,
                            self._squash_record_checksum(plan),
                        )
                    console.print("[green]✓ Updated migration tracking table[/]")
            else:
                self._apply_squash_files(squasher, plans)
        except Exception:
            squasher.rollback_backup()
            raise
        self._finish_squash(squasher)

    async def fix(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
        """
        all_migrations = await self.runner.get_migration_files()

        conversion_map = generate_conversion_map(all_migrations)

        if not conversion_map:
            console.print("[yellow]No timestamp migrations found - nothing to convert[/]")
            return

        fixer = MigrationFixer(self.migrations_path)
        renames = fixer.plan_renames(conversion_map)

        table = Table(title="Migration Conversions")
        table.add_column("Current Version", style="cyan")
        table.add_column("New Version", style="green")
        table.add_column("File")

        for rename in renames:
            table.add_row(rename.old_version, rename.new_version, rename.old_path.name)

        console.print(table)
        console.print(f"\n[yellow]{len(renames)} migrations will be converted[/]")

        if dry_run:
            console.print("[yellow][Preview Mode - No changes made][/]")
            return

        if not yes and not await asyncio.to_thread(Confirm.ask, "Proceed with conversion?", default=False):
            console.print("[yellow]Conversion cancelled[/]")
            return

        try:
            backup_path = fixer.create_backup()
            console.print(f"[green]✓ Created backup in {backup_path.name}[/]")

            fixer.apply_renames(renames)
            for rename in renames:
                console.print(f"[green]✓ Renamed {rename.old_path.name} → {rename.new_path.name}[/]")

            if update_database:
                async with self.config.provide_session() as driver:
                    await self.tracker.ensure_tracking_table(driver)
                    applied_migrations: list[AppliedMigrationRecord] = await self.tracker.get_applied_migrations(driver)
                    applied_versions = {m["version_num"] for m in applied_migrations}

                    updated_count = 0
                    for old_version, new_version in conversion_map.items():
                        if old_version in applied_versions:
                            await self.tracker.update_version_record(
                                driver, old_version, new_version, applied_versions=applied_versions
                            )
                            updated_count += 1

                    if updated_count > 0:
                        console.print(
                            f"[green]✓ Updated {updated_count} version records in migration tracking table[/]"
                        )
                    else:
                        console.print("[green]✓ No applied migrations to update in tracking table[/]")

            fixer.cleanup()
            self.runner.clear_caches()
            console.print("[green]✓ Conversion complete![/]")

        except Exception as e:
            console.print(f"[red]✗ Error: {e}[/]")
            fixer.rollback()
            console.print("[yellow]Restored files from backup[/]")
            raise

    async def _validate_migration_schema(self, driver: Any) -> None:
        """Validate the configured migration schema exists before issuing DDL."""
        default_schema = self._resolve_default_schema()
        if default_schema is None:
            return
        self._require_schema_support(default_schema)
        if not await driver.has_schema(default_schema):
            msg = f"Configured schema '{default_schema}' does not exist"
            raise MigrationError(msg)

    async def _read_applied_migrations(self, driver: Any) -> "list[AppliedMigrationRecord]":
        """Read applied migration records without creating the tracking table.

        Returns:
            Applied records, or an empty list when the tracking table does not exist.
        """
        tables = await driver.data_dictionary.get_tables(driver, schema=self.tracker.version_table_schema)
        if not self._tracking_table_exists(tables):
            return []
        return cast("list[AppliedMigrationRecord]", await self.tracker.get_applied_migrations(driver))

    async def _load_reconcilable_migrations(
        self, all_migrations: "list[tuple[str, Path]]"
    ) -> "dict[str, LoadedMigrationMetadata]":
        """Load metadata for tracking-table reconciliation, skipping files that cannot be loaded.

        A skipped file is never reconciled; if it is pending, ``upgrade`` loads it again and reports the failure.
        """
        loaded: dict[str, LoadedMigrationMetadata] = {}
        for version, file_path in all_migrations:
            try:
                loaded[version] = await self.runner.load_migration(file_path, version)
            except Exception as exc:
                self._log_reconciliation_load_failure(version, file_path, exc)
        return loaded

    async def _synchronized_applied_versions(
        self, driver: Any, *, auto_sync: bool, dry_run: bool, use_logger: bool, echo: bool, summary_only: bool
    ) -> "list[str]":
        """Return applied versions after optional reconciliation, writing nothing in a dry run.

        A dry run neither creates the tracking table nor reconciles records; it reports the reconciliation it would
        perform and returns the applied versions as they would look afterwards. A partially applied squash raises
        even when ``auto_sync`` is disabled.

        Args:
            driver: Active database driver.
            auto_sync: Whether to reconcile renamed and squashed migrations.
            dry_run: Whether database writes are disabled.
            use_logger: Whether logger output is enabled.
            echo: Whether console output is enabled.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Applied version numbers.
        """
        if dry_run:
            applied_migrations = await self._read_applied_migrations(driver)
        else:
            await self.tracker.ensure_tracking_table(driver)
            applied_migrations = await self.tracker.get_applied_migrations(driver)
        all_migrations = await self.runner.get_migration_files()
        plan = self._plan_version_reconciliation(
            all_migrations, applied_migrations, await self._load_reconcilable_migrations(all_migrations)
        )
        if not auto_sync or not plan.change_count:
            return [m["version_num"] for m in applied_migrations]
        self._report_version_reconciliation(
            plan, dry_run=dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only
        )
        if dry_run:
            return list(plan.applied_versions)
        await self._apply_version_reconciliation(driver, plan)
        return [m["version_num"] for m in await self.tracker.get_applied_migrations(driver)]

    async def _apply_version_reconciliation(self, driver: Any, plan: VersionReconciliationPlan) -> None:
        """Write a reconciliation plan to the tracking table."""
        for squash in plan.squashes:
            await self.tracker.replace_with_squash(
                driver, squash.target_version, list(squash.replaced_versions), squash.description, squash.checksum
            )
        for rename in plan.renames:
            await self.tracker.update_version_record(
                driver, rename.old_version, rename.new_version, applied_versions=set(rename.claimed_versions)
            )

    def _create_squash_runner(self) -> SyncMigrationRunner:
        """Create the synchronous runner the squasher uses to load migration files.

        Squash planning only reads files, so async commands share the synchronous squasher through a dedicated
        runner bound to a fresh migration context.
        """
        context = MigrationContext.from_config(self.config)
        context.extension_config = self.extension_configs
        return SyncMigrationRunner(
            self.migrations_path,
            self._discover_extension_migrations(),
            context,
            self.extension_configs,
            runtime=self._runtime,
            description_hints=self._template_settings.description_hints,
        )

    async def _apply_single_migration(
        self,
        driver: Any,
        migration: "LoadedMigrationMetadata",
        version: str,
        use_logger: bool,
        echo: bool,
        summary_only: bool,
    ) -> int | None:
        """Apply a single migration and record it.

        Args:
            driver: Database driver instance.
            migration: Migration dictionary with version, description, checksum.
            version: Version string.
            use_logger: Whether to output to logger instead of console.
            echo: Whether to echo output to the console.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Execution time in ms on success, None on failure.
        """
        use_txn = self.runner.should_use_transaction(migration, self.config)
        try:

            async def record_version(
                exec_time: int,
                migration: "LoadedMigrationMetadata" = migration,
                version: str = version,
                use_txn: bool = use_txn,
            ) -> None:
                recorded_version = migration["version"] or version
                if isinstance(self.tracker, AsyncMigrationTracker):
                    await self.tracker.record_migration(
                        driver,
                        recorded_version,
                        migration["description"],
                        exec_time,
                        migration["checksum"],
                        commit=not use_txn,
                        replaces=migration.get("replaces"),
                    )
                else:
                    await self.tracker.record_migration(
                        driver, recorded_version, migration["description"], exec_time, migration["checksum"]
                    )

            _, execution_time = await self.runner.execute_upgrade(
                driver, migration, use_transaction=use_txn, on_success=record_version
            )
        except Exception as exc:
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_error(
                use_logger,
                echo,
                summary_only,
                "Migration %s failed%s",
                version,
                rollback_msg,
                rich_message=f"[red]✗ Failed{rollback_msg}: {exc}[/]",
            )
            self._last_command_error = exc
            return None
        else:
            _output_info(
                use_logger,
                echo,
                summary_only,
                "Applied migration %s in %dms",
                version,
                execution_time,
                rich_message=f"[green]✓ Applied in {execution_time}ms[/]",
            )
            return execution_time

    async def _revert_single_migration(
        self,
        driver: Any,
        migration: "LoadedMigrationMetadata",
        version: str,
        use_logger: bool,
        echo: bool,
        summary_only: bool,
    ) -> int | None:
        """Revert a single migration.

        Args:
            driver: Database driver instance.
            migration: Migration dictionary.
            version: Version string.
            use_logger: Whether to output to logger instead of console.
            echo: Whether to echo output to the console.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Execution time in ms on success, None on failure.
        """
        use_txn = self.runner.should_use_transaction(migration, self.config)
        reverted = False
        try:

            async def remove_version(exec_time: int, version: str = version, use_txn: bool = use_txn) -> None:
                nonlocal reverted
                reverted = True
                if isinstance(self.tracker, AsyncMigrationTracker):
                    await self.tracker.remove_migration(driver, version, commit=not use_txn)
                else:
                    await self.tracker.remove_migration(driver, version)

            _, execution_time = await self.runner.execute_downgrade(
                driver, migration, use_transaction=use_txn, on_success=remove_version
            )
        except Exception as exc:
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_error(
                use_logger,
                echo,
                summary_only,
                "Migration %s failed%s",
                version,
                rollback_msg,
                rich_message=f"[red]✗ Failed{rollback_msg}: {exc}[/]",
            )
            self._last_command_error = exc
            return None
        if not reverted:
            missing_exc = MigrationError(f"Migration {version} does not support downgrade")
            _output_error(
                use_logger,
                echo,
                summary_only,
                "Migration %s failed",
                version,
                rich_message=f"[red]✗ Failed: {missing_exc}[/]",
            )
            self._last_command_error = missing_exc
            return None
        _output_info(
            use_logger,
            echo,
            summary_only,
            "Reverted migration %s in %dms",
            version,
            execution_time,
            rich_message=f"[green]✓ Reverted in {execution_time}ms[/]",
        )
        return execution_time


@overload
def create_migration_commands(config: "SyncConfigT") -> "SyncMigrationCommands[SyncConfigT]": ...


@overload
def create_migration_commands(config: "AsyncConfigT") -> "AsyncMigrationCommands[AsyncConfigT]": ...


def create_migration_commands(
    config: "SyncConfigT | AsyncConfigT",
) -> "SyncMigrationCommands[SyncConfigT] | AsyncMigrationCommands[AsyncConfigT]":
    """Factory function to create the appropriate migration commands.

    Args:
        config: The SQLSpec configuration.

    Returns:
        Appropriate migration commands instance.
    """
    if config.is_async:
        return cast("AsyncMigrationCommands[AsyncConfigT]", AsyncMigrationCommands(cast("AsyncConfigT", config)))
    return cast("SyncMigrationCommands[SyncConfigT]", SyncMigrationCommands(cast("SyncConfigT", config)))


def _output_info(
    use_logger: bool, echo: bool, summary_only: bool, message: str, *args: Any, rich_message: str | None = None
) -> None:
    """Output an info message to logger or console."""
    if use_logger:
        if summary_only:
            return
        logger.info(message, *args)
    else:
        if not echo:
            return
        console.print(rich_message or message % args if args else message)


def _output_error(
    use_logger: bool, echo: bool, summary_only: bool, message: str, *args: Any, rich_message: str | None = None
) -> None:
    """Output an error message to logger or console."""
    if use_logger:
        logger.error(message, *args)
    else:
        if not echo:
            return
        console.print(rich_message or message % args if args else message)


def _log_command_summary(
    *,
    use_logger: bool,
    summary_only: bool,
    command: str,
    status: str,
    revision: str,
    dry_run: bool,
    pending_count: int,
    applied_count: int | None,
    reverted_count: int | None,
    duration_ms: int,
    db_system: str | None,
    bind_key: str | None,
    config_name: str,
    error: Exception | None = None,
    allow_missing: bool | None = None,
    auto_sync: bool | None = None,
) -> None:
    """Emit a single summary log entry for migration commands."""
    if not use_logger or not summary_only:
        return
    level = logging.ERROR if status == "failed" else logging.INFO
    extra_fields: dict[str, Any] = {
        "command": command,
        "status": status,
        "revision": revision,
        "dry_run": dry_run,
        "pending_count": pending_count,
        "duration_ms": duration_ms,
        "db_system": db_system,
        "bind_key": bind_key,
        "config_name": config_name,
    }
    if applied_count is not None:
        extra_fields["applied_count"] = applied_count
    if reverted_count is not None:
        extra_fields["reverted_count"] = reverted_count
    if allow_missing is not None:
        extra_fields["allow_missing"] = allow_missing
    if auto_sync is not None:
        extra_fields["auto_sync"] = auto_sync
    if error is not None:
        extra_fields["error_type"] = type(error).__name__
    log_with_context(logger, level, "migration.command.summary", **extra_fields)


def _report_no_pending_migrations(use_logger: bool, echo: bool, summary_only: bool, has_migrations: bool) -> None:
    """Report that there are no pending migrations.

    Args:
        use_logger: Whether to output to logger instead of console.
        echo: Whether to echo output to the console.
        summary_only: Whether summary-only logging is enabled.
        has_migrations: Whether any migrations exist at all.
    """
    if not has_migrations:
        _output_info(
            use_logger,
            echo,
            summary_only,
            "No migrations found. Create your first migration with 'sqlspec create-migration'.",
            rich_message="[yellow]No migrations found. Create your first migration with 'sqlspec create-migration'.[/]",
        )
    else:
        _output_info(
            use_logger,
            echo,
            summary_only,
            "Already at latest version",
            rich_message="[green]Already at latest version[/]",
        )


def _bind_arguments(signature: inspect.Signature, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    bound = signature.bind_partial(*args, **kwargs)
    arguments = dict(bound.arguments)
    arguments.pop("self", None)
    return arguments
