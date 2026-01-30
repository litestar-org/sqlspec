"""Migration command implementations for SQLSpec.

This module provides the main command interface for database migrations.
"""

import functools
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

from rich.console import Console
from rich.table import Table

from sqlspec.builder import sql
from sqlspec.migrations.base import BaseMigrationCommands
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner
from sqlspec.migrations.utils import create_migration_file
from sqlspec.migrations.validation import validate_migration_order
from sqlspec.migrations.version import generate_conversion_map, generate_timestamp_version, parse_version
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from pathlib import Path

    from sqlspec.config import AsyncConfigT, SyncConfigT

__all__ = ("AsyncMigrationCommands", "SyncMigrationCommands", "create_migration_commands")

logger = get_logger("sqlspec.migrations.commands")
console = Console()
P = ParamSpec("P")
R = TypeVar("R")


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


def _output_warning(
    use_logger: bool, echo: bool, summary_only: bool, message: str, *args: Any, rich_message: str | None = None
) -> None:
    """Output a warning message to logger or console."""
    if use_logger:
        logger.warning(message, *args)
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


def _output_exception(
    use_logger: bool, echo: bool, summary_only: bool, message: str, *args: Any, rich_message: str | None = None
) -> None:
    """Output an exception message to logger or console."""
    if use_logger:
        logger.exception(message, *args)
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


MetadataBuilder = Callable[[dict[str, Any]], tuple[str | None, dict[str, Any]]]


def _bind_arguments(signature: inspect.Signature, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    bound = signature.bind_partial(*args, **kwargs)
    arguments = dict(bound.arguments)
    arguments.pop("self", None)
    return arguments


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
                except Exception as exc:  # pragma: no cover - passthrough
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
            except Exception as exc:  # pragma: no cover - passthrough
                error = exc
                if runtime is not None:
                    runtime.increment_metric(f"{metric_prefix}.errors")
                    error_recorded = True
                raise
            finally:
                _finalize(self, runtime, span, start, error, error_recorded, dry_run)

        return cast("Callable[P, R]", sync_wrapper)

    return decorator


def _upgrade_metadata(args: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    revision = cast("str | None", args.get("revision"))
    metadata = {"dry_run": str(args.get("dry_run", False)).lower()}
    return revision, metadata


def _downgrade_metadata(args: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    revision = cast("str | None", args.get("revision"))
    metadata = {"dry_run": str(args.get("dry_run", False)).lower()}
    return revision, metadata


class SyncMigrationCommands(BaseMigrationCommands["SyncConfigT", Any]):
    """Synchronous migration commands."""

    def __init__(self, config: "SyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        super().__init__(config)
        self.tracker = config.migration_tracker_type(self.version_table)

        # Create context with extension configurations
        context = MigrationContext.from_config(config)
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

            applied: list[dict[str, Any]] = []
            if verbose:
                applied = self.tracker.get_applied_migrations(driver)

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

    def _load_single_migration_checksum(self, version: str, file_path: "Path") -> "tuple[str, tuple[str, Path]] | None":
        """Load checksum for a single migration.

        Args:
            version: Migration version.
            file_path: Path to migration file.

        Returns:
            Tuple of (version, (checksum, file_path)) or None if load fails.
        """
        try:
            migration = self.runner.load_migration(file_path, version)
            return (version, (migration["checksum"], file_path))
        except Exception as exc:
            log_with_context(
                logger,
                logging.DEBUG,
                "migration.list",
                db_system=resolve_db_system(type(self.config).__name__),
                version=version,
                file_path=str(file_path),
                error_type=type(exc).__name__,
                status="failed",
                operation="load_checksum",
            )
            return None

    def _load_migration_checksums(self, all_migrations: "list[tuple[str, Path]]") -> "dict[str, tuple[str, Path]]":
        """Load checksums for all migrations.

        Args:
            all_migrations: List of (version, file_path) tuples.

        Returns:
            Dictionary mapping version to (checksum, file_path) tuples.
        """
        file_checksums = {}
        for version, file_path in all_migrations:
            result = self._load_single_migration_checksum(version, file_path)
            if result:
                file_checksums[result[0]] = result[1]
        return file_checksums

    def _collect_pending_migrations(
        self, all_migrations: "list[tuple[str, Path]]", applied_set: set[str], revision: str
    ) -> "list[tuple[str, Path]]":
        """Collect pending migrations that need to be applied.

        Args:
            all_migrations: List of all (version, file_path) tuples.
            applied_set: Set of already applied version strings.
            revision: Target revision ("head" or specific version).

        Returns:
            List of (version, file_path) tuples for pending migrations.
        """
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

    def _report_no_pending_migrations(
        self, use_logger: bool, echo: bool, summary_only: bool, has_migrations: bool
    ) -> None:
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

    def _apply_single_migration(
        self, driver: Any, migration: "dict[str, Any]", version: str, use_logger: bool, echo: bool, summary_only: bool
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
        try:

            def record_version(exec_time: int, migration: "dict[str, Any]" = migration) -> None:
                self.tracker.record_migration(
                    driver, migration["version"], migration["description"], exec_time, migration["checksum"]
                )

            _, execution_time = self.runner.execute_upgrade(driver, migration, on_success=record_version)
        except Exception as exc:
            use_txn = self.runner.should_use_transaction(migration, self.config)
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_exception(
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

    def _collect_revert_migrations(self, applied: "list[dict[str, Any]]", revision: str) -> "list[dict[str, Any]]":
        """Collect migrations to revert based on target revision.

        Args:
            applied: List of applied migration records.
            revision: Target revision ("-1", "base", or specific version).

        Returns:
            List of migration records to revert.
        """
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

    def _revert_single_migration(
        self, driver: Any, migration: "dict[str, Any]", version: str, use_logger: bool, echo: bool, summary_only: bool
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
        try:

            def remove_version(exec_time: int, version: str = version) -> None:
                self.tracker.remove_migration(driver, version)

            _, execution_time = self.runner.execute_downgrade(driver, migration, on_success=remove_version)
        except Exception as exc:
            use_txn = self.runner.should_use_transaction(migration, self.config)
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_exception(
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
                "Reverted migration %s in %dms",
                version,
                execution_time,
                rich_message=f"[green]✓ Reverted in {execution_time}ms[/]",
            )
            return execution_time

    def _synchronize_version_records(
        self, driver: Any, *, use_logger: bool = False, echo: bool = True, summary_only: bool = False
    ) -> int:
        """Synchronize database version records with migration files.

        Auto-updates DB tracking when migrations have been renamed by fix command.
        This allows developers to just run upgrade after pulling changes without
        manually running fix.

        Validates checksums match before updating to prevent incorrect matches.

        Args:
            driver: Database driver instance.
            use_logger: If True, output to logger instead of Rich console.
            echo: Whether to echo output to the console.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Number of version records updated.
        """
        all_migrations = self.runner.get_migration_files()

        try:
            applied_migrations = self.tracker.get_applied_migrations(driver)
        except Exception as exc:
            log_with_context(
                logger,
                logging.DEBUG,
                "migration.list",
                db_system=resolve_db_system(type(driver).__name__),
                error_type=type(exc).__name__,
                status="failed",
                operation="applied_fetch",
            )
            return 0

        applied_map = {m["version_num"]: m for m in applied_migrations}

        conversion_map = generate_conversion_map(all_migrations)

        updated_count = 0
        if conversion_map:
            for old_version, new_version in conversion_map.items():
                if old_version in applied_map and new_version not in applied_map:
                    applied_checksum = applied_map[old_version]["checksum"]

                    file_path = next((path for v, path in all_migrations if v == new_version), None)
                    if file_path:
                        migration = self.runner.load_migration(file_path, new_version)
                        if migration["checksum"] == applied_checksum:
                            self.tracker.update_version_record(driver, old_version, new_version)
                            if use_logger:
                                if not summary_only:
                                    logger.info("Reconciled version: %s -> %s", old_version, new_version)
                            elif echo:
                                console.print(f"  [dim]Reconciled version:[/] {old_version} → {new_version}")
                            updated_count += 1
                        elif use_logger:
                            logger.warning(
                                "Checksum mismatch for %s -> %s, skipping auto-sync", old_version, new_version
                            )
                        elif echo:
                            console.print(
                                f"  [yellow]Warning: Checksum mismatch for {old_version} → {new_version}, skipping auto-sync[/]"
                            )
        else:
            file_checksums = self._load_migration_checksums(all_migrations)

            for applied_version, applied_record in applied_map.items():
                for file_version, (file_checksum, _) in file_checksums.items():
                    if file_version not in applied_map and applied_record["checksum"] == file_checksum:
                        self.tracker.update_version_record(driver, applied_version, file_version)
                        if use_logger:
                            if not summary_only:
                                logger.info("Reconciled version: %s -> %s", applied_version, file_version)
                        elif echo:
                            console.print(f"  [dim]Reconciled version:[/] {applied_version} → {file_version}")
                        updated_count += 1
                        break

        if updated_count > 0:
            if use_logger:
                if not summary_only:
                    logger.info("Reconciled %d version record(s)", updated_count)
            elif echo:
                console.print(f"[cyan]Reconciled {updated_count} version record(s)[/]")

        return updated_count

    @_with_command_span("upgrade", metadata_fn=_upgrade_metadata)
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
            auto_sync: If True, automatically reconcile renamed migrations in database.
                Defaults to True. Can be disabled via --no-auto-sync flag.
            dry_run: If True, show what would be done without making changes.
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
                self.tracker.ensure_tracking_table(driver)

                if auto_sync and self.config.migration_config.get("auto_sync", True):
                    self._synchronize_version_records(
                        driver, use_logger=ul, echo=echo_value, summary_only=summary_value
                    )

                applied_migrations = self.tracker.get_applied_migrations(driver)
                applied_versions = [m["version_num"] for m in applied_migrations]
                applied_set = set(applied_versions)

                all_migrations = self.runner.get_migration_files()
                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.available", float(len(all_migrations)))

                pending = self._collect_pending_migrations(all_migrations, applied_set, revision)
                pending_count = len(pending)

                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.pending", float(len(pending)))

                if not pending:
                    self._report_no_pending_migrations(ul, echo_value, summary_value, bool(all_migrations))
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

        except Exception as exc:  # pragma: no cover - passthrough
            error = exc
            raise
        finally:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="upgrade",
                status="failed" if error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=applied_count,
                reverted_count=None,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=error,
                allow_missing=allow_missing,
                auto_sync=auto_sync,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )
        elif applied_count:
            self._record_command_metric("applied", float(applied_count))

    @_with_command_span("downgrade", metadata_fn=_downgrade_metadata)
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
                self.tracker.ensure_tracking_table(driver)
                applied = self.tracker.get_applied_migrations(driver)
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
                        continue

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

        except Exception as exc:  # pragma: no cover - passthrough
            error = exc
            raise
        finally:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="downgrade",
                status="failed" if error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=None,
                reverted_count=reverted_count,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=error,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )
        elif reverted_count:
            self._record_command_metric("applied", float(reverted_count))

    def stamp(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)
            all_migrations = dict(self.runner.get_migration_files())
            if revision not in all_migrations:
                console.print(f"[red]Unknown revision: {revision}[/]")
                return
            clear_sql = sql.delete().from_(self.tracker.version_table)
            driver.execute(clear_sql)
            self.tracker.record_migration(driver, revision, f"Stamped to {revision}", 0, "manual-stamp")
            console.print(f"[green]Database stamped at revision {revision}[/]")

    def revision(self, message: str, file_type: str | None = None) -> None:
        """Create a new migration file with timestamp-based versioning.

        Generates a unique timestamp version (YYYYMMDDHHmmss format) to avoid
        conflicts when multiple developers create migrations concurrently.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
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

    def fix(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.

        Examples:
            >>> commands.fix(dry_run=True)  # Preview only
            >>> commands.fix(yes=True)  # Auto-approve
            >>> commands.fix(update_database=False)  # Files only
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

        if not yes:
            response = input("\nProceed with conversion? [y/N]: ")
            if response.lower() != "y":
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
                    applied_migrations = self.tracker.get_applied_migrations(driver)
                    applied_versions = {m["version_num"] for m in applied_migrations}

                    updated_count = 0
                    for old_version, new_version in conversion_map.items():
                        if old_version in applied_versions:
                            self.tracker.update_version_record(driver, old_version, new_version)
                            updated_count += 1

                    if updated_count > 0:
                        console.print(
                            f"[green]✓ Updated {updated_count} version records in migration tracking table[/]"
                        )
                    else:
                        console.print("[green]✓ No applied migrations to update in tracking table[/]")

            fixer.cleanup()
            console.print("[green]✓ Conversion complete![/]")

        except Exception as e:
            console.print(f"[red]✗ Error: {e}[/]")
            fixer.rollback()
            console.print("[yellow]Restored files from backup[/]")
            raise


class AsyncMigrationCommands(BaseMigrationCommands["AsyncConfigT", Any]):
    """Asynchronous migration commands."""

    def __init__(self, config: "AsyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        super().__init__(config)
        self.tracker = config.migration_tracker_type(self.version_table)

        # Create context with extension configurations
        context = MigrationContext.from_config(config)
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
            applied: list[dict[str, Any]] = []
            if verbose:
                applied = await self.tracker.get_applied_migrations(driver)
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

    async def _load_single_migration_checksum(
        self, version: str, file_path: "Path"
    ) -> "tuple[str, tuple[str, Path]] | None":
        """Load checksum for a single migration.

        Args:
            version: Migration version.
            file_path: Path to migration file.

        Returns:
            Tuple of (version, (checksum, file_path)) or None if load fails.
        """
        try:
            migration = await self.runner.load_migration(file_path, version)
            return (version, (migration["checksum"], file_path))
        except Exception as exc:
            log_with_context(
                logger,
                logging.DEBUG,
                "migration.list",
                db_system=resolve_db_system(type(self.config).__name__),
                version=version,
                file_path=str(file_path),
                error_type=type(exc).__name__,
                status="failed",
                operation="load_checksum",
            )
            return None

    async def _load_migration_checksums(
        self, all_migrations: "list[tuple[str, Path]]"
    ) -> "dict[str, tuple[str, Path]]":
        """Load checksums for all migrations.

        Args:
            all_migrations: List of (version, file_path) tuples.

        Returns:
            Dictionary mapping version to (checksum, file_path) tuples.
        """
        file_checksums = {}
        for version, file_path in all_migrations:
            result = await self._load_single_migration_checksum(version, file_path)
            if result:
                file_checksums[result[0]] = result[1]
        return file_checksums

    def _collect_pending_migrations(
        self, all_migrations: "list[tuple[str, Path]]", applied_set: set[str], revision: str
    ) -> "list[tuple[str, Path]]":
        """Collect pending migrations that need to be applied.

        Args:
            all_migrations: List of all (version, file_path) tuples.
            applied_set: Set of already applied version strings.
            revision: Target revision ("head" or specific version).

        Returns:
            List of (version, file_path) tuples for pending migrations.
        """
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

    def _report_no_pending_migrations(
        self, use_logger: bool, echo: bool, summary_only: bool, has_migrations: bool
    ) -> None:
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

    async def _apply_single_migration(
        self, driver: Any, migration: "dict[str, Any]", version: str, use_logger: bool, echo: bool, summary_only: bool
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
        try:

            async def record_version(exec_time: int, migration: "dict[str, Any]" = migration) -> None:
                await self.tracker.record_migration(
                    driver, migration["version"], migration["description"], exec_time, migration["checksum"]
                )

            _, execution_time = await self.runner.execute_upgrade(driver, migration, on_success=record_version)
        except Exception as exc:
            use_txn = self.runner.should_use_transaction(migration, self.config)
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_exception(
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

    def _collect_revert_migrations(self, applied: "list[dict[str, Any]]", revision: str) -> "list[dict[str, Any]]":
        """Collect migrations to revert based on target revision.

        Args:
            applied: List of applied migration records.
            revision: Target revision ("-1", "base", or specific version).

        Returns:
            List of migration records to revert.
        """
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

    async def _revert_single_migration(
        self, driver: Any, migration: "dict[str, Any]", version: str, use_logger: bool, echo: bool, summary_only: bool
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
        try:

            async def remove_version(exec_time: int, version: str = version) -> None:
                await self.tracker.remove_migration(driver, version)

            _, execution_time = await self.runner.execute_downgrade(driver, migration, on_success=remove_version)
        except Exception as exc:
            use_txn = self.runner.should_use_transaction(migration, self.config)
            rollback_msg = " (transaction rolled back)" if use_txn else ""
            _output_exception(
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
                "Reverted migration %s in %dms",
                version,
                execution_time,
                rich_message=f"[green]✓ Reverted in {execution_time}ms[/]",
            )
            return execution_time

    async def _synchronize_version_records(
        self, driver: Any, *, use_logger: bool = False, echo: bool = True, summary_only: bool = False
    ) -> int:
        """Synchronize database version records with migration files.

        Auto-updates DB tracking when migrations have been renamed by fix command.
        This allows developers to just run upgrade after pulling changes without
        manually running fix.

        Validates checksums match before updating to prevent incorrect matches.

        Args:
            driver: Database driver instance.
            use_logger: If True, output to logger instead of Rich console.
            echo: Whether to echo output to the console.
            summary_only: Whether summary-only logging is enabled.

        Returns:
            Number of version records updated.
        """
        all_migrations = await self.runner.get_migration_files()

        try:
            applied_migrations = await self.tracker.get_applied_migrations(driver)
        except Exception as exc:
            log_with_context(
                logger,
                logging.DEBUG,
                "migration.list",
                db_system=resolve_db_system(type(driver).__name__),
                error_type=type(exc).__name__,
                status="failed",
                operation="applied_fetch",
            )
            return 0

        applied_map = {m["version_num"]: m for m in applied_migrations}

        conversion_map = generate_conversion_map(all_migrations)

        updated_count = 0
        if conversion_map:
            for old_version, new_version in conversion_map.items():
                if old_version in applied_map and new_version not in applied_map:
                    applied_checksum = applied_map[old_version]["checksum"]

                    file_path = next((path for v, path in all_migrations if v == new_version), None)
                    if file_path:
                        migration = await self.runner.load_migration(file_path, new_version)
                        if migration["checksum"] == applied_checksum:
                            await self.tracker.update_version_record(driver, old_version, new_version)
                            if use_logger:
                                if not summary_only:
                                    logger.info("Reconciled version: %s -> %s", old_version, new_version)
                            elif echo:
                                console.print(f"  [dim]Reconciled version:[/] {old_version} → {new_version}")
                            updated_count += 1
                        elif use_logger:
                            logger.warning(
                                "Checksum mismatch for %s -> %s, skipping auto-sync", old_version, new_version
                            )
                        elif echo:
                            console.print(
                                f"  [yellow]Warning: Checksum mismatch for {old_version} → {new_version}, skipping auto-sync[/]"
                            )
        else:
            file_checksums = await self._load_migration_checksums(all_migrations)

            for applied_version, applied_record in applied_map.items():
                for file_version, (file_checksum, _) in file_checksums.items():
                    if file_version not in applied_map and applied_record["checksum"] == file_checksum:
                        await self.tracker.update_version_record(driver, applied_version, file_version)
                        if use_logger:
                            if not summary_only:
                                logger.info("Reconciled version: %s -> %s", applied_version, file_version)
                        elif echo:
                            console.print(f"  [dim]Reconciled version:[/] {applied_version} → {file_version}")
                        updated_count += 1
                        break

        if updated_count > 0:
            if use_logger:
                if not summary_only:
                    logger.info("Reconciled %d version record(s)", updated_count)
            elif echo:
                console.print(f"[cyan]Reconciled {updated_count} version record(s)[/]")

        return updated_count

    @_with_command_span("upgrade", metadata_fn=_upgrade_metadata)
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
            auto_sync: If True, automatically reconcile renamed migrations in database.
                Defaults to True. Can be disabled via --no-auto-sync flag.
            dry_run: If True, show what would be done without making changes.
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
                await self.tracker.ensure_tracking_table(driver)

                if auto_sync and self.config.migration_config.get("auto_sync", True):
                    await self._synchronize_version_records(
                        driver, use_logger=ul, echo=echo_value, summary_only=summary_value
                    )

                applied_migrations = await self.tracker.get_applied_migrations(driver)
                applied_versions = [m["version_num"] for m in applied_migrations]
                applied_set = set(applied_versions)

                all_migrations = await self.runner.get_migration_files()
                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.available", float(len(all_migrations)))

                pending = self._collect_pending_migrations(all_migrations, applied_set, revision)
                pending_count = len(pending)

                if runtime is not None:
                    runtime.increment_metric("migrations.command.upgrade.pending", float(len(pending)))

                if not pending:
                    self._report_no_pending_migrations(ul, echo_value, summary_value, bool(all_migrations))
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

        except Exception as exc:  # pragma: no cover - passthrough
            error = exc
            raise
        finally:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="upgrade",
                status="failed" if error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=applied_count,
                reverted_count=None,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=error,
                allow_missing=allow_missing,
                auto_sync=auto_sync,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )
        elif applied_count:
            self._record_command_metric("applied", float(applied_count))

    @_with_command_span("downgrade", metadata_fn=_downgrade_metadata)
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
                await self.tracker.ensure_tracking_table(driver)

                applied = await self.tracker.get_applied_migrations(driver)
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
                        continue

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

        except Exception as exc:  # pragma: no cover - passthrough
            error = exc
            raise
        finally:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            _log_command_summary(
                use_logger=ul,
                summary_only=summary_value,
                command="downgrade",
                status="failed" if error else "complete",
                revision=revision,
                dry_run=dry_run,
                pending_count=pending_count,
                applied_count=None,
                reverted_count=reverted_count,
                duration_ms=duration_ms,
                db_system=db_system,
                bind_key=getattr(self.config, "bind_key", None),
                config_name=type(self.config).__name__,
                error=error,
            )

        if dry_run:
            output_info(
                "Dry run complete. No changes were made to the database.",
                rich_message="\n[bold yellow]Dry run complete.[/] No changes were made to the database.",
            )
        elif reverted_count:
            self._record_command_metric("applied", float(reverted_count))

    async def stamp(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            all_migrations = dict(await self.runner.get_migration_files())
            if revision not in all_migrations:
                console.print(f"[red]Unknown revision: {revision}[/]")
                return

            clear_sql = sql.delete().from_(self.tracker.version_table)
            await driver.execute(clear_sql)
            await self.tracker.record_migration(driver, revision, f"Stamped to {revision}", 0, "manual-stamp")
            console.print(f"[green]Database stamped at revision {revision}[/]")

    async def revision(self, message: str, file_type: str | None = None) -> None:
        """Create a new migration file with timestamp-based versioning.

        Generates a unique timestamp version (YYYYMMDDHHmmss format) to avoid
        conflicts when multiple developers create migrations concurrently.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
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

    async def fix(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.

        Examples:
            >>> await commands.fix(dry_run=True)  # Preview only
            >>> await commands.fix(yes=True)  # Auto-approve
            >>> await commands.fix(update_database=False)  # Files only
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

        if not yes:
            response = input("\nProceed with conversion? [y/N]: ")
            if response.lower() != "y":
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
                    applied_migrations = await self.tracker.get_applied_migrations(driver)
                    applied_versions = {m["version_num"] for m in applied_migrations}

                    updated_count = 0
                    for old_version, new_version in conversion_map.items():
                        if old_version in applied_versions:
                            await self.tracker.update_version_record(driver, old_version, new_version)
                            updated_count += 1

                    if updated_count > 0:
                        console.print(
                            f"[green]✓ Updated {updated_count} version records in migration tracking table[/]"
                        )
                    else:
                        console.print("[green]✓ No applied migrations to update in tracking table[/]")

            fixer.cleanup()
            console.print("[green]✓ Conversion complete![/]")

        except Exception as e:
            console.print(f"[red]✗ Error: {e}[/]")
            fixer.rollback()
            console.print("[yellow]Restored files from backup[/]")
            raise


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
