# ruff: noqa: C901
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import rich_click as click
from rich import get_console
from rich.markup import escape
from rich.prompt import Confirm, Prompt
from rich.table import Table

from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.migrations.squash import parse_version_range
from sqlspec.utils.config_tools import discover_config_from_pyproject, resolve_config_sync
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.sync_tools import run_

if TYPE_CHECKING:
    from rich_click import Group

    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

    cli: Group

__all__ = ("add_migration_commands", "cli", "get_sqlspec_group")

AnyDatabaseConfig: TypeAlias = (
    AsyncDatabaseConfig[Any, Any, Any]
    | SyncDatabaseConfig[Any, Any, Any]
    | NoPoolAsyncConfig[Any, Any]
    | NoPoolSyncConfig[Any, Any]
)
logger = get_logger("cli")


def get_sqlspec_group() -> "Group":
    """Get the SQLSpec CLI group.

    Returns:
        The SQLSpec CLI group.
    """

    @click.group(name="sqlspec")
    @click.option(
        "--config",
        help="Dotted path to SQLSpec config(s) or callable function (env: SQLSPEC_CONFIG)",
        required=False,
        default=None,
        type=str,
        envvar="SQLSPEC_CONFIG",
    )
    @click.option(
        "--validate-config", is_flag=True, default=False, help="Validate configuration before executing migrations"
    )
    @click.pass_context
    def sqlspec_group(ctx: "click.Context", config: str | None, validate_config: bool) -> None:
        """SQLSpec CLI commands.

        Configuration resolution prefers CLI flag, SQLSPEC_CONFIG env var, and finally the [tool.sqlspec] section.
        Comma-separated paths are split, deduplicated by bind key, and loaded from the current working directory so local modules can be imported.
        When --validate-config is used we report each config's async capability.
        """
        console = get_console()
        ctx.ensure_object(dict)

        if config is None:
            config = discover_config_from_pyproject()
            if config:
                console.print("[dim]Using config from pyproject.toml[/]")

        if config is None:
            console.print("[red]Error: No SQLSpec config found.[/]")
            console.print("\nSpecify config using one of:")
            console.print("  1. CLI flag:        sqlspec --config myapp.config:get_configs <command>")
            console.print("  2. Environment var: export SQLSPEC_CONFIG=myapp.config:get_configs")
            console.print(f"  3. pyproject.toml:  {escape('[tool.sqlspec]')}")
            console.print('                      config = "myapp.config:get_configs"')
            ctx.exit(1)

        cwd = str(Path.cwd())
        cwd_added = False
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
            cwd_added = True

        try:
            all_configs: list[AnyDatabaseConfig] = []
            for config_path in config.split(","):
                config_path = config_path.strip()
                if not config_path:
                    continue
                config_result = resolve_config_sync(config_path)
                if isinstance(config_result, Sequence) and not isinstance(config_result, str):
                    all_configs.extend(config_result)
                else:
                    all_configs.append(config_result)  # pyright: ignore

            configs_list: list[AnyDatabaseConfig] = []
            seen_bind_keys: dict[str, int] = {}
            for cfg in all_configs:
                if cfg.bind_key is not None and cfg.bind_key in seen_bind_keys:
                    configs_list[seen_bind_keys[cfg.bind_key]] = cfg
                else:
                    if cfg.bind_key is not None:
                        seen_bind_keys[cfg.bind_key] = len(configs_list)
                    configs_list.append(cfg)

            ctx.obj["configs"] = configs_list

            if not ctx.obj["configs"]:
                console.print("[red]Error: No valid configs found after resolution.[/]")
                console.print("\nEnsure your config path returns valid config instance(s).")
                ctx.exit(1)

            ctx.obj["validate_config"] = validate_config

            if validate_config:
                console.print(f"[green]✓[/] Successfully loaded {len(ctx.obj['configs'])} config(s)")
                for i, cfg in enumerate(ctx.obj["configs"]):
                    config_name = cfg.bind_key or f"config-{i}"
                    config_type = type(cfg).__name__
                    is_async = cfg.is_async
                    execution_hint = "[dim cyan](async-capable)[/]" if is_async else "[dim](sync)[/]"
                    console.print(f"  [dim]•[/] {config_name}: {config_type} {execution_hint}")

        except (ImportError, SQLSpecError) as e:
            console.print(f"[red]Error loading config: {escape(str(e))}[/]", emoji=False)
            ctx.exit(1)
        finally:
            if cwd_added and cwd in sys.path and sys.path[0] == cwd:
                sys.path.remove(cwd)

    return sqlspec_group


def add_migration_commands(database_group: "Group | None" = None) -> "Group":
    """Add migration commands to the database group.

    Args:
        database_group: The database group to add the commands to.

    Returns:
        The database group with the migration commands added.
    """
    console = get_console()

    if database_group is None:
        database_group = get_sqlspec_group()

    bind_key_option = click.option(
        "--bind-key", help="Specify which SQLSpec config to use by bind key", type=str, default=None
    )
    verbose_option = click.option("--verbose", help="Enable verbose output.", type=bool, default=False, is_flag=True)
    no_prompt_option = click.option(
        "--no-prompt",
        help="Do not prompt for confirmation before executing the command.",
        type=bool,
        default=False,
        required=False,
        show_default=True,
        is_flag=True,
    )
    include_option = click.option(
        "--include", multiple=True, help="Include only specific configurations (can be used multiple times)"
    )
    exclude_option = click.option(
        "--exclude", multiple=True, help="Exclude specific configurations (can be used multiple times)"
    )
    dry_run_option = click.option(
        "--dry-run", is_flag=True, default=False, help="Show what would be executed without making changes"
    )
    use_logger_option = click.option(
        "--use-logger",
        is_flag=True,
        default=False,
        help="Emit migration output via structured logger instead of console output",
    )
    no_echo_option = click.option(
        "--no-echo", is_flag=True, default=False, help="Disable console output for migration commands"
    )
    summary_only_option = click.option(
        "--summary",
        "summary_only",
        is_flag=True,
        default=False,
        help="Emit a single summary log entry when logger output is enabled",
    )
    no_auto_sync_option = click.option(
        "--no-auto-sync",
        is_flag=True,
        default=False,
        help="Disable automatic version reconciliation when migrations have been renamed",
    )

    def get_config_by_bind_key(ctx: "click.Context", bind_key: str | None) -> "AnyDatabaseConfig":
        """Get the SQLSpec config for the specified bind key.

        Args:
            ctx: The click context.
            bind_key: The bind key to get the config for.

        Returns:
            The SQLSpec config for the specified bind key.
        """
        configs = ctx.obj["configs"]
        if bind_key is None:
            config = configs[0]
        else:
            config = None
            for cfg in configs:
                config_name = cfg.bind_key
                if config_name == bind_key:
                    config = cfg
                    break

            if config is None:
                console.print(f"[red]No config found for bind key: {bind_key}[/]")
                sys.exit(1)

        return cast("AnyDatabaseConfig", config)

    def _adk_configs(ctx: "click.Context", bind_key: str | None) -> "list[AnyDatabaseConfig]":
        if bind_key is not None:
            return [get_config_by_bind_key(ctx, bind_key)]

        configs = ctx.obj["configs"]
        return [cfg for cfg in configs if "adk" in cfg.extension_config]

    def _get_memory_store_class(
        config: "AnyDatabaseConfig",
    ) -> "type[BaseAsyncADKMemoryStore[Any] | BaseSyncADKMemoryStore[Any]] | None":
        config_module = type(config).__module__
        config_name = type(config).__name__

        if not config_module.startswith("sqlspec.adapters."):
            return None

        adapter_name = config_module.split(".")[2]
        store_class_name = config_name.replace("Config", "ADKMemoryStore")
        store_path = f"sqlspec.adapters.{adapter_name}.adk.store.{store_class_name}"

        try:
            return cast("type[BaseAsyncADKMemoryStore[Any] | BaseSyncADKMemoryStore[Any]]", import_string(store_path))
        except ImportError:
            return None

    def _is_adk_memory_enabled(config: "AnyDatabaseConfig") -> bool:
        adk_config = cast("dict[str, Any]", config.extension_config.get("adk", {}))
        return bool(adk_config.get("enable_memory", True))

    async def _cleanup_memory_entries_async(store: "BaseAsyncADKMemoryStore[Any]", days: int) -> int:
        return await store.delete_entries_older_than(days)

    async def _verify_memory_table_async(
        config: "AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]", sql: str
    ) -> None:
        async with config.provide_session() as driver:
            await driver.execute(sql)

    def get_configs_with_migrations(
        ctx: "click.Context", enabled_only: bool = False
    ) -> "list[tuple[str, AnyDatabaseConfig]]":
        """Get all configurations that have migrations enabled.

        Args:
            ctx: The click context.
            enabled_only: If True, only return configs with enabled=True.

        Returns:
            List of tuples (config_name, config) for configs with migrations enabled.
        """
        configs: list[AnyDatabaseConfig] = ctx.obj["configs"]
        migration_configs: list[tuple[str, AnyDatabaseConfig]] = []

        for config in configs:
            migration_config = config.migration_config
            if migration_config:
                enabled = migration_config.get("enabled", True)
                if not enabled_only or enabled:
                    config_name = config.bind_key or str(type(config).__name__)
                    migration_configs.append((config_name, config))

        return migration_configs

    def filter_configs(
        configs: "list[tuple[str, Any]]", include: "tuple[str, ...]", exclude: "tuple[str, ...]"
    ) -> "list[tuple[str, Any]]":
        """Filter configuration list based on include/exclude criteria.

        Args:
            configs: List of (config_name, config) tuples.
            include: Config names to include (empty means include all).
            exclude: Config names to exclude.

        Returns:
            Filtered list of configurations.
        """
        filtered = configs
        if include:
            filtered = [(name, config) for name, config in filtered if name in include]
        if exclude:
            filtered = [(name, config) for name, config in filtered if name not in exclude]
        return filtered

    def _execute_for_config(
        config: "AnyDatabaseConfig", sync_fn: "Callable[[], Any]", async_fn: "Callable[[], Any]"
    ) -> Any:
        """Execute a migration command with appropriate sync/async handling.

        For sync configs, executes the sync function directly without an event loop.
        For async configs, wraps the async function in run_() to execute with event loop.

        Args:
            config: The database configuration.
            sync_fn: Function to call for sync configs (should call sync migration methods).
            async_fn: Async function to call for async configs (should await async migration methods).

        Returns:
            The result of the executed function.
        """
        if config.is_async:
            return run_(async_fn)()
        return sync_fn()

    def _dispatch_migration_call(config: Any, method_name: str, **kwargs: Any) -> Any:
        """Run a migration command method with sync/async dispatch for one config.

        SQLSpec errors become ``click.ClickException`` so the CLI exits non-zero with the message instead of a
        traceback.

        Args:
            config: The database configuration.
            method_name: Name of the migration command method to invoke.
            **kwargs: Arguments forwarded to the command method.

        Returns:
            The result of the command method.

        Raises:
            click.ClickException: When the command raises a SQLSpec error.
        """
        from sqlspec.migrations.commands import create_migration_commands

        migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(
            config=config
        )
        method = getattr(migration_commands, method_name)

        def _sync_call() -> Any:
            return method(**kwargs)

        async def _async_call() -> Any:
            return await method(**kwargs)

        try:
            return _execute_for_config(config, _sync_call, _async_call)
        except SQLSpecError as exc:
            raise click.ClickException(str(exc)) from exc

    async def _dispatch_migration_call_async(config: Any, method_name: str, **kwargs: Any) -> Any:
        """Await a migration command method inside an already-running event loop.

        Args:
            config: The async database configuration.
            method_name: Name of the migration command method to invoke.
            **kwargs: Arguments forwarded to the command method.

        Returns:
            The result of the command method.

        Raises:
            click.ClickException: When the command raises a SQLSpec error.
        """
        from sqlspec.migrations.commands import create_migration_commands

        migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(
            config=config
        )
        try:
            return await getattr(migration_commands, method_name)(**kwargs)
        except SQLSpecError as exc:
            raise click.ClickException(str(exc)) from exc

    def _partition_configs_by_async(
        configs: "list[tuple[str, Any]]",
    ) -> "tuple[list[tuple[str, Any]], list[tuple[str, Any]]]":
        """Partition configs into sync and async groups.

        Args:
            configs: List of (config_name, config) tuples.

        Returns:
            Tuple of (sync_configs, async_configs).
        """
        sync_configs = [(name, cfg) for name, cfg in configs if not cfg.is_async]
        async_configs = [(name, cfg) for name, cfg in configs if cfg.is_async]
        return sync_configs, async_configs

    def _run_multi_config_operation(
        configs_to_process: "list[tuple[str, Any]]",
        *,
        sync_apply: "Callable[[Any], None]",
        async_apply: "Callable[[Any], Any]",
        should_echo: "Callable[[Any], bool]",
        progress_message: "Callable[[str], str] | None",
        success_message: "Callable[[str], str] | None",
        failure_message: "Callable[[str, Exception], str]",
    ) -> None:
        """Dispatch a migration operation across multiple configs.

        Sync configs execute inline; async configs are batched into a single
        ``run_`` call so they share one event loop.

        Args:
            configs_to_process: List of (config_name, config) tuples.
            sync_apply: Callable executing the operation for one sync config.
            async_apply: Coroutine function executing the operation for one async config.
            should_echo: Per-config predicate gating console output.
            progress_message: Builds the per-config progress line, or None to skip.
            success_message: Builds the per-config success line, or None to skip.
            failure_message: Builds the per-config failure line.
        """
        sync_configs, async_configs = _partition_configs_by_async(configs_to_process)
        errors: list[tuple[str, Exception]] = []

        for config_name, config in sync_configs:
            if progress_message is not None and should_echo(config):
                console.print(progress_message(config_name))
            try:
                sync_apply(config)
                if success_message is not None and should_echo(config):
                    console.print(success_message(config_name))
            except Exception as e:
                errors.append((config_name, e))
                if should_echo(config):
                    console.print(failure_message(config_name, e))

        if async_configs:

            async def _run_async_configs() -> None:
                for config_name, config in async_configs:
                    if progress_message is not None and should_echo(config):
                        console.print(progress_message(config_name))
                    try:
                        await async_apply(config)
                        if success_message is not None and should_echo(config):
                            console.print(success_message(config_name))
                    except Exception as e:
                        errors.append((config_name, e))
                        if should_echo(config):
                            console.print(failure_message(config_name, e))

            run_(_run_async_configs)()

        if errors:
            failed = ", ".join(name for name, _ in errors)
            msg = f"Operation failed for config(s): {failed}"
            raise click.ClickException(msg)

    def process_multiple_configs(
        ctx: "click.Context",
        bind_key: str | None,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
        operation_name: str,
        echo: bool = True,
    ) -> "list[tuple[str, Any]] | None":
        """Process configuration selection for multi-config operations.

        Requests targeting a single bind key or with only one available config run in single-config mode.
        Enabled configs are used unless include/exclude widen the set, and dry runs simply list the configs that would execute.

        Args:
            ctx: Click context.
            bind_key: Specific bind key to target.
            include: Config names to include.
            exclude: Config names to exclude.
            dry_run: Whether this is a dry run.
            operation_name: Name of the operation for display.
            echo: Whether to echo output to the console.

        Returns:
            List of (config_name, config) tuples to process, or None for single config mode.
        """
        if bind_key and not include and not exclude:
            return None

        enabled_only = not include and not exclude
        migration_configs = get_configs_with_migrations(ctx, enabled_only=enabled_only)

        if len(migration_configs) <= 1 and not include and not exclude:
            return None

        configs_to_process = filter_configs(migration_configs, include, exclude)

        if not configs_to_process:
            if echo:
                console.print("[yellow]No configurations match the specified criteria.[/]")
            return []

        if dry_run:
            if echo:
                console.print(f"[blue]Dry run: Would {operation_name} {len(configs_to_process)} configuration(s)[/]")
                for config_name, _ in configs_to_process:
                    console.print(f"  • {config_name}")
            return []

        return configs_to_process

    @database_group.command(name="show-current-revision", help="Shows the current revision for the database.")
    @bind_key_option
    @verbose_option
    @include_option
    @exclude_option
    def show_database_revision(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None, verbose: bool, include: "tuple[str, ...]", exclude: "tuple[str, ...]"
    ) -> None:
        """Show current database revision.

        Supports multi-config execution by partitioning selected configs into sync and async groups before dispatching them separately.
        """
        ctx = _ensure_click_context()

        def _show_for_config(config: Any) -> None:
            """Show current revision for a single config with sync/async dispatch."""
            _dispatch_migration_call(config, "current", verbose=verbose)

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=False, operation_name="show current revision"
        )

        async def _async_show(config: Any) -> None:
            await _dispatch_migration_call_async(config, "current", verbose=verbose)

        if configs_to_process is not None:
            if not configs_to_process:
                return

            console.rule("[yellow]Listing current revisions for all configurations[/]", align="left")

            _run_multi_config_operation(
                configs_to_process,
                sync_apply=_show_for_config,
                async_apply=_async_show,
                should_echo=lambda _config: True,
                progress_message=lambda name: f"\n[blue]Configuration: {name}[/]",
                success_message=None,
                failure_message=lambda name, e: f"[red]✗ Failed to get current revision for {name}: {e}[/]",
            )
        else:
            console.rule("[yellow]Listing current revision[/]", align="left")
            sqlspec_config = get_config_by_bind_key(ctx, bind_key)
            _show_for_config(sqlspec_config)

    def _create_echo_settings(
        no_echo: bool, summary_only: bool, use_logger: bool
    ) -> tuple[bool | None, bool | None, bool, Callable[[Any], bool]]:
        """Create echo and summary settings with echo predicate closure.

        Args:
            no_echo: Flag indicating console output should be suppressed.
            summary_only: Flag indicating only summary logs should be emitted.
            use_logger: Flag indicating logger is used rather than console.

        Returns:
            Tuple of (echo_setting, summary_setting, echo_enabled, should_echo_predicate).
        """
        effective_no_echo = no_echo or summary_only
        echo_setting = False if effective_no_echo else None
        summary_setting = True if summary_only else None
        echo_enabled = not effective_no_echo and not use_logger

        def _should_echo(config: Any) -> bool:
            if not echo_enabled:
                return False
            migration_config = cast("dict[str, Any]", getattr(config, "migration_config", None)) or {}
            return not bool(migration_config.get("use_logger", False))

        return echo_setting, summary_setting, echo_enabled, _should_echo

    @database_group.command(name="downgrade", help="Downgrade database to a specific revision.")
    @bind_key_option
    @no_prompt_option
    @include_option
    @exclude_option
    @dry_run_option
    @use_logger_option
    @no_echo_option
    @summary_only_option
    @click.argument("revision", type=str, default="-1")
    def downgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None,
        revision: str,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
        use_logger: bool,
        no_echo: bool,
        summary_only: bool,
    ) -> None:
        """Downgrade the database to the latest revision."""
        ctx = _ensure_click_context()
        echo_setting, summary_setting, echo_enabled, _should_echo = _create_echo_settings(
            no_echo, summary_only, use_logger
        )

        def _downgrade_for_config(config: Any) -> None:
            """Downgrade a single config with sync/async dispatch."""
            _dispatch_migration_call(
                config,
                "downgrade",
                revision=revision,
                dry_run=dry_run,
                use_logger=use_logger,
                echo=echo_setting,
                summary_only=summary_setting,
            )

        configs_to_process = process_multiple_configs(
            ctx,
            bind_key,
            include,
            exclude,
            dry_run=dry_run,
            operation_name=f"downgrade to {revision}",
            echo=echo_enabled,
        )

        async def _async_downgrade(config: Any) -> None:
            await _dispatch_migration_call_async(
                config,
                "downgrade",
                revision=revision,
                dry_run=dry_run,
                use_logger=use_logger,
                echo=echo_setting,
                summary_only=summary_setting,
            )

        if configs_to_process is not None:
            if not configs_to_process:
                return

            if not no_prompt and not Confirm.ask(
                f"[bold]Are you sure you want to downgrade {len(configs_to_process)} configuration(s) to revision {revision}?[/]"
            ):
                if echo_enabled:
                    console.print("[yellow]Operation cancelled.[/]")
                return

            if echo_enabled:
                console.rule("[yellow]Starting multi-configuration downgrade process[/]", align="left")

            _run_multi_config_operation(
                configs_to_process,
                sync_apply=_downgrade_for_config,
                async_apply=_async_downgrade,
                should_echo=_should_echo,
                progress_message=lambda name: f"[blue]Downgrading configuration: {name}[/]",
                success_message=lambda name: f"[green]✓ Successfully downgraded: {name}[/]",
                failure_message=lambda name, e: f"[red]✗ Failed to downgrade {name}: {e}[/]",
            )
        else:
            if echo_enabled:
                console.rule("[yellow]Starting database downgrade process[/]", align="left")
            input_confirmed = (
                True
                if no_prompt
                else Confirm.ask(f"Are you sure you want to downgrade the database to the `{revision}` revision?")
            )
            if input_confirmed:
                sqlspec_config = get_config_by_bind_key(ctx, bind_key)
                _downgrade_for_config(sqlspec_config)

    @database_group.command(name="upgrade", help="Upgrade database to a specific revision.")
    @bind_key_option
    @no_prompt_option
    @include_option
    @exclude_option
    @dry_run_option
    @use_logger_option
    @no_echo_option
    @summary_only_option
    @no_auto_sync_option
    @click.option(
        "--allow-missing",
        is_flag=True,
        default=False,
        help="Allow out-of-order migrations even when strict ordering is enabled.",
    )
    @click.argument("revision", type=str, default="head")
    def upgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None,
        revision: str,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
        use_logger: bool,
        no_echo: bool,
        summary_only: bool,
        no_auto_sync: bool,
        allow_missing: bool,
    ) -> None:
        """Upgrade the database to the latest revision."""
        ctx = _ensure_click_context()
        echo_setting, summary_setting, echo_enabled, _should_echo = _create_echo_settings(
            no_echo, summary_only, use_logger
        )
        upgrade_kwargs: dict[str, Any] = {
            "revision": revision,
            "allow_missing": allow_missing,
            "auto_sync": not no_auto_sync,
            "dry_run": dry_run,
            "use_logger": use_logger,
            "echo": echo_setting,
            "summary_only": summary_setting,
        }

        def _upgrade_for_config(config: Any) -> None:
            """Upgrade a single config with sync/async dispatch."""
            _dispatch_migration_call(config, "upgrade", **upgrade_kwargs)

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run, operation_name=f"upgrade to {revision}", echo=echo_enabled
        )

        async def _async_upgrade(config: Any) -> None:
            await _dispatch_migration_call_async(config, "upgrade", **upgrade_kwargs)

        if configs_to_process is not None:
            if not configs_to_process:
                return

            if not no_prompt and not Confirm.ask(
                f"[bold]Are you sure you want to upgrade {len(configs_to_process)} configuration(s) to revision {revision}?[/]"
            ):
                if echo_enabled:
                    console.print("[yellow]Operation cancelled.[/]")
                return

            if echo_enabled:
                console.rule("[yellow]Starting multi-configuration upgrade process[/]", align="left")

            _run_multi_config_operation(
                configs_to_process,
                sync_apply=_upgrade_for_config,
                async_apply=_async_upgrade,
                should_echo=_should_echo,
                progress_message=lambda name: f"[blue]Upgrading configuration: {name}[/]",
                success_message=lambda name: f"[green]✓ Successfully upgraded: {name}[/]",
                failure_message=lambda name, e: f"[red]✗ Failed to upgrade {name}: {e}[/]",
            )
        else:
            if echo_enabled:
                console.rule("[yellow]Starting database upgrade process[/]", align="left")
            input_confirmed = (
                True
                if no_prompt
                else Confirm.ask(f"[bold]Are you sure you want migrate the database to the `{revision}` revision?[/]")
            )
            if input_confirmed:
                sqlspec_config = get_config_by_bind_key(ctx, bind_key)
                _upgrade_for_config(sqlspec_config)

    @database_group.command(help="Stamp the revision table with the given revision")
    @click.argument("revision", type=str)
    @bind_key_option
    def stamp(bind_key: str | None, revision: str) -> None:  # pyright: ignore[reportUnusedFunction]
        """Stamp the revision table with the given revision."""
        ctx = _ensure_click_context()

        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        _dispatch_migration_call(sqlspec_config, "stamp", revision=revision)

    @database_group.command(name="init", help="Initialize migrations for the project.")
    @bind_key_option
    @click.argument("directory", default=None, required=False)
    @click.option("--package", is_flag=True, default=True, help="Create `__init__.py` for created folder")
    @no_prompt_option
    def init_sqlspec(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None, directory: str | None, package: bool, no_prompt: bool
    ) -> None:
        """Initialize the database migrations.

        Sync configs are handled inline while async configs run via a single ``run_`` call so migrations stay in sync.
        """
        from sqlspec.migrations.commands import create_migration_commands

        ctx = _ensure_click_context()

        console.rule("[yellow]Initializing database migrations.", align="left")
        input_confirmed = (
            True if no_prompt else Confirm.ask("[bold]Are you sure you want initialize migrations for the project?[/]")
        )
        if not input_confirmed:
            return

        configs = [get_config_by_bind_key(ctx, bind_key)] if bind_key is not None else ctx.obj["configs"]

        sync_configs = [cfg for cfg in configs if not cfg.is_async]
        async_configs = [cfg for cfg in configs if cfg.is_async]

        for config in sync_configs:
            migration_config_dict = config.migration_config or {}
            target_directory = (
                str(migration_config_dict.get("script_location", "migrations")) if directory is None else directory
            )
            migration_commands: SyncMigrationCommands[Any] = cast(
                "SyncMigrationCommands[Any]", create_migration_commands(config=cast("Any", config))
            )
            migration_commands.init(directory=target_directory, package=package)

        if async_configs:

            async def _init_async_configs() -> None:
                for config in async_configs:
                    migration_config_dict = config.migration_config or {}
                    target_directory = (
                        str(migration_config_dict.get("script_location", "migrations"))
                        if directory is None
                        else directory
                    )
                    migration_commands: AsyncMigrationCommands[Any] = cast(
                        "AsyncMigrationCommands[Any]", create_migration_commands(config=cast("Any", config))
                    )
                    await migration_commands.init(directory=target_directory, package=package)

            run_(_init_async_configs)()

    @_safe_group_command(
        database_group, name="create-migration", help="Create a new migration revision.", aliases=["make-migration"]
    )
    @bind_key_option
    @click.option("-m", "--message", default=None, help="Revision message")
    @click.option(
        "--format",
        "--file-type",
        "file_format",
        type=click.Choice(["sql", "py"]),
        default=None,
        help="File format for the generated migration (defaults to template profile)",
    )
    @click.option(
        "--from-schema", is_flag=True, default=False, help="Bootstrap migration with current database schema DDL."
    )
    @click.option(
        "--from-data",
        default=None,
        type=str,
        help=(
            "Bootstrap migration DDL from a data file or existing staging table. Values containing a path separator "
            "or ending in a data suffix are files; other values are table names."
        ),
    )
    @click.option("--schema", default=None, type=str, help="Database schema name for --from-schema or --from-data.")
    @click.option("--target-table", default=None, type=str, help="Target table name when bootstrapping from data.")
    @no_prompt_option
    def create_revision(
        bind_key: str | None,
        message: str | None,
        file_format: str | None,
        from_schema: bool,
        from_data: str | None,
        schema: str | None,
        target_table: str | None,
        no_prompt: bool,
    ) -> None:
        """Create a new database revision."""
        ctx = _ensure_click_context()

        console.rule("[yellow]Creating new migration revision[/]", align="left")
        message_text = message
        if message_text is None:
            message_text = (
                "new migration" if no_prompt else Prompt.ask("Please enter a message describing this revision")
            )

        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        _dispatch_migration_call(
            sqlspec_config,
            "revision",
            message=message_text,
            file_type=file_format,
            from_schema=from_schema,
            from_data=from_data,
            schema=schema,
            target_table=target_table,
        )

    @database_group.command(name="fix", help="Convert timestamp migrations to sequential format.")
    @bind_key_option
    @dry_run_option
    @click.option("-y", "--yes", is_flag=True, help="Skip confirmation prompt")
    @no_prompt_option
    @include_option
    @exclude_option
    @use_logger_option
    @no_echo_option
    @click.option("--no-database", is_flag=True, help="Skip database record updates")
    def fix_migrations(
        bind_key: str | None,
        dry_run: bool,
        yes: bool,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        use_logger: bool,
        no_echo: bool,
        no_database: bool,
    ) -> None:
        """Convert timestamp migrations to sequential format."""
        ctx = _ensure_click_context()
        _, _, echo_enabled, _should_echo = _create_echo_settings(no_echo, False, use_logger)
        skip_prompt = yes or no_prompt

        def _fix_for_config(config: Any) -> None:
            _dispatch_migration_call(config, "fix", dry_run=dry_run, update_database=not no_database, yes=skip_prompt)

        async def _async_fix(config: Any) -> None:
            await _dispatch_migration_call_async(
                config, "fix", dry_run=dry_run, update_database=not no_database, yes=skip_prompt
            )

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=dry_run, operation_name="fix migrations", echo=echo_enabled
        )
        if configs_to_process is not None:
            if not configs_to_process:
                return
            if echo_enabled:
                console.rule("[yellow]Migration Fix Command[/]", align="left")
            _run_multi_config_operation(
                configs_to_process,
                sync_apply=_fix_for_config,
                async_apply=_async_fix,
                should_echo=_should_echo,
                progress_message=lambda name: f"[blue]Fixing configuration: {name}[/]",
                success_message=lambda name: f"[green]✓ Successfully fixed: {name}[/]",
                failure_message=lambda name, e: f"[red]✗ Failed to fix {name}: {e}[/]",
            )
            return

        if echo_enabled:
            console.rule("[yellow]Migration Fix Command[/]", align="left")
        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        _fix_for_config(sqlspec_config)

    @database_group.command(name="squash")
    @bind_key_option
    @click.argument("version_range", required=False, default=None)
    @click.option("-m", "--message", required=False, default=None, help="Description for the squashed migration.")
    @dry_run_option
    @click.option("-y", "--yes", is_flag=True, help="Skip confirmation prompt")
    @no_prompt_option
    @include_option
    @exclude_option
    @use_logger_option
    @no_echo_option
    @click.option("--no-database", is_flag=True, help="Skip database record updates")
    @click.option("--allow-gaps", is_flag=True, help="Allow gaps in version sequence")
    @click.option(
        "--output-format",
        type=click.Choice(["sql", "py"], case_sensitive=False),
        default="sql",
        help="Output format for squashed migration (default: sql)",
    )
    @click.option(
        "--strategy",
        type=click.Choice(["ddl", "schema", "concat"], case_sensitive=False),
        default="concat",
        show_default=True,
        help=(
            "Squash strategy: keep every statement in order (concat), fold DDL into its final shape and refuse "
            "ranges that cannot be folded faithfully (ddl), or reflect the live database schema (schema)."
        ),
    )
    def squash_migrations(
        bind_key: str | None,
        version_range: str | None,
        message: str | None,
        dry_run: bool,
        yes: bool,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        use_logger: bool,
        no_echo: bool,
        no_database: bool,
        allow_gaps: bool,
        output_format: str,
        strategy: str,
    ) -> None:
        """Squash multiple sequential migrations into a single file.

        VERSION_RANGE accepts START:END, START..END, or START-END, for example 1:7.
        """
        ctx = _ensure_click_context()
        _, _, echo_enabled, _should_echo = _create_echo_settings(no_echo, False, use_logger)

        start_version: str | None = None
        end_version: str | None = None
        if version_range is not None:
            try:
                start_version, end_version = parse_version_range(version_range)
            except ValueError as exc:
                console.print(f"[red]Error: {escape(str(exc))}[/]")
                raise SystemExit(1) from None

        skip_prompt = yes or no_prompt
        squash_kwargs: dict[str, Any] = {
            "start_version": start_version,
            "end_version": end_version,
            "description": message,
            "dry_run": dry_run,
            "update_database": not no_database,
            "yes": skip_prompt,
            "allow_gaps": allow_gaps,
            "output_format": output_format.lower(),
            "strategy": strategy.lower(),
        }

        def _squash_for_config(config: Any) -> None:
            _dispatch_migration_call(config, "squash", **squash_kwargs)

        async def _async_squash(config: Any) -> None:
            await _dispatch_migration_call_async(config, "squash", **squash_kwargs)

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=False, operation_name="squash migrations", echo=echo_enabled
        )
        if configs_to_process is not None:
            if not configs_to_process:
                return
            if echo_enabled:
                console.rule("[yellow]Migration Squash Command[/]", align="left")
            _run_multi_config_operation(
                configs_to_process,
                sync_apply=_squash_for_config,
                async_apply=_async_squash,
                should_echo=_should_echo,
                progress_message=lambda name: f"[blue]Squashing configuration: {name}[/]",
                success_message=lambda name: f"[green]✓ Successfully squashed: {name}[/]",
                failure_message=lambda name, e: f"[red]✗ Failed to squash {name}: {e}[/]",
            )
            return

        if echo_enabled:
            console.rule("[yellow]Migration Squash Command[/]", align="left")
        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        _squash_for_config(sqlspec_config)

    @database_group.command(name="dump-schema", help="Export the current database schema DDL.")
    @click.option(
        "-o", "--output", type=click.Path(path_type=Path), default=None, help="Output file path for schema DDL."
    )
    @click.option("--schema", default=None, help="Database schema name to inspect.")
    @click.option(
        "--include-migrations-table",
        is_flag=True,
        default=False,
        help="Include the migration tracking table in the exported DDL.",
    )
    @click.option(
        "--prefer-native/--synthesize",
        default=True,
        help="Prefer engine-native catalog DDL when available or synthesize via SQLSpec builders.",
    )
    @click.option("--target-dialect", default=None, help="Target SQLGlot dialect for cross-dialect DDL transpilation.")
    @bind_key_option
    @include_option
    @exclude_option
    @no_echo_option
    def dump_schema_cmd(
        output: Path | None,
        schema: str | None,
        include_migrations_table: bool,
        prefer_native: bool,
        target_dialect: str | None,
        bind_key: str | None,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        no_echo: bool,
    ) -> None:
        """Export current database schema DDL.

        Without ``--output`` the DDL is written to stdout byte-for-byte (no Rich markup or wrapping) so it can be
        redirected to a file.
        """
        ctx = _ensure_click_context()
        echo_value = not no_echo

        def _write_ddl(rendered: str) -> None:
            if output is None and echo_value:
                sys.stdout.write(rendered)
                sys.stdout.flush()

        def _dump_for_config(config: Any) -> None:
            _write_ddl(
                _dispatch_migration_call(
                    config,
                    "dump_schema",
                    output_path=output,
                    schema=schema,
                    include_migrations_table=include_migrations_table,
                    prefer_native=prefer_native,
                    target_dialect=target_dialect,
                    echo=echo_value,
                )
            )

        async def _async_dump(config: Any) -> None:
            _write_ddl(
                await _dispatch_migration_call_async(
                    config,
                    "dump_schema",
                    output_path=output,
                    schema=schema,
                    include_migrations_table=include_migrations_table,
                    prefer_native=prefer_native,
                    target_dialect=target_dialect,
                    echo=echo_value,
                )
            )

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=False, operation_name="dump schema", echo=echo_value
        )
        if configs_to_process is not None:
            if not configs_to_process:
                return
            _run_multi_config_operation(
                configs_to_process,
                sync_apply=_dump_for_config,
                async_apply=_async_dump,
                should_echo=lambda _cfg: echo_value,
                progress_message=lambda name: f"[blue]Dumping schema for configuration: {name}[/]",
                success_message=None,
                failure_message=lambda name, e: f"[red]✗ Failed to dump schema for {name}: {e}[/]",
            )
            return

        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        _dump_for_config(sqlspec_config)

    @database_group.command(
        name="verify", help="Verify database migration tracking records against migration files on disk."
    )
    @click.option(
        "--strict",
        is_flag=True,
        default=False,
        help="Exit non-zero on checksum mismatch, orphaned applied migration, or partially applied squash.",
    )
    @click.option("--fail-on-pending", is_flag=True, default=False, help="Exit non-zero when migrations are pending.")
    @bind_key_option
    @include_option
    @exclude_option
    @use_logger_option
    @no_echo_option
    def verify_migrations_cmd(
        strict: bool,
        fail_on_pending: bool,
        bind_key: str | None,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        use_logger: bool,
        no_echo: bool,
    ) -> None:
        """Verify database migration tracking records against migration files on disk.

        Verification is read-only and never creates the tracking table.
        """
        ctx = _ensure_click_context()
        echo_setting, _, echo_enabled, _should_echo = _create_echo_settings(no_echo, False, use_logger)
        verify_kwargs: dict[str, Any] = {
            "strict": strict,
            "fail_on_pending": fail_on_pending,
            "use_logger": use_logger,
            "echo": echo_setting,
        }

        def _verify_for_config(config: Any) -> None:
            _dispatch_migration_call(config, "verify", **verify_kwargs)

        async def _async_verify(config: Any) -> None:
            await _dispatch_migration_call_async(config, "verify", **verify_kwargs)

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=False, operation_name="verify migrations", echo=echo_enabled
        )
        if configs_to_process is not None:
            if not configs_to_process:
                return
            _run_multi_config_operation(
                configs_to_process,
                sync_apply=_verify_for_config,
                async_apply=_async_verify,
                should_echo=_should_echo,
                progress_message=lambda name: f"[blue]Verifying configuration: {name}[/]",
                success_message=None,
                failure_message=lambda name, e: f"[red]✗ Verification failed for {name}: {e}[/]",
            )
            return

        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        _verify_for_config(sqlspec_config)

    @database_group.command(name="show-config", help="Show all configurations with migrations enabled.")
    @bind_key_option
    def show_config(bind_key: str | None = None) -> None:  # pyright: ignore[reportUnusedFunction]
        """Show and display all configurations with migrations enabled.

        Providing a bind key validates that config while still iterating the original config list to remain compatible with existing callers.
        """
        ctx = _ensure_click_context()

        if bind_key is not None:
            get_config_by_bind_key(ctx, bind_key)
            all_configs: list[AnyDatabaseConfig] = ctx.obj["configs"]
            migration_configs: list[tuple[str, AnyDatabaseConfig]] = []
            for cfg in all_configs:
                config_name = cfg.bind_key
                if config_name == bind_key and cfg.migration_config:
                    migration_configs.append((config_name, cfg))  # pyright: ignore[reportArgumentType]
        else:
            migration_configs = get_configs_with_migrations(ctx)

        if not migration_configs:
            console.print("[yellow]No configurations with migrations detected.[/]")
            return

        table = Table(title="Migration Configurations")
        table.add_column("Configuration Name", style="cyan")
        table.add_column("Migration Path", style="blue")
        table.add_column("Status", style="green")

        for config_name, config in migration_configs:
            migration_config_dict = config.migration_config or {}
            script_location = migration_config_dict.get("script_location", "migrations")
            table.add_row(config_name, str(script_location), "Migration Enabled")

        console.print(table)
        console.print(f"[blue]Found {len(migration_configs)} configuration(s) with migrations enabled.[/]")

    @database_group.group(name="adk", help="ADK extension commands")
    def adk_group() -> None:  # pyright: ignore[reportUnusedFunction]
        """ADK extension commands."""

    @adk_group.group(name="memory", help="ADK memory store commands")
    def adk_memory_group() -> None:  # pyright: ignore[reportUnusedFunction]
        """ADK memory store commands."""

    @adk_memory_group.command(name="cleanup", help="Delete memory entries older than N days")
    @bind_key_option
    @click.option("--days", type=int, required=True, help="Delete entries older than this many days")
    def cleanup_memory(bind_key: str | None, days: int) -> None:  # pyright: ignore[reportUnusedFunction]
        """Cleanup memory entries older than N days."""
        ctx = _ensure_click_context()
        configs = _adk_configs(ctx, bind_key)

        if not configs:
            console.print("[yellow]No ADK configurations found.[/]")
            return

        for cfg in configs:
            config_name = cfg.bind_key or "default"
            if not _is_adk_memory_enabled(cfg):
                console.print(f"[yellow]Memory disabled for {config_name}; skipping.[/]")
                continue

            store_class = _get_memory_store_class(cfg)
            if store_class is None:
                console.print(f"[yellow]No memory store found for {config_name}; skipping.[/]")
                continue

            if isinstance(cfg, (AsyncDatabaseConfig, NoPoolAsyncConfig)):
                async_store = cast("BaseAsyncADKMemoryStore[Any]", store_class(cfg))
                deleted = run_(_cleanup_memory_entries_async)(async_store, days)
                console.print(f"[green]✓[/] {config_name}: deleted {deleted} memory entries older than {days} days")
                continue
            sync_store = cast("BaseSyncADKMemoryStore[Any]", store_class(cfg))
            deleted = sync_store.delete_entries_older_than(days)
            console.print(f"[green]✓[/] {config_name}: deleted {deleted} memory entries older than {days} days")

    @adk_memory_group.command(name="verify", help="Verify memory table exists and is reachable")
    @bind_key_option
    def verify_memory(bind_key: str | None) -> None:  # pyright: ignore[reportUnusedFunction]
        """Verify memory tables are reachable for configured adapters."""
        ctx = _ensure_click_context()
        configs = _adk_configs(ctx, bind_key)

        if not configs:
            console.print("[yellow]No ADK configurations found.[/]")
            return

        for cfg in configs:
            config_name = cfg.bind_key or "default"
            if not _is_adk_memory_enabled(cfg):
                console.print(f"[yellow]Memory disabled for {config_name}; skipping.[/]")
                continue

            store_class = _get_memory_store_class(cfg)
            if store_class is None:
                console.print(f"[yellow]No memory store found for {config_name}; skipping.[/]")
                continue

            try:
                if isinstance(cfg, (AsyncDatabaseConfig, NoPoolAsyncConfig)):
                    async_cfg: AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] = cfg
                    async_store = cast("BaseAsyncADKMemoryStore[Any]", store_class(async_cfg))
                    sql = f"SELECT 1 FROM {async_store.memory_table} WHERE 1 = 0"
                    run_(_verify_memory_table_async)(async_cfg, sql)
                    console.print(f"[green]✓[/] {config_name}: memory table reachable")
                    continue
                sync_store = cast("BaseSyncADKMemoryStore[Any]", store_class(cfg))
                sql = f"SELECT 1 FROM {sync_store.memory_table} WHERE 1 = 0"
                with cfg.provide_session() as driver:
                    driver.execute(sql)
                console.print(f"[green]✓[/] {config_name}: memory table reachable")
            except Exception as exc:
                console.print(f"[red]✗[/] {config_name}: {exc}")

    return database_group


def __getattr__(name: str) -> Any:
    """Lazily construct the root Click command group for CLI documentation tools."""
    if name == "cli":
        return add_migration_commands()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def _safe_group_command(
    group: "Group", *, aliases: "list[str] | None" = None, **kwargs: Any
) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    if aliases is None:
        return group.command(**kwargs)
    try:
        return group.command(aliases=aliases, **kwargs)
    except TypeError:
        return group.command(**kwargs)


def _ensure_click_context() -> "click.Context":
    """Return the active Click context, raising if missing (for type-checkers)."""

    context = click.get_current_context()
    if context is None:  # pragma: no cover
        msg = "SQLSpec CLI commands require an active Click context"
        raise RuntimeError(msg)
    return cast("click.Context", context)
