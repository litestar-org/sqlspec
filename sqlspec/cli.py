# ruff: noqa: C901
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import rich_click as click
from click.core import ParameterSource
from rich import get_console
from rich.prompt import Confirm, Prompt
from rich.table import Table

from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.exceptions import ConfigResolverError
from sqlspec.utils.config_tools import discover_config_from_pyproject, resolve_config_sync
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.sync_tools import run_

if TYPE_CHECKING:
    from rich_click import Group

    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

__all__ = ("add_migration_commands", "get_sqlspec_group")


def _safe_group_command(
    group: "Group", *, aliases: "list[str] | None" = None, **kwargs: Any
) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    if aliases is None:
        return group.command(**kwargs)
    try:
        return group.command(aliases=aliases, **kwargs)
    except TypeError:
        return group.command(**kwargs)


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
            console.print("  3. pyproject.toml:  [tool.sqlspec]")
            console.print('                      config = "myapp.config:get_configs"')
            ctx.exit(1)

        cwd = str(Path.cwd())
        cwd_added = False
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
            cwd_added = True

        try:
            all_configs: list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]] = []
            for config_path in config.split(","):
                config_path = config_path.strip()
                if not config_path:
                    continue
                config_result = resolve_config_sync(config_path)
                if isinstance(config_result, Sequence) and not isinstance(config_result, str):
                    all_configs.extend(config_result)
                else:
                    all_configs.append(config_result)  # pyright: ignore

            configs_by_key: dict[
                str | None, AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]
            ] = {}
            for cfg in all_configs:
                configs_by_key[cfg.bind_key] = cfg

            ctx.obj["configs"] = list(configs_by_key.values())

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

        except (ImportError, ConfigResolverError) as e:
            console.print(f"[red]Error loading config: {e}[/]")
            ctx.exit(1)
        finally:
            if cwd_added and cwd in sys.path and sys.path[0] == cwd:
                sys.path.remove(cwd)

    return sqlspec_group


def _ensure_click_context() -> "click.Context":
    """Return the active Click context, raising if missing (for type-checkers)."""

    context = click.get_current_context()
    if context is None:  # pragma: no cover - click guarantees context in commands
        msg = "SQLSpec CLI commands require an active Click context"
        raise RuntimeError(msg)
    return cast("click.Context", context)


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
    execution_mode_option = click.option(
        "--execution-mode",
        type=click.Choice(["auto", "sync", "async"]),
        default="auto",
        help="Force execution mode (auto-detects by default)",
    )
    no_auto_sync_option = click.option(
        "--no-auto-sync",
        is_flag=True,
        default=False,
        help="Disable automatic version reconciliation when migrations have been renamed",
    )

    def get_config_by_bind_key(
        ctx: "click.Context", bind_key: str | None
    ) -> "AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]":
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

        return cast("AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]", config)

    def _get_adk_configs(
        ctx: "click.Context", bind_key: str | None
    ) -> "list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]]":
        if bind_key is not None:
            return [get_config_by_bind_key(ctx, bind_key)]

        configs = ctx.obj["configs"]
        return [cfg for cfg in configs if "adk" in cfg.extension_config]

    def _get_memory_store_class(
        config: "AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]",
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

    def _is_adk_memory_enabled(
        config: "AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]",
    ) -> bool:
        adk_config = cast("dict[str, Any]", config.extension_config.get("adk", {}))
        return bool(adk_config.get("enable_memory", True))

    async def _cleanup_memory_entries_async(store: "BaseAsyncADKMemoryStore[Any]", days: int) -> int:
        return await store.delete_entries_older_than(days)

    async def _verify_memory_table_async(config: "AsyncDatabaseConfig[Any, Any, Any]", sql: str) -> None:
        async with config.provide_session() as driver:
            await driver.execute(sql)

    def get_configs_with_migrations(
        ctx: "click.Context", enabled_only: bool = False
    ) -> "list[tuple[str, AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]]]":
        """Get all configurations that have migrations enabled.

        Args:
            ctx: The click context.
            enabled_only: If True, only return configs with enabled=True.

        Returns:
            List of tuples (config_name, config) for configs with migrations enabled.
        """
        configs: list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]] = ctx.obj["configs"]
        migration_configs: list[tuple[str, AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]]] = []

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
        config: "AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]",
        sync_fn: "Callable[[], Any]",
        async_fn: "Callable[[], Any]",
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

    def process_multiple_configs(
        ctx: "click.Context",
        bind_key: str | None,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
        operation_name: str,
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
            console.print("[yellow]No configurations match the specified criteria.[/]")
            return []

        if dry_run:
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
        from sqlspec.migrations.commands import create_migration_commands

        ctx = _ensure_click_context()

        def _show_for_config(config: Any) -> None:
            """Show current revision for a single config with sync/async dispatch."""
            migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(
                config=config
            )

            def sync_show() -> None:
                migration_commands.current(verbose=verbose)

            async def async_show() -> None:
                await cast("AsyncMigrationCommands[Any]", migration_commands).current(verbose=verbose)

            _execute_for_config(config, sync_show, async_show)

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=False, operation_name="show current revision"
        )

        if configs_to_process is not None:
            if not configs_to_process:
                return

            console.rule("[yellow]Listing current revisions for all configurations[/]", align="left")

            sync_configs, async_configs = _partition_configs_by_async(configs_to_process)

            for config_name, config in sync_configs:
                console.print(f"\n[blue]Configuration: {config_name}[/]")
                try:
                    _show_for_config(config)
                except Exception as e:
                    console.print(f"[red]✗ Failed to get current revision for {config_name}: {e}[/]")

            if async_configs:

                async def _run_async_configs() -> None:
                    for config_name, config in async_configs:
                        console.print(f"\n[blue]Configuration: {config_name}[/]")
                        try:
                            migration_commands: AsyncMigrationCommands[Any] = cast(
                                "AsyncMigrationCommands[Any]", create_migration_commands(config=config)
                            )
                            await migration_commands.current(verbose=verbose)
                        except Exception as e:
                            console.print(f"[red]✗ Failed to get current revision for {config_name}: {e}[/]")

                run_(_run_async_configs)()
        else:
            console.rule("[yellow]Listing current revision[/]", align="left")
            sqlspec_config = get_config_by_bind_key(ctx, bind_key)
            _show_for_config(sqlspec_config)

    @database_group.command(name="downgrade", help="Downgrade database to a specific revision.")
    @bind_key_option
    @no_prompt_option
    @include_option
    @exclude_option
    @dry_run_option
    @click.argument("revision", type=str, default="-1")
    def downgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None,
        revision: str,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
    ) -> None:
        """Downgrade the database to the latest revision."""

        from sqlspec.migrations.commands import create_migration_commands

        ctx = _ensure_click_context()

        def _downgrade_for_config(config: Any) -> None:
            """Downgrade a single config with sync/async dispatch."""
            migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(
                config=config
            )

            def sync_downgrade() -> None:
                migration_commands.downgrade(revision=revision, dry_run=dry_run)

            async def async_downgrade() -> None:
                await cast("AsyncMigrationCommands[Any]", migration_commands).downgrade(
                    revision=revision, dry_run=dry_run
                )

            _execute_for_config(config, sync_downgrade, async_downgrade)

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=dry_run, operation_name=f"downgrade to {revision}"
        )

        if configs_to_process is not None:
            if not configs_to_process:
                return

            if not no_prompt and not Confirm.ask(
                f"[bold]Are you sure you want to downgrade {len(configs_to_process)} configuration(s) to revision {revision}?[/]"
            ):
                console.print("[yellow]Operation cancelled.[/]")
                return

            console.rule("[yellow]Starting multi-configuration downgrade process[/]", align="left")

            sync_configs, async_configs = _partition_configs_by_async(configs_to_process)

            for config_name, config in sync_configs:
                console.print(f"[blue]Downgrading configuration: {config_name}[/]")
                try:
                    _downgrade_for_config(config)
                    console.print(f"[green]✓ Successfully downgraded: {config_name}[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed to downgrade {config_name}: {e}[/]")

            if async_configs:

                async def _run_async_configs() -> None:
                    for config_name, config in async_configs:
                        console.print(f"[blue]Downgrading configuration: {config_name}[/]")
                        try:
                            migration_commands: AsyncMigrationCommands[Any] = cast(
                                "AsyncMigrationCommands[Any]", create_migration_commands(config=config)
                            )
                            await migration_commands.downgrade(revision=revision, dry_run=dry_run)
                            console.print(f"[green]✓ Successfully downgraded: {config_name}[/]")
                        except Exception as e:
                            console.print(f"[red]✗ Failed to downgrade {config_name}: {e}[/]")

                run_(_run_async_configs)()
        else:
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
    @execution_mode_option
    @no_auto_sync_option
    @click.argument("revision", type=str, default="head")
    def upgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None,
        revision: str,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
        execution_mode: str,
        no_auto_sync: bool,
    ) -> None:
        """Upgrade the database to the latest revision.

        Non-automatic execution modes are surfaced in the console, and multi-config flows reuse ``process_multiple_configs`` to split sync/async executions while honoring dry-run and auto-sync flags.
        """
        from sqlspec.migrations.commands import create_migration_commands

        ctx = _ensure_click_context()
        if execution_mode != "auto":
            console.print(f"[dim]Execution mode: {execution_mode}[/]")

        def _upgrade_for_config(config: Any) -> None:
            """Upgrade a single config with sync/async dispatch."""
            migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(
                config=config
            )

            def sync_upgrade() -> None:
                migration_commands.upgrade(revision=revision, auto_sync=not no_auto_sync, dry_run=dry_run)

            async def async_upgrade() -> None:
                await cast("AsyncMigrationCommands[Any]", migration_commands).upgrade(
                    revision=revision, auto_sync=not no_auto_sync, dry_run=dry_run
                )

            _execute_for_config(config, sync_upgrade, async_upgrade)

        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run, operation_name=f"upgrade to {revision}"
        )

        if configs_to_process is not None:
            if not configs_to_process:
                return

            if not no_prompt and not Confirm.ask(
                f"[bold]Are you sure you want to upgrade {len(configs_to_process)} configuration(s) to revision {revision}?[/]"
            ):
                console.print("[yellow]Operation cancelled.[/]")
                return

            console.rule("[yellow]Starting multi-configuration upgrade process[/]", align="left")

            sync_configs, async_configs = _partition_configs_by_async(configs_to_process)

            for config_name, config in sync_configs:
                console.print(f"[blue]Upgrading configuration: {config_name}[/]")
                try:
                    _upgrade_for_config(config)
                    console.print(f"[green]✓ Successfully upgraded: {config_name}[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed to upgrade {config_name}: {e}[/]")

            if async_configs:

                async def _run_async_configs() -> None:
                    for config_name, config in async_configs:
                        console.print(f"[blue]Upgrading configuration: {config_name}[/]")
                        try:
                            migration_commands: AsyncMigrationCommands[Any] = cast(
                                "AsyncMigrationCommands[Any]", create_migration_commands(config=config)
                            )
                            await migration_commands.upgrade(
                                revision=revision, auto_sync=not no_auto_sync, dry_run=dry_run
                            )
                            console.print(f"[green]✓ Successfully upgraded: {config_name}[/]")
                        except Exception as e:
                            console.print(f"[red]✗ Failed to upgrade {config_name}: {e}[/]")

                run_(_run_async_configs)()
        else:
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
        from sqlspec.migrations.commands import create_migration_commands

        ctx = _ensure_click_context()

        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        migration_commands = create_migration_commands(config=sqlspec_config)

        def sync_stamp() -> None:
            migration_commands.stamp(revision=revision)

        async def async_stamp() -> None:
            await cast("AsyncMigrationCommands[Any]", migration_commands).stamp(revision=revision)

        _execute_for_config(sqlspec_config, sync_stamp, async_stamp)

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
            migration_commands = create_migration_commands(config=config)
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
                        "AsyncMigrationCommands[Any]", create_migration_commands(config=config)
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
    @no_prompt_option
    def create_revision(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None, message: str | None, file_format: str | None, no_prompt: bool
    ) -> None:
        """Create a new database revision."""
        from sqlspec.migrations.commands import create_migration_commands

        ctx = _ensure_click_context()

        console.rule("[yellow]Creating new migration revision[/]", align="left")
        message_text = message
        if message_text is None:
            message_text = (
                "new migration" if no_prompt else Prompt.ask("Please enter a message describing this revision")
            )

        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        param_source = ctx.get_parameter_source("file_format")
        effective_format = None if param_source is ParameterSource.DEFAULT else file_format
        migration_commands = create_migration_commands(config=sqlspec_config)

        def sync_revision() -> None:
            migration_commands.revision(message=message_text, file_type=effective_format)

        async def async_revision() -> None:
            await cast("AsyncMigrationCommands[Any]", migration_commands).revision(
                message=message_text, file_type=effective_format
            )

        _execute_for_config(sqlspec_config, sync_revision, async_revision)

    @database_group.command(name="fix", help="Convert timestamp migrations to sequential format.")
    @bind_key_option
    @dry_run_option
    @click.option("--yes", is_flag=True, help="Skip confirmation prompt")
    @click.option("--no-database", is_flag=True, help="Skip database record updates")
    def fix_migrations(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None, dry_run: bool, yes: bool, no_database: bool
    ) -> None:
        """Convert timestamp migrations to sequential format."""
        from sqlspec.migrations.commands import create_migration_commands

        ctx = _ensure_click_context()

        console.rule("[yellow]Migration Fix Command[/]", align="left")
        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        migration_commands = create_migration_commands(config=sqlspec_config)

        def sync_fix() -> None:
            migration_commands.fix(dry_run=dry_run, update_database=not no_database, yes=yes)

        async def async_fix() -> None:
            await cast("AsyncMigrationCommands[Any]", migration_commands).fix(
                dry_run=dry_run, update_database=not no_database, yes=yes
            )

        _execute_for_config(sqlspec_config, sync_fix, async_fix)

    @database_group.command(name="show-config", help="Show all configurations with migrations enabled.")
    @bind_key_option
    def show_config(bind_key: str | None = None) -> None:  # pyright: ignore[reportUnusedFunction]
        """Show and display all configurations with migrations enabled.

        Providing a bind key validates that config while still iterating the original config list to remain compatible with existing callers.
        """
        ctx = _ensure_click_context()

        if bind_key is not None:
            get_config_by_bind_key(ctx, bind_key)
            all_configs: list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]] = ctx.obj[
                "configs"
            ]
            migration_configs: list[
                tuple[str, AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]]
            ] = []
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
        configs = _get_adk_configs(ctx, bind_key)

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

            if isinstance(cfg, AsyncDatabaseConfig):
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
        configs = _get_adk_configs(ctx, bind_key)

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
                if isinstance(cfg, AsyncDatabaseConfig):
                    async_cfg: AsyncDatabaseConfig[Any, Any, Any] = cfg
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
