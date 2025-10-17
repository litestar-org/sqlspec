# ruff: noqa: C901
import inspect
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import rich_click as click

if TYPE_CHECKING:
    from rich_click import Group

    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

__all__ = ("add_migration_commands", "get_sqlspec_group")


def get_sqlspec_group() -> "Group":
    """Get the SQLSpec CLI group.

    Returns:
        The SQLSpec CLI group.
    """

    @click.group(name="sqlspec")
    @click.option(
        "--config",
        help="Dotted path to SQLSpec config(s) or callable function (e.g. 'myapp.config.get_configs')",
        required=True,
        type=str,
    )
    @click.option(
        "--validate-config", is_flag=True, default=False, help="Validate configuration before executing migrations"
    )
    @click.pass_context
    def sqlspec_group(ctx: "click.Context", config: str, validate_config: bool) -> None:
        """SQLSpec CLI commands."""
        from rich import get_console

        from sqlspec.exceptions import ConfigResolverError
        from sqlspec.utils.config_resolver import resolve_config_sync

        console = get_console()
        ctx.ensure_object(dict)

        # Add current working directory to sys.path to allow loading local config modules
        cwd = str(Path.cwd())
        cwd_added = False
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
            cwd_added = True

        try:
            config_result = resolve_config_sync(config)
            if isinstance(config_result, Sequence) and not isinstance(config_result, str):
                ctx.obj["configs"] = list(config_result)
            else:
                ctx.obj["configs"] = [config_result]

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
            # Clean up: remove the cwd from sys.path if we added it
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
    from rich import get_console

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

    def get_configs_with_migrations(ctx: "click.Context", enabled_only: bool = False) -> "list[tuple[str, Any]]":
        """Get all configurations that have migrations enabled.

        Args:
            ctx: The click context.
            enabled_only: If True, only return configs with enabled=True.

        Returns:
            List of tuples (config_name, config) for configs with migrations enabled.
        """
        configs = ctx.obj["configs"]
        migration_configs = []

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

    async def maybe_await(result: Any) -> Any:
        """Await result if it's a coroutine, otherwise return it directly."""
        if inspect.iscoroutine(result):
            return await result
        return result

    def process_multiple_configs(
        ctx: "click.Context",
        bind_key: str | None,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
        operation_name: str,
    ) -> "list[tuple[str, Any]] | None":
        """Process configuration selection for multi-config operations.

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
        # If specific bind_key requested, use single config mode
        if bind_key and not include and not exclude:
            return None

        # Get enabled configs by default, all configs if include/exclude specified
        enabled_only = not include and not exclude
        migration_configs = get_configs_with_migrations(ctx, enabled_only=enabled_only)

        # If only one config and no filtering, use single config mode
        if len(migration_configs) <= 1 and not include and not exclude:
            return None

        # Apply filtering
        configs_to_process = filter_configs(migration_configs, include, exclude)

        if not configs_to_process:
            console.print("[yellow]No configurations match the specified criteria.[/]")
            return []

        # Show what will be processed
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
        """Show current database revision."""
        from sqlspec.migrations.commands import create_migration_commands
        from sqlspec.utils.sync_tools import run_

        ctx = click.get_current_context()

        async def _show_current_revision() -> None:
            # Check if this is a multi-config operation
            configs_to_process = process_multiple_configs(
                cast("click.Context", ctx),
                bind_key,
                include,
                exclude,
                dry_run=False,
                operation_name="show current revision",
            )

            if configs_to_process is not None:
                if not configs_to_process:
                    return

                console.rule("[yellow]Listing current revisions for all configurations[/]", align="left")

                for config_name, config in configs_to_process:
                    console.print(f"\n[blue]Configuration: {config_name}[/]")
                    try:
                        migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = (
                            create_migration_commands(config=config)
                        )
                        await maybe_await(migration_commands.current(verbose=verbose))
                    except Exception as e:
                        console.print(f"[red]✗ Failed to get current revision for {config_name}: {e}[/]")
            else:
                console.rule("[yellow]Listing current revision[/]", align="left")
                sqlspec_config = get_config_by_bind_key(cast("click.Context", ctx), bind_key)
                migration_commands = create_migration_commands(config=sqlspec_config)
                await maybe_await(migration_commands.current(verbose=verbose))

        run_(_show_current_revision)()

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
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import create_migration_commands
        from sqlspec.utils.sync_tools import run_

        ctx = click.get_current_context()

        async def _downgrade_database() -> None:
            # Check if this is a multi-config operation
            configs_to_process = process_multiple_configs(
                cast("click.Context", ctx),
                bind_key,
                include,
                exclude,
                dry_run=dry_run,
                operation_name=f"downgrade to {revision}",
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

                for config_name, config in configs_to_process:
                    console.print(f"[blue]Downgrading configuration: {config_name}[/]")
                    try:
                        migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = (
                            create_migration_commands(config=config)
                        )
                        await maybe_await(migration_commands.downgrade(revision=revision, dry_run=dry_run))
                        console.print(f"[green]✓ Successfully downgraded: {config_name}[/]")
                    except Exception as e:
                        console.print(f"[red]✗ Failed to downgrade {config_name}: {e}[/]")
            else:
                # Single config operation
                console.rule("[yellow]Starting database downgrade process[/]", align="left")
                input_confirmed = (
                    True
                    if no_prompt
                    else Confirm.ask(f"Are you sure you want to downgrade the database to the `{revision}` revision?")
                )
                if input_confirmed:
                    sqlspec_config = get_config_by_bind_key(cast("click.Context", ctx), bind_key)
                    migration_commands = create_migration_commands(config=sqlspec_config)
                    await maybe_await(migration_commands.downgrade(revision=revision, dry_run=dry_run))

        run_(_downgrade_database)()

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
        """Upgrade the database to the latest revision."""
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import create_migration_commands
        from sqlspec.utils.sync_tools import run_

        ctx = click.get_current_context()

        async def _upgrade_database() -> None:
            # Report execution mode when specified
            if execution_mode != "auto":
                console.print(f"[dim]Execution mode: {execution_mode}[/]")

            # Check if this is a multi-config operation
            configs_to_process = process_multiple_configs(
                cast("click.Context", ctx), bind_key, include, exclude, dry_run, operation_name=f"upgrade to {revision}"
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

                for config_name, config in configs_to_process:
                    console.print(f"[blue]Upgrading configuration: {config_name}[/]")
                    try:
                        migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = (
                            create_migration_commands(config=config)
                        )
                        await maybe_await(migration_commands.upgrade(revision=revision, auto_sync=not no_auto_sync, dry_run=dry_run))
                        console.print(f"[green]✓ Successfully upgraded: {config_name}[/]")
                    except Exception as e:
                        console.print(f"[red]✗ Failed to upgrade {config_name}: {e}[/]")
            else:
                # Single config operation
                console.rule("[yellow]Starting database upgrade process[/]", align="left")
                input_confirmed = (
                    True
                    if no_prompt
                    else Confirm.ask(
                        f"[bold]Are you sure you want migrate the database to the `{revision}` revision?[/]"
                    )
                )
                if input_confirmed:
                    sqlspec_config = get_config_by_bind_key(cast("click.Context", ctx), bind_key)
                    migration_commands = create_migration_commands(config=sqlspec_config)
                    await maybe_await(migration_commands.upgrade(revision=revision, auto_sync=not no_auto_sync, dry_run=dry_run))

        run_(_upgrade_database)()

    @database_group.command(help="Stamp the revision table with the given revision")
    @click.argument("revision", type=str)
    @bind_key_option
    def stamp(bind_key: str | None, revision: str) -> None:  # pyright: ignore[reportUnusedFunction]
        """Stamp the revision table with the given revision."""
        from sqlspec.migrations.commands import create_migration_commands
        from sqlspec.utils.sync_tools import run_

        ctx = click.get_current_context()

        async def _stamp() -> None:
            sqlspec_config = get_config_by_bind_key(cast("click.Context", ctx), bind_key)
            migration_commands = create_migration_commands(config=sqlspec_config)
            await maybe_await(migration_commands.stamp(revision=revision))

        run_(_stamp)()

    @database_group.command(name="init", help="Initialize migrations for the project.")
    @bind_key_option
    @click.argument("directory", default=None, required=False)
    @click.option("--package", is_flag=True, default=True, help="Create `__init__.py` for created folder")
    @no_prompt_option
    def init_sqlspec(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None, directory: str | None, package: bool, no_prompt: bool
    ) -> None:
        """Initialize the database migrations."""
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import create_migration_commands
        from sqlspec.utils.sync_tools import run_

        ctx = click.get_current_context()

        async def _init_sqlspec() -> None:
            console.rule("[yellow]Initializing database migrations.", align="left")
            input_confirmed = (
                True
                if no_prompt
                else Confirm.ask("[bold]Are you sure you want initialize migrations for the project?[/]")
            )
            if input_confirmed:
                configs = (
                    [get_config_by_bind_key(cast("click.Context", ctx), bind_key)]
                    if bind_key is not None
                    else cast("click.Context", ctx).obj["configs"]
                )

                for config in configs:
                    migration_config = getattr(config, "migration_config", {})
                    target_directory = (
                        str(migration_config.get("script_location", "migrations")) if directory is None else directory
                    )
                    migration_commands = create_migration_commands(config=config)
                    await maybe_await(migration_commands.init(directory=target_directory, package=package))

        run_(_init_sqlspec)()

    @database_group.command(
        name="create-migration", aliases=["make-migration"], help="Create a new migration revision."
    )
    @bind_key_option
    @click.option("-m", "--message", default=None, help="Revision message")
    @no_prompt_option
    def create_revision(  # pyright: ignore[reportUnusedFunction]
        bind_key: str | None, message: str | None, no_prompt: bool
    ) -> None:
        """Create a new database revision."""
        from rich.prompt import Prompt

        from sqlspec.migrations.commands import create_migration_commands
        from sqlspec.utils.sync_tools import run_

        ctx = click.get_current_context()

        async def _create_revision() -> None:
            console.rule("[yellow]Creating new migration revision[/]", align="left")
            message_text = message
            if message_text is None:
                message_text = (
                    "new migration" if no_prompt else Prompt.ask("Please enter a message describing this revision")
                )

            sqlspec_config = get_config_by_bind_key(cast("click.Context", ctx), bind_key)
            migration_commands = create_migration_commands(config=sqlspec_config)
            await maybe_await(migration_commands.revision(message=message_text))

        run_(_create_revision)()

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
        from sqlspec.utils.sync_tools import run_

        ctx = click.get_current_context()

        async def _fix_migrations() -> None:
            console.rule("[yellow]Migration Fix Command[/]", align="left")
            sqlspec_config = get_config_by_bind_key(cast("click.Context", ctx), bind_key)
            migration_commands = create_migration_commands(config=sqlspec_config)
            await maybe_await(migration_commands.fix(dry_run=dry_run, update_database=not no_database, yes=yes))

        run_(_fix_migrations)()

    @database_group.command(name="show-config", help="Show all configurations with migrations enabled.")
    @bind_key_option
    def show_config(bind_key: str | None = None) -> None:  # pyright: ignore[reportUnusedFunction]
        """Show and display all configurations with migrations enabled."""
        from rich.table import Table

        ctx = click.get_current_context()

        # If bind_key is provided, filter to only that config
        if bind_key is not None:
            get_config_by_bind_key(cast("click.Context", ctx), bind_key)
            # Convert single config to list format for compatibility
            all_configs = cast("click.Context", ctx).obj["configs"]
            migration_configs = []
            for cfg in all_configs:
                config_name = cfg.bind_key
                if config_name == bind_key and hasattr(cfg, "migration_config") and cfg.migration_config:
                    migration_configs.append((config_name, cfg))
        else:
            migration_configs = get_configs_with_migrations(cast("click.Context", ctx))

        if not migration_configs:
            console.print("[yellow]No configurations with migrations detected.[/]")
            return

        table = Table(title="Migration Configurations")
        table.add_column("Configuration Name", style="cyan")
        table.add_column("Migration Path", style="blue")
        table.add_column("Status", style="green")

        for config_name, config in migration_configs:
            migration_config = getattr(config, "migration_config", {})
            script_location = migration_config.get("script_location", "migrations")
            table.add_row(config_name, str(script_location), "Migration Enabled")

        console.print(table)
        console.print(f"[blue]Found {len(migration_configs)} configuration(s) with migrations enabled.[/]")

    return database_group
