# ruff: noqa: C901
import sys
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union, cast

if TYPE_CHECKING:
    from click import Group

    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig

__all__ = ("add_migration_commands", "get_sqlspec_group")


def get_sqlspec_group() -> "Group":
    """Get the SQLSpec CLI group.

    Raises:
        MissingDependencyError: If the `click` package is not installed.

    Returns:
        The SQLSpec CLI group.
    """
    from sqlspec.exceptions import MissingDependencyError

    try:
        import rich_click as click
    except ImportError:
        try:
            import click  # type: ignore[no-redef]
        except ImportError as e:
            raise MissingDependencyError(package="click", install_package="cli") from e

    @click.group(name="sqlspec")
    @click.option(
        "--config",
        help="Dotted path to SQLSpec config(s) (e.g. 'myapp.config.sqlspec_configs')",
        required=True,
        type=str,
    )
    @click.pass_context
    def sqlspec_group(ctx: "click.Context", config: str) -> None:
        """SQLSpec CLI commands."""
        from rich import get_console

        from sqlspec.utils import module_loader

        console = get_console()
        ctx.ensure_object(dict)
        try:
            config_instance = module_loader.import_string(config)
            if isinstance(config_instance, Sequence):
                ctx.obj["configs"] = config_instance
            else:
                ctx.obj["configs"] = [config_instance]
        except ImportError as e:
            console.print(f"[red]Error loading config: {e}[/]")
            ctx.exit(1)

    return sqlspec_group


def add_migration_commands(database_group: Optional["Group"] = None) -> "Group":
    """Add migration commands to the database group.

    Args:
        database_group: The database group to add the commands to.

    Raises:
        MissingDependencyError: If the `click` package is not installed.

    Returns:
        The database group with the migration commands added.
    """
    from sqlspec.exceptions import MissingDependencyError

    try:
        import rich_click as click
    except ImportError:
        try:
            import click  # type: ignore[no-redef]
        except ImportError as e:
            raise MissingDependencyError(package="click", install_package="cli") from e
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

    def get_config_by_bind_key(
        ctx: "click.Context", bind_key: Optional[str]
    ) -> "Union[AsyncDatabaseConfig[Any, Any, Any], SyncDatabaseConfig[Any, Any, Any]]":
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
                config_name = getattr(cfg, "name", None) or getattr(cfg, "bind_key", None)
                if config_name == bind_key:
                    config = cfg
                    break

            if config is None:
                console.print(f"[red]No config found for bind key: {bind_key}[/]")
                sys.exit(1)

        # Extract the actual config from DatabaseConfig wrapper if needed
        from sqlspec.extensions.litestar.config import DatabaseConfig

        if isinstance(config, DatabaseConfig):
            config = config.config

        return cast("Union[AsyncDatabaseConfig[Any, Any, Any], SyncDatabaseConfig[Any, Any, Any]]", config)

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

        from sqlspec.extensions.litestar.config import DatabaseConfig

        for config in configs:
            # Extract the actual config from DatabaseConfig wrapper if needed
            actual_config = config.config if isinstance(config, DatabaseConfig) else config

            migration_config = getattr(actual_config, "migration_config", None)
            if migration_config:
                enabled = migration_config.get("enabled", True)
                if not enabled_only or enabled:
                    config_name = (
                        getattr(actual_config, "name", None)
                        or getattr(actual_config, "bind_key", None)
                        or str(type(actual_config).__name__)
                    )
                    migration_configs.append((config_name, actual_config))

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

    def process_multiple_configs(
        ctx: "click.Context",
        bind_key: Optional[str],
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
        operation_name: str,
    ) -> "Optional[list[tuple[str, Any]]]":
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
        bind_key: Optional[str], verbose: bool, include: "tuple[str, ...]", exclude: "tuple[str, ...]"
    ) -> None:
        """Show current database revision."""
        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()

        # Check if this is a multi-config operation
        configs_to_process = process_multiple_configs(
            ctx, bind_key, include, exclude, dry_run=False, operation_name="show current revision"
        )

        if configs_to_process is not None:
            if not configs_to_process:
                return

            console.rule("[yellow]Listing current revisions for all configurations[/]", align="left")

            for config_name, config in configs_to_process:
                console.print(f"\n[blue]Configuration: {config_name}[/]")
                try:
                    migration_commands = MigrationCommands(config=config)
                    migration_commands.current(verbose=verbose)
                except Exception as e:
                    console.print(f"[red]✗ Failed to get current revision for {config_name}: {e}[/]")
        else:
            # Single config operation
            console.rule("[yellow]Listing current revision[/]", align="left")
            sqlspec_config = get_config_by_bind_key(ctx, bind_key)
            migration_commands = MigrationCommands(config=sqlspec_config)
            migration_commands.current(verbose=verbose)

    @database_group.command(name="downgrade", help="Downgrade database to a specific revision.")
    @bind_key_option
    @no_prompt_option
    @include_option
    @exclude_option
    @dry_run_option
    @click.argument("revision", type=str, default="-1")
    def downgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: Optional[str],
        revision: str,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
    ) -> None:
        """Downgrade the database to the latest revision."""
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()

        # Check if this is a multi-config operation
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

            for config_name, config in configs_to_process:
                console.print(f"[blue]Downgrading configuration: {config_name}[/]")
                try:
                    migration_commands = MigrationCommands(config=config)
                    migration_commands.downgrade(revision=revision)
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
                sqlspec_config = get_config_by_bind_key(ctx, bind_key)
                migration_commands = MigrationCommands(config=sqlspec_config)
                migration_commands.downgrade(revision=revision)

    @database_group.command(name="upgrade", help="Upgrade database to a specific revision.")
    @bind_key_option
    @no_prompt_option
    @include_option
    @exclude_option
    @dry_run_option
    @click.argument("revision", type=str, default="head")
    def upgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: Optional[str],
        revision: str,
        no_prompt: bool,
        include: "tuple[str, ...]",
        exclude: "tuple[str, ...]",
        dry_run: bool,
    ) -> None:
        """Upgrade the database to the latest revision."""
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()

        # Check if this is a multi-config operation
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

            for config_name, config in configs_to_process:
                console.print(f"[blue]Upgrading configuration: {config_name}[/]")
                try:
                    migration_commands = MigrationCommands(config=config)
                    migration_commands.upgrade(revision=revision)
                    console.print(f"[green]✓ Successfully upgraded: {config_name}[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed to upgrade {config_name}: {e}[/]")
        else:
            # Single config operation
            console.rule("[yellow]Starting database upgrade process[/]", align="left")
            input_confirmed = (
                True
                if no_prompt
                else Confirm.ask(f"[bold]Are you sure you want migrate the database to the `{revision}` revision?[/]")
            )
            if input_confirmed:
                sqlspec_config = get_config_by_bind_key(ctx, bind_key)
                migration_commands = MigrationCommands(config=sqlspec_config)
                migration_commands.upgrade(revision=revision)

    @database_group.command(help="Stamp the revision table with the given revision")
    @click.argument("revision", type=str)
    @bind_key_option
    def stamp(bind_key: Optional[str], revision: str) -> None:  # pyright: ignore[reportUnusedFunction]
        """Stamp the revision table with the given revision."""
        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()
        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        migration_commands = MigrationCommands(config=sqlspec_config)
        migration_commands.stamp(revision=revision)

    @database_group.command(name="init", help="Initialize migrations for the project.")
    @bind_key_option
    @click.argument("directory", default=None, required=False)
    @click.option("--package", is_flag=True, default=True, help="Create `__init__.py` for created folder")
    @no_prompt_option
    def init_sqlspec(  # pyright: ignore[reportUnusedFunction]
        bind_key: Optional[str], directory: Optional[str], package: bool, no_prompt: bool
    ) -> None:
        """Initialize the database migrations."""
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()
        console.rule("[yellow]Initializing database migrations.", align="left")
        input_confirmed = (
            True if no_prompt else Confirm.ask("[bold]Are you sure you want initialize migrations for the project?[/]")
        )
        if input_confirmed:
            configs = [get_config_by_bind_key(ctx, bind_key)] if bind_key is not None else ctx.obj["configs"]
            from sqlspec.extensions.litestar.config import DatabaseConfig

            for config in configs:
                # Extract the actual config from DatabaseConfig wrapper if needed
                actual_config = config.config if isinstance(config, DatabaseConfig) else config
                migration_config = getattr(actual_config, "migration_config", {})
                directory = migration_config.get("script_location", "migrations") if directory is None else directory
                migration_commands = MigrationCommands(config=actual_config)
                migration_commands.init(directory=cast("str", directory), package=package)

    @database_group.command(name="make-migrations", help="Create a new migration revision.")
    @bind_key_option
    @click.option("-m", "--message", default=None, help="Revision message")
    @no_prompt_option
    def create_revision(  # pyright: ignore[reportUnusedFunction]
        bind_key: Optional[str], message: Optional[str], no_prompt: bool
    ) -> None:
        """Create a new database revision."""
        from rich.prompt import Prompt

        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()
        console.rule("[yellow]Creating new migration revision[/]", align="left")
        if message is None:
            message = "new migration" if no_prompt else Prompt.ask("Please enter a message describing this revision")

        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        migration_commands = MigrationCommands(config=sqlspec_config)
        migration_commands.revision(message=message)

    @database_group.command(name="show-config", help="Show all configurations with migrations enabled.")
    def show_config() -> None:  # pyright: ignore[reportUnusedFunction]
        """Show and display all configurations with migrations enabled."""
        from rich.table import Table

        ctx = click.get_current_context()
        migration_configs = get_configs_with_migrations(ctx)

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
            table.add_row(config_name, script_location, "Migration Enabled")

        console.print(table)
        console.print(f"[blue]Found {len(migration_configs)} configuration(s) with migrations enabled.[/]")

    return database_group
