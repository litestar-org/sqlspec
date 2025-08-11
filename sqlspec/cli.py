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
            return cast("Union[AsyncDatabaseConfig[Any, Any, Any], SyncDatabaseConfig[Any, Any, Any]]", configs[0])

        for config in configs:
            config_name = getattr(config, "name", None) or getattr(config, "bind_key", None)
            if config_name == bind_key:
                return cast("Union[AsyncDatabaseConfig[Any, Any, Any], SyncDatabaseConfig[Any, Any, Any]]", config)

        console.print(f"[red]No config found for bind key: {bind_key}[/]")
        sys.exit(1)

    @database_group.command(name="show-current-revision", help="Shows the current revision for the database.")
    @bind_key_option
    @verbose_option
    def show_database_revision(bind_key: Optional[str], verbose: bool) -> None:  # pyright: ignore[reportUnusedFunction]
        """Show current database revision."""
        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()
        console.rule("[yellow]Listing current revision[/]", align="left")
        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        migration_commands = MigrationCommands(config=sqlspec_config)
        migration_commands.current(verbose=verbose)

    @database_group.command(name="downgrade", help="Downgrade database to a specific revision.")
    @bind_key_option
    @no_prompt_option
    @click.argument("revision", type=str, default="-1")
    def downgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: Optional[str], revision: str, no_prompt: bool
    ) -> None:
        """Downgrade the database to the latest revision."""
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()
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
    @click.argument("revision", type=str, default="head")
    def upgrade_database(  # pyright: ignore[reportUnusedFunction]
        bind_key: Optional[str], revision: str, no_prompt: bool
    ) -> None:
        """Upgrade the database to the latest revision."""
        from rich.prompt import Confirm

        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()
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
            for config in configs:
                migration_config = getattr(config, "migration_config", {})
                directory = migration_config.get("script_location", "migrations") if directory is None else directory
                migration_commands = MigrationCommands(config=config)
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

    return database_group
