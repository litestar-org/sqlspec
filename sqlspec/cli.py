import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union, cast

if TYPE_CHECKING:
    from click import Group

    from sqlspec.config import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig

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
        help="Dotted path to SQLAlchemy config(s) (e.g. 'myapp.config.sqlspec_configs')",
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


def add_migration_commands(database_group: Optional["Group"] = None) -> "Group":  # noqa: C901
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
        "--bind-key", help="Specify which SQLAlchemy config to use by bind key", type=str, default=None
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
    ) -> "Union[SQLAlchemyAsyncConfig, SQLAlchemySyncConfig]":
        """Get the SQLAlchemy config for the specified bind key.

        Args:
            ctx: The click context.
            bind_key: The bind key to get the config for.

        Returns:
            The SQLAlchemy config for the specified bind key.
        """
        configs = ctx.obj["configs"]
        if bind_key is None:
            return cast("Union[SQLAlchemyAsyncConfig, SQLAlchemySyncConfig]", configs[0])

        for config in configs:
            if config.bind_key == bind_key:
                return cast("Union[SQLAlchemyAsyncConfig, SQLAlchemySyncConfig]", config)

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
        migration_commands = MigrationCommands(sqlspec_config=sqlspec_config)
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
            migration_commands = MigrationCommands(sqlspec_config=sqlspec_config)
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
            migration_commands = MigrationCommands(sqlspec_config=sqlspec_config)
            migration_commands.upgrade(revision=revision)

    @database_group.command(help="Stamp the revision table with the given revision")
    @click.argument("revision", type=str)
    @bind_key_option
    def stamp(bind_key: Optional[str], revision: str) -> None:  # pyright: ignore[reportUnusedFunction]
        """Stamp the revision table with the given revision."""
        from sqlspec.migrations.commands import MigrationCommands

        ctx = click.get_current_context()
        sqlspec_config = get_config_by_bind_key(ctx, bind_key)
        migration_commands = MigrationCommands(sqlspec_config=sqlspec_config)
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
                directory = config.alembic_config.script_location if directory is None else directory
                migration_commands = MigrationCommands(sqlspec_config=config)
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
        migration_commands = MigrationCommands(sqlspec_config=sqlspec_config)
        migration_commands.revision(message=message)

    @database_group.command(name="drop-all", help="Drop all tables from the database.")
    @bind_key_option
    @no_prompt_option
    def drop_all(bind_key: Optional[str], no_prompt: bool) -> None:  # pyright: ignore[reportUnusedFunction]
        """Drop all tables from the database."""
        from anyio import run
        from rich.prompt import Confirm

        from sqlspec.base import metadata_registry
        from sqlspec.migrations.utils import drop_all

        ctx = click.get_current_context()
        console.rule("[yellow]Dropping all tables from the database[/]", align="left")
        input_confirmed = no_prompt or Confirm.ask(
            "[bold red]Are you sure you want to drop all tables from the database?"
        )

        async def _drop_all(configs: "Sequence[Union[SQLAlchemyAsyncConfig, SQLAlchemySyncConfig]]") -> None:
            for config in configs:
                engine = config.get_engine()
                await drop_all(engine, config.alembic_config.version_table_name, metadata_registry.get(config.bind_key))

        if input_confirmed:
            configs = [get_config_by_bind_key(ctx, bind_key)] if bind_key is not None else ctx.obj["configs"]
            run(_drop_all, configs)

    @database_group.command(name="dump-data", help="Dump specified tables from the database to JSON files.")
    @bind_key_option
    @click.option(
        "--table",
        "table_names",
        help="Name of the table to dump. Multiple tables can be specified. Use '*' to dump all tables.",
        type=str,
        required=True,
        multiple=True,
    )
    @click.option(
        "--dir",
        "dump_dir",
        help="Directory to save the JSON files. Defaults to WORKDIR/fixtures",
        type=click.Path(path_type=Path),
        default=Path.cwd() / "fixtures",
        required=False,
    )
    def dump_table_data(bind_key: Optional[str], table_names: tuple[str, ...], dump_dir: Path) -> None:  # pyright: ignore[reportUnusedFunction]
        """Dump table data to JSON files."""
        from anyio import run
        from rich.prompt import Confirm

        from sqlspec.alembic.utils import dump_tables
        from sqlspec.base import metadata_registry, orm_registry

        ctx = click.get_current_context()
        all_tables = "*" in table_names

        if all_tables and not Confirm.ask(
            "[yellow bold]You have specified '*'. Are you sure you want to dump all tables from the database?"
        ):
            return console.rule("[red bold]No data was dumped.", style="red", align="left")

        async def _dump_tables() -> None:
            configs = [get_config_by_bind_key(ctx, bind_key)] if bind_key is not None else ctx.obj["configs"]
            for config in configs:
                target_tables = set(metadata_registry.get(config.bind_key).tables)

                if not all_tables:
                    for table_name in set(table_names) - target_tables:
                        console.rule(
                            f"[red bold]Skipping table '{table_name}' because it is not available in the default registry",
                            style="red",
                            align="left",
                        )
                    target_tables.intersection_update(table_names)
                else:
                    console.rule("[yellow bold]Dumping all tables", style="yellow", align="left")

                models = [
                    mapper.class_ for mapper in orm_registry.mappers if mapper.class_.__table__.name in target_tables
                ]
                await dump_tables(dump_dir, config.get_session(), models)
                console.rule("[green bold]Data dump complete", align="left")

        return run(_dump_tables)

    return database_group
