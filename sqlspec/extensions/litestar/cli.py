"""Litestar CLI integration for SQLSpec migrations."""

from contextlib import suppress
from typing import TYPE_CHECKING

from litestar.cli._utils import LitestarGroup

from sqlspec.cli import add_migration_commands

try:
    import rich_click as click
except ImportError:
    import click  # type: ignore[no-redef]

if TYPE_CHECKING:
    from litestar import Litestar

    from sqlspec.extensions.litestar.plugin import SQLSpec


def get_database_migration_plugin(app: "Litestar") -> "SQLSpec":
    """Retrieve the SQLSpec plugin from the Litestar application's plugins.

    Args:
        app: The Litestar application

    Returns:
        The SQLSpec plugin

    Raises:
        ImproperConfigurationError: If the SQLSpec plugin is not found
    """
    from sqlspec.exceptions import ImproperConfigurationError
    from sqlspec.extensions.litestar.plugin import SQLSpec

    with suppress(KeyError):
        return app.plugins.get(SQLSpec)
    msg = "Failed to initialize database migrations. The required SQLSpec plugin is missing."
    raise ImproperConfigurationError(msg)


@click.group(cls=LitestarGroup, name="db")
def database_group(ctx: "click.Context") -> None:
    """Manage SQLSpec database components."""
    ctx.obj = {"app": ctx.obj, "configs": get_database_migration_plugin(ctx.obj.app).config}


add_migration_commands(database_group)
