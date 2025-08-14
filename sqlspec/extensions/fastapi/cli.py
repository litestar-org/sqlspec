"""FastAPI CLI integration for SQLSpec migrations."""

from contextlib import suppress
from typing import TYPE_CHECKING, cast

from sqlspec.cli import add_migration_commands

try:
    import rich_click as click
except ImportError:
    import click  # type: ignore[no-redef]

if TYPE_CHECKING:
    from fastapi import FastAPI

    from sqlspec.extensions.fastapi.extension import SQLSpec

__all__ = ("get_database_migration_plugin", "register_database_commands")


def get_database_migration_plugin(app: "FastAPI") -> "SQLSpec":
    """Retrieve the SQLSpec plugin from the FastAPI application.

    Args:
        app: The FastAPI application

    Returns:
        The SQLSpec plugin

    Raises:
        ImproperConfigurationError: If the SQLSpec plugin is not found
    """
    from sqlspec.exceptions import ImproperConfigurationError

    # FastAPI doesn't have a built-in plugin system like Litestar
    # Check if SQLSpec was stored in app.state
    with suppress(AttributeError):
        if hasattr(app.state, "sqlspec"):
            return cast("SQLSpec", app.state.sqlspec)

    msg = "Failed to initialize database migrations. The required SQLSpec plugin is missing."
    raise ImproperConfigurationError(msg)


def register_database_commands(app: "FastAPI") -> click.Group:
    """Register database commands with a FastAPI application.

    Args:
        app: The FastAPI application instance

    Returns:
        Click group with database commands
    """

    @click.group(name="db")
    def database_group() -> None:
        """Manage SQLSpec database components."""

    # Add migration commands to the group
    add_migration_commands(database_group)

    return database_group
