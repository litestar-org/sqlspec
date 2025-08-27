"""Flask CLI integration for SQLSpec migrations."""

import contextlib
from typing import TYPE_CHECKING, cast

from flask.cli import with_appcontext

from sqlspec.cli import add_migration_commands

try:
    import rich_click as click
except ImportError:
    import click  # type: ignore[no-redef]

if TYPE_CHECKING:
    from flask import Flask

    from sqlspec.extensions.flask.extension import SQLSpec

__all__ = ("database_group", "get_database_migration_plugin")


def get_database_migration_plugin(app: "Flask") -> "SQLSpec":
    """Retrieve the SQLSpec plugin from the Flask application extensions.

    Args:
        app: The Flask application

    Returns:
        The SQLSpec plugin

    Raises:
        ImproperConfigurationError: If the SQLSpec plugin is not found
    """
    from sqlspec.exceptions import ImproperConfigurationError

    # Check if SQLSpec was stored in app.extensions
    with contextlib.suppress(AttributeError, KeyError):
        if hasattr(app, "extensions") and "sqlspec" in app.extensions:
            # Get the first SQLSpec configuration
            for config in app.extensions["sqlspec"].values():
                if hasattr(config, "__class__") and "SQLSpec" in str(config.__class__):
                    return cast("SQLSpec", config)

    msg = "Failed to initialize database migrations. The required SQLSpec plugin is missing."
    raise ImproperConfigurationError(msg)


@click.group(name="db")
@with_appcontext
def database_group() -> None:
    """Manage SQLSpec database components.

    This command group provides database management commands like migrations.
    """
    from flask import current_app

    # Ensure we have the SQLSpec extension
    get_database_migration_plugin(current_app)


# Add migration commands to the database group
add_migration_commands(database_group)
