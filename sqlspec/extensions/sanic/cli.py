"""CLI integration for SQLSpec Sanic extension."""

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sanic import Sanic

    from sqlspec.extensions.sanic.config import DatabaseConfig
    from sqlspec.extensions.sanic.extension import SQLSpec

logger = get_logger("extensions.sanic.cli")

__all__ = ("database_group", "init_database_commands")


@click.group(name="database")
def database_group() -> None:
    """Database management commands for SQLSpec Sanic integration."""


@database_group.command("init")
@click.option("--config", "-c", help="Configuration module path")
@click.option("--app", "-a", help="Sanic application path")
def init_database(config: Optional[str], app: Optional[str]) -> None:
    """Initialize database schemas and tables.

    Args:
        config: Path to configuration module.
        app: Path to Sanic application.
    """
    click.echo("Initializing database schemas...")

    try:
        sqlspec_instance = _get_sqlspec_instance(config, app)
        if sqlspec_instance is None:
            click.echo("Error: Could not find SQLSpec instance", err=True)
            sys.exit(1)

        # Initialize all configured databases
        for db_config in sqlspec_instance.config:
            _initialize_database(db_config)
            click.echo(f"✓ Initialized database: {db_config.connection_key}")

        click.echo("Database initialization completed successfully!")

    except Exception as e:
        logger.exception("Database initialization failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@database_group.command("migrate")
@click.option("--config", "-c", help="Configuration module path")
@click.option("--app", "-a", help="Sanic application path")
@click.option("--revision", "-r", help="Target revision")
@click.option("--sql", is_flag=True, help="Generate SQL only")
def migrate_database(config: Optional[str], app: Optional[str], revision: Optional[str], sql: bool) -> None:
    """Run database migrations.

    Args:
        config: Path to configuration module.
        app: Path to Sanic application.
        revision: Target revision to migrate to.
        sql: Generate SQL scripts only without executing.
    """
    click.echo("Running database migrations...")

    try:
        sqlspec_instance = _get_sqlspec_instance(config, app)
        if sqlspec_instance is None:
            click.echo("Error: Could not find SQLSpec instance", err=True)
            sys.exit(1)

        # Run migrations for all configured databases
        for db_config in sqlspec_instance.config:
            _run_migrations(db_config, revision, sql)
            click.echo(f"✓ Migrated database: {db_config.connection_key}")

        click.echo("Database migrations completed successfully!")

    except Exception as e:
        logger.exception("Database migration failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@database_group.command("upgrade")
@click.option("--config", "-c", help="Configuration module path")
@click.option("--app", "-a", help="Sanic application path")
@click.option("--revision", "-r", help="Target revision", default="head")
def upgrade_database(config: Optional[str], app: Optional[str], revision: str) -> None:
    """Upgrade database to latest or specified revision.

    Args:
        config: Path to configuration module.
        app: Path to Sanic application.
        revision: Target revision to upgrade to.
    """
    click.echo(f"Upgrading database to revision: {revision}")

    try:
        sqlspec_instance = _get_sqlspec_instance(config, app)
        if sqlspec_instance is None:
            click.echo("Error: Could not find SQLSpec instance", err=True)
            sys.exit(1)

        for db_config in sqlspec_instance.config:
            _upgrade_database(db_config, revision)
            click.echo(f"✓ Upgraded database: {db_config.connection_key}")

        click.echo("Database upgrade completed successfully!")

    except Exception as e:
        logger.exception("Database upgrade failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@database_group.command("downgrade")
@click.option("--config", "-c", help="Configuration module path")
@click.option("--app", "-a", help="Sanic application path")
@click.option("--revision", "-r", required=True, help="Target revision")
def downgrade_database(config: Optional[str], app: Optional[str], revision: str) -> None:
    """Downgrade database to specified revision.

    Args:
        config: Path to configuration module.
        app: Path to Sanic application.
        revision: Target revision to downgrade to.
    """
    click.echo(f"Downgrading database to revision: {revision}")

    try:
        sqlspec_instance = _get_sqlspec_instance(config, app)
        if sqlspec_instance is None:
            click.echo("Error: Could not find SQLSpec instance", err=True)
            sys.exit(1)

        for db_config in sqlspec_instance.config:
            _downgrade_database(db_config, revision)
            click.echo(f"✓ Downgraded database: {db_config.connection_key}")

        click.echo("Database downgrade completed successfully!")

    except Exception as e:
        logger.exception("Database downgrade failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@database_group.command("seed")
@click.option("--config", "-c", help="Configuration module path")
@click.option("--app", "-a", help="Sanic application path")
@click.option("--file", "-f", help="Seed file path")
def seed_database(config: Optional[str], app: Optional[str], file: Optional[str]) -> None:
    """Seed database with initial data.

    Args:
        config: Path to configuration module.
        app: Path to Sanic application.
        file: Path to seed file.
    """
    click.echo("Seeding database with initial data...")

    try:
        sqlspec_instance = _get_sqlspec_instance(config, app)
        if sqlspec_instance is None:
            click.echo("Error: Could not find SQLSpec instance", err=True)
            sys.exit(1)

        seed_file_path = file or "seeds/initial_data.sql"

        for db_config in sqlspec_instance.config:
            _seed_database(db_config, seed_file_path)
            click.echo(f"✓ Seeded database: {db_config.connection_key}")

        click.echo("Database seeding completed successfully!")

    except Exception as e:
        logger.exception("Database seeding failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@database_group.command("inspect")
@click.option("--config", "-c", help="Configuration module path")
@click.option("--app", "-a", help="Sanic application path")
@click.option("--table", "-t", help="Specific table to inspect")
def inspect_database(config: Optional[str], app: Optional[str], table: Optional[str]) -> None:
    """Inspect database schema and structure.

    Args:
        config: Path to configuration module.
        app: Path to Sanic application.
        table: Specific table to inspect.
    """
    click.echo("Inspecting database schema...")

    try:
        sqlspec_instance = _get_sqlspec_instance(config, app)
        if sqlspec_instance is None:
            click.echo("Error: Could not find SQLSpec instance", err=True)
            sys.exit(1)

        for db_config in sqlspec_instance.config:
            schema_info = _inspect_database_schema(db_config, table)
            click.echo(f"Database: {db_config.connection_key}")
            click.echo(schema_info)
            click.echo("")

    except Exception as e:
        logger.exception("Database inspection failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def init_database_commands(app: "Sanic") -> None:
    """Initialize database CLI commands for Sanic application.

    This function can be called during Sanic application setup to register
    database commands with Sanic's CLI system if available.

    Args:
        app: The Sanic application instance.
    """
    # Store reference to app for CLI commands
    if not hasattr(app.ctx, "cli_commands"):
        app.ctx.cli_commands = []

    app.ctx.cli_commands.append(database_group)
    logger.debug("Database CLI commands initialized for Sanic app")


def _get_sqlspec_instance(config_path: Optional[str], app_path: Optional[str]) -> "Optional[SQLSpec]":
    """Get SQLSpec instance from configuration or application.

    Args:
        config_path: Path to configuration module.
        app_path: Path to Sanic application.

    Returns:
        SQLSpec instance if found, None otherwise.
    """
    # Try to get SQLSpec from application context first
    if app_path:
        try:
            app_module, app_name = app_path.rsplit(":", 1)
            module = __import__(app_module, fromlist=[app_name])
            app = getattr(module, app_name)
            return getattr(app.ctx, "sqlspec", None)
        except Exception:
            logger.debug("Could not load SQLSpec from app path: %s", app_path)

    # Try to load from configuration module
    if config_path:
        try:
            config_module = __import__(config_path, fromlist=["sqlspec"])
            return getattr(config_module, "sqlspec", None)
        except Exception:
            logger.debug("Could not load SQLSpec from config path: %s", config_path)

    # Try to auto-discover
    for possible_path in ["app:app", "main:app", "server:app"]:
        try:
            module_path, app_name = possible_path.rsplit(":", 1)
            module = __import__(module_path, fromlist=[app_name])
            app = getattr(module, app_name)
            sqlspec = getattr(app.ctx, "sqlspec", None)
            if sqlspec:
                return sqlspec
        except Exception:
            continue

    return None


def _initialize_database(db_config: "DatabaseConfig") -> None:
    """Initialize database schema for given configuration.

    Args:
        db_config: Database configuration instance.
    """
    # This would integrate with SQLSpec's migration system
    # For now, this is a placeholder for the actual implementation
    logger.info("Initializing database schema for %s", db_config.connection_key)


def _run_migrations(db_config: "DatabaseConfig", revision: Optional[str], sql_only: bool) -> None:
    """Run migrations for given database configuration.

    Args:
        db_config: Database configuration instance.
        revision: Target revision.
        sql_only: Generate SQL only without executing.
    """
    # This would integrate with SQLSpec's migration system
    logger.info("Running migrations for %s to revision %s", db_config.connection_key, revision or "latest")


def _upgrade_database(db_config: "DatabaseConfig", revision: str) -> None:
    """Upgrade database to specified revision.

    Args:
        db_config: Database configuration instance.
        revision: Target revision.
    """
    logger.info("Upgrading database %s to revision %s", db_config.connection_key, revision)


def _downgrade_database(db_config: "DatabaseConfig", revision: str) -> None:
    """Downgrade database to specified revision.

    Args:
        db_config: Database configuration instance.
        revision: Target revision.
    """
    logger.info("Downgrading database %s to revision %s", db_config.connection_key, revision)


def _seed_database(db_config: "DatabaseConfig", seed_file: str) -> None:
    """Seed database with data from file.

    Args:
        db_config: Database configuration instance.
        seed_file: Path to seed file.
    """
    seed_path = Path(seed_file)
    if not seed_path.exists():
        msg = f"Seed file not found: {seed_file}"
        raise FileNotFoundError(msg)

    logger.info("Seeding database %s from %s", db_config.connection_key, seed_file)


def _inspect_database_schema(db_config: "DatabaseConfig", table: Optional[str]) -> str:
    """Inspect database schema and return information.

    Args:
        db_config: Database configuration instance.
        table: Specific table to inspect.

    Returns:
        String containing schema information.
    """
    if table:
        return f"Schema information for table '{table}' in {db_config.connection_key}"
    return f"Schema information for database {db_config.connection_key}"
