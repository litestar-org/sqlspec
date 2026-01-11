"""Create Litestar session table migration using store DDL definitions."""

from typing import TYPE_CHECKING, NoReturn

from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_string

if TYPE_CHECKING:
    from sqlspec.extensions.litestar.store import BaseSQLSpecStore
    from sqlspec.migrations.context import MigrationContext

logger = get_logger("sqlspec.migrations.litestar.session")

__all__ = ("down", "up")


def _get_store_class(context: "MigrationContext | None") -> "type[BaseSQLSpecStore]":
    """Get the appropriate store class based on the config's module path.

    Args:
        context: Migration context containing config.

    Returns:
        Store class matching the config's adapter.

    Notes:
        Dynamically imports the store class from the config's module path.
        For example, AsyncpgConfig at 'sqlspec.adapters.asyncpg.config'
        maps to AsyncpgStore at 'sqlspec.adapters.asyncpg.litestar.store.AsyncpgStore'.
    """
    if not context or not context.config:
        _raise_missing_config()

    config_class = type(context.config)
    config_module = config_class.__module__
    config_name = config_class.__name__

    if not config_module.startswith("sqlspec.adapters."):
        _raise_unsupported_config(f"{config_module}.{config_name}")

    adapter_name = config_module.split(".")[2]
    store_class_name = config_name.replace("Config", "Store")

    store_path = f"sqlspec.adapters.{adapter_name}.litestar.store.{store_class_name}"

    try:
        store_class: type[BaseSQLSpecStore] = import_string(store_path)
    except ImportError as e:
        _raise_store_import_failed(store_path, e)

    return store_class


def _raise_missing_config() -> NoReturn:
    """Raise error when migration context has no config.

    Raises:
        SQLSpecError: Always raised.
    """
    msg = "Migration context must have a config to determine store class"
    raise SQLSpecError(msg)


def _raise_unsupported_config(config_type: str) -> NoReturn:
    """Raise error for unsupported config type.

    Args:
        config_type: The unsupported config type name.

    Raises:
        SQLSpecError: Always raised with config type info.
    """
    msg = f"Unsupported config type for Litestar session migration: {config_type}"
    raise SQLSpecError(msg)


def _raise_store_import_failed(store_path: str, error: ImportError) -> NoReturn:
    """Raise error when store class import fails.

    Args:
        store_path: The import path that failed.
        error: The original import error.

    Raises:
        SQLSpecError: Always raised with import details.
    """
    msg = f"Failed to import Litestar store class from {store_path}: {error}"
    raise SQLSpecError(msg) from error


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Create the litestar session table using store DDL definitions.

    This migration delegates to the appropriate store class to generate
    dialect-specific DDL. The store classes contain the single source of
    truth for session table schemas.

    Args:
        context: Migration context containing config.

    Returns:
        List of SQL statements to execute for upgrade.

    Notes:
        Table configuration is read from context.config.extension_config["litestar"].
    """
    store_class = _get_store_class(context)
    if context is None or context.config is None:
        _raise_missing_config()
    store = store_class(config=context.config)

    return [store._get_create_table_sql()]  # pyright: ignore[reportPrivateUsage]


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Drop the litestar session table using store DDL definitions.

    This migration delegates to the appropriate store class to generate
    dialect-specific DROP statements. The store classes contain the single
    source of truth for session table schemas.

    Args:
        context: Migration context containing config.

    Returns:
        List of SQL statements to execute for downgrade.

    Notes:
        Table configuration is read from context.config.extension_config["litestar"].
    """
    store_class = _get_store_class(context)
    if context is None or context.config is None:
        _raise_missing_config()
    store = store_class(config=context.config)

    return store._get_drop_table_sql()  # pyright: ignore[reportPrivateUsage]
