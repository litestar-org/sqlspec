"""Configuration resolver for SQLSpec CLI.

This module provides utilities for resolving configuration objects from dotted paths,
with support for both direct config instances and callable functions that return configs.
Supports both synchronous and asynchronous callable functions.
"""

import inspect
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import ConfigResolverError
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.sync_tools import async_, await_
from sqlspec.utils.type_guards import (
    has_config_attribute,
    has_connection_config,
    has_database_url_and_bind_key,
    has_migration_config,
)

if TYPE_CHECKING:
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig

__all__ = ("resolve_config_async", "resolve_config_sync")


async def resolve_config_async(
    config_path: str,
) -> "list[AsyncDatabaseConfig | SyncDatabaseConfig] | AsyncDatabaseConfig | SyncDatabaseConfig":
    """Resolve config from dotted path, handling callables and direct instances.

    This is the async-first version that handles both sync and async callables efficiently.

    Args:
        config_path: Dotted path to config object or callable function.

    Returns:
        Resolved config instance or list of config instances.

    Raises:
        ConfigResolverError: If config resolution fails.
    """
    try:
        config_obj = import_string(config_path)
    except ImportError as e:
        msg = f"Failed to import config from path '{config_path}': {e}"
        raise ConfigResolverError(msg) from e

    if not callable(config_obj):
        return _validate_config_result(config_obj, config_path)

    try:
        if inspect.iscoroutinefunction(config_obj):
            result = await config_obj()
        else:
            result = await async_(config_obj)()
    except Exception as e:
        msg = f"Failed to execute callable config '{config_path}': {e}"
        raise ConfigResolverError(msg) from e

    return _validate_config_result(result, config_path)


def resolve_config_sync(
    config_path: str,
) -> "list[AsyncDatabaseConfig | SyncDatabaseConfig] | AsyncDatabaseConfig | SyncDatabaseConfig":
    """Synchronous wrapper for resolve_config.

    Args:
        config_path: Dotted path to config object or callable function.

    Returns:
        Resolved config instance or list of config instances.
    """
    try:
        config_obj = import_string(config_path)
    except ImportError as e:
        msg = f"Failed to import config from path '{config_path}': {e}"
        raise ConfigResolverError(msg) from e

    if not callable(config_obj):
        return _validate_config_result(config_obj, config_path)

    try:
        if inspect.iscoroutinefunction(config_obj):
            result = await_(config_obj, raise_sync_error=False)()
        else:
            result = config_obj()
    except Exception as e:
        msg = f"Failed to execute callable config '{config_path}': {e}"
        raise ConfigResolverError(msg) from e

    return _validate_config_result(result, config_path)


def _validate_config_result(
    config_result: Any, config_path: str
) -> "list[AsyncDatabaseConfig | SyncDatabaseConfig] | AsyncDatabaseConfig | SyncDatabaseConfig":
    """Validate that the config result is a valid config or list of configs.

    Args:
        config_result: The result from config resolution.
        config_path: Original config path for error messages.

    Returns:
        Validated config result.

    Raises:
        ConfigResolverError: If config result is invalid.
    """
    if config_result is None:
        msg = f"Config '{config_path}' resolved to None. Expected config instance or list of configs."
        raise ConfigResolverError(msg)

    if isinstance(config_result, Sequence) and not isinstance(config_result, str):
        if not config_result:
            msg = f"Config '{config_path}' resolved to empty list. Expected at least one config."
            raise ConfigResolverError(msg)

        for i, config in enumerate(config_result):
            if not _is_valid_config(config):
                msg = f"Config '{config_path}' returned invalid config at index {i}. Expected database config instance."
                raise ConfigResolverError(msg)

        return cast("list[AsyncDatabaseConfig | SyncDatabaseConfig]", list(config_result))

    if not _is_valid_config(config_result):
        msg = f"Config '{config_path}' returned invalid type '{type(config_result).__name__}'. Expected database config instance or list."
        raise ConfigResolverError(msg)

    return cast("AsyncDatabaseConfig | SyncDatabaseConfig", config_result)


def _is_valid_config(config: Any) -> bool:
    """Check if an object is a valid SQLSpec database config.

    Args:
        config: Object to validate.

    Returns:
        True if object is a valid config instance (not a class).
    """
    # Reject config classes - must be instances
    if isinstance(config, type):
        return False

    if has_config_attribute(config):
        nested_config = config.config
        if has_migration_config(nested_config):
            return True

    if has_migration_config(config) and config.migration_config is not None:
        if has_connection_config(config):
            return True
        if has_database_url_and_bind_key(config):
            return True

    return False
