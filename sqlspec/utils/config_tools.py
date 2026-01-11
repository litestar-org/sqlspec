"""Configuration utilities for SQLSpec.

This module consolidates configuration-related helpers:
- pyproject.toml discovery for CLI convenience
- dotted path resolution for config objects
- connection config normalization for adapters
"""

import inspect
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import ConfigResolverError, ImproperConfigurationError
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.sync_tools import async_, await_
from sqlspec.utils.type_guards import (
    has_config_attribute,
    has_connection_config,
    has_database_url_and_bind_key,
    has_migration_config,
)

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig

__all__ = (
    "discover_config_from_pyproject",
    "find_pyproject_toml",
    "normalize_connection_config",
    "parse_pyproject_config",
    "reject_pool_aliases",
    "resolve_config_async",
    "resolve_config_sync",
)


# =============================================================================
# pyproject.toml Discovery
# =============================================================================


def discover_config_from_pyproject() -> str | None:
    """Find and parse pyproject.toml for SQLSpec config.

    Walks filesystem upward from current directory to find pyproject.toml.
    Parses [tool.sqlspec] section for 'config' key.

    Returns:
        Config path(s) as string (comma-separated if list), or None if not found.
    """
    pyproject_path = find_pyproject_toml()
    if pyproject_path is None:
        return None

    return parse_pyproject_config(pyproject_path)


def find_pyproject_toml() -> "Path | None":
    """Walk filesystem upward to find pyproject.toml.

    Starts from current working directory and walks up to filesystem root.
    Stops at .git directory boundary (repository root) if found.

    Returns:
        Path to pyproject.toml, or None if not found.
    """
    current = Path.cwd()

    while True:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject

        # Stop at .git boundary (repository root)
        if (current / ".git").exists():
            return None

        # Stop at filesystem root
        if current == current.parent:
            return None

        current = current.parent


def parse_pyproject_config(pyproject_path: "Path") -> str | None:
    """Parse pyproject.toml for [tool.sqlspec] config.

    Args:
        pyproject_path: Path to pyproject.toml file.

    Returns:
        Config path(s) as string (converts list to comma-separated), or None if not found.

    Raises:
        ValueError: If [tool.sqlspec].config has invalid type (not str or list[str]).
    """
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        msg = f"Failed to parse {pyproject_path}: {e}"
        raise ValueError(msg) from e

    # Navigate to [tool.sqlspec] section
    tool_section = data.get("tool", {})
    if not isinstance(tool_section, dict):
        return None

    sqlspec_section = tool_section.get("sqlspec", {})
    if not isinstance(sqlspec_section, dict):
        return None

    # Extract config value
    config = sqlspec_section.get("config")
    if config is None:
        return None

    # Handle string config
    if isinstance(config, str):
        return config

    # Handle list config (convert to comma-separated)
    if isinstance(config, list):
        if not all(isinstance(item, str) for item in config):
            msg = f"Invalid [tool.sqlspec].config in {pyproject_path}: list items must be strings"
            raise ValueError(msg)
        return ",".join(config)

    # Invalid type
    msg = f"Invalid [tool.sqlspec].config in {pyproject_path}: must be string or list of strings, got {type(config).__name__}"
    raise ValueError(msg)


# =============================================================================
# Config Resolution
# =============================================================================


async def resolve_config_async(
    config_path: str,
) -> "list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]] | AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]":
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
) -> "list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]] | AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]":
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
) -> "list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]] | AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]":
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

        for i, config in enumerate(config_result):  # pyright: ignore
            if not _is_valid_config(config):
                msg = f"Config '{config_path}' returned invalid config at index {i}. Expected database config instance."
                raise ConfigResolverError(msg)

        return cast("list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]]", list(config_result))  # pyright: ignore

    if not _is_valid_config(config_result):
        msg = f"Config '{config_path}' returned invalid type '{type(config_result).__name__}'. Expected database config instance or list."
        raise ConfigResolverError(msg)

    return cast("AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]", config_result)


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


# =============================================================================
# Connection Config Normalization
# =============================================================================


def reject_pool_aliases(kwargs: "dict[str, Any]") -> None:
    """Reject legacy pool_config/pool_instance aliases.

    Args:
        kwargs: Keyword arguments passed to the adapter config constructor.

    Raises:
        ImproperConfigurationError: If deprecated pool aliases are supplied.
    """
    if "pool_config" in kwargs or "pool_instance" in kwargs:
        msg = (
            "pool_config and pool_instance are no longer supported. "
            "Use connection_config and connection_instance instead."
        )
        raise ImproperConfigurationError(msg)


def normalize_connection_config(
    connection_config: "Mapping[str, Any] | None", *, extra_key: str = "extra"
) -> "dict[str, Any]":
    """Normalize an adapter connection_config dictionary.

    This function:
    - Copies the provided mapping into a new dict.
    - Merges any nested dict stored under ``extra_key`` into the top-level config.
    - Ensures the extra mapping is a dictionary (or None).

    Args:
        connection_config: Raw connection configuration mapping.
        extra_key: Key holding additional keyword arguments to merge.

    Returns:
        Normalized connection configuration.

    Raises:
        ImproperConfigurationError: If ``extra_key`` exists but is not a dictionary.
    """
    normalized: dict[str, Any] = dict(connection_config) if connection_config else {}
    extras = normalized.pop(extra_key, {})
    if extras is None:
        return normalized
    if not isinstance(extras, dict):
        msg = f"The '{extra_key}' field in connection_config must be a dictionary."
        raise ImproperConfigurationError(msg)
    normalized.update(extras)
    return normalized
