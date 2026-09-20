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
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, unquote, urlsplit

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
    "parse_mysql_dsn",
    "parse_odbc_connection_string",
    "parse_pyproject_config",
    "resolve_config_async",
    "resolve_config_sync",
)


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

    tool_section = data.get("tool", {})
    if not isinstance(tool_section, dict):
        return None

    sqlspec_section = tool_section.get("sqlspec", {})
    if not isinstance(sqlspec_section, dict):
        return None

    config = sqlspec_section.get("config")
    if config is None:
        return None

    if isinstance(config, str):
        return config

    if isinstance(config, list):
        if not all(isinstance(item, str) for item in config):
            msg = f"Invalid [tool.sqlspec].config in {pyproject_path}: list items must be strings"
            raise ValueError(msg)
        return ",".join(config)

    msg = f"Invalid [tool.sqlspec].config in {pyproject_path}: must be string or list of strings, got {type(config).__name__}"
    raise ValueError(msg)


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
        config_obj = import_string(_normalize_config_path(config_path))
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
        config_obj = import_string(_normalize_config_path(config_path))
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


def parse_odbc_connection_string(conn_str: str) -> list[tuple[str, str]]:
    """Tokenize a semicolon-delimited ODBC connection string into key-value pairs.

    Handles quoted values enclosed in braces and doubled closing braces per the
    MS-ODBCSTR specification.

    Args:
        conn_str: Semicolon-delimited ODBC connection string.

    Returns:
        List of key-value tuples preserving occurrence order and brace enclosures.
    """
    pairs: list[tuple[str, str]] = []
    i = 0
    n = len(conn_str)
    while i < n:
        while i < n and conn_str[i] in " ;":
            i += 1
        if i >= n:
            break
        eq = conn_str.find("=", i)
        if eq == -1:
            break
        key = conn_str[i:eq].strip()
        i = eq + 1
        while i < n and conn_str[i] in " \t":
            i += 1
        if i >= n:
            pairs.append((key, ""))
            break
        if conn_str[i] == "{":
            val_chars = ["{"]
            i += 1
            while i < n:
                ch = conn_str[i]
                if ch == "}":
                    if i + 1 < n and conn_str[i + 1] == "}":
                        val_chars.append("}}")
                        i += 2
                    else:
                        val_chars.append("}")
                        i += 1
                        break
                else:
                    val_chars.append(ch)
                    i += 1
            pairs.append((key, "".join(val_chars)))
            while i < n and conn_str[i] != ";":
                i += 1
            if i < n and conn_str[i] == ";":
                i += 1
        else:
            semi = conn_str.find(";", i)
            if semi == -1:
                pairs.append((key, conn_str[i:].strip()))
                break
            pairs.append((key, conn_str[i:semi].strip()))
            i = semi + 1
    return pairs


def parse_mysql_dsn(dsn: str) -> dict[str, Any]:
    """Parse a MySQL connection DSN or URL into keyword arguments.

    Args:
        dsn: Connection string formatted as URL or key-value pairs.

    Returns:
        Dictionary of connection parameter keyword arguments.
    """
    if "://" in dsn:
        parsed = urlsplit(dsn)
        params: dict[str, Any] = {}
        if parsed.username is not None:
            params["user"] = unquote(parsed.username)
        if parsed.password is not None:
            params["password"] = unquote(parsed.password)
        if parsed.hostname is not None:
            params["host"] = parsed.hostname
        if parsed.port is not None:
            params["port"] = parsed.port
        path = parsed.path.lstrip("/")
        if path:
            params["database"] = unquote(path)
        if parsed.query:
            query = parse_qs(parsed.query)
            for k, v in query.items():
                if v:
                    val = v[-1]
                    if val.lower() == "true":
                        params[k] = True
                    elif val.lower() == "false":
                        params[k] = False
                    elif val.isdigit():
                        params[k] = int(val)
                    else:
                        params[k] = val
        return params
    key_value_params: dict[str, Any] = {}
    for item in dsn.split(";"):
        if "=" in item:
            param_key, param_val = item.split("=", 1)
            key_value_params[param_key.strip()] = param_val.strip()
    return key_value_params


def _normalize_config_path(config_path: str) -> str:
    """Normalize supported config resolver path syntax to a dotted path.

    Args:
        config_path: Dotted ``module.attribute`` or ``module:attribute`` path.

    Returns:
        A dotted path accepted by :func:`import_string`.

    Raises:
        ConfigResolverError: If the path uses ``:`` but is not ``module:attribute``.
    """
    module_path, separator, attribute_path = config_path.partition(":")
    if not separator:
        return config_path
    if not module_path or not attribute_path or ":" in attribute_path:
        msg = (
            f"Config path '{config_path}' is not a valid reference. "
            "Use 'module:attribute' with a single ':', or dotted 'module.attribute'."
        )
        raise ConfigResolverError(msg)
    return f"{module_path}.{attribute_path}"


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
    if isinstance(config_result, ModuleType):
        raise ConfigResolverError(_describe_module_reference(config_result, config_path))

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

        return cast(
            "list[AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]]",
            [_unwrap_nested_config(config) for config in config_result],  # pyright: ignore
        )

    if not _is_valid_config(config_result):
        msg = f"Config '{config_path}' returned invalid type '{type(config_result).__name__}'. Expected database config instance or list."
        raise ConfigResolverError(msg)

    return cast(
        "AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]", _unwrap_nested_config(config_result)
    )


def _is_direct_config(config: Any) -> bool:
    """Check whether an object is itself a database config rather than a wrapper.

    Args:
        config: Object to inspect.

    Returns:
        True if the object carries migration and connection configuration itself.
    """
    if isinstance(config, type) or not has_migration_config(config) or config.migration_config is None:
        return False
    return has_connection_config(config) or has_database_url_and_bind_key(config)


def _unwrap_nested_config(config: Any) -> Any:
    """Return the database config held by a wrapper object.

    Args:
        config: Resolved object, either a config or a wrapper exposing ``.config``.

    Returns:
        The nested config when the object only wraps one, otherwise the object itself.
    """
    if _is_direct_config(config):
        return config
    if has_config_attribute(config) and has_migration_config(config.config):
        return config.config
    return config


def _describe_module_reference(module: "ModuleType", config_path: str) -> str:
    """Build an actionable error message for a config path that names a module.

    Args:
        module: Module the config path resolved to.
        config_path: Original config path supplied by the user.

    Returns:
        Error message naming the configurations the module exports, when it has any.
    """
    candidates = sorted(
        name
        for name, value in vars(module).items()
        if not name.startswith("_")
        and (
            _is_valid_config(value)
            or (
                isinstance(value, Sequence)
                and not isinstance(value, str)
                and bool(value)
                and all(_is_valid_config(item) for item in value)
            )
        )
    )
    if candidates:
        examples = ", ".join(f"'{config_path}:{name}'" for name in candidates)
        return (
            f"Config '{config_path}' names a module, not a database configuration. "
            f"Point at the configuration itself, for example {examples}."
        )
    return (
        f"Config '{config_path}' names a module that exports no database configuration. "
        "Point at a config instance, a list of configs, or a factory returning them, "
        "using 'module:attribute' or 'module.attribute'."
    )


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
