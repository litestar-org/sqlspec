"""Utility functions for SQLSpec migrations."""

import importlib
import inspect
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import MigrationError
from sqlspec.migrations.templates import MigrationTemplateSettings, TemplateValidationError, build_template_settings
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import module_to_os_path
from sqlspec.utils.text import slugify

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.config import DatabaseConfigProtocol
__all__ = (
    "create_migration_file",
    "get_author",
    "resolve_default_schema",
    "resolve_extension_migrations_path",
    "resolve_tracker_schema",
)

logger = get_logger(__name__)

_MIN_MODULE_NAME_LENGTH = 2


def resolve_default_schema(migration_config: "Mapping[str, Any] | None") -> str | None:
    """Resolve the configured default migration schema.

    Args:
        migration_config: Migration configuration mapping.

    Returns:
        Default schema string when configured, otherwise ``None``.
    """
    if not migration_config:
        return None
    default_schema = migration_config.get("default_schema")
    if isinstance(default_schema, str) and default_schema:
        return default_schema
    return None


def resolve_tracker_schema(migration_config: "Mapping[str, Any] | None") -> str | None:
    """Resolve the schema for the migration tracking table.

    Args:
        migration_config: Migration configuration mapping.

    Returns:
        Explicit tracker schema, default migration schema, or None.
    """
    if not migration_config:
        return None
    version_table_schema = migration_config.get("version_table_schema")
    if isinstance(version_table_schema, str) and version_table_schema:
        return version_table_schema
    return resolve_default_schema(migration_config)


def create_migration_file(
    migrations_dir: Path,
    version: str,
    message: str,
    file_type: str | None = None,
    *,
    config: "DatabaseConfigProtocol[Any, Any, Any] | None" = None,
    template_settings: "MigrationTemplateSettings | None" = None,
) -> Path:
    """Create a new migration file from template."""

    migration_config = cast("dict[str, Any]", config.migration_config) if config is not None else {}
    settings = template_settings or build_template_settings(migration_config)
    author = get_author(migration_config.get("author"), config=config)
    safe_message = _slugify_message(message)
    file_format = settings.resolve_format(file_type)
    extension = "py" if file_format == "py" else "sql"
    filename = f"{version}_{safe_message or 'migration'}.{extension}"
    file_path = migrations_dir / filename
    context = _build_template_context(
        settings=settings,
        version=version,
        message=message,
        author=author,
        adapter=_resolve_adapter_name(config),
        project_slug=_derive_project_slug(config),
        safe_message=safe_message,
    )
    renderer = settings.profile.python.render if file_format == "py" else settings.profile.sql.render
    content = renderer(context)
    file_path.write_text(content, encoding="utf-8")
    return file_path


def get_author(
    author_config: "str | dict[str, Any] | None" = None,
    *,
    config: "DatabaseConfigProtocol[Any, Any, Any] | None" = None,
) -> str:
    """Resolve author metadata for migration templates."""

    if isinstance(author_config, str):
        token = author_config.strip()
        if not token:
            return _resolve_git_author()
        lowered = token.lower()
        if lowered == "git":
            return _resolve_git_author()
        if lowered == "system":
            return _get_system_username()
        if lowered.startswith("env:"):
            env_var = token.split(":", 1)[1].strip()
            if not env_var:
                msg = "Environment author token requires a variable name"
                raise TemplateValidationError(msg)
            return _resolve_author_from_env(env_var)
        if lowered.startswith("callable:"):
            import_path = token.split(":", 1)[1].strip()
            if not import_path:
                msg = "Callable author token requires an import path"
                raise TemplateValidationError(msg)
            return _resolve_author_callable(import_path, config)
        if ":" in token and " " not in token:
            return _resolve_author_callable(token, config)
        return token

    if isinstance(author_config, dict):
        mode = str(author_config.get("mode") or "static").lower()
        value = author_config.get("value")
        if mode == "static":
            if not isinstance(value, str) or not value.strip():
                msg = "Static author value must be a non-empty string"
                raise TemplateValidationError(msg)
            return value.strip()
        if mode == "env":
            if not isinstance(value, str) or not value.strip():
                msg = "Environment author mode requires an environment variable name"
                raise TemplateValidationError(msg)
            return _resolve_author_from_env(value.strip())
        if mode == "callable":
            if not isinstance(value, str) or not value.strip():
                msg = "Callable author mode requires an import path"
                raise TemplateValidationError(msg)
            return _resolve_author_callable(value.strip(), config)
        if mode == "system":
            return _get_system_username()
        if mode == "git":
            return _resolve_git_author()
        msg = f"Unsupported author mode '{mode}'"
        raise TemplateValidationError(msg)

    return _resolve_git_author()


def resolve_extension_migrations_path(ext_name: str, spec: "str | Path") -> "Path | None":
    """Resolve an extension ``migrations_path`` setting to a migrations directory.

    Accepts a filesystem path, or a ``<dotted.module>:<subdir>`` specification resolved
    against the installed package. Relative filesystem paths resolve against the current
    working directory, matching ``script_location``.

    Args:
        ext_name: Extension name the setting belongs to, used in error messages.
        spec: Filesystem path or ``<dotted.module>:<subdir>`` specification.

    Raises:
        MigrationError: The value is not a string or path, is empty, or names a module
            that cannot be imported.

    Returns:
        The resolved directory, or None when it does not exist.
    """
    if isinstance(spec, Path):
        candidate = spec
    elif isinstance(spec, str):
        if not spec.strip():
            msg = (
                f"Extension '{ext_name}' has an empty migrations_path; "
                f"set extension_config['{ext_name}']['migrations_path'] to a directory "
                f"or a '<dotted.module>:<subdir>' specification."
            )
            raise MigrationError(msg)
        candidate = _resolve_migrations_spec(ext_name, spec)
    else:
        msg = (
            f"Extension '{ext_name}' has an invalid migrations_path of type "
            f"{type(spec).__name__}; set extension_config['{ext_name}']['migrations_path'] "
            f"to a string or Path."
        )
        raise MigrationError(msg)

    return candidate if candidate.is_dir() else None


def _resolve_migrations_spec(ext_name: str, spec: str) -> "Path":
    """Resolve a string migrations_path to a directory, honoring module specifications.

    Args:
        ext_name: Extension name the setting belongs to, used in error messages.
        spec: Filesystem path or ``<dotted.module>:<subdir>`` specification.

    Raises:
        MigrationError: The specification names a module that cannot be imported.

    Returns:
        The resolved directory, which may not exist.
    """
    module_name, separator, subdir = spec.partition(":")
    if not separator or not _is_module_name(module_name):
        return Path(spec)

    try:
        module_path = module_to_os_path(module_name)
    except ImportError as exc:
        msg = (
            f"Extension '{ext_name}' migrations_path names module '{module_name}', "
            f"which could not be imported ({exc}); correct "
            f"extension_config['{ext_name}']['migrations_path'] or install the package."
        )
        raise MigrationError(msg) from exc

    return module_path / subdir if subdir else module_path


def _is_module_name(value: str) -> bool:
    r"""Return whether a string is a dotted Python module name.

    Single-character values are rejected so Windows drive letters in paths such as
    ``C:\\srv\\migrations`` are treated as filesystem paths rather than modules.

    Args:
        value: Candidate module name.

    Returns:
        True when every dot-separated segment is a valid identifier.
    """
    if len(value) < _MIN_MODULE_NAME_LENGTH:
        return False
    return all(segment.isidentifier() for segment in value.split("."))


def _get_git_config(config_key: str) -> str | None:
    """Retrieve git configuration value.

    Args:
        config_key: Git config key.

    Returns:
        Configuration value if found, None otherwise.
    """
    try:
        result = subprocess.run(  # noqa: S603
            ["git", "config", config_key],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError, OSError) as e:
        logger.debug("Failed to get git config %s: %s", config_key, e)

    return None


def _get_system_username() -> str:
    """Get system username from environment.

    Returns:
        Username from USER environment variable, or 'unknown' if not set.
    """
    return os.environ.get("USER", "unknown")


def _resolve_git_author() -> str:
    git_name = _get_git_config("user.name")
    git_email = _get_git_config("user.email")
    if git_name and git_email:
        return f"{git_name} <{git_email}>"
    return _get_system_username()


def _resolve_author_from_env(env_var: str) -> str:
    value = os.environ.get(env_var)
    if value:
        return value.strip()
    msg = f"Environment variable '{env_var}' is not set for migration author"
    raise TemplateValidationError(msg)


def _resolve_author_callable(import_path: str, config: "DatabaseConfigProtocol[Any, Any, Any] | None") -> str:
    def _raise_callable_error(message: str) -> None:
        msg = message
        raise TemplateValidationError(msg)

    module_name, _, attr_name = import_path.partition(":")
    if not module_name or not attr_name:
        _raise_callable_error("Callable author path must be in 'module:function' format")
    module = importlib.import_module(module_name)
    candidate_obj = module.__dict__.get(attr_name)
    if candidate_obj is None or not callable(candidate_obj):
        _raise_callable_error(f"Callable '{import_path}' is not callable")
    candidate = cast("Callable[..., Any]", candidate_obj)
    signature = inspect.signature(candidate)
    param_count = len(signature.parameters)
    if param_count > 1:
        _raise_callable_error("Author callable must accept zero or one positional argument")
    try:
        result_value: object = candidate() if param_count == 0 else candidate(config)
    except Exception as exc:  # pragma: no cover
        msg = f"Author callable '{import_path}' raised an error: {exc}"
        raise TemplateValidationError(msg) from exc
    result_str: str = str(result_value)
    return result_str


def _build_template_context(
    *,
    settings: "MigrationTemplateSettings",
    version: str,
    message: str,
    author: str,
    adapter: str,
    project_slug: str,
    safe_message: str,
) -> "dict[str, str]":
    created_at = datetime.now(timezone.utc).isoformat()
    display_message = message or "New migration"
    description = display_message.strip() or safe_message or version
    return {
        "title": settings.profile.title,
        "version": version,
        "message": display_message,
        "description": description,
        "created_at": created_at,
        "author": author,
        "adapter": adapter,
        "project_slug": project_slug,
        "slug": safe_message,
    }


def _derive_project_slug(config: "DatabaseConfigProtocol[Any, Any, Any] | None") -> str:
    if config and config.bind_key:
        source = config.bind_key
    elif config:
        source = config.__class__.__module__.split(".")[0]
    else:
        source = Path.cwd().name
    return _slugify_message(source)


def _resolve_adapter_name(config: "DatabaseConfigProtocol[Any, Any, Any] | None") -> str:
    if config is None:
        return "UnknownAdapter"
    driver_type = config.driver_type
    if driver_type is not None:
        return str(driver_type.__name__)
    return type(config).__name__


def _slugify_message(message: str) -> str:
    slug = slugify(message or "", separator="_")
    return slug[:50]
