"""Utility functions for SQLSpec migrations."""

import importlib
import inspect
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from sqlspec.migrations.templates import MigrationTemplateSettings, TemplateValidationError, build_template_settings
from sqlspec.utils.logging import get_logger
from sqlspec.utils.text import slugify

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.driver import AsyncDriverAdapterBase

__all__ = ("create_migration_file", "drop_all", "get_author")

logger = get_logger(__name__)


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
    author_config: Any | None = None, *, config: "DatabaseConfigProtocol[Any, Any, Any] | None" = None
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


def _get_git_config(config_key: str) -> str | None:
    """Retrieve git configuration value.

    Args:
        config_key: Git config key (e.g., 'user.name', 'user.email').

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
    except Exception as exc:  # pragma: no cover - passthrough
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


async def drop_all(engine: "AsyncDriverAdapterBase", version_table_name: str, metadata: Any | None = None) -> None:
    """Drop all tables from the database.

    Args:
        engine: The database engine/driver.
        version_table_name: Name of the version tracking table.
        metadata: Optional metadata object.

    Raises:
        NotImplementedError: Always raised.
    """
    msg = "drop_all functionality requires database-specific implementation"
    raise NotImplementedError(msg)
