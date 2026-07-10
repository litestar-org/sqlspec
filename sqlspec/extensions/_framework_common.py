"""Shared helpers for the FastAPI, Starlette, Sanic, and Flask extensions.

This module is interpreted (not mypyc-compiled). It holds framework-agnostic
logic that the per-framework extension modules delegate to.
"""

from typing import Any

__all__ = ("extract_extension_settings", "should_commit", "should_rollback")

DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"
DEFAULT_COMMIT_MODE = "manual"
HTTP_200_OK = 200
HTTP_300_MULTIPLE_CHOICES = 300
HTTP_400_BAD_REQUEST = 400


def extract_extension_settings(config: Any, *, framework_key: str, sqlcommenter_framework: str) -> "dict[str, Any]":
    """Extract framework settings from config.extension_config.

    Args:
        config: Database configuration instance.
        framework_key: Key under ``config.extension_config`` holding this
            framework's settings (e.g. ``"fastapi"``, ``"starlette"``, ``"sanic"``).
        sqlcommenter_framework: Default value for the ``sqlcommenter_framework``
            setting when not explicitly configured.

    Returns:
        Dictionary of framework-specific settings.
    """
    framework_config = config.extension_config.get(framework_key, {})

    connection_key = framework_config.get("connection_key", DEFAULT_CONNECTION_KEY)
    pool_key = framework_config.get("pool_key", DEFAULT_POOL_KEY)
    session_key = framework_config.get("session_key", DEFAULT_SESSION_KEY)
    commit_mode = framework_config.get("commit_mode", DEFAULT_COMMIT_MODE)

    if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
        pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"

    correlation_headers = framework_config.get("correlation_headers")
    if correlation_headers is not None:
        correlation_headers = tuple(correlation_headers)

    return {
        "connection_key": connection_key,
        "pool_key": pool_key,
        "session_key": session_key,
        "commit_mode": commit_mode,
        "extra_commit_statuses": framework_config.get("extra_commit_statuses"),
        "extra_rollback_statuses": framework_config.get("extra_rollback_statuses"),
        "disable_di": framework_config.get("disable_di", False),
        "enable_correlation_middleware": framework_config.get("enable_correlation_middleware", False),
        "correlation_header": framework_config.get("correlation_header", "x-request-id"),
        "correlation_headers": correlation_headers,
        "auto_trace_headers": framework_config.get("auto_trace_headers", True),
        "enable_sqlcommenter_middleware": framework_config.get("enable_sqlcommenter_middleware", True),
        "sqlcommenter_framework": framework_config.get("sqlcommenter_framework", sqlcommenter_framework),
    }


def should_commit(
    status_code: int,
    commit_mode: str,
    extra_commit_statuses: "set[int] | None",
    extra_rollback_statuses: "set[int] | None",
) -> bool:
    """Determine whether a response status should trigger a commit.

    Extra commit/rollback status overrides take precedence over the commit
    mode's default status ranges. Manual mode never commits.

    Args:
        status_code: HTTP response status code.
        commit_mode: Commit mode for the configuration.
        extra_commit_statuses: Status codes that always commit.
        extra_rollback_statuses: Status codes that always roll back.

    Returns:
        ``True`` when the transaction should commit.
    """
    if extra_commit_statuses and status_code in extra_commit_statuses:
        return True
    if extra_rollback_statuses and status_code in extra_rollback_statuses:
        return False
    if commit_mode == "manual":
        return False
    if commit_mode == "autocommit":
        return HTTP_200_OK <= status_code < HTTP_300_MULTIPLE_CHOICES
    if commit_mode == "autocommit_include_redirect":
        return HTTP_200_OK <= status_code < HTTP_400_BAD_REQUEST
    return False


def should_rollback(
    status_code: int,
    commit_mode: str,
    extra_commit_statuses: "set[int] | None",
    extra_rollback_statuses: "set[int] | None",
) -> bool:
    """Determine whether a response status should trigger a rollback.

    In autocommit modes, any status that does not commit rolls back.
    Manual mode never rolls back.

    Args:
        status_code: HTTP response status code.
        commit_mode: Commit mode for the configuration.
        extra_commit_statuses: Status codes that always commit.
        extra_rollback_statuses: Status codes that always roll back.

    Returns:
        ``True`` when the transaction should roll back.
    """
    if commit_mode == "manual":
        return False
    return not should_commit(status_code, commit_mode, extra_commit_statuses, extra_rollback_statuses)
