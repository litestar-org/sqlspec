"""Shared helpers for the FastAPI, Starlette, and Sanic extensions.

Interpreted module (not mypyc-compiled) — safe to hold plain Python helpers
that FastAPI/Starlette/Sanic (and, where field shapes align, Flask) extension
modules delegate to instead of duplicating logic per framework.
"""

from typing import Any

__all__ = ("extract_extension_settings",)

DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"
DEFAULT_COMMIT_MODE = "manual"


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
