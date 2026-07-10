"""Shared helpers for the FastAPI, Starlette, Sanic, and Flask extensions.

This module is interpreted (not mypyc-compiled). It holds framework-agnostic
logic that the per-framework extension modules delegate to.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlspec.config import DatabaseConfigProtocol

__all__ = (
    "BaseConfigState",
    "CommitMode",
    "config_state_by_key",
    "ensure_unique_keys",
    "extract_extension_settings",
    "get_or_create_session",
    "should_commit",
    "should_rollback",
    "validate_extra_statuses",
)

CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]

DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"
DEFAULT_COMMIT_MODE = "manual"
HTTP_200_OK = 200
HTTP_300_MULTIPLE_CHOICES = 300
HTTP_400_BAD_REQUEST = 400


def ensure_unique_keys(states: "Sequence[Any]", *, key_getter: "Callable[[Any], set[str]]", message: str) -> None:
    """Validate that configuration state keys are unique across configs.

    Args:
        states: Configuration state instances.
        key_getter: Callable returning the set of keys for one state.
        message: Error message template with a ``{duplicates}`` placeholder.

    Raises:
        ImproperConfigurationError: If duplicate keys are found.
    """
    all_keys: set[str] = set()

    for state in states:
        keys = key_getter(state)
        duplicates = all_keys & keys

        if duplicates:
            raise ImproperConfigurationError(message.format(duplicates=duplicates))

        all_keys.update(keys)


def config_state_by_key(states: "Sequence[Any]", key: str, *, not_found_exc: "type[Exception]", message: str) -> Any:
    """Get configuration state by session key.

    Args:
        states: Configuration state instances.
        key: Session key to search for.
        not_found_exc: Exception type raised when no state matches.
        message: Error message template with a ``{key}`` placeholder.

    Returns:
        Configuration state matching the key.

    Raises:
        Exception: The ``not_found_exc`` type when no state matches the key.
    """
    for state in states:
        if state.session_key == key:
            return state

    raise not_found_exc(message.format(key=key))


def validate_extra_statuses(
    extra_commit_statuses: "set[int] | None", extra_rollback_statuses: "set[int] | None"
) -> None:
    """Validate that extra commit and rollback statuses do not overlap.

    Args:
        extra_commit_statuses: Status codes that always commit.
        extra_rollback_statuses: Status codes that always roll back.

    Raises:
        ImproperConfigurationError: If both sets share any status codes.
    """
    if (extra_commit_statuses or set()) & (extra_rollback_statuses or set()):
        msg = "Extra rollback statuses and commit statuses must not share any status codes"
        raise ImproperConfigurationError(msg)


@dataclass
class BaseConfigState:
    """Shared per-configuration state for the Starlette and Sanic extensions.

    Tracks the keys and behavior needed to bind one SQLSpec config into a
    framework application and its request context.
    """

    config: "DatabaseConfigProtocol[Any, Any, Any]"
    connection_key: str
    pool_key: str
    session_key: str
    commit_mode: "CommitMode"
    extra_commit_statuses: "set[int] | None"
    extra_rollback_statuses: "set[int] | None"
    disable_di: bool
    enable_correlation_middleware: bool = False
    correlation_header: str = "x-request-id"
    correlation_headers: "tuple[str, ...] | None" = None
    auto_trace_headers: bool = True
    enable_sqlcommenter_middleware: bool = True

    def __post_init__(self) -> None:
        """Validate status configuration."""
        validate_extra_statuses(self.extra_commit_statuses, self.extra_rollback_statuses)


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


def get_or_create_session(
    target: Any,
    cache_key: str,
    config_state: Any,
    *,
    get_value: "Callable[[Any, str, Any], Any]",
    set_value: "Callable[[Any, str, Any], None]",
    connection_getter: "Callable[[], Any]",
) -> Any:
    """Get or create a request-scoped database session.

    Sessions are cached on the request-scoped storage target so the same
    session instance is returned for multiple calls within one request. The
    connection is only fetched when no cached session exists.

    Args:
        target: Request-scoped storage object holding cached sessions.
        cache_key: Storage key for the cached session.
        config_state: Configuration state for the database.
        get_value: Callable reading ``(target, key, default)`` from storage.
        set_value: Callable writing ``(target, key, value)`` to storage.
        connection_getter: Callable returning the request's connection.

    Returns:
        Database session (driver instance).
    """
    existing_session = get_value(target, cache_key, None)
    if existing_session is not None:
        return existing_session

    connection = connection_getter()
    session = config_state.config.driver_type(
        connection=connection,
        statement_config=config_state.config.statement_config,
        driver_features=config_state.config.driver_features,
    )

    set_value(target, cache_key, session)
    return session


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
