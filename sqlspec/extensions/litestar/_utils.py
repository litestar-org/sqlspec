"""ASGI scope state utilities for the Litestar SQLSpec plugin."""

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from litestar.types import Scope

__all__ = (
    "borrowed_request_session",
    "clear_request_session",
    "delete_sqlspec_scope_state",
    "get_sqlspec_scope_state",
    "publish_request_session",
    "request_failed",
    "set_sqlspec_scope_state",
)

_SCOPE_NAMESPACE = "_sqlspec"
_REQUEST_FAILED: Any = object()
_REQUEST_SESSIONS: "ContextVar[tuple[tuple[Any, Any], ...]]" = ContextVar(
    "sqlspec_litestar_request_sessions", default=()
)


def get_sqlspec_scope_state(scope: "Scope", key: str, default: Any = None, pop: bool = False) -> Any:
    """Get an internal value from connection scope state.

    Args:
        scope: The connection scope.
        key: Key to get from internal namespace in scope state.
        default: Default value to return.
        pop: Boolean flag dictating whether the value should be deleted from the state.

    Returns:
        Value mapped to ``key`` in internal connection scope namespace.
    """
    namespace = scope.setdefault(_SCOPE_NAMESPACE, {})  # type: ignore[misc]
    return namespace.pop(key, default) if pop else namespace.get(key, default)  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]


def set_sqlspec_scope_state(scope: "Scope", key: str, value: Any) -> None:
    """Set an internal value in connection scope state.

    Args:
        scope: The connection scope.
        key: Key to set under internal namespace in scope state.
        value: Value for key.
    """
    scope.setdefault(_SCOPE_NAMESPACE, {})[key] = value  # type: ignore[misc]


def delete_sqlspec_scope_state(scope: "Scope", key: str) -> None:
    """Remove an internal value from connection scope state.

    Args:
        scope: The connection scope.
        key: Key to remove under internal namespace in scope state.
    """
    scope.setdefault(_SCOPE_NAMESPACE, {}).pop(key, None)  # type: ignore[misc]


def publish_request_session(config: Any, session: Any) -> None:
    """Record the open request session for a database configuration in the current context.

    Args:
        config: Database configuration that owns the session.
        session: Open driver session holding the request connection.
    """
    remaining = tuple(pair for pair in _REQUEST_SESSIONS.get() if pair[0] is not config)
    _REQUEST_SESSIONS.set((*remaining, (config, session)))


def clear_request_session(config_or_session: Any, *, failed: bool = False) -> None:
    """Remove a published request session by its configuration or by the session itself.

    Args:
        config_or_session: Database configuration, or the published session object.
        failed: Whether the request ended with an error. Later store operations in the same
            context then discard their writes instead of committing them.
    """
    current = _REQUEST_SESSIONS.get()
    remaining: list[tuple[Any, Any]] = []
    removed_configs: list[Any] = []
    for pair in current:
        if pair[0] is config_or_session or pair[1] is config_or_session:
            removed_configs.append(pair[0])
        else:
            remaining.append(pair)
    if not removed_configs:
        return
    if failed:
        remaining.extend((config, _REQUEST_FAILED) for config in removed_configs)
    _REQUEST_SESSIONS.set(tuple(remaining))


def borrowed_request_session(config: Any) -> Any | None:
    """Return the open request session published for a database configuration, if any.

    Args:
        config: Database configuration to match by identity.

    Returns:
        The published driver session, otherwise ``None``.
    """
    for published_config, session in _REQUEST_SESSIONS.get():
        if published_config is config and session is not _REQUEST_FAILED:
            return session
    return None


def request_failed(config: Any) -> bool:
    """Return whether the request that held a session for this configuration ended with an error.

    Args:
        config: Database configuration to match by identity.

    Returns:
        ``True`` when the request's session was released after an error in the current context.
    """
    return any(published is config and session is _REQUEST_FAILED for published, session in _REQUEST_SESSIONS.get())
