"""Helper utilities for Flask extension."""

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from sqlspec.extensions.flask._state import FlaskConfigState
    from sqlspec.protocols import DictProtocol
    from sqlspec.utils.portal import Portal

__all__ = ("get_context_value", "get_or_create_session", "has_context_value", "pop_context_value", "set_context_value")

_MISSING = object()


def _get_context_dict(target: Any) -> dict[str, Any]:
    """Return the underlying context dictionary."""
    return cast("DictProtocol", target).__dict__


def get_context_value(target: Any, key: str, default: Any = _MISSING) -> Any:
    """Get a value from a Flask context object."""
    data = _get_context_dict(target)
    if default is _MISSING:
        return data[key]
    return data.get(key, default)


def set_context_value(target: Any, key: str, value: Any) -> None:
    """Set a value on a Flask context object."""
    _get_context_dict(target)[key] = value


def pop_context_value(target: Any, key: str) -> Any | None:
    """Remove a value from a Flask context object."""
    return _get_context_dict(target).pop(key, None)


def has_context_value(target: Any, key: str) -> bool:
    """Check if a Flask context object has a stored value."""
    return key in _get_context_dict(target)


def get_or_create_session(config_state: "FlaskConfigState", portal: "Portal | None") -> Any:
    """Get or create database session for current request.

    Sessions are cached per request in Flask g object to ensure
    the same session is reused throughout the request lifecycle.

    Args:
        config_state: Configuration state for this database.
        portal: Portal for async operations (None for sync).

    Returns:
        Database session (driver instance).
    """
    from flask import g

    cache_key = f"sqlspec_session_cache_{config_state.session_key}"

    cached_session = get_context_value(g, cache_key, None)
    if cached_session is not None:
        return cached_session

    connection = get_context_value(g, config_state.connection_key)

    session = config_state.config.driver_type(
        connection=connection, statement_config=config_state.config.statement_config
    )

    set_context_value(g, cache_key, session)
    return session
