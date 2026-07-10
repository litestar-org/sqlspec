from typing import TYPE_CHECKING, Any, cast

from sqlspec.extensions._framework_common import get_or_create_session as _shared_get_or_create_session

if TYPE_CHECKING:
    from starlette.requests import Request

    from sqlspec.extensions.starlette._state import SQLSpecConfigState

__all__ = (
    "get_connection_from_request",
    "get_or_create_session",
    "get_state_value",
    "has_state_value",
    "pop_state_value",
    "set_state_value",
)

_MISSING = object()


def get_state_value(state: Any, key: str, default: Any = _MISSING) -> Any:
    """Get a value from a Starlette state object."""
    data = _state_dict(state)
    if default is _MISSING:
        try:
            return data[key]
        except KeyError as exc:
            msg = f"'{state.__class__.__name__}' object has no attribute '{key}'"
            raise AttributeError(msg) from exc
    return data.get(key, default)


def set_state_value(state: Any, key: str, value: Any) -> None:
    """Set a value on a Starlette state object."""
    _state_dict(state)[key] = value


def pop_state_value(state: Any, key: str) -> Any | None:
    """Remove a value from a Starlette state object."""
    return _state_dict(state).pop(key, None)


def has_state_value(state: Any, key: str) -> bool:
    """Check if a Starlette state object has a stored value."""
    return key in _state_dict(state)


def get_connection_from_request(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Get database connection from request state.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database connection object.
    """
    return get_state_value(request.state, config_state.connection_key)


def get_or_create_session(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Get or create database session for request.

    Sessions are cached per request to ensure the same session instance
    is returned for multiple calls within the same request.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database session (driver instance).
    """
    session_instance_key = f"{config_state.session_key}_instance"

    return _shared_get_or_create_session(
        request.state,
        session_instance_key,
        config_state,
        get_value=get_state_value,
        set_value=set_state_value,
        connection_getter=lambda: get_connection_from_request(request, config_state),
    )


def _state_dict(state: Any) -> dict[str, Any]:
    """Return the underlying state dictionary."""
    try:
        return cast("dict[str, Any]", object.__getattribute__(state, "_state"))
    except AttributeError:
        return cast("dict[str, Any]", state.__dict__)
