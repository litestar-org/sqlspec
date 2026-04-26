from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from sqlspec.extensions.sanic._state import SanicConfigState

__all__ = (
    "get_connection_from_request",
    "get_context_value",
    "get_or_create_session",
    "has_context_value",
    "pop_context_value",
    "set_context_value",
)

_MISSING = object()


def get_context_value(context: Any, key: str, default: Any = _MISSING) -> Any:
    """Get a value from a Sanic ``ctx`` object.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to retrieve.
        default: Optional value returned when the key is missing.

    Returns:
        Stored context value.
    """
    if default is _MISSING:
        return getattr(context, key)
    return getattr(context, key, default)


def set_context_value(context: Any, key: str, value: Any) -> None:
    """Set a value on a Sanic ``ctx`` object.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to set.
        value: Value to store.
    """
    setattr(context, key, value)


def pop_context_value(context: Any, key: str) -> Any | None:
    """Remove a value from a Sanic ``ctx`` object.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to remove.

    Returns:
        Removed value if present, otherwise ``None``.
    """
    if not hasattr(context, key):
        return None
    value = getattr(context, key)
    delattr(context, key)
    return value


def has_context_value(context: Any, key: str) -> bool:
    """Check if a Sanic ``ctx`` object has a stored value.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to check.

    Returns:
        ``True`` when the key is present.
    """
    return hasattr(context, key)


def get_connection_from_request(request: Any, config_state: "SanicConfigState") -> Any:
    """Get database connection from request context.

    Args:
        request: Sanic request instance.
        config_state: Configuration state for the database.

    Returns:
        Database connection object.

    Raises:
        ImproperConfigurationError: If SQLSpec request middleware has not stored a connection.
    """
    try:
        return get_context_value(request.ctx, config_state.connection_key)
    except AttributeError as exc:
        msg = (
            f"Sanic request context does not contain SQLSpec connection '{config_state.connection_key}'. "
            "Ensure SQLSpecPlugin is initialized and its request middleware runs before accessing sessions."
        )
        raise ImproperConfigurationError(msg) from exc


def get_or_create_session(request: Any, config_state: "SanicConfigState") -> Any:
    """Get or create database session for request.

    Sessions are cached per request to return the same session instance across
    multiple calls in one request.

    Args:
        request: Sanic request instance.
        config_state: Configuration state for the database.

    Returns:
        Database session driver instance.
    """
    session_instance_key = f"{config_state.session_key}_instance"

    existing_session = get_context_value(request.ctx, session_instance_key, None)
    if existing_session is not None:
        return existing_session

    connection = get_connection_from_request(request, config_state)
    session = config_state.config.driver_type(
        connection=connection,
        statement_config=config_state.config.statement_config,
        driver_features=config_state.config.driver_features,
    )

    set_context_value(request.ctx, session_instance_key, session)
    return session
