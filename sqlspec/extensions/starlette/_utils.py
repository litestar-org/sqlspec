from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from starlette.requests import Request

    from sqlspec.extensions.starlette._state import _ConfigState

__all__ = ("get_connection_from_request", "get_or_create_session")


def get_connection_from_request(request: "Request", config_state: "_ConfigState") -> Any:
    """Get database connection from request state.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database connection object.

    Raises:
        AttributeError: If connection not found in request state.
    """
    return getattr(request.state, config_state.connection_key)


def get_or_create_session(request: "Request", config_state: "_ConfigState") -> Any:
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

    existing_session = getattr(request.state, session_instance_key, None)
    if existing_session is not None:
        return existing_session

    connection = get_connection_from_request(request, config_state)

    session = config_state.config.driver_type(
        connection=connection,
        statement_config=config_state.config.statement_config,
        driver_features=config_state.config.driver_features,
    )

    setattr(request.state, session_instance_key, session)
    return session
