"""Utility mixins and helpers for SQLSpec Flask integration."""

import contextlib
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlspec.config import DatabaseConfigProtocol, DriverT
    from sqlspec.extensions.flask.config import DatabaseConfig

__all__ = (
    "FlaskServiceMixin",
    "get_current_connection",
    "get_current_session",
    "get_flask_app",
    "is_flask_context_active",
    "with_flask_session",
)


class FlaskServiceMixin:
    """Mixin providing Flask-specific utilities for SQLSpec services."""

    def jsonify(self, data: Any, status: int = 200, **kwargs: Any) -> Any:
        """Create a JSON response using Flask's response system.

        Args:
            data: Data to serialize to JSON.
            status: HTTP status code for the response.
            **kwargs: Additional arguments to pass to the response.

        Returns:
            Flask response object with JSON content.
        """
        from flask import jsonify as flask_jsonify

        response = flask_jsonify(data)
        response.status_code = status

        # Apply any additional response modifications
        for key, value in kwargs.items():
            setattr(response, key, value)

        return response

    def get_request_args(self) -> dict[str, Any]:
        """Get request arguments from Flask request.

        Returns:
            Dictionary of request arguments.
        """
        try:
            from flask import request

            return dict(request.args) if hasattr(request, "args") else {}
        except (ImportError, RuntimeError):
            return {}

    def get_request_json(self) -> dict[str, Any]:
        """Get JSON data from Flask request.

        Returns:
            Dictionary of JSON data, empty if not available.
        """
        try:
            from flask import request

            return request.get_json() or {} if hasattr(request, "get_json") else {}
        except (ImportError, RuntimeError):
            return {}

    def get_session_for_config(self, config: "DatabaseConfig") -> Optional["DriverT"]:
        """Get database session for a specific configuration.

        Args:
            config: The database configuration.

        Returns:
            Database session if available, None otherwise.
        """
        try:
            from flask import g

            connection = getattr(g, config.connection_key, None)
            if connection and config.session_provider:
                return config.session_provider(connection)
        except (ImportError, RuntimeError):
            return None
        else:
            return None


def get_current_session(connection_key: str = "db_connection") -> Optional["DriverT"]:
    """Get the current database session from Flask's g object.

    Args:
        connection_key: Key used to identify the connection.

    Returns:
        Database session if available, None otherwise.
    """
    try:
        from flask import g

        session_key = f"_sqlspec_session_{connection_key}"
        return getattr(g, session_key, None)
    except (ImportError, RuntimeError):
        return None


def get_current_connection(connection_key: str = "db_connection") -> Any:
    """Get the current database connection from Flask's g object.

    Args:
        connection_key: Key used to identify the connection.

    Returns:
        Database connection if available, None otherwise.
    """
    try:
        from flask import g

        return getattr(g, connection_key, None)
    except (ImportError, RuntimeError):
        return None


def get_flask_app() -> Optional[Any]:
    """Get the current Flask application instance.

    Returns:
        Flask app instance if available, None otherwise.
    """
    try:
        from flask import current_app

        return current_app._get_current_object()
    except (ImportError, RuntimeError):
        return None


def is_flask_context_active() -> bool:
    """Check if Flask application context is active.

    Returns:
        True if Flask context is active, False otherwise.
    """
    try:
        from flask import has_app_context, has_request_context

        return has_app_context() or has_request_context()
    except (ImportError, RuntimeError):
        return False


@contextlib.asynccontextmanager
async def with_flask_session(
    config: "DatabaseConfigProtocol[Any, Any, Any]", connection_key: str = "db_connection"
) -> "AsyncGenerator[DriverT, None]":
    """Context manager for working with Flask database sessions.

    Args:
        config: The database configuration.
        connection_key: Key used to store connection in Flask's g object.

    Yields:
        Database session instance.
    """
    from sqlspec.extensions.flask._context import provide_flask_session

    async with provide_flask_session(config, connection_key) as session:
        yield session


def get_sqlspec_from_flask(app: Any) -> Optional[Any]:
    """Get SQLSpec extension from Flask app extensions.

    Args:
        app: Flask application instance.

    Returns:
        SQLSpec extension instance if found, None otherwise.
    """
    if not hasattr(app, "extensions"):
        return None

    extensions = app.extensions
    if "sqlspec" not in extensions:
        return None

    # Return the first SQLSpec configuration found
    for config in extensions["sqlspec"].values():
        if hasattr(config, "__class__") and "SQLSpec" in str(config.__class__):
            return config

    return None


def validate_flask_context() -> None:
    """Validate that Flask context is available.

    Raises:
        RuntimeError: If Flask context is not available.
    """
    if not is_flask_context_active():
        msg = "This operation requires an active Flask context"
        raise RuntimeError(msg)


def get_blueprint_name() -> Optional[str]:
    """Get the current blueprint name from Flask request context.

    Returns:
        Blueprint name if available, None otherwise.
    """
    try:
        from flask import request

        return request.blueprint if hasattr(request, "blueprint") else None
    except (ImportError, RuntimeError):
        return None


def get_request_endpoint() -> Optional[str]:
    """Get the current request endpoint from Flask context.

    Returns:
        Endpoint name if available, None otherwise.
    """
    try:
        from flask import request

        return request.endpoint if hasattr(request, "endpoint") else None
    except (ImportError, RuntimeError):
        return None


def create_flask_error_response(error: Exception, status_code: int = 500) -> Any:
    """Create a Flask error response from an exception.

    Args:
        error: The exception that occurred.
        status_code: HTTP status code for the response.

    Returns:
        Flask response object with error information.
    """
    try:
        from flask import jsonify

        error_data = {"error": str(error), "type": error.__class__.__name__, "status_code": status_code}

        response = jsonify(error_data)
        response.status_code = status_code
    except ImportError:
        # Fallback if Flask is not available
        return {"error": str(error), "status_code": status_code}
    else:
        return response
