"""Flask-specific context managers for SQLSpec database sessions."""

import contextlib
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, DriverT, SyncConfigT
    from sqlspec.typing import ConnectionT


__all__ = ("FlaskSessionContext", "get_flask_connection", "provide_flask_session")


class FlaskSessionContext:
    """Context manager for Flask request-scoped database sessions.

    This context manager integrates with Flask's g object to provide
    request-scoped database connections and sessions.
    """

    def __init__(self, config: "DatabaseConfigProtocol[Any, Any, Any]", connection_key: str) -> None:
        """Initialize Flask session context.

        Args:
            config: The database configuration.
            connection_key: Key to store connection in Flask's g object.
        """
        self.config = config
        self.connection_key = connection_key
        self._connection: ConnectionT | None = None

    def __enter__(self) -> "DriverT":
        """Enter the context and return a database session.

        Returns:
            Database driver/session instance.
        """
        try:
            from flask import g
        except ImportError:
            msg = "Flask is required for FlaskSessionContext"
            raise RuntimeError(msg) from None

        # Check if connection already exists in Flask's g
        connection = getattr(g, self.connection_key, None)

        if connection is None:
            # Create new connection - this should be handled by the extension
            from sqlspec.extensions.flask._providers import create_connection_provider

            connection_provider = create_connection_provider(self.config, "pool", self.connection_key)
            connection = connection_provider()
            setattr(g, self.connection_key, connection)

        self._connection = connection

        # Create and return session/driver
        return cast("DriverT", self.config.driver_type(connection=connection))

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context and handle cleanup.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        # Connection cleanup is handled by Flask's teardown handlers
        # We don't close it here as it might be reused within the same request


def get_flask_connection(config: "DatabaseConfigProtocol[Any, Any, Any]", connection_key: str) -> "ConnectionT | None":
    """Get the current Flask connection from g object.

    Args:
        config: The database configuration.
        connection_key: Key used to store connection in Flask's g object.

    Returns:
        The connection instance if available, None otherwise.
    """
    try:
        from flask import g
        return getattr(g, connection_key, None)
    except ImportError:
        return None


@contextlib.asynccontextmanager
async def provide_flask_session(
    config: "SyncConfigT | AsyncConfigT", connection_key: str = "db_connection"
) -> "AsyncGenerator[DriverT, None]":
    """Async context manager for Flask database sessions.

    This provides a bridge between SQLSpec's async session management
    and Flask's synchronous request context.

    Args:
        config: The database configuration.
        connection_key: Key used to store connection in Flask's g object.

    Yields:
        Database driver/session instance.
    """
    try:
        from flask import g
    except ImportError:
        msg = "Flask is required for provide_flask_session"
        raise RuntimeError(msg) from None

    # Check if connection already exists in Flask's g
    connection = getattr(g, connection_key, None)

    connection_created = False
    try:
        if connection is None:
            # Create new connection
            if hasattr(config, "create_pool"):
                pool = await config.create_pool()  # type: ignore[attr-defined]
                connection_cm = config.provide_connection(pool)  # type: ignore[attr-defined]

                if hasattr(connection_cm, "__aenter__"):
                    connection = await connection_cm.__aenter__()
                else:
                    connection = await connection_cm if hasattr(connection_cm, "__await__") else connection_cm

                setattr(g, connection_key, connection)
                connection_created = True
            else:
                msg = f"Configuration {config} does not support connection creation"
                raise RuntimeError(msg)

        # Create and yield session/driver
        yield cast("DriverT", config.driver_type(connection=connection))  # type: ignore[attr-defined]

    finally:
        # Only clean up connection if we created it
        if connection_created and connection:
            with contextlib.suppress(Exception):
                if hasattr(connection, "close") and callable(connection.close):
                    await connection.close() if hasattr(connection.close, "__await__") else connection.close()

            # Remove from Flask's g object
            if hasattr(g, connection_key):
                delattr(g, connection_key)


def create_flask_request_session_provider(
    config: "DatabaseConfigProtocol[Any, Any, Any]", connection_key: str
) -> "Callable[[], DriverT]":
    """Create a Flask request-scoped session provider.

    Args:
        config: The database configuration.
        connection_key: Key used to store connection in Flask's g object.

    Returns:
        A function that provides sessions within Flask request context.
    """

    def get_session() -> "DriverT":
        """Get or create a database session for the current Flask request.

        Returns:
            Database driver/session instance.
        """
        try:
            from flask import g
        except ImportError:
            msg = "Flask is required for request-scoped sessions"
            raise RuntimeError(msg) from None

        session_key = f"_sqlspec_session_{connection_key}"

        # Check if session already exists in Flask's g
        session = getattr(g, session_key, None)
        if session is not None:
            return session

        # Get or create connection
        connection = getattr(g, connection_key, None)
        if connection is None:
            msg = f"No database connection available. Key: {connection_key}"
            raise RuntimeError(msg)

        # Create new session
        session = cast("DriverT", config.driver_type(connection=connection))
        setattr(g, session_key, session)

        return session

    return get_session


def setup_flask_session_cleanup(app: Any, connection_key: str, session_key: str) -> None:
    """Setup Flask teardown handlers for database session cleanup.

    Args:
        app: The Flask application instance.
        connection_key: Key used for connection storage.
        session_key: Key used for session storage.
    """

    @app.teardown_appcontext
    def cleanup_database_resources(exception: "Exception | None" = None) -> None:
        """Clean up database sessions and connections on request teardown.

        Args:
            exception: Exception that occurred during request processing, if any.
        """
        try:
            from flask import g
        except ImportError:
            return

        # Clean up session
        session_storage_key = f"_sqlspec_session_{connection_key}"
        session = getattr(g, session_storage_key, None)
        if session is not None:
            with contextlib.suppress(Exception):
                if hasattr(session, "close") and callable(session.close):
                    session.close()
            delattr(g, session_storage_key)

        # Clean up connection (if it exists and needs cleanup)
        connection = getattr(g, connection_key, None)
        if connection is not None:
            with contextlib.suppress(Exception):
                if hasattr(connection, "close") and callable(connection.close):
                    connection.close()
            delattr(g, connection_key)
