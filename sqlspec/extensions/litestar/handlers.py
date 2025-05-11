import contextlib
from typing import TYPE_CHECKING, Any, Callable, Optional, cast

from litestar.constants import HTTP_DISCONNECT, HTTP_RESPONSE_START, WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import (
    delete_sqlspec_scope_state,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Coroutine
    from contextlib import AbstractAsyncContextManager

    from litestar import Litestar
    from litestar.datastructures.state import State
    from litestar.types import Message, Scope

    from sqlspec.base import DatabaseConfigProtocol, DriverT
    from sqlspec.typing import ConnectionT, PoolT


SESSION_TERMINUS_ASGI_EVENTS = {HTTP_RESPONSE_START, HTTP_DISCONNECT, WEBSOCKET_DISCONNECT, WEBSOCKET_CLOSE}
"""ASGI events that terminate a session scope."""


def manual_handler_maker(connection_scope_key: str) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Set up the handler to close the connection.

    Args:
        connection_scope_key: The key used to store the connection in the ASGI scope.

    Returns:
        The handler callable.
    """

    async def handler(message: "Message", scope: "Scope") -> None:
        """Handle closing and cleaning up connections before sending the response.

        Args:
            message: ASGI Message.
            scope: ASGI Scope.
        """
        connection = get_sqlspec_scope_state(scope, connection_scope_key)
        if connection and message["type"] in SESSION_TERMINUS_ASGI_EVENTS:
            await ensure_async_(connection.close)()
            delete_sqlspec_scope_state(scope, connection_scope_key)

    return handler


def autocommit_handler_maker(
    connection_scope_key: str,
    commit_on_redirect: bool = False,
    extra_commit_statuses: "Optional[set[int]]" = None,
    extra_rollback_statuses: "Optional[set[int]]" = None,
) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Set up the handler to issue a transaction commit or rollback based on response status codes.

    Args:
        connection_scope_key: The key used to store the connection in the ASGI scope.
        commit_on_redirect: Issue a commit when the response status is a redirect (3XX).
        extra_commit_statuses: A set of additional status codes that trigger a commit.
        extra_rollback_statuses: A set of additional status codes that trigger a rollback.

    Raises:
        ImproperConfigurationError: If extra_commit_statuses and extra_rollback_statuses share status codes.

    Returns:
        The handler callable.
    """
    if extra_commit_statuses is None:
        extra_commit_statuses = set()

    if extra_rollback_statuses is None:
        extra_rollback_statuses = set()

    if len(extra_commit_statuses & extra_rollback_statuses) > 0:
        msg = "Extra rollback statuses and commit statuses must not share any status codes"
        raise ImproperConfigurationError(msg)

    commit_range = range(200, 400 if commit_on_redirect else 300)

    async def handler(message: "Message", scope: "Scope") -> None:
        """Handle commit/rollback, closing and cleaning up connections before sending.

        Args:
            message: ASGI Message.
            scope: ASGI Scope.
        """
        connection = get_sqlspec_scope_state(scope, connection_scope_key)
        try:
            if connection is not None and message["type"] == HTTP_RESPONSE_START:
                if (message["status"] in commit_range or message["status"] in extra_commit_statuses) and message[
                    "status"
                ] not in extra_rollback_statuses:
                    await ensure_async_(connection.commit)()
                else:
                    await ensure_async_(connection.rollback)()
        finally:
            if connection and message["type"] in SESSION_TERMINUS_ASGI_EVENTS:
                await ensure_async_(connection.close)()
                delete_sqlspec_scope_state(scope, connection_scope_key)

    return handler


def lifespan_handler_maker(
    config: "DatabaseConfigProtocol[Any, Any, Any]",
    pool_key: str,
) -> "Callable[[Litestar], AbstractAsyncContextManager[None]]":
    """Build the lifespan handler for managing the database connection pool.

    The pool is created on application startup and closed on shutdown.

    Args:
        config: The database configuration object.
        pool_key: The key under which the connection pool will be stored in `app.state`.

    Returns:
        The generated lifespan handler.
    """

    @contextlib.asynccontextmanager
    async def lifespan_handler(app: "Litestar") -> "AsyncGenerator[None, None]":
        """Manages the database pool lifecycle.

        Args:
            app: The Litestar application instance.

        Yields:
            The generated lifespan handler.
        """
        db_pool = await ensure_async_(config.create_pool)()
        app.state.update({pool_key: db_pool})
        try:
            yield
        finally:
            app.state.pop(pool_key, None)
            try:
                await ensure_async_(config.close_pool)()
            except Exception as e:  # noqa: BLE001
                if app.logger:  # pragma: no cover
                    app.logger.warning("Error closing database pool for %s. Error: %s", pool_key, e)

    return lifespan_handler


def pool_provider_maker(
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]", pool_key: str
) -> "Callable[[State, Scope], Awaitable[PoolT]]":
    """Build the pool provider to inject the application-level database pool.

    Args:
        config: The database configuration object.
        pool_key: The key used to store the connection pool in `app.state`.

    Returns:
        The generated pool provider.
    """

    async def provide_pool(state: "State", scope: "Scope") -> "PoolT":  # pylint: disable=unused-argument
        """Provides the database pool from `app.state`.

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope (unused for app-level pool).


        Returns:
            The database connection pool.

        Raises:
            ImproperConfigurationError: If the pool is not found in `app.state`.
        """
        # The pool is stored in app.state by the lifespan handler.
        # state.get(key) accesses app.state[key]
        db_pool = state.get(pool_key)
        if db_pool is None:
            # This case should ideally not happen if the lifespan handler ran correctly.
            msg = (
                f"Database pool with key '{pool_key}' not found in application state. "
                "Ensure the SQLSpec lifespan handler is correctly configured and has run."
            )
            raise ImproperConfigurationError(msg)
        return cast("PoolT", db_pool)

    return provide_pool


def connection_provider_maker(
    connection_key: str,  # Key for storing connection in request scope
    pool_key: str,  # Key for retrieving pool from app state
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]",  # Needed for acquire_connection_from_pool
) -> "Callable[[State, Scope], Awaitable[ConnectionT]]":
    """Build the connection provider to inject a database connection acquired from the pool.

    Args:
        connection_key: The key to store the acquired connection in the ASGI scope for reuse
                        within the same request.
        pool_key: The key used to retrieve the connection pool from `app.state`.
        config: The database configuration object, expected to have a method
                for acquiring connections from a pool instance.

    Returns:
        The generated connection provider.
    """

    async def provide_connection(state: "State", scope: "Scope") -> "ConnectionT":
        """Provides a database connection from the application pool.

        A connection is acquired from the pool and stored in the request scope
        to be reused if the dependency is requested multiple times in the same request.
        The `before_send` handler is responsible for closing this connection (returning it to the pool).

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope.

        Returns:
            A database connection.

        Raises:
            ImproperConfigurationError: If the pool is not found or cannot provide a connection.
        """
        # Check if a connection is already stored in the current request's scope
        connection = get_sqlspec_scope_state(scope, connection_key)
        if connection is None:
            # Get the application-level pool from app.state
            db_pool = state.get(pool_key)
            if db_pool is None:
                msg = f"Database pool with key '{pool_key}' not found in application state. Cannot create a connection."
                raise ImproperConfigurationError(msg)

            connection = await ensure_async_(config.provide_connection)(db_pool)
            set_sqlspec_scope_state(scope, connection_key, connection)
        return cast("ConnectionT", connection)

    return provide_connection


def session_provider_maker(
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]", session_key: str, connection_key: str, pool_key: str
) -> "Callable[[State, Scope], Awaitable[DriverT]]":
    """Build the session provider (DriverT instance) using a pooled connection.

    Args:
        session_key: The key to store the DriverT instance in the ASGI scope.
        connection_key: The key for the underlying ConnectionT in the ASGI scope. This
                        ensures the same connection instance is used and managed.
        pool_key: The key to retrieve the connection pool from `app.state`.
        config: The database configuration object.

    Returns:
        The generated session provider.
    """

    async def provide_session(state: "State", scope: "Scope") -> "DriverT":
        """Provides a DriverT instance (session) wrapping a pooled database connection.

        The underlying connection is managed (acquired and stored in scope) similarly
        to `provide_connection`.

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope.

        Returns:
            A DriverT instance.

        Raises:
            ImproperConfigurationError: If the pool is not found or cannot provide a connection.
        """
        session = get_sqlspec_scope_state(scope, session_key)
        if session is None:
            # Get or create the underlying connection for this request scope
            connection = get_sqlspec_scope_state(scope, connection_key)
            if connection is None:
                db_pool = state.get(pool_key)
                if db_pool is None:
                    msg = f"Database pool with key '{pool_key}' not found in application state while trying to create a session."
                    raise ImproperConfigurationError(msg)
                connection = await ensure_async_(config.provide_session)(db_pool)
                set_sqlspec_scope_state(scope, connection_key, connection)

            # Create the driver/session instance with the (pooled) connection
            session = config.driver_type(connection=connection)  # pyright: ignore[reportCallIssue]
            set_sqlspec_scope_state(scope, session_key, session)
        return cast("DriverT", session)

    return provide_session
