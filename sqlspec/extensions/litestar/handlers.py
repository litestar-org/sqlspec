import contextlib
from typing import TYPE_CHECKING, Any, Callable, Optional, cast

from litestar.constants import HTTP_DISCONNECT, HTTP_RESPONSE_START, WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import (
    delete_sqlspec_scope_state,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
from sqlspec.utils.sync_tools import maybe_async_

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Coroutine
    from contextlib import AbstractAsyncContextManager

    from litestar import Litestar
    from litestar.datastructures.state import State
    from litestar.types import Message, Scope

    from sqlspec.base import ConnectionT, DatabaseConfigProtocol, DriverT, PoolT


SESSION_TERMINUS_ASGI_EVENTS = {HTTP_RESPONSE_START, HTTP_DISCONNECT, WEBSOCKET_DISCONNECT, WEBSOCKET_CLOSE}
"""ASGI events that terminate a session scope."""


def manual_handler_maker(connection_scope_key: str) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Set up the handler to issue a transaction commit or rollback based on specified status codes
    Args:
        connection_scope_key: The key to use within the application state

    Returns:
        The handler callable
    """

    async def handler(message: "Message", scope: "Scope") -> None:
        """Handle commit/rollback, closing and cleaning up sessions before sending.

        Args:
            message: ASGI-``Message``
            scope: An ASGI-``Scope``

        """
        connection = get_sqlspec_scope_state(scope, connection_scope_key)
        if connection and message["type"] in SESSION_TERMINUS_ASGI_EVENTS:
            with contextlib.suppress(Exception):
                await maybe_async_(connection.close)()
            delete_sqlspec_scope_state(scope, connection_scope_key)

    return handler


def autocommit_handler_maker(
    connection_scope_key: str,
    commit_on_redirect: bool = False,
    extra_commit_statuses: "Optional[set[int]]" = None,
    extra_rollback_statuses: "Optional[set[int]]" = None,
) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Set up the handler to issue a transaction commit or rollback based on specified status codes
    Args:
        commit_on_redirect: Issue a commit when the response status is a redirect (``3XX``)
        extra_commit_statuses: A set of additional status codes that trigger a commit
        extra_rollback_statuses: A set of additional status codes that trigger a rollback
        connection_scope_key: The key to use within the application state

    Raises:
        ImproperConfigurationError: If extra_commit_statuses and extra_rollback_statuses share any status codes

    Returns:
        The handler callable
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
        """Handle commit/rollback, closing and cleaning up sessions before sending.

        Args:
            message: ASGI-``litestar.types.Message``
            scope: An ASGI-``litestar.types.Scope``

        """
        connection = get_sqlspec_scope_state(scope, connection_scope_key)
        try:
            if connection is not None and message["type"] == HTTP_RESPONSE_START:
                if (message["status"] in commit_range or message["status"] in extra_commit_statuses) and message[
                    "status"
                ] not in extra_rollback_statuses:
                    await maybe_async_(connection.commit)()
                else:
                    await maybe_async_(connection.rollback)()
        finally:
            if connection and message["type"] in SESSION_TERMINUS_ASGI_EVENTS:
                with contextlib.suppress(Exception):
                    await maybe_async_(connection.close)()
                delete_sqlspec_scope_state(scope, connection_scope_key)

    return handler


def lifespan_handler_maker(
    config: "DatabaseConfigProtocol[Any, Any, Any]",
    pool_key: str,
) -> "Callable[[Litestar], AbstractAsyncContextManager[None]]":
    """Build the lifespan handler for the database configuration.

    Args:
        config: The database configuration.
        pool_key: The key to use for the connection pool within Litestar.

    Returns:
        The generated lifespan handler for the connection.
    """

    @contextlib.asynccontextmanager
    async def lifespan_handler(app: "Litestar") -> "AsyncGenerator[None, None]":
        db_pool = await maybe_async_(config.create_pool)()
        app.state.update({pool_key: db_pool})
        try:
            yield
        finally:
            app.state.pop(pool_key, None)
            try:
                await maybe_async_(config.close_pool)()
            except Exception as e:  # noqa: BLE001
                if app.logger:
                    app.logger.warning("Error closing database pool for %s. Error: %s", pool_key, e)

    return lifespan_handler


def connection_provider_maker(
    connection_key: str,
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]",
) -> "Callable[[State,Scope], Awaitable[ConnectionT]]":
    """Build the connection provider for the database configuration.

    Args:
        connection_key: The dependency key to use for the session within Litestar.
        config: The database configuration.

    Returns:
        The generated connection provider for the connection.
    """

    async def provide_connection(state: "State", scope: "Scope") -> "ConnectionT":
        connection = get_sqlspec_scope_state(scope, connection_key)
        if connection is None:
            connection = await maybe_async_(config.create_connection)()
            set_sqlspec_scope_state(scope, connection_key, connection)
        return cast("ConnectionT", connection)

    return provide_connection


def pool_provider_maker(
    pool_key: str,
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]",
) -> "Callable[[State,Scope], Awaitable[PoolT]]":
    """Build the pool provider for the database configuration.

    Args:
        pool_key: The dependency key to use for the pool within Litestar.
        config: The database configuration.

    Returns:
        The generated connection pool for the database.
    """

    async def provide_pool(state: "State", scope: "Scope") -> "PoolT":
        pool = get_sqlspec_scope_state(scope, pool_key)
        if pool is None:
            pool = await maybe_async_(config.create_pool)()
            set_sqlspec_scope_state(scope, pool_key, pool)
        return cast("PoolT", pool)

    return provide_pool
