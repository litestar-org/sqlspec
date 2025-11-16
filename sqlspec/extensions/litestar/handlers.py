import contextlib
import inspect
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, cast

from litestar.constants import HTTP_DISCONNECT, HTTP_RESPONSE_START, WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT
from litestar.params import Dependency

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import (
    delete_sqlspec_scope_state,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_

if TYPE_CHECKING:
    from collections.abc import Awaitable, Coroutine

    from litestar import Litestar
    from litestar.datastructures.state import State
    from litestar.types import Message, Scope

    from sqlspec.config import DatabaseConfigProtocol, DriverT
    from sqlspec.typing import ConnectionT, PoolT

SESSION_TERMINUS_ASGI_EVENTS = {HTTP_RESPONSE_START, HTTP_DISCONNECT, WEBSOCKET_DISCONNECT, WEBSOCKET_CLOSE}

__all__ = (
    "SESSION_TERMINUS_ASGI_EVENTS",
    "autocommit_handler_maker",
    "connection_provider_maker",
    "lifespan_handler_maker",
    "manual_handler_maker",
    "pool_provider_maker",
    "session_provider_maker",
)


def manual_handler_maker(
    connection_scope_key: str, is_async: bool = False
) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Create handler for manual connection management.

    Args:
        connection_scope_key: The key used to store the connection in the ASGI scope.
        is_async: Whether the database driver is async (uses direct await) or sync (uses ensure_async_).

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
            if is_async:
                await connection.close()
            else:
                await ensure_async_(connection.close)()
            delete_sqlspec_scope_state(scope, connection_scope_key)

    return handler


def autocommit_handler_maker(
    connection_scope_key: str,
    is_async: bool = False,
    commit_on_redirect: bool = False,
    extra_commit_statuses: "set[int] | None" = None,
    extra_rollback_statuses: "set[int] | None" = None,
) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Create handler for automatic transaction commit/rollback based on response status.

    Args:
        connection_scope_key: The key used to store the connection in the ASGI scope.
        is_async: Whether the database driver is async (uses direct await) or sync (uses ensure_async_).
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
                    if is_async:
                        await connection.commit()
                    else:
                        await ensure_async_(connection.commit)()
                elif is_async:
                    await connection.rollback()
                else:
                    await ensure_async_(connection.rollback)()
        finally:
            if connection and message["type"] in SESSION_TERMINUS_ASGI_EVENTS:
                if is_async:
                    await connection.close()
                else:
                    await ensure_async_(connection.close)()
                delete_sqlspec_scope_state(scope, connection_scope_key)

    return handler


def lifespan_handler_maker(
    config: "DatabaseConfigProtocol[Any, Any, Any]", pool_key: str
) -> "Callable[[Litestar], AbstractAsyncContextManager[None]]":
    """Create lifespan handler for managing database connection pool lifecycle.

    Args:
        config: The database configuration object.
        pool_key: The key under which the connection pool will be stored in `app.state`.

    Returns:
        The lifespan handler function.
    """

    @contextlib.asynccontextmanager
    async def lifespan_handler(app: "Litestar") -> "AsyncGenerator[None, None]":
        """Manage database pool lifecycle for the application.

        Args:
            app: The Litestar application instance.

        Yields:
            Control to application during pool lifetime.
        """
        db_pool: Any
        if config.is_async:
            db_pool = await config.create_pool()
        else:
            db_pool = await ensure_async_(config.create_pool)()
        app.state.update({pool_key: db_pool})
        try:
            yield
        finally:
            app.state.pop(pool_key, None)
            try:
                if config.is_async:
                    close_result = config.close_pool()
                    if close_result is not None:
                        await close_result
                else:
                    await ensure_async_(config.close_pool)()
            except Exception as e:
                if app.logger:
                    app.logger.warning("Error closing database pool for %s. Error: %s", pool_key, e)

    return lifespan_handler


def pool_provider_maker(
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]", pool_key: str
) -> "Callable[[State, Scope], Awaitable[PoolT]]":
    """Create provider for injecting the application-level database pool.

    Args:
        config: The database configuration object.
        pool_key: The key used to store the connection pool in `app.state`.

    Returns:
        The pool provider function.
    """

    async def provide_pool(state: "State", scope: "Scope") -> "PoolT":
        """Provide the database pool from application state.

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope (unused for app-level pool).

        Returns:
            The database connection pool.

        Raises:
            ImproperConfigurationError: If the pool is not found in `app.state`.
        """
        if (db_pool := state.get(pool_key)) is None:
            msg = (
                f"Database pool with key '{pool_key}' not found in application state. "
                "Ensure the SQLSpec lifespan handler is correctly configured and has run."
            )
            raise ImproperConfigurationError(msg)
        return cast("PoolT", db_pool)

    return provide_pool


def connection_provider_maker(
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]", pool_key: str, connection_key: str
) -> "Callable[[State, Scope], AsyncGenerator[ConnectionT, None]]":
    """Create provider for database connections with proper lifecycle management.

    Args:
        config: The database configuration object.
        pool_key: The key used to retrieve the connection pool from `app.state`.
        connection_key: The key used to store the connection in the ASGI scope.

    Returns:
        The connection provider function.
    """

    async def provide_connection(state: "State", scope: "Scope") -> "AsyncGenerator[ConnectionT, None]":
        if (db_pool := state.get(pool_key)) is None:
            msg = f"Database pool with key '{pool_key}' not found. Cannot create a connection."
            raise ImproperConfigurationError(msg)

        connection_cm: Any = config.provide_connection(db_pool)
        context_manager: AbstractAsyncContextManager[ConnectionT] | None = None

        if isinstance(connection_cm, AbstractAsyncContextManager):
            context_manager = connection_cm
        elif isinstance(connection_cm, AbstractContextManager):
            context_manager = with_ensure_async_(connection_cm)

        if context_manager is None:
            conn_instance: ConnectionT
            if inspect.isawaitable(connection_cm):
                conn_instance = await cast("Awaitable[ConnectionT]", connection_cm)
            else:
                conn_instance = cast("ConnectionT", connection_cm)
            set_sqlspec_scope_state(scope, connection_key, conn_instance)
            yield conn_instance
            return

        entered_connection = await context_manager.__aenter__()
        try:
            set_sqlspec_scope_state(scope, connection_key, entered_connection)
            yield entered_connection
        finally:
            await context_manager.__aexit__(None, None, None)
            delete_sqlspec_scope_state(scope, connection_key)

    return provide_connection


def session_provider_maker(
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]", connection_dependency_key: str
) -> "Callable[[Any], AsyncGenerator[DriverT, None]]":
    """Create provider for database driver sessions.

    Args:
        config: The database configuration object.
        connection_dependency_key: The key used for connection dependency injection.

    Returns:
        The session provider function.
    """

    async def provide_session(*args: Any, **kwargs: Any) -> "AsyncGenerator[DriverT, None]":
        connection_obj = args[0] if args else kwargs.get(connection_dependency_key)
        yield cast(
            "DriverT",
            config.driver_type(
                connection=connection_obj,
                statement_config=config.statement_config,
                driver_features=config.driver_features,
            ),
        )  # pyright: ignore

    conn_type_annotation = config.connection_type

    db_conn_param = inspect.Parameter(
        name=connection_dependency_key,
        kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
        annotation=conn_type_annotation,
        default=Dependency(skip_validation=True),
    )

    provider_signature = inspect.Signature(
        parameters=[db_conn_param],
        return_annotation=AsyncGenerator[config.driver_type, None],  # type: ignore[name-defined]
    )

    provide_session.__signature__ = provider_signature  # type: ignore[attr-defined]

    if provide_session.__annotations__ is None:
        provide_session.__annotations__ = {}

    provide_session.__annotations__[connection_dependency_key] = conn_type_annotation
    provide_session.__annotations__["return"] = AsyncGenerator[config.driver_type, None]  # type: ignore[name-defined]

    return provide_session
