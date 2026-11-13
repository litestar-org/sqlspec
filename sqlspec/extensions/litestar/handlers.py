"""Litestar ASGI lifecycle and dependency injection providers for SQLSpec.

This module stays uncompiled under mypyc because session providers combine
*args/**kwargs with yield in async def, participate in Litestar's
@contextlib.asynccontextmanager dependency wrapping, and mutate
__signature__/__annotations__ at runtime for dependency injection.
"""

import contextlib
import inspect
from collections.abc import AsyncGenerator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, Final, cast

from litestar.constants import HTTP_RESPONSE_START, WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT
from litestar.di import NamedDependency
from litestar.enums import ScopeType
from litestar.params import SkipValidation
from litestar.types import Scope
from litestar.utils.scope.state import ScopeState

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import (
    delete_sqlspec_scope_state,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
from sqlspec.utils.adapters import resolve_adapter_name
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

if TYPE_CHECKING:
    from collections.abc import Awaitable, Coroutine

    from litestar import Litestar
    from litestar.datastructures.state import State
    from litestar.types import Message

    from sqlspec.config import DatabaseConfigProtocol, DriverT
    from sqlspec.extensions.litestar.plugin import CommitMode
    from sqlspec.typing import ConnectionT, PoolT

__all__ = (
    "SESSION_TERMINUS_ASGI_EVENTS",
    "autocommit_handler_maker",
    "build_request_session",
    "connection_provider_maker",
    "has_tenant_strategy",
    "lifespan_handler_maker",
    "manual_handler_maker",
    "pool_provider_maker",
    "record_request_exception",
    "session_provider_maker",
    "tenant_payload_key",
)

_REQUEST_EXCEPTION_KEY: Final = "request_exception"
SESSION_TERMINUS_ASGI_EVENTS = {HTTP_RESPONSE_START, WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT}
_REQUEST_SCOPE_TYPES: Final = {ScopeType.HTTP, ScopeType.WEBSOCKET, "http", "websocket"}
_LAZY_BEGIN_ADAPTERS: Final[frozenset[str]] = frozenset({
    "adbc",
    "arrow_odbc",
    "asyncpg",
    "cockroach_asyncpg",
    "duckdb",
    "psqlpy",
    "teradata",
})


async def record_request_exception(exc: BaseException, scope: "Scope") -> None:
    """Record an unhandled exception in the ASGI scope state for connection cleanup.

    Args:
        exc: The unhandled exception.
        scope: The ASGI connection scope.
    """
    set_sqlspec_scope_state(scope, _REQUEST_EXCEPTION_KEY, exc)


def tenant_payload_key(connection_key: str) -> str:
    """Return the scope-state key holding a connection's captured tenant payload."""
    return f"{connection_key}_tenant_payload"


def build_request_session(
    config: "DatabaseConfigProtocol[Any, Any, Any]", connection: Any, commit_mode: "CommitMode"
) -> Any:
    """Construct and arm a request-scoped driver session for a Litestar connection.

    Args:
        config: Database configuration supplying the driver class and runtime settings.
        connection: Active request database connection.
        commit_mode: Configured Litestar transaction commit mode.

    Returns:
        Prepared driver session bound to ``connection``.
    """
    driver = config._prepare_driver(
        config.driver_type(
            connection=connection, statement_config=config.statement_config, driver_features=config.driver_features
        )
    )
    if (
        commit_mode in {"autocommit", "autocommit_include_redirect"}
        and resolve_adapter_name(config) in _LAZY_BEGIN_ADAPTERS
    ):
        driver._auto_begin_on_write = True
    return driver


def manual_handler_maker(
    connection_scope_key: str, *, session_scope_key: str | None = None
) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Create handler for manual connection management.

    Args:
        connection_scope_key: The key used to store the connection in the ASGI scope.
        session_scope_key: Optional key used to store the driver session in the ASGI scope.

    Returns:
        The handler callable.
    """
    provider_owned_key = f"{connection_scope_key}_provider_owned"
    stream_deferred_key = f"{connection_scope_key}_stream_deferred"

    async def handler(message: "Message", scope: "Scope") -> None:
        """Handle closing and cleaning up connections on response completion or disconnect.

        Args:
            message: ASGI Message.
            scope: ASGI Scope.
        """
        connection = get_sqlspec_scope_state(scope, connection_scope_key)
        if connection is None or get_sqlspec_scope_state(scope, provider_owned_key):
            return
        message_type = message["type"]
        if message_type == "http.response.body":
            if get_sqlspec_scope_state(scope, stream_deferred_key):
                return
            if message.get("more_body", False):
                set_sqlspec_scope_state(scope, stream_deferred_key, True)
                return
            await _release_request_session(scope, connection_scope_key, session_scope_key, connection)
        elif message_type in {WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT}:
            await _release_request_session(scope, connection_scope_key, session_scope_key, connection)

    return handler


def autocommit_handler_maker(
    connection_scope_key: str,
    commit_on_redirect: bool = False,
    extra_commit_statuses: "set[int] | None" = None,
    extra_rollback_statuses: "set[int] | None" = None,
    *,
    session_scope_key: str | None = None,
) -> "Callable[[Message, Scope], Coroutine[Any, Any, None]]":
    """Create handler for automatic transaction commit/rollback based on response status.

    Args:
        connection_scope_key: The key used to store the connection in the ASGI scope.
        commit_on_redirect: Issue a commit when the response status is a redirect (3XX).
        extra_commit_statuses: A set of additional status codes that trigger a commit.
        extra_rollback_statuses: A set of additional status codes that trigger a rollback.
        session_scope_key: Optional key used to store the driver session in the ASGI scope.

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
    provider_owned_key = f"{connection_scope_key}_provider_owned"
    stream_deferred_key = f"{connection_scope_key}_stream_deferred"

    async def handler(message: "Message", scope: "Scope") -> None:
        """Handle commit/rollback on response start and release on body completion.

        Args:
            message: ASGI Message.
            scope: ASGI Scope.
        """
        connection = get_sqlspec_scope_state(scope, connection_scope_key)
        if connection is None or get_sqlspec_scope_state(scope, provider_owned_key):
            return
        if message["type"] == HTTP_RESPONSE_START:
            session = get_sqlspec_scope_state(scope, session_scope_key) if session_scope_key is not None else None
            has_exception = get_sqlspec_scope_state(scope, _REQUEST_EXCEPTION_KEY) is not None
            should_commit = (
                not has_exception
                and (message["status"] in commit_range or message["status"] in extra_commit_statuses)
                and message["status"] not in extra_rollback_statuses
            )
            finalization_error: BaseException | None = None
            try:
                await _finalize_request_session(session, connection, commit=should_commit)
            except BaseException as exc:
                finalization_error = exc
                raise
            finally:
                if finalization_error is not None:
                    await _release_request_session(
                        scope, connection_scope_key, session_scope_key, connection, exc_override=finalization_error
                    )
            return
        if message["type"] == "http.response.body":
            if get_sqlspec_scope_state(scope, stream_deferred_key):
                return
            if message.get("more_body", False):
                set_sqlspec_scope_state(scope, stream_deferred_key, True)
                return
            await _release_request_session(scope, connection_scope_key, session_scope_key, connection)
        elif message["type"] in {WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT}:
            await _release_request_session(scope, connection_scope_key, session_scope_key, connection)

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
        db_pool: Any = await ensure_async_(config.create_pool)()
        app.state.update({pool_key: db_pool})
        try:
            yield
        finally:
            app.state.pop(pool_key, None)
            try:
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
    provider_owned_key = f"{connection_key}_provider_owned"

    async def provide_connection(state: "State", scope: "Scope") -> "AsyncGenerator[ConnectionT, None]":
        if (db_pool := state.get(pool_key)) is None:
            msg = f"Database pool with key '{pool_key}' not found. Cannot create a connection."
            raise ImproperConfigurationError(msg)

        payload = await _capture_connection_tenant(config, scope, connection_key)
        try:
            context_manager: AbstractAsyncContextManager[ConnectionT] | None = None
            with TenantContext.bind(payload):
                connection_cm: Any = config.provide_connection(db_pool)
                if isinstance(connection_cm, AbstractAsyncContextManager):
                    context_manager = connection_cm
                elif isinstance(connection_cm, AbstractContextManager):
                    context_manager = with_ensure_async_(connection_cm)
                conn_instance: ConnectionT
                if context_manager is not None:
                    conn_instance = await context_manager.__aenter__()
                elif inspect.isawaitable(connection_cm):
                    conn_instance = await cast("Awaitable[ConnectionT]", connection_cm)
                else:
                    conn_instance = cast("ConnectionT", connection_cm)

            set_sqlspec_scope_state(scope, connection_key, conn_instance)
            if context_manager is None:
                try:
                    yield conn_instance
                except BaseException as exc:
                    delete_sqlspec_scope_state(scope, connection_key)
                    await _exit_connection_context(
                        None, conn_instance, scope, exc_override=exc, connection_key=connection_key
                    )
                    raise
                return

            ctx_scope_key = f"{connection_key}_ctx"
            try:
                set_sqlspec_scope_state(scope, provider_owned_key, True)
                yield conn_instance
            except BaseException as exc:
                delete_sqlspec_scope_state(scope, provider_owned_key)
                delete_sqlspec_scope_state(scope, connection_key)
                await _exit_connection_context(
                    context_manager, conn_instance, scope, exc_override=exc, connection_key=connection_key
                )
                raise
            delete_sqlspec_scope_state(scope, provider_owned_key)
            if scope.get("type") == ScopeType.HTTP and not ScopeState.from_scope(scope).response_started:
                set_sqlspec_scope_state(scope, ctx_scope_key, context_manager)
                return
            delete_sqlspec_scope_state(scope, connection_key)
            await _exit_connection_context(context_manager, conn_instance, scope, connection_key=connection_key)
        except BaseException:
            delete_sqlspec_scope_state(scope, tenant_payload_key(connection_key))
            raise

    return provide_connection


def session_provider_maker(
    config: "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]",
    connection_dependency_key: str,
    *,
    session_scope_key: str | None = None,
    commit_mode: "CommitMode" = "manual",
) -> "Callable[..., AsyncGenerator[DriverT, None]]":
    """Create a Litestar dependency provider yielding a configured SQLSpec driver session.

    Args:
        config: Database configuration supplying the driver class and statement settings.
        connection_dependency_key: Dependency key used to inject the active connection.
        session_scope_key: Optional key used to cache the driver session in the ASGI scope.
        commit_mode: Configured Litestar transaction commit mode.

    Returns:
        Async generator dependency yielding a driver session bound to the request connection.
    """

    async def provide_session(*args: Any, **kwargs: Any) -> "AsyncGenerator[DriverT, None]":
        connection_obj = args[0] if args else kwargs.get(connection_dependency_key)
        scope = cast("Scope | None", kwargs.get("scope"))
        if scope is not None and session_scope_key is not None:
            existing = get_sqlspec_scope_state(scope, session_scope_key)
            if existing is not None and getattr(existing, "connection", None) is connection_obj:
                yield cast("DriverT", existing)  # pyright: ignore
                return
            session = build_request_session(config, connection_obj, commit_mode)
            set_sqlspec_scope_state(scope, session_scope_key, session)
            runtime = session.observability
            if not runtime.is_idle:
                await runtime.emit_session_start_async(session)
            yield cast("DriverT", session)  # pyright: ignore
            return
        session = build_request_session(config, connection_obj, commit_mode)
        runtime = session.observability
        if not runtime.is_idle:
            await runtime.emit_session_start_async(session)
        yield cast("DriverT", session)  # pyright: ignore

    conn_type_annotation = config.connection_type
    injected_conn_annotation = SkipValidation[NamedDependency[conn_type_annotation]]  # type: ignore[valid-type]

    db_conn_param = inspect.Parameter(
        name=connection_dependency_key,
        kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
        annotation=injected_conn_annotation,
    )
    scope_param = inspect.Parameter(name="scope", kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=Scope)

    provider_signature = inspect.Signature(
        parameters=[db_conn_param, scope_param],
        return_annotation=AsyncGenerator[config.driver_type, None],  # type: ignore[name-defined]
    )

    provide_session.__signature__ = provider_signature  # type: ignore[attr-defined]

    if provide_session.__annotations__ is None:
        provide_session.__annotations__ = {}

    provide_session.__annotations__[connection_dependency_key] = injected_conn_annotation
    provide_session.__annotations__["scope"] = Scope
    provide_session.__annotations__["return"] = AsyncGenerator[config.driver_type, None]  # type: ignore[name-defined]

    return provide_session


def has_tenant_strategy(config: Any) -> bool:
    """Return whether a database configuration enables an active tenant strategy."""
    features = getattr(config, "driver_features", None)
    return isinstance(features, Mapping) and features.get("tenant_strategy") not in (None, False)


async def _finalize_request_session(session: Any, connection: Any, *, commit: bool) -> None:
    """Finalize a request transaction through the driver session or raw connection."""
    if (
        session is not None
        and getattr(session, "_auto_begin_on_write", False) is True
        and not session._connection_in_transaction()
    ):
        return
    if commit:
        commit_target = session if session is not None and hasattr(session, "commit") else connection
        try:
            await ensure_async_(commit_target.commit)()
        except BaseException:
            rollback_target = session if session is not None and hasattr(session, "rollback") else connection
            with contextlib.suppress(Exception):
                await ensure_async_(rollback_target.rollback)()
            raise
        return
    rollback_target = session if session is not None and hasattr(session, "rollback") else connection
    await ensure_async_(rollback_target.rollback)()


async def _release_request_session(
    scope: "Scope",
    connection_scope_key: str,
    session_scope_key: str | None,
    connection: Any,
    exc_override: BaseException | None = None,
) -> None:
    """Emit session end, detach the session, exit the connection context, and clear scope state."""
    connection_cm = get_sqlspec_scope_state(scope, f"{connection_scope_key}_ctx", pop=True)
    session = get_sqlspec_scope_state(scope, session_scope_key) if session_scope_key is not None else None
    try:
        try:
            if session is not None:
                runtime = getattr(session, "observability", None)
                if runtime is not None and not runtime.is_idle:
                    await runtime.emit_session_end_async(session)
        finally:
            if session is not None:
                detach = getattr(session, "_detach_connection", None)
                if detach is not None:
                    detach()
    finally:
        try:
            await _exit_connection_context(
                connection_cm, connection, scope, exc_override=exc_override, connection_key=connection_scope_key
            )
        finally:
            delete_sqlspec_scope_state(scope, connection_scope_key)
            delete_sqlspec_scope_state(scope, f"{connection_scope_key}_provider_owned")
            delete_sqlspec_scope_state(scope, f"{connection_scope_key}_stream_deferred")
            if session_scope_key is not None:
                delete_sqlspec_scope_state(scope, session_scope_key)
                if session_scope_key.endswith("_instance"):
                    deferred = get_sqlspec_scope_state(scope, f"{session_scope_key[:-9]}_deferred", pop=True)
                    if deferred is not None:
                        deferred_detach = getattr(deferred, "_detach_connection", None)
                        if deferred_detach is not None:
                            deferred_detach()


async def _capture_connection_tenant(
    config: "DatabaseConfigProtocol[Any, Any, Any]", scope: "Scope", connection_key: str
) -> TenantContextPayload | None:
    """Resolve and record the tenant for one request connection.

    Returns:
        The request tenant for a tenant-enabled configuration, otherwise None.
    """
    payload = await TenantContext.arequire(config) if has_tenant_strategy(config) else None
    set_sqlspec_scope_state(scope, tenant_payload_key(connection_key), payload)
    return payload


async def _exit_connection_context(
    connection_cm: Any,
    connection: Any,
    scope: "Scope",
    exc_override: BaseException | None = None,
    *,
    connection_key: str | None = None,
) -> None:
    """Exit connection context manager forwarding recorded or override exception if any.

    Args:
        connection_cm: The context manager or None.
        connection: The database connection instance.
        scope: The ASGI connection scope.
        exc_override: Optional exception overriding any scope-recorded exception.
        connection_key: Connection scope key holding its captured immutable tenant payload.
    """
    exc = exc_override if exc_override is not None else get_sqlspec_scope_state(scope, _REQUEST_EXCEPTION_KEY)
    exit_args = (type(exc), exc, exc.__traceback__) if exc is not None else (None, None, None)
    payload = (
        get_sqlspec_scope_state(scope, tenant_payload_key(connection_key), pop=True)
        if connection_key is not None
        else None
    )
    with TenantContext.bind(payload):
        if isinstance(connection_cm, AbstractAsyncContextManager):
            await connection_cm.__aexit__(*exit_args)
        elif isinstance(connection_cm, AbstractContextManager):
            await with_ensure_async_(connection_cm).__aexit__(*exit_args)
        else:
            await ensure_async_(connection.close)()
