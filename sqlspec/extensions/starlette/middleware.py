"""HTTP middleware classes for the Starlette SQLSpec extension."""

import asyncio
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager, AsyncExitStack, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, NoReturn, cast

from anyio import CancelScope
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig
from sqlspec.core import CorrelationExtractor
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.starlette._utils import (
    get_scope_tenant,
    get_state_value,
    has_state_value,
    pop_state_value,
    set_state_value,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_
from sqlspec.utils.tenant import TenantContext, TenantContextPayload, TenantRequestBinding, is_tenant_bypass_route
from sqlspec.utils.type_guards import has_name

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from starlette.responses import Response
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from sqlspec.extensions.starlette._state import SQLSpecConfigState

__all__ = (
    "CorrelationMiddleware",
    "SQLCommenterMiddleware",
    "SQLSpecAutocommitMiddleware",
    "SQLSpecManualMiddleware",
    "SQLSpecTenantRequestMiddleware",
    "acquire_pending_connection_async",
    "acquire_pending_connection_sync",
    "has_tenant_strategy",
)


class SQLSpecManualMiddleware(BaseHTTPMiddleware):
    """Middleware for manual transaction mode.

    Acquires connection from pool, stores in request.state, releases after request.
    No automatic commit or rollback - user code must handle transactions.
    """

    def __init__(self, app: Any, config_state: "SQLSpecConfigState") -> None:
        """Initialize middleware.

        Args:
            app: Starlette application instance.
            config_state: Configuration state for this database.
        """
        super().__init__(_track_downstream_completion(app, config_state))
        self.config_state = config_state

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        """Own the connection until response streaming and background work finish."""
        await _run_connection_lifecycle(self, scope, receive, send, super().__call__)

    async def dispatch(self, request: "Request", call_next: Any) -> Any:
        """Process request with manual transaction mode.

        Args:
            request: Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response.
        """
        if get_state_value(request.state, _response_owner_key(self.config_state.connection_key), False):
            return await call_next(request)
        async with self._connection_cm(request):
            return await call_next(request)

    @asynccontextmanager
    async def _connection_cm(self, request: "Request") -> "AsyncGenerator[Any]":
        """Yield a request-scoped connection for manual transaction mode."""
        async with _request_connection_cm(self.config_state, request) as connection:
            yield connection


class SQLSpecAutocommitMiddleware(BaseHTTPMiddleware):
    """Middleware for autocommit transaction mode.

    Acquires connection, commits on success status codes, rollbacks on error status codes.
    """

    def __init__(self, app: Any, config_state: "SQLSpecConfigState") -> None:
        """Initialize middleware.

        Args:
            app: Starlette application instance.
            config_state: Configuration state for this database.
        """
        super().__init__(_track_downstream_completion(app, config_state))
        self.config_state = config_state

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        """Own the connection until response streaming and background work finish."""
        await _run_connection_lifecycle(self, scope, receive, send, super().__call__)

    async def dispatch(self, request: "Request", call_next: Any) -> Any:
        """Process request with autocommit transaction mode.

        Args:
            request: Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response.
        """
        if get_state_value(request.state, _response_owner_key(self.config_state.connection_key), False):
            return await call_next(request)
        async with self._connection_cm(request):
            try:
                response = await call_next(request)
                active_connection = get_state_value(request.state, self.config_state.connection_key, None)
                if active_connection is not None:
                    await _finalize_request_transaction(
                        request, self.config_state, active_connection, commit=self._should_commit(response.status_code)
                    )
            except Exception:
                active_connection = get_state_value(request.state, self.config_state.connection_key, None)
                if active_connection is not None:
                    await _finalize_request_transaction(request, self.config_state, active_connection, commit=False)
                raise
            else:
                return response

    @asynccontextmanager
    async def _connection_cm(self, request: "Request") -> "AsyncGenerator[Any]":
        """Yield a request-scoped connection for autocommit transaction mode."""
        async with _request_connection_cm(self.config_state, request) as connection:
            yield connection

    def _should_commit(self, status_code: int) -> bool:
        """Determine if response status code should trigger commit.

        Args:
            status_code: HTTP status code.

        Returns:
            True if should commit, False if should rollback.
        """
        return self.config_state.should_commit(status_code)


class CorrelationMiddleware(BaseHTTPMiddleware):
    """Middleware for correlation ID extraction and propagation.

    Extracts correlation IDs from request headers (or generates new ones)
    and propagates them through the request lifecycle via CorrelationContext.

    The middleware:
        1. Extracts correlation ID from configurable headers
        2. Sets it in the CorrelationContext for async/sync access
        3. Stores it in request.state.correlation_id
        4. Adds X-Correlation-ID header to the response
        5. Cleans up the context on request completion
    """

    def __init__(
        self,
        app: Any,
        *,
        primary_header: str = "x-request-id",
        additional_headers: tuple[str, ...] | None = None,
        auto_trace_headers: bool = True,
        max_length: int = 128,
    ) -> None:
        """Initialize correlation middleware.

        Args:
            app: Starlette application instance.
            primary_header: The primary header to check first. Defaults to "x-request-id".
            additional_headers: Additional headers to check after the primary header.
            auto_trace_headers: If True, include standard trace context headers as fallbacks.
            max_length: Maximum length for correlation IDs. Defaults to 128.
        """
        super().__init__(app)
        self._extractor = CorrelationExtractor(
            primary_header=primary_header,
            additional_headers=additional_headers,
            auto_trace_headers=auto_trace_headers,
            max_length=max_length,
        )

    async def dispatch(self, request: "Request", call_next: Any) -> "Response":
        """Extract correlation ID and propagate through request lifecycle.

        Args:
            request: Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response with X-Correlation-ID header.
        """
        correlation_id = self._extractor.extract(lambda h: request.headers.get(h))
        previous_id = CorrelationContext.get()

        CorrelationContext.set(correlation_id)
        set_state_value(request.state, "correlation_id", correlation_id)

        try:
            response: Response = await call_next(request)
            response.headers["X-Correlation-ID"] = correlation_id
            return response
        finally:
            CorrelationContext.set(previous_id)
            pop_state_value(request.state, "correlation_id")


class SQLCommenterMiddleware(BaseHTTPMiddleware):
    """Middleware that populates SQLCommenterContext with request attributes.

    Extracts route and endpoint information from the Starlette/FastAPI request
    and sets them in :class:`~sqlspec.core.sqlcommenter.SQLCommenterContext`
    for the duration of the request.
    """

    def __init__(self, app: Any, *, framework: str = "starlette") -> None:
        """Initialize SQLCommenter middleware.

        Args:
            app: Starlette application instance.
            framework: Framework name to include in attributes.
        """
        super().__init__(app)
        self._framework = framework

    async def dispatch(self, request: "Request", call_next: Any) -> "Response":
        """Extract request context and set SQLCommenter attributes.

        Args:
            request: Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response.
        """
        attrs: dict[str, str] = {"route": request.url.path, "framework": self._framework}
        endpoint = request.scope.get("endpoint")
        if has_name(endpoint):
            attrs["action"] = endpoint.__name__

        previous = SQLCommenterContext.get()
        SQLCommenterContext.set(attrs)
        try:
            response: Response = await call_next(request)
            return response
        finally:
            SQLCommenterContext.set(previous)


class SQLSpecTenantRequestMiddleware:
    """Bind one lazy tenant resolver for each HTTP or WebSocket request.

    The plugin installs this middleware only when a configuration enables ``tenant_strategy``.
    Request start costs one context variable assignment; the route bypass marker, the request
    receipt, and the configuration's ``tenant_extractor`` run only when a lease, session, or
    ``TenantContext`` first needs the tenant. Pending resolutions finish before the final response
    message, and the binding is reset when the ASGI call returns.
    """

    __slots__ = ("_default_state", "_keys", "_states", "app")

    def __init__(self, app: "ASGIApp", config_states: "Sequence[SQLSpecConfigState]") -> None:
        """Configure the tenant-enabled configuration states.

        Args:
            app: Downstream ASGI application.
            config_states: Registered configuration states.
        """
        self.app = app
        tenant_states = [state for state in config_states if has_tenant_strategy(state.config)]
        self._states = {id(state.config): state for state in tenant_states}
        self._keys = tuple(state.config for state in tenant_states)
        self._default_state = tenant_states[0] if tenant_states else None

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        """Bind the lazy resolver for the downstream call and restore the caller afterwards."""
        if scope["type"] not in {"http", "websocket"} or self._default_state is None:
            await self.app(scope, receive, send)
            return
        binding = TenantRequestBinding(
            lambda key: self._resolve(scope, key),
            keys=self._keys,
            ambient=TenantContext.current_binding(),
            missing=lambda _key: _reject_missing_tenant(scope),
        )
        token = TenantContext.bind_request(binding)
        try:
            if scope["type"] == "http":

                async def frozen_send(message: "Message") -> None:
                    if _completes_response(message):
                        await binding.afreeze()
                    await send(message)

                await self.app(scope, receive, frozen_send)
            else:
                await self.app(scope, receive, send)
        finally:
            TenantContext.reset(token)

    def _resolve(self, scope: "Scope", key: Any) -> Any:
        state = self._states.get(id(key), self._default_state)
        if is_tenant_bypass_route(scope.get("endpoint")) or is_tenant_bypass_route(scope.get("route")):
            return TenantContextPayload.bypass()
        if state is None:
            return None
        receipt = get_scope_tenant(scope, state)
        if receipt is not None:
            return receipt
        if state.tenant_extractor is not None:
            return state.tenant_extractor(scope)
        return None


def _reject_missing_tenant(scope: "Scope") -> NoReturn:
    """Raise the Starlette response for a request without an authorized tenant."""
    from starlette.exceptions import HTTPException, WebSocketException

    user = scope.get("user")
    authenticated = user is not None and bool(getattr(user, "is_authenticated", True))
    if scope.get("type") == "websocket":
        if authenticated:
            raise WebSocketException(code=4003, reason="An authorized tenant is required")
        raise WebSocketException(code=4001, reason="Authentication required")
    if authenticated:
        raise HTTPException(status_code=403, detail="An authorized tenant is required")
    raise HTTPException(status_code=401, detail="Authentication required")


async def acquire_pending_connection_async(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Acquire a deferred tenant-scoped connection asynchronously if not yet checked out."""
    if has_state_value(request.state, config_state.connection_key):
        return get_state_value(request.state, config_state.connection_key)
    pending_key = _pending_connection_key(config_state.connection_key)
    pending = cast("_PendingTenantConnection | None", get_state_value(request.state, pending_key, None))
    if pending is None:
        return get_state_value(request.state, config_state.connection_key)
    async with pending.lock:
        if has_state_value(request.state, config_state.connection_key):
            return get_state_value(request.state, config_state.connection_key)
        connection = await pending.stack.enter_async_context(_open_config_connection(config_state, request))
        pop_state_value(request.state, pending_key)
        return connection


def acquire_pending_connection_sync(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Acquire a deferred tenant-scoped synchronous connection on demand.

    Raises:
        ImproperConfigurationError: The configuration is asynchronous.
    """
    if has_state_value(request.state, config_state.connection_key):
        return get_state_value(request.state, config_state.connection_key)
    pending_key = _pending_connection_key(config_state.connection_key)
    pending = cast("_PendingTenantConnection | None", get_state_value(request.state, pending_key, None))
    if pending is None:
        return get_state_value(request.state, config_state.connection_key)

    config = config_state.config
    if isinstance(config, (AsyncDatabaseConfig, NoPoolAsyncConfig)) or bool(getattr(config, "is_async", False)):
        msg = (
            "Async tenant-scoped connections are acquired on first use; call await plugin.get_session_async(request), "
            "await plugin.get_connection_async(request), or use Depends(plugin.provide_session())."
        )
        raise ImproperConfigurationError(msg)

    provided: Any = None
    if config.supports_connection_pooling:
        provided = config.provide_connection(get_state_value(request.app.state, config_state.pool_key))
    elif has_tenant_strategy(config):
        provided = config.provide_connection()
    if provided is not None:
        cm = cast("AbstractContextManager[Any]", provided)
        connection: Any = cm.__enter__()
        set_state_value(request.state, config_state.connection_key, connection)

        async def _exit_sync_cm(exc_type: Any, exc: Any, tb: Any) -> bool:
            _clear_published_connection(request, config_state)
            return bool(await with_ensure_async_(cm).__aexit__(exc_type, exc, tb))

        pending.stack.push_async_exit(_exit_sync_cm)
    else:
        connection = config.create_connection()
        set_state_value(request.state, config_state.connection_key, connection)

        async def _close_sync_conn(_exc_type: Any, _exc: Any, _tb: Any) -> bool:
            _clear_published_connection(request, config_state)
            await ensure_async_(connection.close)()
            return False

        pending.stack.push_async_exit(_close_sync_conn)

    pop_state_value(request.state, pending_key)
    return connection


class _PendingTenantConnection:
    """Deferred request-scoped connection slot for a tenant-enabled configuration."""

    __slots__ = ("config_state", "lock", "request", "stack")

    def __init__(self, stack: AsyncExitStack, request: "Request", config_state: "SQLSpecConfigState") -> None:
        self.stack = stack
        self.request = request
        self.config_state = config_state
        self.lock = asyncio.Lock()


def _response_owner_key(connection_key: str) -> str:
    return f"sqlspec_response_owner_{connection_key}"


def _pending_connection_key(connection_key: str) -> str:
    return f"sqlspec_pending_connection_{connection_key}"


def has_tenant_strategy(config: Any) -> bool:
    """Return whether a configuration enables an active tenant strategy."""
    features = getattr(config, "driver_features", None)
    return isinstance(features, Mapping) and features.get("tenant_strategy") not in (None, False)


def _completes_response(message: "Message") -> bool:
    return message["type"] == "http.response.pathsend" or (
        message["type"] == "http.response.body" and not message.get("more_body", False)
    )


def _clear_published_connection(request: "Request", config_state: "SQLSpecConfigState") -> None:
    pop_state_value(request.state, f"sqlspec_session_cache_{config_state.session_key}")
    pop_state_value(request.state, f"{config_state.session_key}_instance")
    pop_state_value(request.state, config_state.connection_key)


@contextmanager
def _published_connection(request: "Request", config_state: "SQLSpecConfigState", connection: Any) -> "Generator[None]":
    """Expose a connection on request state and clear every derived entry on exit.

    Yields:
        Control while the connection is visible to the request.
    """
    set_state_value(request.state, config_state.connection_key, connection)
    try:
        yield
    finally:
        _clear_published_connection(request, config_state)


@asynccontextmanager
async def _open_config_connection(config_state: "SQLSpecConfigState", request: "Request") -> "AsyncGenerator[Any]":
    config = config_state.config
    provided: Any = None
    if config.supports_connection_pooling:
        provided = config.provide_connection(get_state_value(request.app.state, config_state.pool_key))
    elif has_tenant_strategy(config):
        provided = config.provide_connection()

    if provided is not None:
        cm = with_ensure_async_(provided)
        connection = await cm.__aenter__()
        try:
            with _published_connection(request, config_state, connection):
                yield connection
        except BaseException as exc:
            if not await cm.__aexit__(type(exc), exc, exc.__traceback__):
                raise
        else:
            await cm.__aexit__(None, None, None)
        return

    connection = await ensure_async_(config.create_connection)()
    try:
        with _published_connection(request, config_state, connection):
            yield connection
    finally:
        await ensure_async_(connection.close)()


@asynccontextmanager
async def _request_connection_cm(config_state: "SQLSpecConfigState", request: "Request") -> "AsyncGenerator[Any]":
    """Own an eager or deferred request connection through verified cleanup.

    Tenant-enabled configurations defer checkout until the request first asks for a session or
    connection; the lease resolves the request tenant at that point.

    Yields:
        The active request connection, or None while a tenant connection is deferred.
    """
    async with AsyncExitStack() as stack:
        if not has_tenant_strategy(config_state.config):
            yield await stack.enter_async_context(_open_config_connection(config_state, request))
            return
        pending_key = _pending_connection_key(config_state.connection_key)
        set_state_value(request.state, pending_key, _PendingTenantConnection(stack, request, config_state))
        try:
            yield None
        finally:
            pop_state_value(request.state, pending_key)


def _track_downstream_completion(app: "ASGIApp", config_state: "SQLSpecConfigState") -> "ASGIApp":
    """Track downstream completion separately from synthesized middleware messages."""

    async def tracked_app(scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] != "http":
            await app(scope, receive, send)
            return
        completion = scope.get("state", {}).get(_response_owner_key(config_state.connection_key))

        async def tracked_send(message: "Message") -> None:
            await send(message)
            if completion is not None and _completes_response(message):
                completion["complete"] = True

        await app(scope, receive, tracked_send)

    return tracked_app


async def _run_connection_lifecycle(
    middleware: SQLSpecManualMiddleware | SQLSpecAutocommitMiddleware,
    scope: "Scope",
    receive: "Receive",
    send: "Send",
    dispatch_app: "ASGIApp",
) -> None:
    """Own request resources through the complete ASGI response invocation."""
    if scope["type"] != "http":
        await dispatch_app(scope, receive, send)
        return
    request = Request(scope, receive=receive, send=send)
    state = middleware.config_state
    owner_key = _response_owner_key(state.connection_key)
    status_code = 500
    response_complete = False

    async def tracked_send(message: "Message") -> None:
        nonlocal status_code, response_complete
        await send(message)
        if message["type"] == "http.response.start":
            status_code = message["status"]
        elif _completes_response(message):
            response_complete = True

    async with middleware._connection_cm(request):
        completion = {"complete": False}
        set_state_value(request.state, owner_key, completion)
        try:
            try:
                await dispatch_app(scope, receive, tracked_send)
            except BaseException as body_error:
                if isinstance(middleware, SQLSpecAutocommitMiddleware):
                    active_connection = get_state_value(request.state, state.connection_key, None)
                    if active_connection is not None:
                        try:
                            with CancelScope(shield=True):
                                await _finalize_request_transaction(request, state, active_connection, commit=False)
                        except BaseException as cleanup_error:
                            body_error.add_note(f"Request transaction rollback failed ({type(cleanup_error).__name__})")
                raise
            else:
                if isinstance(middleware, SQLSpecAutocommitMiddleware):
                    active_connection = get_state_value(request.state, state.connection_key, None)
                    if active_connection is not None:
                        await _finalize_request_transaction(
                            request,
                            state,
                            active_connection,
                            commit=response_complete
                            and completion["complete"]
                            and middleware._should_commit(status_code),
                        )
        finally:
            pop_state_value(request.state, owner_key)


async def _finalize_request_transaction(
    request: "Request", config_state: "SQLSpecConfigState", connection: Any, *, commit: bool
) -> None:
    """Finalize the injected driver transaction, falling back to the connection."""
    session = get_state_value(request.state, f"sqlspec_session_cache_{config_state.session_key}", None)
    if session is None:
        session = get_state_value(request.state, f"{config_state.session_key}_instance", None)
    operation = "commit" if commit else "rollback"
    target = session if session is not None and hasattr(session, operation) else connection
    callback = getattr(target, operation, None)
    if callback is not None:
        await ensure_async_(callback)()
