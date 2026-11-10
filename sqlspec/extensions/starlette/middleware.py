"""HTTP middleware classes for the Starlette SQLSpec extension."""

from contextlib import asynccontextmanager
from inspect import isawaitable
from typing import TYPE_CHECKING, Any

from anyio import CancelScope
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from sqlspec.core import CorrelationExtractor
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.extensions.starlette._utils import get_state_value, pop_state_value, set_state_value
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_
from sqlspec.utils.tenant import TenantContext, coerce_tenant_payload
from sqlspec.utils.type_guards import has_name

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.responses import Response
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from sqlspec.extensions.starlette._state import SQLSpecConfigState

__all__ = ("CorrelationMiddleware", "SQLCommenterMiddleware", "SQLSpecAutocommitMiddleware", "SQLSpecManualMiddleware")


@asynccontextmanager
async def _request_connection_cm(config_state: "SQLSpecConfigState", request: "Request") -> "AsyncIterator[Any]":
    """Bind an optional extractor around connection entry and verified cleanup.

    Yields:
        The active request connection.
    """
    config = config_state.config
    token = None
    extractor = config_state.tenant_extractor
    if extractor is not None:
        raw = extractor(request.scope)
        if isawaitable(raw):
            raw = await raw
        token = TenantContext.set(coerce_tenant_payload(raw))
    connection_key = config_state.connection_key
    cache_key = f"sqlspec_session_cache_{config_state.session_key}"
    try:
        if config.supports_connection_pooling:
            pool = get_state_value(request.app.state, config_state.pool_key)
            async with with_ensure_async_(config.provide_connection(pool)) as connection:
                set_state_value(request.state, connection_key, connection)
                try:
                    yield connection
                finally:
                    pop_state_value(request.state, cache_key)
                    pop_state_value(request.state, f"{config_state.session_key}_instance")
                    pop_state_value(request.state, connection_key)
        elif getattr(config, "driver_features", {}).get("tenant_strategy") is not None:
            async with with_ensure_async_(config.provide_connection()) as connection:
                set_state_value(request.state, connection_key, connection)
                try:
                    yield connection
                finally:
                    pop_state_value(request.state, cache_key)
                    pop_state_value(request.state, f"{config_state.session_key}_instance")
                    pop_state_value(request.state, connection_key)
        else:
            connection = await ensure_async_(config.create_connection)()
            set_state_value(request.state, connection_key, connection)
            try:
                yield connection
            finally:
                pop_state_value(request.state, cache_key)
                pop_state_value(request.state, f"{config_state.session_key}_instance")
                pop_state_value(request.state, connection_key)
                await ensure_async_(connection.close)()
    finally:
        if token is not None:
            TenantContext.reset(token)


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
        if get_state_value(request.state, f"sqlspec_response_owner_{self.config_state.connection_key}", False):
            return await call_next(request)
        async with self._connection_cm(request):
            return await call_next(request)

    @asynccontextmanager
    async def _connection_cm(self, request: "Request") -> "AsyncIterator[Any]":
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
        if get_state_value(request.state, f"sqlspec_response_owner_{self.config_state.connection_key}", False):
            return await call_next(request)
        cache_key = f"sqlspec_session_cache_{self.config_state.session_key}"
        async with self._connection_cm(request) as connection:
            try:
                response = await call_next(request)
                session = get_state_value(request.state, cache_key, None)

                if self._should_commit(response.status_code):
                    target = session if session is not None and hasattr(session, "commit") else connection
                    if hasattr(target, "commit"):
                        await ensure_async_(target.commit)()
                else:
                    target = session if session is not None and hasattr(session, "rollback") else connection
                    if hasattr(target, "rollback"):
                        await ensure_async_(target.rollback)()
            except Exception:
                session = get_state_value(request.state, cache_key, None)
                target = session if session is not None and hasattr(session, "rollback") else connection
                if hasattr(target, "rollback"):
                    await ensure_async_(target.rollback)()
                raise
            else:
                return response

    @asynccontextmanager
    async def _connection_cm(self, request: "Request") -> "AsyncIterator[Any]":
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


def _track_downstream_completion(app: "ASGIApp", config_state: "SQLSpecConfigState") -> "ASGIApp":
    """Track downstream completion separately from synthesized middleware messages."""

    async def tracked_app(scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] != "http":
            await app(scope, receive, send)
            return
        completion = scope.get("state", {}).get(f"sqlspec_response_owner_{config_state.connection_key}")

        async def tracked_send(message: "Message") -> None:
            await send(message)
            if completion is not None and (
                message["type"] == "http.response.pathsend"
                or (message["type"] == "http.response.body" and not message.get("more_body", False))
            ):
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
    owner_key = f"sqlspec_response_owner_{state.connection_key}"
    status_code = 500
    response_complete = False

    async def tracked_send(message: "Message") -> None:
        nonlocal status_code, response_complete
        await send(message)
        if message["type"] == "http.response.start":
            status_code = message["status"]
        elif message["type"] == "http.response.pathsend" or (
            message["type"] == "http.response.body" and not message.get("more_body", False)
        ):
            response_complete = True

    async with middleware._connection_cm(request) as connection:
        completion = {"complete": False}
        set_state_value(request.state, owner_key, completion)
        try:
            try:
                await dispatch_app(scope, receive, tracked_send)
            except BaseException as body_error:
                if isinstance(middleware, SQLSpecAutocommitMiddleware):
                    try:
                        with CancelScope(shield=True):
                            await _finalize_request_transaction(request, state, connection, commit=False)
                    except BaseException as cleanup_error:
                        body_error.add_note(f"Request transaction rollback failed ({type(cleanup_error).__name__})")
                raise
            else:
                if isinstance(middleware, SQLSpecAutocommitMiddleware):
                    await _finalize_request_transaction(
                        request,
                        state,
                        connection,
                        commit=response_complete and completion["complete"] and middleware._should_commit(status_code),
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
