"""HTTP middleware classes for the Starlette SQLSpec extension."""

import asyncio
from collections.abc import Mapping
from contextlib import AbstractContextManager, AsyncExitStack, asynccontextmanager, contextmanager
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

from anyio import CancelScope
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig
from sqlspec.core import CorrelationExtractor
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.extensions.starlette._utils import (
    get_request_tenant,
    get_state_value,
    has_state_value,
    pop_state_value,
    set_state_value,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_
from sqlspec.utils.tenant import TenantContext, TenantContextPayload, coerce_tenant_payload
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
    "acquire_pending_connection_async",
    "acquire_pending_connection_sync",
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
        payload = await _resolve_request_tenant(config_state, request, run_extractor=not pending.extractor_evaluated)
        connection = await pending.stack.enter_async_context(_open_config_connection(config_state, request, payload))
        pop_state_value(request.state, pending_key)
        return connection


def acquire_pending_connection_sync(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Acquire a deferred tenant-scoped synchronous connection on demand."""
    if has_state_value(request.state, config_state.connection_key):
        return get_state_value(request.state, config_state.connection_key)
    pending_key = _pending_connection_key(config_state.connection_key)
    pending = cast("_PendingTenantConnection | None", get_state_value(request.state, pending_key, None))
    if pending is None:
        return get_state_value(request.state, config_state.connection_key)

    config = config_state.config
    if isinstance(config, (AsyncDatabaseConfig, NoPoolAsyncConfig)) or bool(getattr(config, "is_async", False)):
        msg = (
            "Async tenant-scoped connections require an upfront tenant_extractor, SQLSpecTenantMiddleware, "
            "or await plugin.get_session_async(request) / Depends(plugin.provide_session()) after set_request_tenant."
        )
        raise TenantContextError(msg)

    payload = _resolve_request_tenant_sync(config_state, request, run_extractor=not pending.extractor_evaluated)
    provided: Any = None
    if config.supports_connection_pooling:
        provided = config.provide_connection(get_state_value(request.app.state, config_state.pool_key))
    elif _has_tenant_strategy(config):
        provided = config.provide_connection()
    if provided is not None:
        cm = cast("AbstractContextManager[Any]", provided)
        with TenantContext.bind(payload):
            connection: Any = cm.__enter__()
        set_state_value(request.state, config_state.connection_key, connection)
        set_state_value(request.state, _captured_tenant_key(config_state.connection_key), payload)

        async def _exit_sync_cm(exc_type: Any, exc: Any, tb: Any) -> bool:
            _clear_published_connection(request, config_state)
            with TenantContext.bind(payload):
                return bool(await with_ensure_async_(cm).__aexit__(exc_type, exc, tb))

        pending.stack.push_async_exit(_exit_sync_cm)
    else:
        with TenantContext.bind(payload):
            connection = config.create_connection()
        set_state_value(request.state, config_state.connection_key, connection)
        set_state_value(request.state, _captured_tenant_key(config_state.connection_key), payload)

        async def _close_sync_conn(_exc_type: Any, _exc: Any, _tb: Any) -> bool:
            _clear_published_connection(request, config_state)
            with TenantContext.bind(payload):
                await ensure_async_(connection.close)()
            return False

        pending.stack.push_async_exit(_close_sync_conn)

    pop_state_value(request.state, pending_key)
    return connection


class _PendingTenantConnection:
    """Deferred request-scoped connection slot awaiting tenant resolution."""

    __slots__ = ("config_state", "extractor_evaluated", "lock", "request", "stack")

    def __init__(
        self,
        stack: AsyncExitStack,
        request: "Request",
        config_state: "SQLSpecConfigState",
        *,
        extractor_evaluated: bool = False,
    ) -> None:
        self.stack = stack
        self.request = request
        self.config_state = config_state
        self.extractor_evaluated = extractor_evaluated
        self.lock = asyncio.Lock()


def _response_owner_key(connection_key: str) -> str:
    return f"sqlspec_response_owner_{connection_key}"


def _pending_connection_key(connection_key: str) -> str:
    return f"sqlspec_pending_connection_{connection_key}"


def _captured_tenant_key(connection_key: str) -> str:
    return f"sqlspec_captured_tenant_{connection_key}"


def _has_tenant_strategy(config: Any) -> bool:
    features = getattr(config, "driver_features", None)
    return isinstance(features, Mapping) and features.get("tenant_strategy") not in (None, False)


def _completes_response(message: "Message") -> bool:
    return message["type"] == "http.response.pathsend" or (
        message["type"] == "http.response.body" and not message.get("more_body", False)
    )


def _clear_published_connection(request: "Request", config_state: "SQLSpecConfigState") -> None:
    pop_state_value(request.state, f"sqlspec_session_cache_{config_state.session_key}")
    pop_state_value(request.state, f"{config_state.session_key}_instance")
    pop_state_value(request.state, _captured_tenant_key(config_state.connection_key))
    pop_state_value(request.state, config_state.connection_key)


@contextmanager
def _published_connection(
    request: "Request",
    config_state: "SQLSpecConfigState",
    connection: Any,
    payload: TenantContextPayload | None = None,
    *,
    bind_payload: bool = False,
) -> "Generator[None]":
    """Expose a connection on request state and clear every derived entry on exit.

    Yields:
        Control while the connection is visible to the request.
    """
    set_state_value(request.state, config_state.connection_key, connection)
    if bind_payload:
        set_state_value(request.state, _captured_tenant_key(config_state.connection_key), payload)
    try:
        yield
    finally:
        _clear_published_connection(request, config_state)


async def _resolve_request_tenant(
    config_state: "SQLSpecConfigState", request: "Request", *, run_extractor: bool = True
) -> TenantContextPayload | None:
    receipt = get_request_tenant(request, config_state)
    if receipt is not None:
        return coerce_tenant_payload(receipt)
    extractor = config_state.tenant_extractor
    if run_extractor and extractor is not None:
        raw = extractor(request.scope)
        if isawaitable(raw):
            raw = await raw
        return coerce_tenant_payload(raw)
    return TenantContext.get()


def _resolve_request_tenant_sync(
    config_state: "SQLSpecConfigState", request: "Request", *, run_extractor: bool = True
) -> TenantContextPayload | None:
    receipt = get_request_tenant(request, config_state)
    if receipt is not None:
        return coerce_tenant_payload(receipt)
    extractor = config_state.tenant_extractor
    if run_extractor and extractor is not None:
        raw = extractor(request.scope)
        if isawaitable(raw):
            close = getattr(raw, "close", None)
            if callable(close):
                close()
            msg = "Synchronous on-demand connections require a synchronous tenant_extractor"
            raise ImproperConfigurationError(msg)
        return coerce_tenant_payload(raw)
    return TenantContext.get()


@asynccontextmanager
async def _open_config_connection(
    config_state: "SQLSpecConfigState",
    request: "Request",
    payload: TenantContextPayload | None,
    *,
    bind_payload: bool = True,
) -> "AsyncGenerator[Any]":
    config = config_state.config
    provided: Any = None
    if bind_payload:
        with TenantContext.bind(payload):
            if config.supports_connection_pooling:
                provided = config.provide_connection(get_state_value(request.app.state, config_state.pool_key))
            elif _has_tenant_strategy(config):
                provided = config.provide_connection()
    elif config.supports_connection_pooling:
        provided = config.provide_connection(get_state_value(request.app.state, config_state.pool_key))
    elif _has_tenant_strategy(config):
        provided = config.provide_connection()

    if provided is not None:
        cm = with_ensure_async_(provided)
        if bind_payload:
            with TenantContext.bind(payload):
                connection = await cm.__aenter__()
        else:
            connection = await cm.__aenter__()
        try:
            with _published_connection(request, config_state, connection, payload, bind_payload=bind_payload):
                yield connection
        except BaseException as exc:
            if bind_payload:
                with TenantContext.bind(payload):
                    suppressed = await cm.__aexit__(type(exc), exc, exc.__traceback__)
            else:
                suppressed = await cm.__aexit__(type(exc), exc, exc.__traceback__)
            if not suppressed:
                raise
        else:
            if bind_payload:
                with TenantContext.bind(payload):
                    await cm.__aexit__(None, None, None)
            else:
                await cm.__aexit__(None, None, None)
        return

    if bind_payload:
        with TenantContext.bind(payload):
            connection = await ensure_async_(config.create_connection)()
    else:
        connection = await ensure_async_(config.create_connection)()
    try:
        with _published_connection(request, config_state, connection, payload, bind_payload=bind_payload):
            yield connection
    finally:
        if bind_payload:
            with TenantContext.bind(payload):
                await ensure_async_(connection.close)()
        else:
            await ensure_async_(connection.close)()


@asynccontextmanager
async def _request_connection_cm(config_state: "SQLSpecConfigState", request: "Request") -> "AsyncGenerator[Any]":
    """Own an eager or deferred request connection through verified cleanup.

    Yields:
        The active request connection, or None while deferred awaiting a tenant.
    """
    async with AsyncExitStack() as stack:
        if not _has_tenant_strategy(config_state.config):
            extractor = config_state.tenant_extractor
            if extractor is not None:
                raw = extractor(request.scope)
                if isawaitable(raw):
                    raw = await raw
                token = TenantContext.set(coerce_tenant_payload(raw))
                stack.callback(TenantContext.reset, token)
            else:
                receipt = get_request_tenant(request, config_state)
                if receipt is not None:
                    token = TenantContext.set(coerce_tenant_payload(receipt))
                    stack.callback(TenantContext.reset, token)
            connection = await stack.enter_async_context(
                _open_config_connection(config_state, request, None, bind_payload=False)
            )
            yield connection
            return

        extractor_evaluated = (
            config_state.tenant_extractor is not None and get_request_tenant(request, config_state) is None
        )
        payload = await _resolve_request_tenant(config_state, request)
        if payload is not None:
            token = TenantContext.set(payload)
            stack.callback(TenantContext.reset, token)
            connection = await stack.enter_async_context(_open_config_connection(config_state, request, payload))
            yield connection
            return
        if extractor_evaluated:
            token = TenantContext.set(None)
            stack.callback(TenantContext.reset, token)

        pending_key = _pending_connection_key(config_state.connection_key)
        set_state_value(
            request.state,
            pending_key,
            _PendingTenantConnection(stack, request, config_state, extractor_evaluated=extractor_evaluated),
        )
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
        tenant_key = _captured_tenant_key(config_state.connection_key)
        if has_state_value(request.state, tenant_key):
            with TenantContext.bind(get_state_value(request.state, tenant_key, None)):
                await ensure_async_(callback)()
        else:
            await ensure_async_(callback)()
