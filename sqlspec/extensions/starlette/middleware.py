"""HTTP middleware classes for the Starlette SQLSpec extension."""

import asyncio
from collections.abc import Mapping, Sequence
from contextlib import (
    AbstractContextManager,
    AsyncExitStack,
    ExitStack,
    asynccontextmanager,
    contextmanager,
    nullcontext,
    suppress,
)
from typing import TYPE_CHECKING, Any, NoReturn, cast

from anyio import CancelScope
from starlette.datastructures import MutableHeaders
from starlette.requests import HTTPConnection, Request

from sqlspec.core import CorrelationExtractor
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.driver._routing import AsyncRoutingSession
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import DeferredAsyncSession, raise_async_connection_required
from sqlspec.extensions.starlette._utils import (
    deferred_session_key,
    get_or_create_driver_session,
    get_scope_tenant,
    get_state_value,
    has_state_value,
    pop_state_value,
    set_state_value,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.routing import (
    RoutingContext,
    RoutingContextPayload,
    RoutingEntry,
    apply_request_routing_hints,
    extract_asgi_routing_headers,
)
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
    "deferred_request_session",
    "get_request_resource",
    "get_request_resource_async",
    "has_tenant_strategy",
)


class SQLSpecRequestLeaseMiddleware:
    """Own every database lease through ASGI streaming and background completion."""

    __slots__ = ("app", "config_states")

    def __init__(self, app: "ASGIApp", config_states: "Sequence[SQLSpecConfigState]") -> None:
        """Bind the downstream application and its active database configurations."""
        self.app = app
        self.config_states = tuple(config_states)

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        request = (
            Request(scope, receive=receive, send=send)
            if scope["type"] == "http"
            else cast("Request", HTTPConnection(scope))
        )
        routing_states = [state for state in self.config_states if state.enable_routing_middleware]
        payload = RoutingContext.current() or RoutingContextPayload()
        for state in routing_states:
            if state.default_routing_mode == "primary" and payload.get(state.config.bind_key) is None:
                payload = payload.with_entry(state.config.bind_key, RoutingEntry("primary"))
            cookie_header, routing_header_value = extract_asgi_routing_headers(
                scope, state.routing_header_bytes, include_cookies=state.write_pin_cookie is not None
            )
            payload = apply_request_routing_hints(
                payload,
                bind_key=state.config.bind_key,
                routing_header_value=routing_header_value,
                cookie_header=cookie_header,
                write_pin_cookie=state.write_pin_cookie,
                routing_secret=state.routing_secret,
            )
        status_code = 500
        response_complete = False

        async def tracked_send(message: "Message") -> None:
            nonlocal status_code, response_complete
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)
            if _completes_response(message):
                response_complete = True

        routing_scope: Any = RoutingContext.scope(payload) if routing_states else nullcontext()
        async with routing_scope, AsyncExitStack() as stack:
            with ExitStack() as sync_stack:
                for state in self.config_states:
                    set_state_value(request.state, _response_owner_key(state.connection_key), True)
                    set_state_value(
                        request.state,
                        _pending_connection_key(state.connection_key),
                        _PendingConnection(stack, request, state),
                    )
                    for resource_key, kind in (
                        (state.read_connection_key, "connection"),
                        (state.read_session_key, "session"),
                        (state.routing_session_key, "routing"),
                    ):
                        if resource_key is not None:
                            set_state_value(
                                request.state,
                                _resource_slot_key(resource_key),
                                _RequestResourceSlot(stack, sync_stack, state, resource_key, kind),
                            )
                try:
                    try:
                        await self.app(scope, receive, tracked_send)
                    except BaseException as body_error:
                        for state in self.config_states:
                            try:
                                with CancelScope(shield=True):
                                    await self._finalize(request, state, False)
                            except BaseException as cleanup_error:
                                body_error.add_note(
                                    f"Request transaction rollback failed ({type(cleanup_error).__name__})"
                                )
                        raise
                    else:
                        finalize_error: BaseException | None = None
                        for state in self.config_states:
                            try:
                                await self._finalize(
                                    request, state, response_complete and state.should_commit(status_code)
                                )
                            except BaseException as exc:
                                if finalize_error is None:
                                    finalize_error = exc
                        if finalize_error is not None:
                            raise finalize_error
                finally:
                    for state in self.config_states:
                        pop_state_value(request.state, _response_owner_key(state.connection_key))
                        pop_state_value(request.state, _pending_connection_key(state.connection_key))
                        for resource_key in (
                            state.read_connection_key,
                            state.read_session_key,
                            state.routing_session_key,
                        ):
                            if resource_key is not None:
                                pop_state_value(request.state, _resource_slot_key(resource_key))
                                pop_state_value(request.state, resource_key)
                                pop_state_value(request.state, f"{resource_key}_deferred")

    async def _finalize(self, request: "Request", state: "SQLSpecConfigState", commit: bool) -> None:
        if state.commit_mode == "manual":
            return
        connection = get_state_value(request.state, state.connection_key, None)
        if connection is not None:
            if commit:
                try:
                    await _finalize_request_transaction(request, state, connection, commit=True)
                except BaseException:
                    with suppress(Exception):
                        await _finalize_request_transaction(request, state, connection, commit=False)
                    raise
            else:
                await _finalize_request_transaction(request, state, connection, commit=False)
        if state.routing_session_key is not None:
            facade = get_state_value(request.state, state.routing_session_key, None)
            if facade is not None and facade.pinned_to_primary:
                if commit:
                    try:
                        await ensure_async_(facade.commit)()
                    except BaseException:
                        with suppress(Exception):
                            await ensure_async_(facade.rollback)()
                        raise
                else:
                    await ensure_async_(facade.rollback)()


class SQLSpecManualMiddleware(SQLSpecRequestLeaseMiddleware):
    """Middleware for manual transaction mode.

    Acquires connection from pool, stores in request.state, releases after request.
    No automatic commit or rollback - user code must handle transactions.
    """

    __slots__ = ("config_state",)

    def __init__(self, app: Any, config_state: "SQLSpecConfigState") -> None:
        """Initialize middleware.

        Args:
            app: Starlette application instance.
            config_state: Configuration state for this database.
        """
        super().__init__(app, [config_state])
        self.config_state = config_state

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        """Own the connection until response streaming and background work finish."""
        await super().__call__(scope, receive, send)

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


class SQLSpecAutocommitMiddleware(SQLSpecRequestLeaseMiddleware):
    """Middleware for autocommit transaction mode.

    Acquires connection, commits on success status codes, rollbacks on error status codes.
    """

    __slots__ = ("config_state",)

    def __init__(self, app: Any, config_state: "SQLSpecConfigState") -> None:
        """Initialize middleware.

        Args:
            app: Starlette application instance.
            config_state: Configuration state for this database.
        """
        super().__init__(app, [config_state])
        self.config_state = config_state

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        """Own the connection until response streaming and background work finish."""
        await super().__call__(scope, receive, send)

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


class CorrelationMiddleware:
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

    __slots__ = ("_extractor", "app")

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        connection = HTTPConnection(scope)
        correlation_id = self._extractor.extract(lambda name: connection.headers.get(name))
        previous = CorrelationContext.get()
        CorrelationContext.set(correlation_id)
        set_state_value(connection.state, "correlation_id", correlation_id)

        async def tracked_send(message: "Message") -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)["X-Correlation-ID"] = correlation_id
            await send(message)

        try:
            await self.app(scope, receive, tracked_send)
        finally:
            CorrelationContext.set(previous)
            pop_state_value(connection.state, "correlation_id")

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
        self.app = app
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


class SQLCommenterMiddleware:
    """Middleware that populates SQLCommenterContext with request attributes.

    Extracts route and endpoint information from the Starlette/FastAPI request
    and sets them in :class:`~sqlspec.core.sqlcommenter.SQLCommenterContext`
    for the duration of the request.
    """

    __slots__ = ("_framework", "app")

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        attrs = {"route": scope.get("path", ""), "framework": self._framework}
        endpoint = scope.get("endpoint")
        if has_name(endpoint):
            attrs["action"] = endpoint.__name__
        previous = SQLCommenterContext.get()
        SQLCommenterContext.set(attrs)
        try:
            await self.app(scope, receive, send)
        finally:
            SQLCommenterContext.set(previous)

    def __init__(self, app: Any, *, framework: str = "starlette") -> None:
        """Initialize SQLCommenter middleware.

        Args:
            app: Starlette application instance.
            framework: Framework name to include in attributes.
        """
        self.app = app
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


async def acquire_pending_connection_async(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Acquire the deferred request connection asynchronously if not yet checked out."""
    if has_state_value(request.state, config_state.connection_key):
        return get_state_value(request.state, config_state.connection_key)
    pending_key = _pending_connection_key(config_state.connection_key)
    pending = cast("_PendingConnection | None", get_state_value(request.state, pending_key, None))
    if pending is None:
        return get_state_value(request.state, config_state.connection_key)
    async with pending.lock:
        if has_state_value(request.state, config_state.connection_key):
            return get_state_value(request.state, config_state.connection_key)
        connection = await pending.stack.enter_async_context(_open_config_connection(config_state, request))
        pop_state_value(request.state, pending_key)
        return connection


def acquire_pending_connection_sync(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Acquire the deferred request connection synchronously on first access.

    Raises:
        ImproperConfigurationError: The configuration is asynchronous and its connection is not open.
    """
    if has_state_value(request.state, config_state.connection_key):
        return get_state_value(request.state, config_state.connection_key)
    pending_key = _pending_connection_key(config_state.connection_key)
    pending = cast("_PendingConnection | None", get_state_value(request.state, pending_key, None))
    if pending is None:
        return get_state_value(request.state, config_state.connection_key)

    config = config_state.config
    if config.is_async:
        raise_async_connection_required("await plugin.get_connection_async(request)")

    captured_payload = TenantContext.require(config) if has_tenant_strategy(config) else None
    provided: Any = None
    if config.supports_connection_pooling:
        provided = config.provide_connection(get_state_value(request.app.state, config_state.pool_key))
    elif has_tenant_strategy(config):
        provided = config.provide_connection()
    if provided is not None:
        cm = cast("AbstractContextManager[Any]", provided)
        with TenantContext.bind(captured_payload):
            connection: Any = cm.__enter__()
        set_state_value(request.state, config_state.connection_key, connection)

        async def _exit_sync_cm(exc_type: Any, exc: Any, tb: Any) -> bool:
            _clear_published_connection(request, config_state)
            with TenantContext.bind(captured_payload):
                return bool(await with_ensure_async_(cm).__aexit__(exc_type, exc, tb))

        pending.stack.push_async_exit(_exit_sync_cm)
    else:
        with TenantContext.bind(captured_payload):
            connection = config.create_connection()
        set_state_value(request.state, config_state.connection_key, connection)

        async def _close_sync_conn(_exc_type: Any, _exc: Any, _tb: Any) -> bool:
            _clear_published_connection(request, config_state)
            with TenantContext.bind(captured_payload):
                await ensure_async_(connection.close)()
            return False

        pending.stack.push_async_exit(_close_sync_conn)

    pop_state_value(request.state, pending_key)
    return connection


def deferred_request_session(request: "Request", config_state: "SQLSpecConfigState") -> DeferredAsyncSession:
    """Return a session that opens the asynchronous request connection on its first awaited call.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for an asynchronous configuration.

    Returns:
        A deferred session bound to the request's pending connection.
    """
    if get_state_value(request.state, _pending_connection_key(config_state.connection_key), None) is None:
        get_state_value(request.state, config_state.connection_key)

    async def open_session() -> Any:
        await acquire_pending_connection_async(request, config_state)
        return get_or_create_driver_session(request, config_state)

    def current_session() -> Any:
        if has_state_value(request.state, config_state.connection_key):
            return get_or_create_driver_session(request, config_state)
        return None

    return DeferredAsyncSession(
        config_state.config, open_session, current_session, "await plugin.get_session_async(request)"
    )


def has_tenant_strategy(config: Any) -> bool:
    """Return whether a configuration enables an active tenant strategy."""
    features = getattr(config, "driver_features", None)
    return isinstance(features, Mapping) and features.get("tenant_strategy") not in (None, False)


def get_request_resource(request: "Request", config_state: "SQLSpecConfigState", key: str, kind: str) -> Any:
    """Return a lazy read or routing resource owned by the request lease stack."""
    current = get_state_value(request.state, key, None)
    if current is not None:
        return current
    slot = get_state_value(request.state, _resource_slot_key(key), None)
    if slot is None:
        msg = "Request lease middleware is required before accessing database resources"
        raise ImproperConfigurationError(msg)
    if config_state.config.is_async:
        if kind == "connection":
            raise_async_connection_required("await plugin.get_read_connection_async(request)")
        deferred_key = f"{key}_deferred"
        current = get_state_value(request.state, deferred_key, None)
        if current is None:
            accessor = (
                "await plugin.get_routing_session_async(request)"
                if kind == "routing"
                else "await plugin.get_read_session_async(request)"
            )
            current = DeferredAsyncSession(
                config_state.config,
                lambda: get_request_resource_async(request, config_state, key, kind),
                lambda: get_state_value(request.state, key, None),
                accessor,
                target_type=AsyncRoutingSession if kind == "routing" else None,
            )
            set_state_value(request.state, deferred_key, current)
        return current
    current = slot.sync_stack.enter_context(_captured_sync_resource(slot))
    set_state_value(request.state, key, current)
    return current


async def get_request_resource_async(
    request: "Request", config_state: "SQLSpecConfigState", key: str, kind: str
) -> Any:
    """Acquire one request-owned resource under its per-slot asynchronous lock."""
    current = get_state_value(request.state, key, None)
    if current is not None:
        return current
    slot = get_state_value(request.state, _resource_slot_key(key), None)
    if slot is None:
        msg = "Request lease middleware is required before accessing database resources"
        raise ImproperConfigurationError(msg)
    async with slot.lock:
        current = get_state_value(request.state, key, None)
        if current is None:
            current = await slot.stack.enter_async_context(_captured_async_resource(slot))
            set_state_value(request.state, key, current)
        return current


class _PendingConnection:
    """Deferred request-scoped connection slot checked out on first access."""

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


def _completes_response(message: "Message") -> bool:
    return message["type"] == "http.response.pathsend" or (
        message["type"] == "http.response.body" and not message.get("more_body", False)
    )


def _clear_published_connection(request: "Request", config_state: "SQLSpecConfigState") -> None:
    pop_state_value(request.state, f"sqlspec_session_cache_{config_state.session_key}")
    pop_state_value(request.state, f"{config_state.session_key}_instance")
    pop_state_value(request.state, deferred_session_key(config_state))
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
    captured_payload = await TenantContext.arequire(config) if has_tenant_strategy(config) else None
    provided: Any = None
    if config.supports_connection_pooling:
        provided = config.provide_connection(get_state_value(request.app.state, config_state.pool_key))
    elif has_tenant_strategy(config):
        provided = config.provide_connection()

    if provided is not None:
        cm = with_ensure_async_(provided)
        with TenantContext.bind(captured_payload):
            connection = await cm.__aenter__()
        try:
            with _published_connection(request, config_state, connection):
                yield connection
        except BaseException as exc:
            with TenantContext.bind(captured_payload):
                suppressed = await cm.__aexit__(type(exc), exc, exc.__traceback__)
            if not suppressed:
                raise
        else:
            with TenantContext.bind(captured_payload):
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
    """Own a deferred request connection through verified cleanup.

    No connection is checked out until the request first asks for a session or connection, so
    routes that never touch the database open none; a tenant lease resolves the request tenant
    at that point.

    Yields:
        None; the connection is published on request state once it is checked out.
    """
    async with AsyncExitStack() as stack:
        pending_key = _pending_connection_key(config_state.connection_key)
        set_state_value(request.state, pending_key, _PendingConnection(stack, request, config_state))
        try:
            yield None
        finally:
            pop_state_value(request.state, pending_key)


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


def _resource_slot_key(resource_key: str) -> str:
    return f"_sqlspec_resource_{resource_key}"


class _RequestResourceSlot:
    __slots__ = ("config_state", "key", "kind", "lock", "stack", "sync_stack")

    def __init__(
        self, stack: AsyncExitStack, sync_stack: ExitStack, config_state: "SQLSpecConfigState", key: str, kind: str
    ) -> None:
        self.stack = stack
        self.sync_stack = sync_stack
        self.config_state = config_state
        self.key = key
        self.kind = kind
        self.lock = asyncio.Lock()


def _resource_context(slot: _RequestResourceSlot) -> Any:
    config = slot.config_state.config
    if slot.kind == "routing":
        return config.provide_routing_session()
    if slot.kind == "session":
        return config.provide_read_session()
    return config.provide_read_connection()


@contextmanager
def _captured_sync_resource(slot: _RequestResourceSlot) -> "Generator[Any]":
    config = slot.config_state.config
    captured_payload = TenantContext.require(config) if has_tenant_strategy(config) else None
    with TenantContext.bind(captured_payload):
        context = _resource_context(slot)
        value = context.__enter__()
    try:
        yield value
    except BaseException as error:
        with TenantContext.bind(captured_payload):
            suppressed = context.__exit__(type(error), error, error.__traceback__)
        if not suppressed:
            raise
    else:
        with TenantContext.bind(captured_payload):
            context.__exit__(None, None, None)


@asynccontextmanager
async def _captured_async_resource(slot: _RequestResourceSlot) -> "AsyncGenerator[Any]":
    config = slot.config_state.config
    captured_payload = await TenantContext.arequire(config) if has_tenant_strategy(config) else None
    with TenantContext.bind(captured_payload):
        context = with_ensure_async_(_resource_context(slot))
        value = await context.__aenter__()
    try:
        yield value
    except BaseException as error:
        with TenantContext.bind(captured_payload):
            suppressed = await context.__aexit__(type(error), error, error.__traceback__)
        if not suppressed:
            raise
    else:
        with TenantContext.bind(captured_payload):
            await context.__aexit__(None, None, None)


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
