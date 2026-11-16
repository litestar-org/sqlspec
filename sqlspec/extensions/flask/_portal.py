"""Portal-backed blocking session view for the Flask plugin.

The Flask plugin returns :class:`PortalSession` for asynchronous configurations, a blocking view that
runs each driver call on the plugin's portal event loop.
"""

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterator
from contextvars import copy_context
from typing import TYPE_CHECKING, Any, Literal

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.driver._routing import AsyncRoutingSession
from sqlspec.utils.routing import RoutingContext, RoutingContextPayload
from sqlspec.utils.tenant import TenantContext, TenantContextPayload, TenantRequestBinding

if TYPE_CHECKING:
    from sqlspec.utils.portal import Portal

__all__ = ("PortalSession", "call_portal_with_context")


def call_portal_with_context(
    portal: "Portal",
    coroutine_function: Callable[..., Awaitable[Any]],
    /,
    *args: Any,
    tenant_override: TenantContextPayload | Literal["ambient"] | None = "ambient",
    routing_bind_key: str | None = None,
    **kwargs: Any,
) -> Any:
    """Run a portal operation with caller identity and monotonic primary pin receipts.

    Args:
        portal: Running background event-loop portal.
        coroutine_function: Async operation to execute.
        args: Positional arguments forwarded to the operation.
        tenant_override: Explicit checkout identity, or ambient caller identity.
        routing_bind_key: Owning bind conservatively pinned if completion is unknown.
        kwargs: Operation keywords; timeout retains the portal's deadline semantics.

    Returns:
        The operation's result.
    """
    tenant = TenantContext.current_binding() if tenant_override == "ambient" else tenant_override
    caller_context = copy_context()
    routing = RoutingContext.current()
    receipt: list[RoutingContextPayload | None] = []
    timeout = kwargs.pop("timeout", 300.0)

    async def run() -> Any:
        tenant_token = (
            TenantContext.bind_request(tenant)
            if isinstance(tenant, TenantRequestBinding)
            else TenantContext.set(tenant)
        )
        routing_token = RoutingContext.set(routing)
        try:
            return await coroutine_function(*args, **kwargs)
        finally:
            updated = RoutingContext.current()
            RoutingContext.reset(routing_token)
            TenantContext.reset(tenant_token)
            receipt.append(updated)

    async def invoke() -> Any:
        return await asyncio.create_task(run(), context=caller_context)

    try:
        return portal.call(invoke, timeout=timeout)
    finally:
        if receipt:
            updated = receipt[0]
            if updated is not None:
                for bind_key, entry in updated.entries.items():
                    if entry.pinned_by_write:
                        RoutingContext.mark_write(bind_key, causal_token=entry.causal_token)
        else:
            RoutingContext.mark_write(routing_bind_key)


class PortalSession:
    """Blocking view of an asynchronous database session for synchronous request handlers.

    Every call runs on the portal's event loop and blocks until it finishes, so
    ``session.select(...)`` returns rows, ``for row in session.select_stream(...)`` iterates rows,
    and ``with session.transaction():`` commits or rolls back. A call made from code already
    running on an event loop, such as a coroutine passed to ``portal.call``, returns the
    driver's awaitable result unchanged. Non-callable attributes are read from the wrapped
    session unchanged.
    """

    __slots__ = ("_portal", "_routing_bind_key", "_session")

    def __init__(self, session: Any, portal: "Portal", *, routing_bind_key: str | None = None) -> None:
        """Wrap an asynchronous database session.

        Args:
            session: Asynchronous database session bound to the request connection.
            portal: Portal whose event loop owns the session's connection.
            routing_bind_key: Owning database bind for conservative primary pinning.
        """
        self._session = session
        self._portal = portal
        self._routing_bind_key = routing_bind_key

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._session, name)
        if not callable(value) or isinstance(value, type):
            return value
        portal = self._portal

        def call(*args: Any, **kwargs: Any) -> Any:
            result = value(*args, **kwargs)
            if _in_event_loop():
                return result
            return _to_blocking(portal, result, routing_bind_key=self._routing_bind_key)

        return call

    def __repr__(self) -> str:
        return f"<PortalSession {type(self._session).__name__}>"

    def _close_open_streams(self, *, error: bool = False) -> None:
        """Close the wrapped session's row streams on the portal's event loop.

        Args:
            error: Whether the request is ending because of an error.
        """
        close = self._session._close_open_streams
        if _in_event_loop():
            return

        async def run() -> None:
            await close(error=error)

        call_portal_with_context(self._portal, run, tenant_override=None)

    def _mark_closed(self) -> None:
        """Mark the wrapped asynchronous database session closed without dispatching via the portal."""
        self._session._mark_closed()


class _PortalResource:
    """Blocking view of an async iterator or async context manager returned by a driver call."""

    __slots__ = ("_iterator", "_portal", "_resource", "_routing_bind_key")

    def __init__(self, portal: "Portal", resource: Any, *, routing_bind_key: str | None = None) -> None:
        self._portal = portal
        self._resource = resource
        self._routing_bind_key = routing_bind_key
        self._iterator: Any = None

    def __iter__(self) -> "Iterator[Any]":
        return self

    def __next__(self) -> Any:
        if self._iterator is None:
            self._iterator = self._resource.__aiter__()
        try:
            return call_portal_with_context(
                self._portal, _anext, self._iterator, routing_bind_key=self._routing_bind_key
            )
        except StopAsyncIteration:
            raise StopIteration from None

    def __enter__(self) -> Any:
        result = call_portal_with_context(
            self._portal, _aenter, self._resource, routing_bind_key=self._routing_bind_key
        )
        return _to_blocking(self._portal, result, routing_bind_key=self._routing_bind_key)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return call_portal_with_context(
            self._portal, _aexit, self._resource, exc_type, exc, tb, routing_bind_key=self._routing_bind_key
        )


def _in_event_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _to_blocking(portal: "Portal", result: Any, *, routing_bind_key: str | None = None) -> Any:
    if inspect.isawaitable(result):
        result = call_portal_with_context(portal, _await, result, routing_bind_key=routing_bind_key)
    if isinstance(result, (AsyncDriverAdapterBase, AsyncRoutingSession)):
        return PortalSession(result, portal, routing_bind_key=routing_bind_key)
    if hasattr(result, "__aiter__") or hasattr(result, "__aenter__"):
        return _PortalResource(portal, result, routing_bind_key=routing_bind_key)
    return result


async def _await(awaitable: "Awaitable[Any]") -> Any:
    return await awaitable


async def _anext(iterator: Any) -> Any:
    return await iterator.__anext__()


async def _aenter(resource: Any) -> Any:
    return await resource.__aenter__()


async def _aexit(resource: Any, exc_type: Any, exc: Any, tb: Any) -> Any:
    return await resource.__aexit__(exc_type, exc, tb)
