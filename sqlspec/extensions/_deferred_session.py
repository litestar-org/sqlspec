"""Request session views for asynchronous database configurations.

ASGI plugins return :class:`DeferredAsyncSession` from their synchronous session accessors when the
configuration is asynchronous and the request connection has not been checked out yet. The request
connection opens through the plugin's asynchronous accessor on the first awaited use, so tenant
resolution and commit or rollback bookkeeping are the same as for the asynchronous accessors.

The Flask plugin returns :class:`PortalSession` for asynchronous configurations, a blocking view that
runs each driver call on the plugin's portal event loop.
"""

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Coroutine, Generator, Iterator
from typing import TYPE_CHECKING, Any, NoReturn

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from sqlspec.utils.portal import Portal

__all__ = ("DeferredAsyncSession", "PortalSession", "raise_async_connection_required")

_CONFIG_ATTRIBUTES = frozenset({"driver_features", "statement_config"})


class DeferredAsyncSession:
    """Request session that checks out its connection on the first awaited use.

    Before the connection is open:

    - Methods return an object usable with ``await``, ``async for``, or ``async with``. Each form
      opens the request connection first and then forwards the call to the real driver, so
      ``await session.select(...)``, ``async for row in session.select_stream(...)``, and
      ``async with session.transaction():`` all work.
    - Plain class attributes such as ``dialect``, static helpers, and the configuration's
      ``statement_config`` and ``driver_features`` are served without opening a connection.
    - Attributes that need a live connection, such as ``connection``, raise
      :class:`~sqlspec.exceptions.ImproperConfigurationError`.

    Once the connection is open, every attribute is read from the real driver session.
    """

    __slots__ = ("_accessor", "_config", "_current", "_open")

    def __init__(
        self,
        config: Any,
        open_session: "Callable[[], Awaitable[Any]]",
        current_session: "Callable[[], Any | None]",
        accessor: str,
    ) -> None:
        """Bind the deferred session to one request and configuration.

        Args:
            config: Asynchronous database configuration that owns the session.
            open_session: Coroutine factory that checks out the request connection and returns the
                real driver session.
            current_session: Callable returning the real driver session when the request connection
                is already open, otherwise ``None``.
            accessor: Asynchronous accessor named in error messages, for example
                ``"await plugin.get_session_async(request)"``.
        """
        self._config = config
        self._open = open_session
        self._current = current_session
        self._accessor = accessor

    async def resolve(self) -> Any:
        """Return the real driver session, checking out the request connection when needed.

        Returns:
            The driver session bound to the request connection.
        """
        session = self._current()
        if session is not None:
            return session
        return await self._open()

    def __getattr__(self, name: str) -> Any:
        session = self._current()
        if session is not None:
            return getattr(session, name)
        driver_type = self._config.driver_type
        try:
            static = inspect.getattr_static(driver_type, name)
        except AttributeError:
            msg = f"'{driver_type.__name__}' object has no attribute '{name}'"
            raise AttributeError(msg) from None
        if name in _CONFIG_ATTRIBUTES:
            value = getattr(self._config, name, None)
            if value is not None:
                return value
            self._raise_not_open(name)
        if isinstance(static, (staticmethod, classmethod)):
            return getattr(driver_type, name)
        if callable(static):
            return _MethodProxy(self, name)
        if hasattr(type(static), "__get__"):
            self._raise_not_open(name)
        return static

    def __repr__(self) -> str:
        return f"<DeferredAsyncSession {self._config.driver_type.__name__}>"

    def _raise_not_open(self, name: str) -> NoReturn:
        msg = (
            f"'{name}' needs an open request connection. The connection opens on the session's first awaited "
            f"call; to open it now, use {self._accessor}."
        )
        raise ImproperConfigurationError(msg)


class _MethodProxy:
    """Bound method of a deferred session that defers the call until the connection is open."""

    __slots__ = ("_name", "_session")

    def __init__(self, session: DeferredAsyncSession, name: str) -> None:
        self._session = session
        self._name = name

    def __call__(self, *args: Any, **kwargs: Any) -> "_DeferredCall":
        return _DeferredCall(self._session, self._name, args, kwargs)

    def __repr__(self) -> str:
        return f"<deferred method {self._name}>"


class _DeferredCall(Coroutine[Any, Any, Any]):
    """Pending driver call usable as a coroutine, an async iterator, or an async context manager."""

    __slots__ = ("_args", "_coroutine", "_kwargs", "_name", "_session", "_target")

    def __init__(
        self, session: DeferredAsyncSession, name: str, args: "tuple[Any, ...]", kwargs: "dict[str, Any]"
    ) -> None:
        self._session = session
        self._name = name
        self._args = args
        self._kwargs = kwargs
        self._coroutine: Coroutine[Any, Any, Any] | None = None
        self._target: Any = None

    async def _invoke(self) -> Any:
        session = await self._session.resolve()
        return getattr(session, self._name)(*self._args, **self._kwargs)

    async def _run(self) -> Any:
        result = await self._invoke()
        if inspect.isawaitable(result):
            return await result
        return result

    def _ensure_coroutine(self) -> "Coroutine[Any, Any, Any]":
        if self._coroutine is None:
            self._coroutine = self._run()
        return self._coroutine

    def __await__(self) -> "Generator[Any, None, Any]":
        return self._ensure_coroutine().__await__()

    def send(self, value: Any) -> Any:
        return self._ensure_coroutine().send(value)

    def throw(self, typ: Any, val: Any = None, tb: Any = None) -> Any:
        if val is None and tb is None:
            return self._ensure_coroutine().throw(typ)
        return self._ensure_coroutine().throw(typ, val, tb)

    def close(self) -> None:
        if self._coroutine is not None:
            self._coroutine.close()

    def __aiter__(self) -> "_DeferredCall":
        return self

    async def __anext__(self) -> Any:
        if self._target is None:
            self._target = (await self._invoke()).__aiter__()
        return await self._target.__anext__()

    async def __aenter__(self) -> Any:
        self._target = await self._invoke()
        return await self._target.__aenter__()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return await self._target.__aexit__(exc_type, exc, tb)


def raise_async_connection_required(accessor: str) -> NoReturn:
    """Raise the error for a synchronous connection accessor on an unopened asynchronous connection.

    Args:
        accessor: Asynchronous accessor that opens the connection, for example
            ``"await plugin.get_connection_async(request)"``.

    Raises:
        ImproperConfigurationError: Always.
    """
    msg = (
        "The request connection for this asynchronous configuration is not open yet, and a synchronous "
        f"accessor cannot open it. Use {accessor}, or use the session from the synchronous session accessor, "
        "which opens the connection on its first awaited call."
    )
    raise ImproperConfigurationError(msg)


class PortalSession:
    """Blocking view of an asynchronous driver session for synchronous request handlers.

    Every call runs on the portal's event loop and blocks until it finishes, so
    ``session.select(...)`` returns rows, ``for row in session.select_stream(...)`` iterates rows,
    and ``with session.transaction():`` commits or rolls back. A call made from code already
    running on an event loop, such as a coroutine passed to ``portal.call``, returns the
    driver's awaitable result unchanged. Non-callable attributes are read from the wrapped
    session unchanged.
    """

    __slots__ = ("_portal", "_session")

    def __init__(self, session: Any, portal: "Portal") -> None:
        """Wrap an asynchronous driver session.

        Args:
            session: Asynchronous driver session bound to the request connection.
            portal: Portal whose event loop owns the session's connection.
        """
        self._session = session
        self._portal = portal

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._session, name)
        if not callable(value) or isinstance(value, type):
            return value
        portal = self._portal

        def call(*args: Any, **kwargs: Any) -> Any:
            result = value(*args, **kwargs)
            if _in_event_loop():
                return result
            return _to_blocking(portal, result)

        return call

    def __repr__(self) -> str:
        return f"<PortalSession {type(self._session).__name__}>"


class _PortalResource:
    """Blocking view of an async iterator or async context manager returned by a driver call."""

    __slots__ = ("_iterator", "_portal", "_resource")

    def __init__(self, portal: "Portal", resource: Any) -> None:
        self._portal = portal
        self._resource = resource
        self._iterator: Any = None

    def __iter__(self) -> "Iterator[Any]":
        return self

    def __next__(self) -> Any:
        if self._iterator is None:
            self._iterator = self._resource.__aiter__()
        try:
            return self._portal.call(_anext, self._iterator)
        except StopAsyncIteration:
            raise StopIteration from None

    def __enter__(self) -> Any:
        return _to_blocking(self._portal, self._portal.call(_aenter, self._resource))

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return self._portal.call(_aexit, self._resource, exc_type, exc, tb)


def _in_event_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _to_blocking(portal: "Portal", result: Any) -> Any:
    if inspect.isawaitable(result):
        result = portal.call(_await, result)
    if isinstance(result, AsyncDriverAdapterBase):
        return PortalSession(result, portal)
    if hasattr(result, "__aiter__") or hasattr(result, "__aenter__"):
        return _PortalResource(portal, result)
    return result


async def _await(awaitable: "Awaitable[Any]") -> Any:
    return await awaitable


async def _anext(iterator: Any) -> Any:
    return await iterator.__anext__()


async def _aenter(resource: Any) -> Any:
    return await resource.__aenter__()


async def _aexit(resource: Any, exc_type: Any, exc: Any, tb: Any) -> Any:
    return await resource.__aexit__(exc_type, exc, tb)
