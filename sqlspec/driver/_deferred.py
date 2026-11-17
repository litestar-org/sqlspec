"""Request session views for asynchronous database configurations.

ASGI plugins return :class:`DeferredAsyncSession` from their synchronous session accessors when the
configuration is asynchronous and the request connection has not been checked out yet. The request
connection opens through the plugin's asynchronous accessor on the first awaited use, so tenant
resolution and commit or rollback bookkeeping are the same as for the asynchronous accessors.
"""

import inspect
from collections.abc import Awaitable, Callable, Coroutine, Generator
from typing import Any, NoReturn

from sqlspec.driver._stream import _CLOSED_CONNECTION_MESSAGE
from sqlspec.exceptions import ImproperConfigurationError, with_cause

__all__ = ("DeferredAsyncSession", "raise_async_connection_required")

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
      When the driver stores ``dialect`` per instance, it is read from the configuration's
      statement config.
    - Attributes that need a live connection, such as ``connection``, raise
      :class:`~sqlspec.exceptions.ImproperConfigurationError`.

    Once the connection is open, every attribute is read from the real driver session.
    """

    __slots__ = ("_accessor", "_closed", "_config", "_current", "_open", "_target_type")

    def __init__(
        self,
        config: Any,
        open_session: "Callable[[], Awaitable[Any]]",
        current_session: "Callable[[], Any | None]",
        accessor: str,
        *,
        target_type: type[Any] | None = None,
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
            target_type: Optional facade type inspected before first access.
        """
        self._config = config
        self._open = open_session
        self._current = current_session
        self._accessor = accessor
        self._target_type = target_type or config.driver_type
        self._closed = False

    async def resolve(self) -> Any:
        """Return the real driver session, checking out the request connection when needed.

        Returns:
            The driver session bound to the request connection.
        """
        if self._closed:
            raise ImproperConfigurationError(_CLOSED_CONNECTION_MESSAGE)
        session = self._current()
        if session is not None:
            return session
        return await self._open()

    def __getattr__(self, name: str) -> Any:
        if self._closed:
            raise ImproperConfigurationError(_CLOSED_CONNECTION_MESSAGE)
        session = self._current()
        if session is not None:
            return getattr(session, name)
        driver_type = self._target_type
        try:
            static = inspect.getattr_static(driver_type, name)
        except AttributeError:
            msg = f"'{driver_type.__name__}' object has no attribute '{name}'"
            raise with_cause(AttributeError(msg), None) from None
        if name in _CONFIG_ATTRIBUTES:
            value = getattr(self._config, name, None)
            if value is not None:
                return value
            self._raise_not_open(name)
        if isinstance(static, (staticmethod, classmethod)):
            return getattr(driver_type, name)
        if callable(static):
            return _MethodProxy(self, name)
        if name == "dialect" and not isinstance(static, str):
            dialect = getattr(getattr(self._config, "statement_config", None), "dialect", None)
            if dialect is not None:
                return dialect
        if hasattr(type(static), "__get__"):
            self._raise_not_open(name)
        return static

    def __repr__(self) -> str:
        return f"<DeferredAsyncSession {self._config.driver_type.__name__}>"

    async def _close_open_streams(self, *, error: bool = False) -> None:
        """Close the opened database session's row streams, if the connection was ever opened.

        Args:
            error: Whether the request is ending because of an error.
        """
        session = self._current()
        if session is not None:
            await session._close_open_streams(error=error)

    def _mark_closed(self) -> None:
        """Mark the deferred session closed and close the underlying database session when open."""
        self._closed = True
        session = self._current()
        if session is not None:
            session._mark_closed()

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
