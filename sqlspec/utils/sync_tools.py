"""Utilities for async/sync interoperability in SQLSpec.

This module provides utilities for converting between async and sync functions,
managing concurrency limits, and handling context managers. Used primarily
for adapter implementations that need to support both sync and async patterns.
"""

import asyncio
import concurrent.futures
import functools
import inspect
import os
import sys
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from typing_extensions import ParamSpec

from sqlspec.utils.module_loader import module_available
from sqlspec.utils.portal import get_global_portal

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine
    from types import TracebackType

if module_available("uvloop"):
    import uvloop  # pyright: ignore[reportMissingImports]
else:
    uvloop = None  # type: ignore[assignment,unused-ignore]


ReturnT = TypeVar("ReturnT")
ParamSpecT = ParamSpec("ParamSpecT")
T = TypeVar("T")


class NoValue:
    """Sentinel class for missing values."""


NO_VALUE = NoValue()


class CapacityLimiter:
    """Limits the number of concurrent operations using a semaphore."""

    def __init__(self, total_tokens: int) -> None:
        """Initialize the capacity limiter.

        Args:
            total_tokens: Maximum number of concurrent operations allowed
        """
        self._total_tokens = total_tokens
        self._semaphore_instance: asyncio.Semaphore | None = None
        self._pid: int | None = None

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        """Lazy initialization of asyncio.Semaphore with per-process tracking.

        Reinitializes the semaphore if running in a new process (detected via PID).
        This ensures pytest-xdist workers each get their own semaphore bound to
        their event loop, preventing cross-process deadlocks.
        """
        current_pid = os.getpid()
        if self._semaphore_instance is None or self._pid != current_pid:
            self._semaphore_instance = asyncio.Semaphore(self._total_tokens)
            self._pid = current_pid
        return self._semaphore_instance

    async def acquire(self) -> None:
        """Acquire a token from the semaphore."""
        await self._semaphore.acquire()

    def release(self) -> None:
        """Release a token back to the semaphore."""
        self._semaphore.release()

    @property
    def total_tokens(self) -> int:
        """Get the total number of tokens available."""
        return self._total_tokens

    @total_tokens.setter
    def total_tokens(self, value: int) -> None:
        self._total_tokens = value
        self._semaphore_instance = None
        self._pid = None

    async def __aenter__(self) -> None:
        """Async context manager entry."""
        await self.acquire()

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        """Async context manager exit."""
        self.release()


_default_limiter = CapacityLimiter(1000)


def _return_value(value: Any) -> Any:
    return value


class _RunWrapper(Generic[ParamSpecT, ReturnT]):
    __slots__ = ("__dict__", "_function")

    def __init__(self, async_function: "Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]") -> None:
        self._function = async_function
        functools.update_wrapper(self, async_function)

    def __call__(self, *args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        partial_f = functools.partial(self._function, *args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            if loop.is_running():
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, partial_f())
                    return future.result()
            return asyncio.run(partial_f())
        if uvloop and sys.platform != "win32":
            uvloop.install()  # pyright: ignore[reportUnknownMemberType]
        return asyncio.run(partial_f())


def run_(async_function: "Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]") -> "Callable[ParamSpecT, ReturnT]":
    """Convert an async function to a blocking function using asyncio.run().

    Args:
        async_function: The async function to convert.

    Returns:
        A blocking function that runs the async function.
    """

    return _RunWrapper(async_function)


def await_(
    async_function: "Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]", raise_sync_error: bool = False
) -> "Callable[ParamSpecT, ReturnT]":
    """Convert an async function to a blocking one, running in the main async loop.

    When no event loop exists, automatically creates and uses a global portal for
    async-to-sync bridging via background thread. Set raise_sync_error=True to
    disable this behavior and raise errors instead.

    Args:
        async_function: The async function to convert.
        raise_sync_error: If True, raises RuntimeError when no loop exists.
                         If False (default), uses portal pattern for automatic bridging.

    Returns:
        A blocking function that runs the async function.
    """

    return _AwaitWrapper(async_function, raise_sync_error)


def async_(
    function: "Callable[ParamSpecT, ReturnT]", *, limiter: "CapacityLimiter | None" = None
) -> "Callable[ParamSpecT, Awaitable[ReturnT]]":
    """Convert a blocking function to an async one using asyncio.to_thread().

    Args:
        function: The blocking function to convert.
        limiter: Limit the total number of threads.

    Returns:
        An async function that runs the original function in a thread.
    """

    return _AsyncWrapper(function, limiter)


def ensure_async_(
    function: "Callable[ParamSpecT, Awaitable[ReturnT] | ReturnT]",
) -> "Callable[ParamSpecT, Awaitable[ReturnT]]":
    """Convert a function to an async one if it is not already.

    Args:
        function: The function to convert.

    Returns:
        An async function that runs the original function.
    """
    if inspect.iscoroutinefunction(function):
        return function

    return _EnsureAsyncWrapper(function)


class _AwaitWrapper(Generic[ParamSpecT, ReturnT]):
    __slots__ = ("__dict__", "_function", "_raise_sync_error")

    def __init__(
        self, async_function: "Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]", raise_sync_error: bool
    ) -> None:
        self._function = async_function
        self._raise_sync_error = raise_sync_error
        functools.update_wrapper(self, async_function)

    def __call__(self, *args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        partial_f = functools.partial(self._function, *args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._raise_sync_error:
                msg = "Cannot run async function"
                raise RuntimeError(msg) from None
            portal = get_global_portal()
            typed_partial = cast("Callable[[], Coroutine[Any, Any, ReturnT]]", partial_f)
            return portal.call(typed_partial)
        if loop.is_running():
            try:
                current_task = asyncio.current_task(loop=loop)
            except RuntimeError:
                current_task = None

            if current_task is not None:
                msg = "await_ cannot be called from within an async task running on the same event loop. Use 'await' instead."
                raise RuntimeError(msg)
            future = asyncio.run_coroutine_threadsafe(partial_f(), loop)
            return future.result()
        if self._raise_sync_error:
            msg = "Cannot run async function"
            raise RuntimeError(msg)
        portal = get_global_portal()
        typed_partial = cast("Callable[[], Coroutine[Any, Any, ReturnT]]", partial_f)
        return portal.call(typed_partial)


class _AsyncWrapper(Generic[ParamSpecT, ReturnT]):
    __slots__ = ("__dict__", "_function", "_limiter")

    def __init__(self, function: "Callable[ParamSpecT, ReturnT]", limiter: "CapacityLimiter | None") -> None:
        self._function = function
        self._limiter = limiter
        functools.update_wrapper(self, function)

    async def __call__(self, *args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        partial_f = functools.partial(self._function, *args, **kwargs)
        used_limiter = self._limiter or _default_limiter
        async with used_limiter:
            return await asyncio.to_thread(partial_f)


class _EnsureAsyncWrapper(Generic[ParamSpecT, ReturnT]):
    __slots__ = ("__dict__", "_function")

    def __init__(self, function: "Callable[ParamSpecT, Awaitable[ReturnT] | ReturnT]") -> None:
        self._function = function
        functools.update_wrapper(self, function)

    async def __call__(self, *args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        result = self._function(*args, **kwargs)
        if inspect.isawaitable(result):
            return await cast("Awaitable[ReturnT]", result)
        return result


class _ContextManagerWrapper(Generic[T]):
    def __init__(self, cm: AbstractContextManager[T]) -> None:
        self._cm = cm

    async def __aenter__(self) -> T:
        return self._cm.__enter__()

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        return self._cm.__exit__(exc_type, exc_val, exc_tb)


def with_ensure_async_(
    obj: "AbstractContextManager[T] | AbstractAsyncContextManager[T]",
) -> "AbstractAsyncContextManager[T]":
    """Convert a context manager to an async one if it is not already.

    Args:
        obj: The context manager to convert.

    Returns:
        An async context manager that runs the original context manager.
    """
    if isinstance(obj, AbstractContextManager):
        return cast("AbstractAsyncContextManager[T]", _ContextManagerWrapper(obj))
    return obj


async def get_next(iterable: Any, default: Any = NO_VALUE, *args: Any) -> Any:  # pragma: no cover
    """Return the next item from an async iterator.

    Args:
        iterable: An async iterable.
        default: An optional default value to return if the iterable is empty.
        *args: The remaining args

    Returns:
        The next value of the iterable.
    """
    if isinstance(default, NoValue):
        return await anext(iterable)
    return await anext(iterable, default)
