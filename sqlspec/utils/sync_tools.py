"""Utilities for async/sync interoperability in SQLSpec.

This module provides utilities for converting between async and sync functions,
managing concurrency limits, and handling context managers. Used primarily
for adapter implementations that need to support both sync and async patterns.
"""

import asyncio
import functools
import inspect
import sys
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar, Union, cast

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine
    from types import TracebackType

try:
    import uvloop  # pyright: ignore[reportMissingImports]
except ImportError:
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
        self._semaphore_instance: Optional[asyncio.Semaphore] = None

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        """Lazy initialization of asyncio.Semaphore for Python 3.9 compatibility."""
        if self._semaphore_instance is None:
            self._semaphore_instance = asyncio.Semaphore(self._total_tokens)
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

    async def __aenter__(self) -> None:
        """Async context manager entry."""
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: "Optional[type[BaseException]]",
        exc_val: "Optional[BaseException]",
        exc_tb: "Optional[TracebackType]",
    ) -> None:
        """Async context manager exit."""
        self.release()


_default_limiter = CapacityLimiter(15)


def run_(async_function: "Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]") -> "Callable[ParamSpecT, ReturnT]":
    """Convert an async function to a blocking function using asyncio.run().

    Args:
        async_function: The async function to convert.

    Returns:
        A blocking function that runs the async function.
    """

    @functools.wraps(async_function)
    def wrapper(*args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        partial_f = functools.partial(async_function, *args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            if loop.is_running():
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, partial_f())
                    return future.result()
            else:
                return asyncio.run(partial_f())
        if uvloop and sys.platform != "win32":
            uvloop.install()  # pyright: ignore[reportUnknownMemberType]
        return asyncio.run(partial_f())

    return wrapper


def await_(
    async_function: "Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]", raise_sync_error: bool = True
) -> "Callable[ParamSpecT, ReturnT]":
    """Convert an async function to a blocking one, running in the main async loop.

    Args:
        async_function: The async function to convert.
        raise_sync_error: If False, runs in a new event loop if no loop is present.
                         If True (default), raises RuntimeError if no loop is running.

    Returns:
        A blocking function that runs the async function.
    """

    @functools.wraps(async_function)
    def wrapper(*args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        partial_f = functools.partial(async_function, *args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if raise_sync_error:
                msg = "Cannot run async function"
                raise RuntimeError(msg) from None
            return asyncio.run(partial_f())
        else:
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
            if raise_sync_error:
                msg = "Cannot run async function"
                raise RuntimeError(msg)
            return asyncio.run(partial_f())

    return wrapper


def async_(
    function: "Callable[ParamSpecT, ReturnT]", *, limiter: "Optional[CapacityLimiter]" = None
) -> "Callable[ParamSpecT, Awaitable[ReturnT]]":
    """Convert a blocking function to an async one using asyncio.to_thread().

    Args:
        function: The blocking function to convert.
        limiter: Limit the total number of threads.

    Returns:
        An async function that runs the original function in a thread.
    """

    @functools.wraps(function)
    async def wrapper(*args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        partial_f = functools.partial(function, *args, **kwargs)
        used_limiter = limiter or _default_limiter
        async with used_limiter:
            return await asyncio.to_thread(partial_f)

    return wrapper


def ensure_async_(
    function: "Callable[ParamSpecT, Union[Awaitable[ReturnT], ReturnT]]",
) -> "Callable[ParamSpecT, Awaitable[ReturnT]]":
    """Convert a function to an async one if it is not already.

    Args:
        function: The function to convert.

    Returns:
        An async function that runs the original function.
    """
    if inspect.iscoroutinefunction(function):
        return function

    @functools.wraps(function)
    async def wrapper(*args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        result = function(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return await async_(lambda: result)()

    return wrapper


class _ContextManagerWrapper(Generic[T]):
    def __init__(self, cm: AbstractContextManager[T]) -> None:
        self._cm = cm

    async def __aenter__(self) -> T:
        return self._cm.__enter__()

    async def __aexit__(
        self,
        exc_type: "Optional[type[BaseException]]",
        exc_val: "Optional[BaseException]",
        exc_tb: "Optional[TracebackType]",
    ) -> "Optional[bool]":
        return self._cm.__exit__(exc_type, exc_val, exc_tb)


def with_ensure_async_(
    obj: "Union[AbstractContextManager[T], AbstractAsyncContextManager[T]]",
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

    Raises:
        StopAsyncIteration: The iterable given is not async.
    """
    has_default = bool(not isinstance(default, NoValue))
    try:
        return await iterable.__anext__()

    except StopAsyncIteration as exc:
        if has_default:
            return default

        raise StopAsyncIteration from exc
