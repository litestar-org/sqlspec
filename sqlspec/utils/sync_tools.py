"""Utilities for async/sync interoperability in SQLSpec.

This module provides utilities for converting between async and sync functions,
managing concurrency limits, and handling context managers. Used primarily
for adapter implementations that need to support both sync and async patterns.
"""

import asyncio
import atexit
import concurrent.futures
import contextvars
import functools
import inspect
import os
import sys
import threading
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from typing_extensions import ParamSpec

from sqlspec.utils.env import get_env
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

DEFAULT_ASYNC_THREAD_LIMIT = 8
ASYNC_THREAD_LIMIT_ENV = "SQLSPEC_ASYNC_THREAD_LIMIT"
_ASYNC_THREAD_NAME_PREFIX = "sqlspec-async"


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
_default_async_executor: concurrent.futures.ThreadPoolExecutor | None = None
_default_async_executor_pid: int | None = None
_managed_async_executor: concurrent.futures.ThreadPoolExecutor | None = None
_managed_async_executor_enabled: bool = False
_managed_async_executor_lock = threading.Lock()
_managed_async_executor_max_workers: int | None = None
_managed_async_executor_pid: int | None = None


def set_default_async_executor(executor: "concurrent.futures.ThreadPoolExecutor | None") -> None:
    """Set a caller-owned process-local default thread executor for ``async_``.

    SQLSpec never shuts down caller-provided executors. If a process fork is
    detected later, the stored executor is cleared instead of reused.

    Args:
        executor: Caller-owned thread executor to use by default, or ``None`` to clear it.
    """
    global _default_async_executor, _default_async_executor_pid
    validated_executor = _validate_async_thread_executor(executor)
    with _managed_async_executor_lock:
        _default_async_executor = validated_executor
        _default_async_executor_pid = os.getpid() if validated_executor is not None else None


def enable_default_async_thread_pool(max_workers: int | None = None) -> None:
    """Enable SQLSpec's managed default executor for ``async_``.

    Args:
        max_workers: Optional worker limit. When omitted, ``SQLSPEC_ASYNC_THREAD_LIMIT``
            is read, falling back to ``DEFAULT_ASYNC_THREAD_LIMIT``.
    """
    global _managed_async_executor_enabled, _managed_async_executor_max_workers
    with _managed_async_executor_lock:
        _managed_async_executor_enabled = True
        _managed_async_executor_max_workers = max_workers
        _ensure_managed_async_executor_locked(os.getpid())


def get_default_async_executor() -> "concurrent.futures.ThreadPoolExecutor | None":
    """Return the configured or managed default executor for ``async_``.

    Returns:
        Caller-owned default thread executor, SQLSpec-managed executor, or ``None``.
    """
    global _default_async_executor, _default_async_executor_pid
    current_pid = os.getpid()
    with _managed_async_executor_lock:
        if _default_async_executor is not None:
            if _default_async_executor_pid == current_pid:
                return _default_async_executor
            _default_async_executor = None
            _default_async_executor_pid = None

        if _managed_async_executor_enabled or ASYNC_THREAD_LIMIT_ENV in os.environ:
            return _ensure_managed_async_executor_locked(current_pid)
    return None


def shutdown_default_async_executor(wait: bool = False) -> None:
    """Clear default async executors and shut down SQLSpec-owned pools.

    Args:
        wait: Whether to wait for SQLSpec-managed executor tasks to finish.
    """
    global _default_async_executor, _default_async_executor_pid
    global _managed_async_executor, _managed_async_executor_enabled, _managed_async_executor_max_workers
    global _managed_async_executor_pid
    with _managed_async_executor_lock:
        if _managed_async_executor is not None:
            _managed_async_executor.shutdown(wait=wait)
        _managed_async_executor = None
        _managed_async_executor_enabled = False
        _managed_async_executor_max_workers = None
        _managed_async_executor_pid = None
        _default_async_executor = None
        _default_async_executor_pid = None


def _ensure_managed_async_executor_locked(current_pid: int) -> concurrent.futures.ThreadPoolExecutor:
    global _managed_async_executor, _managed_async_executor_pid
    if _managed_async_executor is None or _managed_async_executor_pid != current_pid:
        if _managed_async_executor is not None:
            _managed_async_executor.shutdown(wait=False)
        max_workers = _resolve_managed_async_thread_limit()
        _managed_async_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=_ASYNC_THREAD_NAME_PREFIX
        )
        _managed_async_executor_pid = current_pid
    return _managed_async_executor


def _validate_async_thread_executor(executor: object) -> "concurrent.futures.ThreadPoolExecutor | None":
    if executor is None or isinstance(executor, concurrent.futures.ThreadPoolExecutor):
        return executor
    msg = "async_ executors must be concurrent.futures.ThreadPoolExecutor instances to preserve contextvars"
    raise TypeError(msg)


def _resolve_managed_async_thread_limit() -> int:
    if _managed_async_executor_max_workers is None:
        max_workers = get_env(ASYNC_THREAD_LIMIT_ENV, DEFAULT_ASYNC_THREAD_LIMIT)()
    else:
        max_workers = _managed_async_executor_max_workers
    return max(1, max_workers)


atexit.register(shutdown_default_async_executor)


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
            return uvloop.run(partial_f())  # pyright: ignore[reportUnknownMemberType]
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
    function: "Callable[ParamSpecT, ReturnT]",
    *,
    limiter: "CapacityLimiter | None" = None,
    executor: "concurrent.futures.ThreadPoolExecutor | None" = None,
) -> "Callable[ParamSpecT, Awaitable[ReturnT]]":
    """Convert a blocking function to an async one using asyncio.to_thread().

    Args:
        function: The blocking function to convert.
        limiter: Limit the total number of threads.
        executor: Optional thread executor to use instead of the event loop's default thread executor.

    Returns:
        An async function that runs the original function in a thread.
    """

    return _AsyncWrapper(function, limiter, executor)


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
                if self._raise_sync_error:
                    msg = "await_ cannot be called from within an async task running on the same event loop. Use 'await' instead."
                    raise RuntimeError(msg)
                portal = get_global_portal()
                typed_partial = cast("Callable[[], Coroutine[Any, Any, ReturnT]]", partial_f)
                return portal.call(typed_partial)
            future = asyncio.run_coroutine_threadsafe(partial_f(), loop)
            return future.result()
        if self._raise_sync_error:
            msg = "Cannot run async function"
            raise RuntimeError(msg)
        portal = get_global_portal()
        typed_partial = cast("Callable[[], Coroutine[Any, Any, ReturnT]]", partial_f)
        return portal.call(typed_partial)


class _AsyncWrapper(Generic[ParamSpecT, ReturnT]):
    __slots__ = ("__dict__", "_executor", "_function", "_limiter")

    def __init__(
        self,
        function: "Callable[ParamSpecT, ReturnT]",
        limiter: "CapacityLimiter | None",
        executor: "concurrent.futures.ThreadPoolExecutor | None",
    ) -> None:
        self._executor = _validate_async_thread_executor(executor)
        self._function = function
        self._limiter = limiter
        functools.update_wrapper(self, function)

    async def __call__(self, *args: "ParamSpecT.args", **kwargs: "ParamSpecT.kwargs") -> "ReturnT":
        executor = self._executor if self._executor is not None else get_default_async_executor()
        if self._limiter is not None:
            async with self._limiter:
                return await self._run_sync(executor, *args, **kwargs)
        return await self._run_sync(executor, *args, **kwargs)

    async def _run_sync(
        self,
        executor: "concurrent.futures.ThreadPoolExecutor | None",
        *args: "ParamSpecT.args",
        **kwargs: "ParamSpecT.kwargs",
    ) -> "ReturnT":
        if executor is None:
            partial_f = functools.partial(self._function, *args, **kwargs)
            return await asyncio.to_thread(partial_f)

        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        call = functools.partial(ctx.run, self._function, *args, **kwargs)
        return await loop.run_in_executor(executor, call)


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
