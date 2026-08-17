"""Shared exception handler bases for driver adapters.

Deferred Exception Pattern
--------------------------
mypyc-compiled ``__aexit__``/``__exit__`` methods cannot propagate new exceptions
raised inside the handler back through the ABI boundary reliably. To work around
this, the handler **stores** the mapped exception in ``pending_exception`` and
returns ``True`` (suppressing the original). After the ``async with`` / ``with``
operation completes, the calling dispatch method checks ``pending_exception``
and re-raises it explicitly in pure-Python control flow.

How to use in new dispatch methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1. Obtain an exc_handler via ``self.handle_database_exceptions()``.
2. For compiled async code, run the database call through the driver's
 ``_run_with_exception_handler`` helper. Synchronous code uses ``with``.
3. **After** the context manager exits, call
 ``self._check_pending_exception(exc_handler)`` to raise any mapped error.
 For dispatch methods that also record observability spans, use
 ``_raise_database_exception`` from ``sqlspec.driver._common`` which
 additionally re-chains the original exception.

Class hierarchy
^^^^^^^^^^^^^^^
``BaseAsyncExceptionHandler`` / ``BaseSyncExceptionHandler`` (this module)
 -> adapter-specific subclasses in ``sqlspec/adapters/{adapter}/driver.py``
 -> consumed by ``_check_pending_exception`` and ``_raise_database_exception``
 helpers on ``AsyncDriverAdapterBase`` / ``SyncDriverAdapterBase``.
"""

import asyncio
import sys
from typing import TYPE_CHECKING, Any, TypeVar

from mypy_extensions import mypyc_attr
from typing_extensions import Self

from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from sqlspec.driver._common import AsyncExceptionHandler

__all__ = ("BaseAsyncExceptionHandler", "BaseSyncExceptionHandler")

_AsyncResultT = TypeVar("_AsyncResultT")


async def _run_with_async_exception_handler(
    exc_handler: "AsyncExceptionHandler",
    operation: "Callable[..., Awaitable[_AsyncResultT]]",
    *args: Any,
    **kwargs: Any,
) -> "_AsyncResultT | None":
    """Run an async operation without inheriting an active exception state."""
    await exc_handler.__aenter__()
    result: _AsyncResultT | None = None
    error: BaseException | None = None
    traceback: TracebackType | None = None
    ambient_exception = sys.exc_info()[1]
    try:
        operation_awaitable = operation(*args, **kwargs)
        if ambient_exception is None:
            result = await operation_awaitable
        else:
            result = await asyncio.ensure_future(operation_awaitable)
    except BaseException as caught:
        error = caught
        traceback = caught.__traceback__

    if error is None:
        await exc_handler.__aexit__(None, None, None)
        return result

    suppressed = await exc_handler.__aexit__(type(error), error, traceback)
    if not suppressed:
        raise error.with_traceback(traceback)
    return None


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseAsyncExceptionHandler:
    """Base async exception handler using the deferred exception pattern."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool:
        _ = exc_tb
        if exc_val is None:
            return False

        # Do not re-map if already a SQLSpecError
        if isinstance(exc_val, SQLSpecError):
            return False

        return self._handle_exception(exc_type, exc_val)

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        """Handle an adapter exception.

        Subclasses should set ``pending_exception`` before returning ``True``.
        """
        _ = (exc_type, exc_val)
        return False


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseSyncExceptionHandler:
    """Base sync exception handler using the deferred exception pattern."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool:
        _ = exc_tb
        if exc_val is None:
            return False

        # Do not re-map if already a SQLSpecError
        if isinstance(exc_val, SQLSpecError):
            return False

        return self._handle_exception(exc_type, exc_val)

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        """Handle an adapter exception.

        Subclasses should set ``pending_exception`` before returning ``True``.
        """
        _ = (exc_type, exc_val)
        return False
