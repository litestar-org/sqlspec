"""Shared exception handler bases for driver adapters."""

from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr
from typing_extensions import Self

from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ("BaseAsyncExceptionHandler", "BaseSyncExceptionHandler")


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
