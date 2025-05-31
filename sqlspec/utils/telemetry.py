from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    TypeVar,
    cast,
)

from typing_extensions import Concatenate, ParamSpec

if TYPE_CHECKING:
    from collections.abc import Awaitable

__all__ = (
    "instrument_async",
    "instrument_sync",
)

P = ParamSpec("P")
R = TypeVar("R")
SelfT = TypeVar("SelfT")


def instrument_sync(
    operation_name: "Optional[str]" = None,
    operation_type: "str" = "database",
    **custom_tags: "Any",
) -> "Callable[[Callable[Concatenate[SelfT, P], R]], Callable[Concatenate[SelfT, P], R]]":
    """Decorator for adding instrumentation to synchronous driver methods.

    Args:
        operation_name: Optional name for the operation.
        operation_type: Type of operation, defaults to "database".
        custom_tags: Additional tags for the span.

    Returns:
        Callable: Decorated function that instruments the operation.
    """

    def decorator(func: "Callable[Concatenate[SelfT, P], R]") -> "Callable[Concatenate[SelfT, P], R]":
        @wraps(func)
        def wrapper(self_obj: "SelfT", *args: "P.args", **kwargs: "P.kwargs") -> "R":
            if not hasattr(self_obj, "instrument_sync_operation"):
                return func(self_obj, *args, **kwargs)

            span_name = operation_name or f"{self_obj.__class__.__name__}.{func.__name__}"
            return cast(
                "R",
                self_obj.instrument_sync_operation(  # type: ignore[attr-defined]
                    span_name, operation_type, custom_tags, func, self_obj, *args, **kwargs
                ),
            )

        return wrapper  # type: ignore[return-value]

    return decorator


def instrument_async(
    operation_name: "Optional[str]" = None,
    operation_type: "str" = "database",
    **custom_tags: "Any",
) -> "Callable[[Callable[Concatenate[SelfT, P], Awaitable[R]]], Callable[Concatenate[SelfT, P], Awaitable[R]]]":
    """Decorator for adding instrumentation to asynchronous driver methods.

    Args:
        operation_name: Optional name for the operation.
        operation_type: Type of operation, defaults to "database".
        custom_tags: Additional tags for the span.

    Returns:
        Callable: Decorated function that instruments the operation.
    """

    def decorator(
        func: "Callable[Concatenate[SelfT, P], Awaitable[R]]",  # pyright: ignore
    ) -> "Callable[Concatenate[SelfT, P], Awaitable[R]]":  # pyright: ignore
        @wraps(func)
        async def wrapper(self_obj: "SelfT", *args: "P.args", **kwargs: "P.kwargs") -> "R":
            if not hasattr(self_obj, "instrument_async_operation"):
                return await func(self_obj, *args, **kwargs)

            span_name = operation_name or f"{self_obj.__class__.__name__}.{func.__name__}"
            return cast(
                "R",
                await self_obj.instrument_async_operation(  # type: ignore[attr-defined]
                    span_name, operation_type, custom_tags, func, self_obj, *args, **kwargs
                ),
            )

        return wrapper  # type: ignore[return-value]

    return decorator
