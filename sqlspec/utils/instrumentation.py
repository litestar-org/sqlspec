from typing import Callable

from typing_extensions import ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def with_instrumentation(func: Callable[P, T]) -> Callable[P, T]:
    """Decorator to instrument a function with timing and logging."""

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        import time

        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        end_time - start_time
        return result

    return wrapper
