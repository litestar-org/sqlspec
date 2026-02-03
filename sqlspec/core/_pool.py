"""Thread-local object pool primitives for performance-sensitive hot paths."""

from typing import TYPE_CHECKING, Generic, TypeVar

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")


@mypyc_attr(allow_interpreted_subclasses=False)
class ObjectPool(Generic[T]):
    """Reusable object pool with reset-instead-of-recreate semantics."""

    __slots__ = ("_factory", "_max_size", "_pool", "_resetter")

    def __init__(
        self,
        factory: "Callable[[], T]",
        resetter: "Callable[[T], None]",
        max_size: int = 100,
    ) -> None:
        self._pool: list[T] = []
        self._max_size = max_size
        self._factory = factory
        self._resetter = resetter

    def acquire(self) -> T:
        if self._pool:
            return self._pool.pop()
        return self._factory()

    def release(self, obj: T) -> None:
        self._resetter(obj)
        if len(self._pool) < self._max_size:
            self._pool.append(obj)
