"""A simple cached property decorator."""

from typing import Any, Callable, Generic, TypeVar

__all__ = ("CachedProperty",)


T = TypeVar("T")
Cls = TypeVar("Cls")


class CachedProperty(Generic[T]):
    """A property that is only computed once per instance and then replaces
    itself with an ordinary attribute. Deleting the attribute resets the
    property.
    """

    def __init__(self, func: Callable[[Any], T]) -> None:
        self.func = func

    def __get__(self, obj: Any, cls: type[Any]) -> Any:
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value
