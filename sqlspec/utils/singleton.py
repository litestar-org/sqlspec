"""Singleton pattern implementation for SQLSpec.

Provides a thread-safe metaclass for implementing the singleton pattern.
Used for creating classes that should have only one instance per class type.
"""

import threading
from typing import Any, TypeVar

from mypy_extensions import mypyc_attr

__all__ = ("SingletonMeta",)


_T = TypeVar("_T")


@mypyc_attr(native_class=False)
class SingletonMeta(type):
    """Metaclass for singleton pattern."""

    _instances: "dict[type, object]" = {}
    _lock = threading.Lock()

    def __call__(cls: type[_T], *args: Any, **kwargs: Any) -> _T:
        """Call method for the singleton metaclass.

        Args:
            cls: The class being instantiated.
            *args: Positional arguments for the class constructor.
            **kwargs: Keyword arguments for the class constructor.

        Returns:
            The singleton instance of the class.
        """
        if cls not in SingletonMeta._instances:  # pyright: ignore[reportUnnecessaryContains]
            with SingletonMeta._lock:
                if cls not in SingletonMeta._instances:
                    instance = super().__call__(*args, **kwargs)  # type: ignore[misc]
                    SingletonMeta._instances[cls] = instance
        return SingletonMeta._instances[cls]  # type: ignore[return-value]
