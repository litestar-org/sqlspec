from typing import Any, Generic, TypeVar

__all__ = ("TypeDispatcher",)


T = TypeVar("T")


class TypeDispatcher(Generic[T]):
    """O(1) type lookup cache for Mypyc-compatible dispatch.

    Provides fast lookups for objects based on their type, with caching of MRO resolution.
    This replaces expensive isinstance checks in hot paths.
    """

    __slots__ = ("_cache", "_registry")

    def __init__(self) -> None:
        self._cache: dict[type, T] = {}
        self._registry: dict[type, T] = {}

    def register(self, type_: type, value: T) -> None:
        """Register a value for a specific type.

        Args:
            type_: The type to register.
            value: The value associated with the type.
        """
        self._registry[type_] = value
        self._cache.clear()  # Invalidate cache on new registration

    def get(self, obj: Any) -> T | None:
        """Get the value associated with the object's type.

        Uses O(1) cache lookup first, then falls back to MRO resolution.

        Args:
            obj: The object to lookup.

        Returns:
            The associated value or None if not found.
        """
        obj_type = type(obj)
        if obj_type in self._cache:
            return self._cache[obj_type]

        return self._resolve(obj_type)

    def _resolve(self, obj_type: type) -> T | None:
        """Resolve the value by walking the MRO.

        Args:
            obj_type: The type to resolve.

        Returns:
            The resolved value or None.
        """
        # Fast path: check registry directly
        if obj_type in self._registry:
            self._cache[obj_type] = self._registry[obj_type]
            return self._registry[obj_type]

        # Slow path: walk MRO
        for base in obj_type.__mro__:
            if base in self._registry:
                value = self._registry[base]
                self._cache[obj_type] = value
                return value

        return None

    def clear_cache(self) -> None:
        """Clear the resolution cache."""
        self._cache.clear()
