from typing import Any, Final, Generic, TypeVar, cast

from mypy_extensions import mypyc_attr

__all__ = ("TypeDispatcher",)


T = TypeVar("T")
_CACHE_MISS: Final[object] = object()
_NO_MATCH: Final[object] = object()


@mypyc_attr(allow_interpreted_subclasses=False)
class TypeDispatcher(Generic[T]):
    """O(1) type lookup cache for Mypyc-compatible dispatch.

    Provides fast lookups for objects based on their type, with caching of MRO resolution.
    This replaces expensive isinstance checks in hot paths.
    """

    __slots__ = ("_cache", "_registry")

    def __init__(self) -> None:
        self._cache: dict[type, T | object] = {}
        self._registry: dict[type, T] = {}

    def register(self, type_: type, value: T) -> None:
        """Register a value for a specific type.

        Args:
            type_: The type to register.
            value: The value associated with the type.
        """
        self._registry[type_] = value
        self._cache.clear()  # Invalidate cache on new registration

    def register_all(self, registrations: "tuple[tuple[type, T], ...]") -> None:
        """Register multiple values while invalidating the cache once."""
        for type_, value in registrations:
            self._registry[type_] = value
        self._cache.clear()

    def get(self, obj: Any) -> T | None:
        """Get the value associated with the object's type.

        Uses O(1) cache lookup first, then falls back to MRO resolution.

        Args:
            obj: The object to lookup.

        Returns:
            The associated value or None if not found.
        """
        return self.resolve_type(type(obj))

    def resolve_type(self, obj_type: type) -> T | None:
        """Resolve a value directly from a concrete runtime type."""
        cached_value = self._cache.get(obj_type, _CACHE_MISS)
        if cached_value is _NO_MATCH:
            return None
        if cached_value is not _CACHE_MISS:
            return cast("T", cached_value)

        return self._resolve(obj_type)

    def _resolve(self, obj_type: type) -> T | None:
        """Resolve the value by walking the MRO.

        Args:
            obj_type: The type to resolve.

        Returns:
            The resolved value or None.
        """
        # Fast path: check registry directly
        direct_value = self._registry.get(obj_type, _CACHE_MISS)
        if direct_value is not _CACHE_MISS:
            self._cache[obj_type] = direct_value
            return cast("T", direct_value)

        # Slow path: walk MRO
        for base in obj_type.__mro__[1:]:
            value = self._registry.get(base, _CACHE_MISS)
            if value is not _CACHE_MISS:
                self._cache[obj_type] = value
                return cast("T", value)

        # ABC/protocol fallback: issubclass() resolves virtual hierarchies not present in __mro__.
        for registered_type, value in self._registry.items():
            if registered_type is obj_type:
                continue
            try:
                if not issubclass(obj_type, registered_type):
                    continue
            except TypeError:
                continue
            self._cache[obj_type] = value
            return value

        self._cache[obj_type] = _NO_MATCH
        return None

    def clear_cache(self) -> None:
        """Clear the resolution cache."""
        self._cache.clear()
