"""Cache implementation for SQL statement processing."""

import threading
from collections import OrderedDict
from typing import Any, Optional

__all__ = ("SQLCache",)


DEFAULT_CACHE_MAX_SIZE = 1000


class SQLCache:
    """A thread-safe LRU cache for processed SQL states."""

    def __init__(self, max_size: int = DEFAULT_CACHE_MAX_SIZE) -> None:
        self.cache: OrderedDict[str, Any] = OrderedDict()
        self.max_size = max_size
        self.lock = threading.Lock()

    @property
    def size(self) -> int:
        """Get current cache size."""
        return len(self.cache)

    def get(self, key: str) -> Optional[Any]:
        """Get an item from the cache, marking it as recently used."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def set(self, key: str, value: Any) -> None:
        """Set an item in the cache with LRU eviction."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            # Add new entry
            elif len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
            self.cache[key] = value

    def clear(self) -> None:
        """Clear the cache."""
        with self.lock:
            self.cache.clear()


sql_cache = SQLCache()
