# sqlspec/statement_new/cache.py
import threading
from collections import OrderedDict
from typing import Any, Optional

from sqlspec.statement_new.config import DEFAULT_CACHE_MAX_SIZE

__all__ = ("SQLCache", )


class SQLCache:
    """A thread-safe LRU cache for SQLState objects."""

    def __init__(self, max_size: int = DEFAULT_CACHE_MAX_SIZE) -> None:
        self.cache: OrderedDict[str, Any] = OrderedDict()
        self.max_size = max_size
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """Get an item from the cache, marking it as recently used."""
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def set(self, key: str, value: Any) -> None:
        """Set an item in the cache with LRU eviction."""
        with self.lock:
            if key in self.cache:
                # Update existing and move to end
                self.cache.move_to_end(key)
            # Add new entry
            elif len(self.cache) >= self.max_size:
                # Remove least recently used (first item)
                self.cache.popitem(last=False)
            self.cache[key] = value

    def clear(self) -> None:
        """Clear the cache."""
        with self.lock:
            self.cache.clear()


# Global cache instance
sql_cache = SQLCache()
