"""Query cache for fast-path statement execution."""

from collections import OrderedDict
from typing import TYPE_CHECKING, Final, NamedTuple

if TYPE_CHECKING:
    from sqlspec.core.compiler import OperationProfile, OperationType
    from sqlspec.core.parameters import ParameterProfile

__all__ = ("QC_MAX_SIZE", "CachedQuery", "QueryCache")

QC_MAX_SIZE: Final[int] = 1024


class CachedQuery(NamedTuple):
    """Cached query metadata for fast-path execution."""

    compiled_sql: str
    parameter_profile: "ParameterProfile"
    input_named_parameters: "tuple[str, ...]"
    applied_wrap_types: bool
    parameter_casts: "dict[int, str]"
    operation_type: "OperationType"
    operation_profile: "OperationProfile"
    param_count: int


class QueryCache:
    """LRU cache for compiled query metadata."""

    __slots__ = ("_cache", "_max_size")

    def __init__(self, max_size: int = QC_MAX_SIZE) -> None:
        self._cache: OrderedDict[str, CachedQuery] = OrderedDict()
        self._max_size = max_size

    def get(self, sql: str) -> "CachedQuery | None":
        entry = self._cache.get(sql)
        if entry is None:
            return None
        self._cache.move_to_end(sql)
        return entry

    def set(self, sql: str, entry: "CachedQuery") -> None:
        if sql in self._cache:
            self._cache.move_to_end(sql)
        elif len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)
        self._cache[sql] = entry

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)
