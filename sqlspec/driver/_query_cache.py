"""Query cache for fast-path statement execution."""

import threading
from collections import OrderedDict
from typing import TYPE_CHECKING, Final, final

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters import ParameterProcessor

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlspec.core.compiler import OperationProfile, OperationType
    from sqlspec.core.parameters import ParameterProfile
    from sqlspec.core.statement import SQL, ProcessedState, StatementConfig

__all__ = (
    "STMT_CACHE_MAX_SIZE",
    "CachedQuery",
    "QueryCache",
    "StatementCacheHolder",
    "resolve_statement_cache_size",
)

STMT_CACHE_MAX_SIZE: Final[int] = 1024


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class CachedQuery:
    """Cached query metadata for fast-path execution."""

    compiled_sql: str
    parameter_profile: "ParameterProfile"
    input_named_parameters: "tuple[str, ...]"
    applied_wrap_types: bool
    parameter_casts: "dict[int, str]"
    operation_type: "OperationType"
    operation_profile: "OperationProfile"
    param_count: int
    processed_state: "ProcessedState"

    __slots__ = (
        "applied_wrap_types",
        "compiled_sql",
        "input_named_parameters",
        "operation_profile",
        "operation_type",
        "param_count",
        "parameter_casts",
        "parameter_profile",
        "processed_state",
    )

    def __init__(
        self,
        compiled_sql: str,
        parameter_profile: "ParameterProfile",
        input_named_parameters: "tuple[str, ...]",
        applied_wrap_types: bool,
        parameter_casts: "dict[int, str]",
        operation_type: "OperationType",
        operation_profile: "OperationProfile",
        param_count: int,
        processed_state: "ProcessedState",
    ) -> None:
        self.compiled_sql = compiled_sql
        self.parameter_profile = parameter_profile
        self.input_named_parameters = input_named_parameters
        self.applied_wrap_types = applied_wrap_types
        self.parameter_casts = parameter_casts
        self.operation_type = operation_type
        self.operation_profile = operation_profile
        self.param_count = param_count
        self.processed_state = processed_state


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class QueryCache:
    """LRU cache for compiled query metadata."""

    __slots__ = ("_cache", "_lock", "_max_size")

    def __init__(self, max_size: int = STMT_CACHE_MAX_SIZE) -> None:
        self._cache: OrderedDict[str, CachedQuery] = OrderedDict()
        self._lock = threading.Lock()
        self._max_size = max_size

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    def get(self, sql: str) -> "CachedQuery | None":
        with self._lock:
            entry = self._cache.get(sql)
            if entry is None:
                return None
            self._cache.move_to_end(sql)
            return entry

    def set(self, sql: str, entry: "CachedQuery") -> None:
        if self._max_size <= 0:
            return
        with self._lock:
            if sql in self._cache:
                self._cache.move_to_end(sql)
            elif len(self._cache) >= self._max_size:
                self._cache.popitem(last=False)
            self._cache[sql] = entry

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class StatementCacheHolder:
    """Shared per-config holder for statement caches, rebind processor, and execution hints."""

    statement_config: "StatementConfig"
    max_size: int
    query_cache: QueryCache
    rebind_processor: ParameterProcessor
    _statement_cache: "OrderedDict[str, SQL]"
    _row_count_hints: "OrderedDict[str, int]"
    _bulk_insert_sql: "OrderedDict[str, tuple[str, str]]"
    _lock: threading.Lock

    __slots__ = (
        "_bulk_insert_sql",
        "_lock",
        "_row_count_hints",
        "_statement_cache",
        "max_size",
        "query_cache",
        "rebind_processor",
        "statement_config",
    )

    def __init__(self, statement_config: "StatementConfig", max_size: int = STMT_CACHE_MAX_SIZE) -> None:
        self.statement_config = statement_config
        self.max_size = max_size
        self.query_cache = QueryCache(max_size)
        self.rebind_processor = ParameterProcessor(
            converter=statement_config.parameter_converter,
            validator=statement_config.parameter_validator,
            cache_max_size=0,
            validator_cache_max_size=0,
        )
        self._statement_cache = OrderedDict()
        self._row_count_hints = OrderedDict()
        self._bulk_insert_sql = OrderedDict()
        self._lock = threading.Lock()

    def get_statement(self, sql: str) -> "SQL | None":
        """Retrieve a cached SQL object and mark it as recently used."""
        with self._lock:
            entry = self._statement_cache.get(sql)
            if entry is None:
                return None
            self._statement_cache.move_to_end(sql)
            return entry

    def set_statement(self, sql: str, stmt: "SQL", max_size: int | None = None) -> None:
        """Store a prepared SQL object in the bounded LRU cache."""
        effective_max = self.max_size if max_size is None else max_size
        if effective_max <= 0:
            return
        with self._lock:
            if sql in self._statement_cache:
                self._statement_cache.move_to_end(sql)
            else:
                while len(self._statement_cache) >= effective_max:
                    self._statement_cache.popitem(last=False)
            self._statement_cache[sql] = stmt

    def get_row_count_hint(self, compiled_sql: str) -> int | None:
        """Return the last observed row count for a compiled SQL string."""
        with self._lock:
            entry = self._row_count_hints.get(compiled_sql)
            if entry is None:
                return None
            self._row_count_hints.move_to_end(compiled_sql)
            return entry

    def set_row_count_hint(self, compiled_sql: str, row_count: int) -> None:
        """Record the observed row count for a compiled SQL string."""
        if self.max_size <= 0:
            return
        with self._lock:
            if compiled_sql in self._row_count_hints:
                self._row_count_hints.move_to_end(compiled_sql)
            elif len(self._row_count_hints) >= self.max_size:
                self._row_count_hints.popitem(last=False)
            self._row_count_hints[compiled_sql] = row_count

    def get_bulk_insert_sql(self, compiled_sql: str) -> "tuple[str, str] | None":
        """Return cached (table_sql, columns_sql) fragments for a bulk INSERT statement."""
        with self._lock:
            entry = self._bulk_insert_sql.get(compiled_sql)
            if entry is None:
                return None
            self._bulk_insert_sql.move_to_end(compiled_sql)
            return entry

    def set_bulk_insert_sql(self, compiled_sql: str, entry: "tuple[str, str]") -> None:
        """Cache (table_sql, columns_sql) fragments for a bulk INSERT statement."""
        if self.max_size <= 0:
            return
        with self._lock:
            if compiled_sql in self._bulk_insert_sql:
                self._bulk_insert_sql.move_to_end(compiled_sql)
            elif len(self._bulk_insert_sql) >= self.max_size:
                self._bulk_insert_sql.popitem(last=False)
            self._bulk_insert_sql[compiled_sql] = entry

    def clear(self) -> None:
        """Clear all cached entries."""
        self.query_cache.clear()
        with self._lock:
            self._statement_cache.clear()
            self._row_count_hints.clear()
            self._bulk_insert_sql.clear()


def resolve_statement_cache_size(driver_features: "Mapping[str, object] | None") -> int:
    """Resolve the statement cache capacity from driver_features."""
    if not driver_features:
        return STMT_CACHE_MAX_SIZE
    cache_size = driver_features.get("sqlspec_statement_cache_size", STMT_CACHE_MAX_SIZE)
    if isinstance(cache_size, int):
        return max(0, cache_size)
    if isinstance(cache_size, str):
        try:
            return max(0, int(cache_size))
        except ValueError:
            return STMT_CACHE_MAX_SIZE
    return STMT_CACHE_MAX_SIZE
