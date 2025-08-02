"""Cache implementation for SQL statement processing."""

import copy
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Final, Optional

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = (
    "ASTFragmentCache",
    "BaseStatementCache",
    "CacheConfig",
    "CacheStats",
    "CachedFragment",
    "FilterCache",
    "SQLCache",
    "analysis_cache",
    "anonymous_returns_rows_cache",
    "ast_fragment_cache",
    "base_statement_cache",
    "builder_cache",
    "file_cache",
    "filtered_ast_cache",
    "get_cache_config",
    "get_cache_stats",
    "log_cache_stats",
    "optimized_expression_cache",
    "reset_cache_stats",
    "sql_cache",
    "update_cache_config",
)


DEFAULT_CACHE_MAX_SIZE: Final[int] = 1000
DEFAULT_FRAGMENT_CACHE_SIZE: Final[int] = 5000
DEFAULT_BASE_STATEMENT_CACHE_SIZE: Final[int] = 2000
DEFAULT_FILTER_CACHE_SIZE: Final[int] = 1000
DEFAULT_COMPILED_STATEMENT_CACHE_SIZE: Final[int] = 1000
DEFAULT_BUILDER_CACHE_SIZE: Final[int] = 500
DEFAULT_FILE_CACHE_SIZE: Final[int] = 100


@dataclass
class CacheConfig:
    """Configuration for SQLSpec caching layers."""

    sql_cache_size: int = DEFAULT_CACHE_MAX_SIZE
    sql_cache_enabled: bool = True
    fragment_cache_size: int = DEFAULT_FRAGMENT_CACHE_SIZE
    fragment_cache_enabled: bool = True
    optimized_cache_size: int = DEFAULT_CACHE_MAX_SIZE
    optimized_cache_enabled: bool = True
    anonymous_returns_rows_cache_size: int = DEFAULT_CACHE_MAX_SIZE
    anonymous_returns_rows_cache_enabled: bool = True
    compiled_cache_size: int = DEFAULT_COMPILED_STATEMENT_CACHE_SIZE
    compiled_cache_enabled: bool = True

    # Analysis caching configuration
    analysis_cache_size: int = DEFAULT_CACHE_MAX_SIZE
    analysis_cache_enabled: bool = True

    # QueryBuilder caching configuration
    builder_cache_size: int = DEFAULT_BUILDER_CACHE_SIZE
    builder_cache_enabled: bool = True

    # SQLFileLoader caching configuration
    file_cache_size: int = DEFAULT_FILE_CACHE_SIZE
    file_cache_enabled: bool = True

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.sql_cache_size < 0:
            msg = "sql_cache_size must be non-negative"
            raise ValueError(msg)
        if self.fragment_cache_size < 0:
            msg = "fragment_cache_size must be non-negative"
            raise ValueError(msg)
        if self.optimized_cache_size < 0:
            msg = "optimized_cache_size must be non-negative"
            raise ValueError(msg)
        if self.anonymous_returns_rows_cache_size < 0:
            msg = "anonymous_returns_rows_cache_size must be non-negative"
            raise ValueError(msg)
        if self.compiled_cache_size < 0:
            msg = "compiled_cache_size must be non-negative"
            raise ValueError(msg)
        if self.analysis_cache_size < 0:
            msg = "analysis_cache_size must be non-negative"
            raise ValueError(msg)
        if self.builder_cache_size < 0:
            msg = "builder_cache_size must be non-negative"
            raise ValueError(msg)
        if self.file_cache_size < 0:
            msg = "file_cache_size must be non-negative"
            raise ValueError(msg)


@dataclass
class CacheStats:
    """Statistics for cache performance monitoring."""

    # SQL cache stats
    sql_hits: int = 0
    sql_misses: int = 0
    sql_evictions: int = 0
    sql_size: int = 0

    # Fragment cache stats
    fragment_hits: int = 0
    fragment_misses: int = 0
    fragment_evictions: int = 0
    fragment_size: int = 0

    # Optimized cache stats
    optimized_hits: int = 0
    optimized_misses: int = 0
    optimized_evictions: int = 0
    optimized_size: int = 0

    # Anonymous returns rows cache stats
    anonymous_returns_rows_hits: int = 0
    anonymous_returns_rows_misses: int = 0
    anonymous_returns_rows_evictions: int = 0
    anonymous_returns_rows_size: int = 0

    # Compiled statement cache stats
    compiled_hits: int = 0
    compiled_misses: int = 0
    compiled_evictions: int = 0
    compiled_size: int = 0

    # Analysis cache stats
    analysis_hits: int = 0
    analysis_misses: int = 0
    analysis_evictions: int = 0
    analysis_size: int = 0

    # Builder cache stats
    builder_hits: int = 0
    builder_misses: int = 0
    builder_evictions: int = 0
    builder_size: int = 0

    # File cache stats
    file_hits: int = 0
    file_misses: int = 0
    file_evictions: int = 0
    file_size: int = 0

    # Timing stats (in seconds)
    avg_cache_lookup_time: float = 0.0
    avg_parse_time: float = 0.0
    avg_optimize_time: float = 0.0

    @property
    def sql_hit_rate(self) -> float:
        """Calculate SQL cache hit rate."""
        total = self.sql_hits + self.sql_misses
        return self.sql_hits / total if total > 0 else 0.0

    @property
    def fragment_hit_rate(self) -> float:
        """Calculate fragment cache hit rate."""
        total = self.fragment_hits + self.fragment_misses
        return self.fragment_hits / total if total > 0 else 0.0

    @property
    def optimized_hit_rate(self) -> float:
        """Calculate optimized expression cache hit rate."""
        total = self.optimized_hits + self.optimized_misses
        return self.optimized_hits / total if total > 0 else 0.0

    @property
    def anonymous_returns_rows_hit_rate(self) -> float:
        """Calculate anonymous returns rows cache hit rate."""
        total = self.anonymous_returns_rows_hits + self.anonymous_returns_rows_misses
        return self.anonymous_returns_rows_hits / total if total > 0 else 0.0

    @property
    def compiled_hit_rate(self) -> float:
        """Calculate compiled statement cache hit rate."""
        total = self.compiled_hits + self.compiled_misses
        return self.compiled_hits / total if total > 0 else 0.0

    @property
    def analysis_hit_rate(self) -> float:
        """Calculate analysis cache hit rate."""
        total = self.analysis_hits + self.analysis_misses
        return self.analysis_hits / total if total > 0 else 0.0

    @property
    def builder_hit_rate(self) -> float:
        """Calculate builder cache hit rate."""
        total = self.builder_hits + self.builder_misses
        return self.builder_hits / total if total > 0 else 0.0

    @property
    def file_hit_rate(self) -> float:
        """Calculate file cache hit rate."""
        total = self.file_hits + self.file_misses
        return self.file_hits / total if total > 0 else 0.0

    @property
    def overall_hit_rate(self) -> float:
        """Calculate overall cache hit rate across all caches."""
        total_hits = (
            self.sql_hits
            + self.fragment_hits
            + self.optimized_hits
            + self.anonymous_returns_rows_hits
            + self.compiled_hits
            + self.analysis_hits
            + self.builder_hits
            + self.file_hits
        )
        total_accesses = (
            self.sql_hits
            + self.sql_misses
            + self.fragment_hits
            + self.fragment_misses
            + self.optimized_hits
            + self.optimized_misses
            + self.anonymous_returns_rows_hits
            + self.anonymous_returns_rows_misses
            + self.compiled_hits
            + self.compiled_misses
            + self.analysis_hits
            + self.analysis_misses
            + self.builder_hits
            + self.builder_misses
            + self.file_hits
            + self.file_misses
        )
        return total_hits / total_accesses if total_accesses > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert stats to dictionary for logging/monitoring."""
        return {
            "sql_cache": {
                "hits": self.sql_hits,
                "misses": self.sql_misses,
                "hit_rate": self.sql_hit_rate,
                "evictions": self.sql_evictions,
                "size": self.sql_size,
            },
            "fragment_cache": {
                "hits": self.fragment_hits,
                "misses": self.fragment_misses,
                "hit_rate": self.fragment_hit_rate,
                "evictions": self.fragment_evictions,
                "size": self.fragment_size,
            },
            "optimized_cache": {
                "hits": self.optimized_hits,
                "misses": self.optimized_misses,
                "hit_rate": self.optimized_hit_rate,
                "evictions": self.optimized_evictions,
                "size": self.optimized_size,
            },
            "anonymous_returns_rows_cache": {
                "hits": self.anonymous_returns_rows_hits,
                "misses": self.anonymous_returns_rows_misses,
                "hit_rate": self.anonymous_returns_rows_hit_rate,
                "evictions": self.anonymous_returns_rows_evictions,
                "size": self.anonymous_returns_rows_size,
            },
            "compiled_cache": {
                "hits": self.compiled_hits,
                "misses": self.compiled_misses,
                "hit_rate": self.compiled_hit_rate,
                "evictions": self.compiled_evictions,
                "size": self.compiled_size,
            },
            "analysis_cache": {
                "hits": self.analysis_hits,
                "misses": self.analysis_misses,
                "hit_rate": self.analysis_hit_rate,
                "evictions": self.analysis_evictions,
                "size": self.analysis_size,
            },
            "file_cache": {
                "hits": self.file_hits,
                "misses": self.file_misses,
                "hit_rate": self.file_hit_rate,
                "evictions": self.file_evictions,
                "size": self.file_size,
            },
            "performance": {
                "avg_cache_lookup_time_ms": self.avg_cache_lookup_time * 1000,
                "avg_parse_time_ms": self.avg_parse_time * 1000,
                "avg_optimize_time_ms": self.avg_optimize_time * 1000,
            },
            "overall": {
                "hit_rate": self.overall_hit_rate,
                "total_size": self.sql_size
                + self.fragment_size
                + self.optimized_size
                + self.anonymous_returns_rows_size
                + self.compiled_size
                + self.analysis_size
                + self.file_size,
            },
        }


_cache_config = CacheConfig()
_cache_stats = CacheStats()


class SQLCache:
    """A thread-safe LRU cache for processed SQL states."""

    __slots__ = ("_eviction_count", "_max_size", "cache", "cache_name", "lock")

    def __init__(self, max_size: int = DEFAULT_CACHE_MAX_SIZE, cache_name: str = "sql") -> None:
        self.cache: OrderedDict[str, Any] = OrderedDict()
        self._max_size = max_size
        self.lock = threading.Lock()
        self.cache_name = cache_name
        self._eviction_count = 0

    @property
    def max_size(self) -> int:
        """Get maximum cache size."""
        return self._max_size

    @max_size.setter
    def max_size(self, value: int) -> None:
        """Set maximum cache size."""
        with self.lock:
            self._max_size = value
            # If cache is over new size limit, evict oldest entries
            while len(self.cache) > self._max_size:
                self.cache.popitem(last=False)
                self._eviction_count += 1
                self._record_eviction()

    @property
    def size(self) -> int:
        """Get current cache size."""
        return len(self.cache)

    @property
    def enabled(self) -> bool:
        """Check if cache is enabled (has non-zero max size)."""
        return self._max_size > 0

    def get(self, key: str) -> Optional[Any]:
        """Get an item from the cache, marking it as recently used."""
        if not self.enabled:
            return None
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                self._record_hit()
                return self._safe_copy(self.cache[key])
            self._record_miss()
            return None

    def set(self, key: str, value: Any) -> None:
        """Set an item in the cache with LRU eviction."""
        if not self.enabled:
            return
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            # Add new entry
            elif len(self.cache) >= self._max_size:
                self.cache.popitem(last=False)
                self._eviction_count += 1
                self._record_eviction()
            self.cache[key] = value

    def clear(self) -> None:
        """Clear the cache."""
        with self.lock:
            self.cache.clear()

    def _safe_copy(self, value: Any) -> Any:
        """Create a safe copy of cached values to prevent mutation.

        For compiled SQL results, we need to copy the parameters list/dict
        but can reuse immutable strings and expressions.
        """
        if value is None:
            return None

        # Handle tuple of (sql_string, parameters) - common for compiled SQL cache
        sql_params_tuple_size = 2
        if isinstance(value, tuple) and len(value) == sql_params_tuple_size:
            sql_string, parameters = value
            # SQL string is immutable, but parameters might be mutable
            return (sql_string, self._safe_copy(parameters))

        if isinstance(value, (list, dict)):
            return value.copy()

        # For complex objects or when in doubt, use deep copy
        # This handles nested structures safely
        if isinstance(value, (tuple, list, dict, set)):
            return copy.deepcopy(value)

        # Immutable objects can be returned as-is
        return value

    def _record_hit(self) -> None:
        """Record a cache hit in statistics."""
        hit_counters: dict[str, Callable[[], None]] = {
            "sql": lambda: setattr(_cache_stats, "sql_hits", _cache_stats.sql_hits + 1),
            "optimized": lambda: setattr(_cache_stats, "optimized_hits", _cache_stats.optimized_hits + 1),
            "anonymous_returns_rows": lambda: setattr(
                _cache_stats, "anonymous_returns_rows_hits", _cache_stats.anonymous_returns_rows_hits + 1
            ),
            "analysis": lambda: setattr(_cache_stats, "analysis_hits", _cache_stats.analysis_hits + 1),
            "builder": lambda: setattr(_cache_stats, "builder_hits", _cache_stats.builder_hits + 1),
            "file": lambda: setattr(_cache_stats, "file_hits", _cache_stats.file_hits + 1),
        }
        counter = hit_counters.get(self.cache_name)
        if counter is not None:
            counter()

    def _record_miss(self) -> None:
        """Record a cache miss in statistics."""
        miss_counters: dict[str, Callable[[], None]] = {
            "sql": lambda: setattr(_cache_stats, "sql_misses", _cache_stats.sql_misses + 1),
            "optimized": lambda: setattr(_cache_stats, "optimized_misses", _cache_stats.optimized_misses + 1),
            "anonymous_returns_rows": lambda: setattr(
                _cache_stats, "anonymous_returns_rows_misses", _cache_stats.anonymous_returns_rows_misses + 1
            ),
            "analysis": lambda: setattr(_cache_stats, "analysis_misses", _cache_stats.analysis_misses + 1),
            "builder": lambda: setattr(_cache_stats, "builder_misses", _cache_stats.builder_misses + 1),
            "file": lambda: setattr(_cache_stats, "file_misses", _cache_stats.file_misses + 1),
        }
        counter = miss_counters.get(self.cache_name)
        if counter is not None:
            counter()

    def _record_eviction(self) -> None:
        """Record a cache eviction in statistics."""
        eviction_counters: dict[str, Callable[[], None]] = {
            "sql": lambda: setattr(_cache_stats, "sql_evictions", _cache_stats.sql_evictions + 1),
            "optimized": lambda: setattr(_cache_stats, "optimized_evictions", _cache_stats.optimized_evictions + 1),
            "anonymous_returns_rows": lambda: setattr(
                _cache_stats, "anonymous_returns_rows_evictions", _cache_stats.anonymous_returns_rows_evictions + 1
            ),
            "analysis": lambda: setattr(_cache_stats, "analysis_evictions", _cache_stats.analysis_evictions + 1),
            "builder": lambda: setattr(_cache_stats, "builder_evictions", _cache_stats.builder_evictions + 1),
            "file": lambda: setattr(_cache_stats, "file_evictions", _cache_stats.file_evictions + 1),
        }
        counter = eviction_counters.get(self.cache_name)
        if counter is not None:
            counter()


class CachedFragment:
    """Cached AST fragment with metadata."""

    __slots__ = ("dialect", "expression", "fragment_type", "parameter_count", "sql")

    def __init__(
        self,
        expression: exp.Expression,
        sql: str,
        fragment_type: str,
        dialect: "Optional[DialectType]" = None,
        parameter_count: int = 0,
    ) -> None:
        self.expression = expression
        self.sql = sql
        self.fragment_type = fragment_type
        self.dialect = dialect
        self.parameter_count = parameter_count


class ASTFragmentCache:
    """Thread-safe cache for parsed AST fragments.

    This cache stores parsed expressions for common SQL fragments to avoid
    re-parsing. It uses a two-level cache structure:
    1. Fragment cache: Individual WHERE/JOIN/subquery expressions
    2. Template cache: Parameterized SQL templates with placeholders
    """

    def __init__(self, max_size: int = DEFAULT_FRAGMENT_CACHE_SIZE) -> None:
        self.fragment_cache: OrderedDict[str, CachedFragment] = OrderedDict()
        self.template_cache: OrderedDict[str, CachedFragment] = OrderedDict()
        self._max_size = max_size
        self.lock = threading.Lock()
        self._hit_count = 0
        self._miss_count = 0

    @property
    def max_size(self) -> int:
        """Get maximum cache size."""
        return self._max_size

    @max_size.setter
    def max_size(self, value: int) -> None:
        """Set maximum cache size."""
        with self.lock:
            self._max_size = value
            # Evict from fragment cache if needed
            while len(self.fragment_cache) > self._max_size // 2:
                self.fragment_cache.popitem(last=False)
                _cache_stats.fragment_evictions += 1
            # Evict from template cache if needed
            while len(self.template_cache) > self._max_size // 2:
                self.template_cache.popitem(last=False)
                _cache_stats.fragment_evictions += 1

    @property
    def size(self) -> int:
        """Get total cache size (fragments + templates)."""
        return len(self.fragment_cache) + len(self.template_cache)

    @property
    def enabled(self) -> bool:
        """Check if cache is enabled (has non-zero max size)."""
        return self._max_size > 0

    @property
    def hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self._hit_count + self._miss_count
        return self._hit_count / total if total > 0 else 0.0

    def get_fragment(
        self, sql: str, fragment_type: str, dialect: "Optional[DialectType]" = None
    ) -> Optional[CachedFragment]:
        """Get a cached fragment by SQL and type.

        Args:
            sql: The SQL fragment text
            fragment_type: Type of fragment (WHERE, JOIN, SUBQUERY, etc.)
            dialect: SQL dialect

        Returns:
            Cached fragment if found, None otherwise
        """
        cache_key = self._make_fragment_key(sql, fragment_type, dialect)

        with self.lock:
            if cache_key in self.fragment_cache:
                self._hit_count += 1
                _cache_stats.fragment_hits += 1
                self.fragment_cache.move_to_end(cache_key)
                return self.fragment_cache[cache_key]

            self._miss_count += 1
            _cache_stats.fragment_misses += 1
            return None

    def set_fragment(
        self,
        sql: str,
        expression: exp.Expression,
        fragment_type: str,
        dialect: "Optional[DialectType]" = None,
        parameter_count: int = 0,
    ) -> None:
        """Cache a parsed fragment.

        Args:
            sql: The SQL fragment text
            expression: Parsed expression
            fragment_type: Type of fragment
            dialect: SQL dialect
            parameter_count: Number of parameters in the fragment
        """
        cache_key = self._make_fragment_key(sql, fragment_type, dialect)

        with self.lock:
            if cache_key in self.fragment_cache:
                self.fragment_cache.move_to_end(cache_key)
                return

            # Evict if needed
            if len(self.fragment_cache) >= self._max_size // 2:
                self.fragment_cache.popitem(last=False)
                _cache_stats.fragment_evictions += 1

            cached = CachedFragment(
                expression=expression.copy(),  # Store a copy to avoid mutations
                sql=sql,
                fragment_type=fragment_type,
                dialect=dialect,
                parameter_count=parameter_count,
            )
            self.fragment_cache[cache_key] = cached

    def parse_with_cache(
        self, sql: str, fragment_type: str = "QUERY", dialect: "Optional[DialectType]" = None
    ) -> Optional[exp.Expression]:
        """Parse SQL with caching support.

        This method first checks the cache, and if not found, parses
        the SQL and caches the result.

        Args:
            sql: SQL to parse
            fragment_type: Type of SQL fragment
            dialect: SQL dialect

        Returns:
            Parsed expression or None if parsing fails
        """
        # Check cache first
        cached = self.get_fragment(sql, fragment_type, dialect)
        if cached:
            return cached.expression.copy()

        # Parse the SQL
        try:
            expressions = sqlglot.parse(sql, dialect=dialect)
            if expressions and expressions[0]:
                expression = expressions[0]

                # Count parameters
                param_count = self._count_parameters(expression)

                # Cache the result
                self.set_fragment(
                    sql=sql,
                    expression=expression,
                    fragment_type=fragment_type,
                    dialect=dialect,
                    parameter_count=param_count,
                )

                return expression.copy()
        except ParseError:
            pass

        return None

    def clear(self) -> None:
        """Clear all caches."""
        with self.lock:
            self.fragment_cache.clear()
            self.template_cache.clear()
            self._hit_count = 0
            self._miss_count = 0

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            return {
                "fragment_count": len(self.fragment_cache),
                "template_count": len(self.template_cache),
                "total_size": self.size,
                "max_size": self._max_size,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_rate": self.hit_rate,
            }

    @staticmethod
    def _make_fragment_key(sql: str, fragment_type: str, dialect: "Optional[DialectType]") -> str:
        """Create cache key for a fragment."""
        # Normalize SQL for better cache hits
        normalized_sql = sql.strip().lower()
        dialect_str = str(dialect) if dialect else "default"
        return f"fragment:{fragment_type}:{dialect_str}:{hash(normalized_sql)}"

    @staticmethod
    def _count_parameters(expression: exp.Expression) -> int:
        """Count parameter placeholders in an expression."""
        count = 0
        for node in expression.walk():
            if isinstance(node, exp.Placeholder):
                count += 1
        return count


class BaseStatementCache:
    """Thread-safe cache for base SQL statements before any modifications.

    This cache stores parsed AST for raw SQL strings to avoid re-parsing
    the same base statements repeatedly. This provides the biggest performance
    improvement as the same "SELECT * FROM users" is parsed thousands of times.
    """

    def __init__(self, max_size: int = DEFAULT_BASE_STATEMENT_CACHE_SIZE) -> None:
        self._cache: OrderedDict[tuple[str, str], exp.Expression] = OrderedDict()
        self._max_size = max_size
        self._lock = threading.Lock()
        self._hit_count = 0
        self._miss_count = 0

    @property
    def size(self) -> int:
        """Get current cache size."""
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self._hit_count + self._miss_count
        return self._hit_count / total if total > 0 else 0.0

    def get_or_parse(self, sql: str, dialect: Optional[str] = None) -> exp.Expression:
        """Retrieves a copied AST from cache or parses and caches it.

        Args:
            sql: Raw SQL string to parse
            dialect: Optional SQL dialect

        Returns:
            Parsed expression (always a copy to prevent mutation)
        """
        key = (sql.strip(), dialect or "default")

        with self._lock:
            if key in self._cache:
                self._hit_count += 1
                self._cache.move_to_end(key)
                # CRITICAL: Always return a copy to prevent cache poisoning
                return self._cache[key].copy()

        # Parse outside the lock to avoid blocking other threads
        # Let ParseError exceptions propagate to the caller
        ast = sqlglot.parse_one(sql, read=dialect)

        with self._lock:
            # Double-check pattern to prevent cache stampede
            if key in self._cache:
                self._hit_count += 1
                self._cache.move_to_end(key)
                return self._cache[key].copy()

            self._miss_count += 1

            if len(self._cache) >= self._max_size:
                self._cache.popitem(last=False)

            self._cache[key] = ast

        return ast.copy()

    def clear(self) -> None:
        """Clear the cache."""
        with self._lock:
            self._cache.clear()
            self._hit_count = 0
            self._miss_count = 0

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                "size": self.size,
                "max_size": self._max_size,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_rate": self.hit_rate,
            }


class FilterCache:
    """Thread-safe cache for filter chain results.

    This cache stores the results of applying filter chains to base statements.
    Since only StatementFilters modify SQL after creation, this cache has high
    hit rates for common filter patterns.
    """

    def __init__(self, max_size: int = DEFAULT_FILTER_CACHE_SIZE) -> None:
        self._cache: OrderedDict[tuple[int, tuple[Any, ...]], exp.Expression] = OrderedDict()
        self._max_size = max_size
        self._lock = threading.Lock()
        self._hit_count = 0
        self._miss_count = 0

    @property
    def size(self) -> int:
        """Get current cache size."""
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self._hit_count + self._miss_count
        return self._hit_count / total if total > 0 else 0.0

    def get(self, key: tuple[int, tuple[Any, ...]]) -> Optional[exp.Expression]:
        """Get cached filter result.

        Args:
            key: Tuple of (base_ast_hash, filter_chain_hash)

        Returns:
            Cached expression (copy) if found, None otherwise
        """
        with self._lock:
            if key in self._cache:
                self._hit_count += 1
                self._cache.move_to_end(key)
                return self._cache[key].copy()

            self._miss_count += 1
            return None

    def set(self, key: tuple[int, tuple[Any, ...]], value: exp.Expression) -> None:
        """Cache filter result.

        Args:
            key: Tuple of (base_ast_hash, filter_chain_hash)
            value: Expression to cache
        """
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return

            if len(self._cache) >= self._max_size:
                self._cache.popitem(last=False)

            self._cache[key] = value

    def clear(self) -> None:
        """Clear the cache."""
        with self._lock:
            self._cache.clear()
            self._hit_count = 0
            self._miss_count = 0

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                "size": self.size,
                "max_size": self._max_size,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_rate": self.hit_rate,
            }


def get_cache_config() -> CacheConfig:
    """Get the current cache configuration."""
    return _cache_config


def update_cache_config(config: CacheConfig) -> None:
    """Update the cache configuration.

    Note: This will clear all existing caches.
    """
    global _cache_config  # noqa: PLW0603
    _cache_config = config

    # Apply new configuration to caches
    if config.sql_cache_enabled:
        sql_cache.max_size = config.sql_cache_size
    else:
        sql_cache.clear()

    if config.fragment_cache_enabled:
        ast_fragment_cache.max_size = config.fragment_cache_size
    else:
        ast_fragment_cache.clear()

    if config.optimized_cache_enabled:
        optimized_expression_cache.max_size = config.optimized_cache_size
    else:
        optimized_expression_cache.clear()

    if config.anonymous_returns_rows_cache_enabled:
        anonymous_returns_rows_cache.max_size = config.anonymous_returns_rows_cache_size
    else:
        anonymous_returns_rows_cache.clear()

    if config.analysis_cache_enabled:
        analysis_cache.max_size = config.analysis_cache_size
    else:
        analysis_cache.clear()

    if config.builder_cache_enabled:
        builder_cache.max_size = config.builder_cache_size
    else:
        builder_cache.clear()

    if config.file_cache_enabled:
        file_cache.max_size = config.file_cache_size
    else:
        file_cache.clear()


def get_cache_stats() -> CacheStats:
    """Get current cache statistics."""
    # Update sizes
    _cache_stats.sql_size = sql_cache.size
    _cache_stats.fragment_size = ast_fragment_cache.size
    _cache_stats.optimized_size = optimized_expression_cache.size
    _cache_stats.anonymous_returns_rows_size = anonymous_returns_rows_cache.size
    _cache_stats.compiled_size = base_statement_cache.size
    _cache_stats.analysis_size = analysis_cache.size
    _cache_stats.builder_size = builder_cache.size
    _cache_stats.file_size = file_cache.size
    # Update fragment cache stats from internal counters
    _cache_stats.fragment_hits = ast_fragment_cache._hit_count
    _cache_stats.fragment_misses = ast_fragment_cache._miss_count

    return _cache_stats


def reset_cache_stats() -> None:
    """Reset all cache statistics."""
    global _cache_stats  # noqa: PLW0603
    _cache_stats = CacheStats()

    # Reset fragment cache internal counters
    ast_fragment_cache._hit_count = 0
    ast_fragment_cache._miss_count = 0


def log_cache_stats() -> None:
    """Log current cache statistics."""
    logger = get_logger("sqlspec.cache")
    stats = get_cache_stats()
    logger.info("Cache Statistics", extra=stats.to_dict())


sql_cache = SQLCache(max_size=_cache_config.sql_cache_size, cache_name="sql")
ast_fragment_cache = ASTFragmentCache(max_size=_cache_config.fragment_cache_size)
optimized_expression_cache = SQLCache(max_size=_cache_config.optimized_cache_size, cache_name="optimized")
base_statement_cache = BaseStatementCache()
filtered_ast_cache = FilterCache()
anonymous_returns_rows_cache = SQLCache(
    max_size=_cache_config.anonymous_returns_rows_cache_size, cache_name="anonymous_returns_rows"
)
analysis_cache = SQLCache(max_size=_cache_config.analysis_cache_size, cache_name="analysis")
builder_cache = SQLCache(max_size=_cache_config.builder_cache_size, cache_name="builder")
file_cache = SQLCache(max_size=_cache_config.file_cache_size, cache_name="file")
