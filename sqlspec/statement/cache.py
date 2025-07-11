"""Cache implementation for SQL statement processing."""

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = (
    "ASTFragmentCache",
    "BaseStatementCache",
    "CachedFragment",
    "FilteredASTCache",
    "SQLCache",
    "ast_fragment_cache",
    "base_statement_cache",
    "filtered_ast_cache",
    "optimized_expression_cache",
    "sql_cache",
)


DEFAULT_CACHE_MAX_SIZE = 1000
DEFAULT_FRAGMENT_CACHE_SIZE = 5000
DEFAULT_BASE_STATEMENT_CACHE_SIZE = 2000
DEFAULT_FILTER_CACHE_SIZE = 1000


class SQLCache:
    """A thread-safe LRU cache for processed SQL states."""

    def __init__(self, max_size: int = DEFAULT_CACHE_MAX_SIZE, cache_name: str = "sql") -> None:
        self.cache: OrderedDict[str, Any] = OrderedDict()
        self.max_size = max_size
        self.lock = threading.Lock()
        self.cache_name = cache_name
        self._eviction_count = 0

    @property
    def size(self) -> int:
        """Get current cache size."""
        return len(self.cache)

    def get(self, key: str) -> Optional[Any]:
        """Get an item from the cache, marking it as recently used."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                self._record_hit()
                return self.cache[key]
            self._record_miss()
            return None

    def set(self, key: str, value: Any) -> None:
        """Set an item in the cache with LRU eviction."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            # Add new entry
            elif len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
                self._eviction_count += 1
                self._record_eviction()
            self.cache[key] = value

    def clear(self) -> None:
        """Clear the cache."""
        with self.lock:
            self.cache.clear()

    def _record_hit(self) -> None:
        """Record a cache hit in statistics."""
        try:
            from sqlspec.statement.cache_config import _cache_stats

            if self.cache_name == "sql":
                _cache_stats.sql_hits += 1
            elif self.cache_name == "optimized":
                _cache_stats.optimized_hits += 1
        except ImportError:
            pass

    def _record_miss(self) -> None:
        """Record a cache miss in statistics."""
        try:
            from sqlspec.statement.cache_config import _cache_stats

            if self.cache_name == "sql":
                _cache_stats.sql_misses += 1
            elif self.cache_name == "optimized":
                _cache_stats.optimized_misses += 1
        except ImportError:
            pass

    def _record_eviction(self) -> None:
        """Record a cache eviction in statistics."""
        try:
            from sqlspec.statement.cache_config import _cache_stats

            if self.cache_name == "sql":
                _cache_stats.sql_evictions += 1
            elif self.cache_name == "optimized":
                _cache_stats.optimized_evictions += 1
        except ImportError:
            pass


@dataclass
class CachedFragment:
    """Cached AST fragment with metadata."""

    expression: exp.Expression
    sql: str
    fragment_type: str
    dialect: "Optional[DialectType]" = None
    parameter_count: int = 0


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
        self.max_size = max_size
        self.lock = threading.Lock()
        self._hit_count = 0
        self._miss_count = 0

    @property
    def size(self) -> int:
        """Get total cache size (fragments + templates)."""
        return len(self.fragment_cache) + len(self.template_cache)

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
                self.fragment_cache.move_to_end(cache_key)
                return self.fragment_cache[cache_key]

            self._miss_count += 1
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
            if len(self.fragment_cache) >= self.max_size // 2:
                self.fragment_cache.popitem(last=False)

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
                "max_size": self.max_size,
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

        Raises:
            sqlglot.errors.ParseError: On invalid SQL
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


class FilteredASTCache:
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


# Global cache instances
sql_cache = SQLCache(cache_name="sql")
ast_fragment_cache = ASTFragmentCache()
optimized_expression_cache = SQLCache(max_size=1500, cache_name="optimized")  # Smaller cache for optimized expressions
base_statement_cache = BaseStatementCache()
filtered_ast_cache = FilteredASTCache()
