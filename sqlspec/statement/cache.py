"""Cache implementation for SQL statement processing."""

import threading
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Optional

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("ASTFragmentCache", "CachedFragment", "SQLCache", "ast_fragment_cache", "sql_cache")


DEFAULT_CACHE_MAX_SIZE = 1000
DEFAULT_FRAGMENT_CACHE_SIZE = 5000


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


sql_cache = SQLCache()
ast_fragment_cache = ASTFragmentCache()
