"""Adapter-level caching mixin for compiled SQL and prepared statements."""

from typing import TYPE_CHECKING, Any, Optional

from sqlspec.statement.cache import SQLCache
from sqlspec.statement.parameters import ParameterStyle

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL

__all__ = ("AsyncAdapterCacheMixin", "SyncAdapterCacheMixin")


class SyncAdapterCacheMixin:
    """Mixin for adapter-level SQL compilation caching.

    This mixin provides:
    - Compiled SQL caching to avoid repeated compilation
    - Parameter style conversion caching
    - Prepared statement name management (for supported databases)

    Integrates transparently with existing adapter execution flow.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize adapter with caching support."""
        super().__init__(*args, **kwargs)

        # Get cache configuration from config or use defaults
        config = getattr(self, "config", None)
        cache_size = getattr(config, "adapter_cache_size", 500) if config else 500
        enable_cache = getattr(config, "enable_adapter_cache", True) if config else True

        # Initialize caches
        self._compiled_cache: Optional[SQLCache] = SQLCache(max_size=cache_size) if enable_cache else None
        self._prepared_statements: dict[str, str] = {}
        self._prepared_counter = 0

    def _get_compiled_sql(self, statement: "SQL", target_style: ParameterStyle) -> tuple[str, Any]:
        """Get compiled SQL with caching.

        Args:
            statement: SQL statement to compile
            target_style: Target parameter style for compilation

        Returns:
            Tuple of (compiled_sql, parameters)
        """
        if self._compiled_cache is None:
            # Caching disabled
            return statement.compile(placeholder_style=target_style)

        # Generate cache key
        cache_key = self._adapter_cache_key(statement, target_style)

        # Check cache
        cached = self._compiled_cache.get(cache_key)
        if cached is not None:
            return cached  # type: ignore[no-any-return]

        # Compile and cache
        result = statement.compile(placeholder_style=target_style)
        self._compiled_cache.set(cache_key, result)
        return result

    def _adapter_cache_key(self, statement: "SQL", style: ParameterStyle) -> str:
        """Generate adapter-specific cache key.

        Args:
            statement: SQL statement
            style: Parameter style

        Returns:
            Cache key string
        """
        # Use statement's internal cache key which includes SQL hash, params, and dialect
        base_key = statement._cache_key()
        # Add adapter-specific context
        return f"{self.__class__.__name__}:{style.value}:{base_key}"

    def _get_or_create_prepared_statement_name(self, sql_hash: str) -> str:
        """Get or create a prepared statement name for the given SQL.

        Used by PostgreSQL and other databases that support prepared statements.

        Args:
            sql_hash: Hash of the SQL statement

        Returns:
            Prepared statement name
        """
        if sql_hash in self._prepared_statements:
            return self._prepared_statements[sql_hash]

        # Create new prepared statement name
        self._prepared_counter += 1
        stmt_name = f"sqlspec_ps_{self._prepared_counter}"
        self._prepared_statements[sql_hash] = stmt_name
        return stmt_name

    def _clear_adapter_cache(self) -> None:
        """Clear all adapter-level caches."""
        if self._compiled_cache is not None:
            self._compiled_cache.clear()
        self._prepared_statements.clear()
        self._prepared_counter = 0


class AsyncAdapterCacheMixin(SyncAdapterCacheMixin):
    """Async version of AdapterCacheMixin.

    Identical to AdapterCacheMixin since caching operations are synchronous.
    Provided for naming consistency with async adapters.
    """
