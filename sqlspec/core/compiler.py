"""SQL processor with integrated caching and compilation.

This module implements the core compilation system for SQL statements with
integrated parameter processing and caching.

Components:
- CompiledSQL: Immutable compilation result with complete information
- SQLProcessor: Single-pass compiler with integrated caching
- Integrated parameter processing via ParameterProcessor

Features:
- Single SQLGlot parse for efficient processing
- AST-based operation type detection
- Unified caching system with LRU eviction
- Complete StatementConfig support
"""

import hashlib
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Optional

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import expressions as exp
from sqlglot.errors import ParseError
from typing_extensions import Literal

from sqlspec.core.parameters import ParameterProcessor
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.core.statement import StatementConfig

# Define OperationType here to avoid circular import
OperationType = Literal[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "COPY",
    "COPY_FROM",
    "COPY_TO",
    "EXECUTE",
    "SCRIPT",
    "DDL",
    "PRAGMA",
    "UNKNOWN",
]


__all__ = ("CompiledSQL", "OperationType", "SQLProcessor")

logger = get_logger("sqlspec.core.compiler")

_OPERATION_TYPES = {
    "SELECT": "SELECT",
    "INSERT": "INSERT",
    "UPDATE": "UPDATE",
    "DELETE": "DELETE",
    "COPY": "COPY",
    "COPY_FROM": "COPY_FROM",
    "COPY_TO": "COPY_TO",
    "EXECUTE": "EXECUTE",
    "SCRIPT": "SCRIPT",
    "DDL": "DDL",
    "PRAGMA": "PRAGMA",
    "UNKNOWN": "UNKNOWN",
}


@mypyc_attr(allow_interpreted_subclasses=True)
class CompiledSQL:
    """Immutable compiled SQL result with complete information.

    This class represents the result of SQL compilation, containing all
    information needed for execution.

    Features:
    - Immutable design for safe sharing
    - Cached hash for efficient dictionary operations
    - Complete operation type detection
    - Parameter style and execution information
    - Support for execute_many operations
    """

    __slots__ = (
        "_hash",
        "compiled_sql",
        "execution_parameters",
        "expression",
        "operation_type",
        "parameter_style",
        "supports_many",
    )

    def __init__(
        self,
        compiled_sql: str,
        execution_parameters: Any,
        operation_type: str,
        expression: Optional["exp.Expression"] = None,
        parameter_style: Optional[str] = None,
        supports_many: bool = False,
    ) -> None:
        """Initialize immutable compiled result.

        Args:
            compiled_sql: Final SQL string ready for execution
            execution_parameters: Parameters in driver-specific format
            operation_type: Detected SQL operation type (SELECT, INSERT, etc.)
            expression: SQLGlot AST expression
            parameter_style: Parameter style used in compilation
            supports_many: Whether this supports execute_many operations
        """
        self.compiled_sql = compiled_sql
        self.execution_parameters = execution_parameters
        self.operation_type = operation_type
        self.expression = expression
        self.parameter_style = parameter_style
        self.supports_many = supports_many
        self._hash: Optional[int] = None

    def __hash__(self) -> int:
        """Cached hash value."""
        if self._hash is None:
            hash_data = (self.compiled_sql, str(self.execution_parameters), self.operation_type, self.parameter_style)
            self._hash = hash(hash_data)
        return self._hash

    def __eq__(self, other: object) -> bool:
        """Equality comparison for compiled results."""
        if not isinstance(other, CompiledSQL):
            return False
        return (
            self.compiled_sql == other.compiled_sql
            and self.execution_parameters == other.execution_parameters
            and self.operation_type == other.operation_type
            and self.parameter_style == other.parameter_style
        )

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"CompiledSQL(sql={self.compiled_sql!r}, "
            f"params={self.execution_parameters!r}, "
            f"type={self.operation_type!r})"
        )


@mypyc_attr(allow_interpreted_subclasses=True)
class SQLProcessor:
    """SQL processor with integrated caching and compilation.

    This is the core compilation engine that processes SQL statements with
    integrated parameter processing and caching.

    Processing Flow:
    1. Parameter detection and normalization (if needed)
    2. Single SQLGlot parse
    3. AST-based operation type detection
    4. Parameter conversion (if needed)
    5. Final SQL generation with execution parameters

    Features:
    - LRU cache with O(1) operations
    - Integrated parameter processing
    - Cached compilation results
    - Complete StatementConfig support
    """

    __slots__ = ("_cache", "_cache_hits", "_cache_misses", "_config", "_max_cache_size", "_parameter_processor")

    def __init__(self, config: "StatementConfig", max_cache_size: int = 1000) -> None:
        """Initialize processor with configuration and caching.

        Args:
            config: Statement configuration with parameter processing settings
            max_cache_size: Maximum number of cached compilation results
        """
        self._config = config
        self._cache: OrderedDict[str, CompiledSQL] = OrderedDict()
        self._parameter_processor = ParameterProcessor()
        self._max_cache_size = max_cache_size
        self._cache_hits = 0
        self._cache_misses = 0

    def compile(self, sql: str, parameters: Any = None, is_many: bool = False) -> CompiledSQL:
        """Compile SQL statement with integrated caching.

        Args:
            sql: Raw SQL string for compilation
            parameters: Parameter values in any format
            is_many: Whether this is for execute_many operation

        Returns:
            CompiledSQL with all information for execution
        """
        if not self._config.enable_caching:
            return self._compile_uncached(sql, parameters, is_many)

        cache_key = self._make_cache_key(sql, parameters)

        if cache_key in self._cache:
            # Move to end for LRU behavior
            result = self._cache[cache_key]
            del self._cache[cache_key]
            self._cache[cache_key] = result
            self._cache_hits += 1
            return result

        self._cache_misses += 1
        result = self._compile_uncached(sql, parameters, is_many)

        if len(self._cache) >= self._max_cache_size:
            self._cache.popitem(last=False)

        self._cache[cache_key] = result
        return result

    def _compile_uncached(self, sql: str, parameters: Any, is_many: bool = False) -> CompiledSQL:
        """Compile SQL without caching.

        Args:
            sql: Raw SQL string
            parameters: Parameter values
            is_many: Whether this is for execute_many operation

        Returns:
            CompiledSQL result
        """
        try:
            dialect_str = str(self._config.dialect) if self._config.dialect else None
            processed_sql, processed_params_tuple = self._parameter_processor.process(
                sql=sql,
                parameters=parameters,
                config=self._config.parameter_config,
                dialect=dialect_str,
                is_many=is_many,
            )
            processed_params: Any = processed_params_tuple

            if self._config.parameter_config.needs_static_script_compilation and processed_params is None:
                sqlglot_sql = processed_sql
            else:
                sqlglot_sql, _ = self._parameter_processor._get_sqlglot_compatible_sql(
                    sql, parameters, self._config.parameter_config, dialect_str
                )

            final_parameters: Any = processed_params
            ast_was_transformed = False

            if self._config.enable_parsing:
                try:
                    expression = sqlglot.parse_one(sqlglot_sql, dialect=dialect_str)
                    operation_type = self._detect_operation_type(expression)

                    if self._config.parameter_config.ast_transformer:
                        expression, final_parameters = self._config.parameter_config.ast_transformer(
                            expression, processed_params
                        )
                        ast_was_transformed = True

                except ParseError:
                    expression = None
                    operation_type = "EXECUTE"
            else:
                expression = None
                operation_type = "EXECUTE"

            if self._config.parameter_config.needs_static_script_compilation and processed_params is None:
                final_sql, final_params = processed_sql, processed_params
            elif ast_was_transformed and expression is not None:
                final_sql = expression.sql(dialect=dialect_str)
                final_params = final_parameters
                logger.debug("AST was transformed - final SQL: %s, final params: %s", final_sql, final_params)
                if self._config.output_transformer:
                    final_sql, final_params = self._config.output_transformer(final_sql, final_params)
            else:
                final_sql, final_params = self._apply_final_transformations(
                    expression, processed_sql, final_parameters, dialect_str
                )

            return CompiledSQL(
                compiled_sql=final_sql,
                execution_parameters=final_params,
                operation_type=operation_type,
                expression=expression,
                parameter_style=self._config.parameter_config.default_parameter_style.value,
                supports_many=isinstance(final_params, list) and len(final_params) > 0,
            )

        except Exception as e:
            logger.warning("Compilation failed, using fallback: %s", e)
            return CompiledSQL(
                compiled_sql=sql, execution_parameters=parameters, operation_type=_OPERATION_TYPES["UNKNOWN"]
            )

    def _make_cache_key(self, sql: str, parameters: Any) -> str:
        """Generate cache key for compilation result.

        Args:
            sql: SQL string
            parameters: Parameter values

        Returns:
            Cache key string
        """
        hash_data = (
            sql,
            repr(parameters),
            self._config.parameter_config.default_parameter_style.value,
            str(self._config.dialect),
            self._config.enable_parsing,
            self._config.enable_transformations,
        )
        hash_str = hashlib.sha256(str(hash_data).encode()).hexdigest()[:16]
        return f"sql_{hash_str}"

    def _detect_operation_type(self, expression: "exp.Expression") -> str:
        """Detect operation type from AST.

        Uses SQLGlot AST structure to determine operation type.

        Args:
            expression: SQLGlot AST expression

        Returns:
            Operation type string
        """
        if isinstance(expression, exp.Select):
            return _OPERATION_TYPES["SELECT"]
        if isinstance(expression, exp.Insert):
            return _OPERATION_TYPES["INSERT"]
        if isinstance(expression, exp.Update):
            return _OPERATION_TYPES["UPDATE"]
        if isinstance(expression, exp.Delete):
            return _OPERATION_TYPES["DELETE"]
        if isinstance(expression, (exp.Create, exp.Drop, exp.Alter)):
            return _OPERATION_TYPES["DDL"]
        if isinstance(expression, exp.Copy):
            if expression.args["kind"] is True:
                return _OPERATION_TYPES["COPY_FROM"]
            if expression.args["kind"] is False:
                return _OPERATION_TYPES["COPY_TO"]
            return _OPERATION_TYPES["COPY"]
        if isinstance(expression, exp.Pragma):
            return _OPERATION_TYPES["PRAGMA"]
        if isinstance(expression, exp.Command):
            return _OPERATION_TYPES["EXECUTE"]
        return _OPERATION_TYPES["UNKNOWN"]

    def _apply_final_transformations(
        self, expression: "Optional[exp.Expression]", sql: str, parameters: Any, dialect_str: "Optional[str]"
    ) -> "tuple[str, Any]":
        """Apply final transformations.

        Args:
            expression: SQLGlot AST expression (if available)
            sql: Compiled SQL string (fallback)
            parameters: Execution parameters
            dialect_str: SQL dialect for AST-to-SQL conversion

        Returns:
            Tuple of (final_sql, final_parameters)
        """
        if self._config.output_transformer:
            if expression is not None:
                ast_sql = expression.sql(dialect=dialect_str)
                return self._config.output_transformer(ast_sql, parameters)
            return self._config.output_transformer(sql, parameters)

        return sql, parameters

    def clear_cache(self) -> None:
        """Clear compilation cache."""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0

    @property
    def cache_stats(self) -> "dict[str, int]":
        """Get cache statistics.

        Returns:
            Dictionary with cache hit/miss statistics
        """
        total_requests = self._cache_hits + self._cache_misses
        hit_rate_pct = int((self._cache_hits / total_requests) * 100) if total_requests > 0 else 0

        return {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "size": len(self._cache),
            "max_size": self._max_cache_size,
            "hit_rate_percent": hit_rate_pct,
        }
