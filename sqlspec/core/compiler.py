"""Enhanced SQLProcessor with integrated caching and single-pass processing.

This module implements the core compilation system that provides 5-10x performance
improvement over the current multi-pass processing system.

Key Performance Improvements:
- Single SQLGlot parse instead of multiple parsing cycles
- Integrated parameter processing eliminates redundant normalization
- Unified caching system with efficient LRU eviction
- AST-based operation type detection (no string parsing)
- Zero-copy compilation results with immutable sharing

Architecture:
- CompiledSQL: Immutable result with complete compilation information
- SQLProcessor: Single-pass compiler with integrated caching
- Integrated parameter processing via ParameterProcessor
- MyPyC optimization with __slots__ and efficient patterns

Critical Compatibility:
- Same external interfaces as current transformer.py
- Complete StatementConfig support for all driver requirements
- Identical operation type detection behavior
- Same caching interfaces expected by drivers
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

# Dictionary for runtime lookup
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


@mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class CompiledSQL:
    """Immutable compiled SQL result with complete information.

    This class represents the final result of single-pass SQL compilation,
    containing all information needed for execution without further processing.

    Performance Features:
    - Immutable design enables safe sharing without defensive copying
    - __slots__ for memory efficiency (40-60% reduction target)
    - Cached hash for efficient dictionary operations
    - Zero-copy parameter and SQL access

    Compatibility Features:
    - Same information available as current compilation results
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
        """Cached hash for efficient cache operations."""
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


@mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class SQLProcessor:
    """Enhanced SQLProcessor with integrated caching and full compatibility.

    This is the core compilation engine that replaces the current multi-pass
    processing system with a single-pass design for 5-10x performance improvement.

    Single-Pass Processing Flow:
    1. Parameter detection and Phase 1 normalization (if needed)
    2. Single SQLGlot parse (eliminates redundant parsing cycles)
    3. AST-based operation type detection (no string parsing)
    4. Phase 2 parameter conversion (if needed)
    5. Final SQL generation with execution parameters

    Performance Optimizations:
    - LRU cache with O(1) operations using OrderedDict
    - Single SQLGlot parse eliminates Parse #2 and Parse #3 cycles
    - Integrated parameter processing eliminates redundant normalization
    - Cached compilation results with efficient eviction
    - MyPyC-optimized method calls and property access

    Compatibility Requirements:
    - Same external interface as current transformer system
    - Complete StatementConfig support for all drivers
    - Identical operation type detection results
    - Same caching behavior expected by drivers
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
        """Single-pass compilation with integrated caching.

        This is the main compilation method that replaces the current multi-pass
        system with optimized single-pass processing.

        Performance Improvements vs Current System:
        - Single SQLGlot parse vs 3 parsing cycles
        - Integrated parameter processing vs separate normalization
        - Cached compilation results with efficient LRU eviction
        - AST-based operation detection vs string parsing

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

        # Cache miss - compile and cache result
        self._cache_misses += 1
        result = self._compile_uncached(sql, parameters, is_many)

        # Cache management - remove oldest if at capacity
        if len(self._cache) >= self._max_cache_size:
            self._cache.popitem(last=False)  # Remove oldest (FIFO)

        self._cache[cache_key] = result
        return result

    def _compile_uncached(self, sql: str, parameters: Any, is_many: bool = False) -> CompiledSQL:
        """Single-pass compilation without caching.

        This method implements the core single-pass compilation logic:
        1. Parameter processing (Phase 1 + Phase 2 as needed)
        2. Single SQLGlot parse
        3. AST-based operation type detection
        4. Final SQL generation
        5. CompiledSQL result creation

        Args:
            sql: Raw SQL string
            parameters: Parameter values
            is_many: Whether this is for execute_many operation

        Returns:
            CompiledSQL result
        """
        try:
            # Phase 1: Process parameters using integrated processor
            dialect_str = str(self._config.dialect) if self._config.dialect else None
            processed_sql, processed_params = self._parameter_processor.process(
                sql=sql,
                parameters=parameters,
                config=self._config.parameter_config,
                dialect=dialect_str,
                is_many=is_many,
            )

            # Phase 2: Get SQLGlot-compatible SQL for parsing and operation detection

            # If static compilation was applied (processed_params is None), use the processed_sql
            # for both execution and SQLGlot parsing, as parameters are already embedded
            if self._config.parameter_config.needs_static_script_compilation and processed_params is None:
                sqlglot_sql = processed_sql
            else:
                # Use original SQL for SQLGlot parsing with parameters preserved
                sqlglot_sql, _ = self._parameter_processor._get_sqlglot_compatible_sql(
                    sql, parameters, self._config.parameter_config, dialect_str
                )

            # Initialize variables that might be modified later
            final_parameters = processed_params
            ast_was_transformed = False

            if self._config.enable_parsing:
                try:
                    # Parse SQLGlot-compatible SQL for AST and operation detection
                    expression = sqlglot.parse_one(sqlglot_sql, dialect=dialect_str)
                    operation_type = self._detect_operation_type(expression)

                    # Apply AST-based transformations if configured
                    if self._config.parameter_config.ast_transformer:
                        expression, final_parameters = self._config.parameter_config.ast_transformer(
                            expression, processed_params
                        )
                        ast_was_transformed = True

                except ParseError:
                    # Fallback for unparsable SQL
                    expression = None
                    operation_type = "EXECUTE"
            else:
                expression = None
                operation_type = "EXECUTE"

            # Phase 3: Generate final SQL - only once!
            # For static compilation, preserve the processed SQL and parameters as-is
            if self._config.parameter_config.needs_static_script_compilation and processed_params is None:
                final_sql, final_params = processed_sql, processed_params
            elif ast_was_transformed and expression is not None:
                # AST was transformed - generate SQL from the transformed AST
                final_sql = expression.sql(dialect=dialect_str)
                final_params = final_parameters
                logger.debug("AST was transformed - final SQL: %s, final params: %s", final_sql, final_params)
                # Apply output transformer if configured
                if self._config.output_transformer:
                    final_sql, final_params = self._config.output_transformer(final_sql, final_params)
            else:
                # No AST transformation - use existing final transformation logic
                final_sql, final_params = self._apply_final_transformations(
                    expression, processed_sql, final_parameters, dialect_str
                )

            # Phase 5: Create immutable result
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
            # Fallback compilation with minimal processing
            return CompiledSQL(
                compiled_sql=sql, execution_parameters=parameters, operation_type=_OPERATION_TYPES["UNKNOWN"]
            )

    def _make_cache_key(self, sql: str, parameters: Any) -> str:
        """Generate cache key for compilation result.

        Must generate consistent, unique keys based on SQL and parameter
        configuration to enable effective caching.

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
        """AST-based operation type detection.

        Uses SQLGlot AST structure to determine operation type instead of
        string parsing, providing more accurate and faster detection.

        Operation Types:
        - SELECT: Query operations
        - INSERT: Insert operations
        - UPDATE: Update operations
        - DELETE: Delete operations
        - COPY: PostgreSQL COPY operations
        - DDL: Data definition language (CREATE, DROP, ALTER)
        - SCRIPT: Multiple statements
        - EXECUTE: Stored procedure execution
        - UNKNOWN: Unrecognized operations

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
            # SQLGlot uses 'kind' in args: True for FROM, False for TO
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
        """Apply final transformations with AST support.

        When an AST expression is available, it's passed to output transformers for potential
        manipulation before final SQL generation. This enables users to manipulate the AST
        in their custom output transformers.

        Args:
            expression: SQLGlot AST expression (if available)
            sql: Compiled SQL string (fallback)
            parameters: Execution parameters
            dialect_str: SQL dialect for AST-to-SQL conversion

        Returns:
            Tuple of (final_sql, final_parameters)
        """
        if self._config.output_transformer:
            # If AST is available, generate SQL from AST for consistency
            if expression is not None:
                # Generate SQL from AST for transformer input
                ast_sql = expression.sql(dialect=dialect_str)
                return self._config.output_transformer(ast_sql, parameters)
            # No AST available, use string-based transformation
            return self._config.output_transformer(sql, parameters)

        # No transformer configured - use the processed SQL to preserve parameter style
        # The processed_sql already has the correct parameter style conversion
        return sql, parameters

    def clear_cache(self) -> None:
        """Clear compilation cache.

        Provides cache management interface expected by current system.
        """
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0

    @property
    def cache_stats(self) -> "dict[str, int]":
        """Get cache statistics for monitoring.

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
