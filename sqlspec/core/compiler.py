"""SQL compilation and caching.

Components:
- CompiledSQL: Immutable compilation result
- SQLProcessor: SQL compiler with caching
- Parameter processing via ParameterProcessor
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
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.core.statement import StatementConfig


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

OPERATION_TYPE_MAP: "dict[type[exp.Expression], OperationType]" = {
    exp.Select: "SELECT",
    exp.Insert: "INSERT",
    exp.Update: "UPDATE",
    exp.Delete: "DELETE",
    exp.Pragma: "PRAGMA",
    exp.Command: "EXECUTE",
    exp.Create: "DDL",
    exp.Drop: "DDL",
    exp.Alter: "DDL",
}


@mypyc_attr(allow_interpreted_subclasses=False)
class CompiledSQL:
    """Compiled SQL result.

    Contains the result of SQL compilation with information needed for execution.
    Immutable container holding compiled SQL text, processed parameters, operation
    type, and execution metadata.
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

    operation_type: "OperationType"

    def __init__(
        self,
        compiled_sql: str,
        execution_parameters: Any,
        operation_type: "OperationType",
        expression: Optional["exp.Expression"] = None,
        parameter_style: Optional[str] = None,
        supports_many: bool = False,
    ) -> None:
        """Initialize compiled result.

        Args:
            compiled_sql: SQL string ready for execution
            execution_parameters: Parameters in driver-specific format
            operation_type: SQL operation type (SELECT, INSERT, etc.)
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
            param_str = str(self.execution_parameters)
            self._hash = hash((self.compiled_sql, param_str, self.operation_type, self.parameter_style))
        return self._hash

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, CompiledSQL):
            return False
        return (
            self.compiled_sql == other.compiled_sql
            and self.execution_parameters == other.execution_parameters
            and self.operation_type == other.operation_type
            and self.parameter_style == other.parameter_style
        )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"CompiledSQL(sql={self.compiled_sql!r}, "
            f"params={self.execution_parameters!r}, "
            f"type={self.operation_type!r})"
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class SQLProcessor:
    """SQL processor with compilation and caching.

    Processes SQL statements by compiling them into executable format with
    parameter substitution. Includes LRU-style caching for compilation results
    to avoid re-processing identical statements.
    """

    __slots__ = ("_cache", "_cache_hits", "_cache_misses", "_config", "_max_cache_size", "_parameter_processor")

    def __init__(self, config: "StatementConfig", max_cache_size: int = 1000) -> None:
        """Initialize processor.

        Args:
            config: Statement configuration
            max_cache_size: Maximum number of compilation results to cache
        """
        self._config = config
        self._cache: OrderedDict[str, CompiledSQL] = OrderedDict()
        self._parameter_processor = ParameterProcessor()
        self._max_cache_size = max_cache_size
        self._cache_hits = 0
        self._cache_misses = 0

    def compile(self, sql: str, parameters: Any = None, is_many: bool = False) -> CompiledSQL:
        """Compile SQL statement.

        Args:
            sql: SQL string for compilation
            parameters: Parameter values for substitution
            is_many: Whether this is for execute_many operation

        Returns:
            CompiledSQL with execution information
        """
        if not self._config.enable_caching:
            return self._compile_uncached(sql, parameters, is_many)

        cache_key = self._make_cache_key(sql, parameters)

        if cache_key in self._cache:
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
            sql: SQL string
            parameters: Parameter values
            is_many: Whether this is for execute_many operation

        Returns:
            CompiledSQL result
        """
        try:
            dialect_str = str(self._config.dialect) if self._config.dialect else None

            processed_sql: str
            processed_params: Any
            processed_sql, processed_params = self._parameter_processor.process(
                sql=sql,
                parameters=parameters,
                config=self._config.parameter_config,
                dialect=dialect_str,
                is_many=is_many,
            )

            if self._config.parameter_config.needs_static_script_compilation and processed_params is None:
                sqlglot_sql = processed_sql
            else:
                sqlglot_sql, _ = self._parameter_processor._get_sqlglot_compatible_sql(
                    sql, parameters, self._config.parameter_config, dialect_str
                )

            final_parameters = processed_params
            ast_was_transformed = False
            expression = None
            operation_type: OperationType = "EXECUTE"

            if self._config.enable_parsing:
                try:
                    expression = sqlglot.parse_one(sqlglot_sql, dialect=dialect_str)
                    operation_type = self._detect_operation_type(expression)

                    ast_transformer = self._config.parameter_config.ast_transformer
                    if ast_transformer:
                        expression, final_parameters = ast_transformer(expression, processed_params)
                        ast_was_transformed = True

                except ParseError:
                    expression = None
                    operation_type = "EXECUTE"

            if self._config.parameter_config.needs_static_script_compilation and processed_params is None:
                final_sql, final_params = processed_sql, processed_params
            elif ast_was_transformed and expression is not None:
                final_sql, final_params = self._parameter_processor.process(
                    sql=expression.sql(dialect=dialect_str),
                    parameters=final_parameters,
                    config=self._config.parameter_config,
                    dialect=dialect_str,
                    is_many=is_many,
                )
                output_transformer = self._config.output_transformer
                if output_transformer:
                    final_sql, final_params = output_transformer(final_sql, final_params)
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

        except SQLSpecError:
            # Re-raise SQLSpecError (validation errors, parameter mismatches) - these should fail hard
            raise
        except Exception as e:
            logger.warning("Compilation failed, using fallback: %s", e)
            return CompiledSQL(compiled_sql=sql, execution_parameters=parameters, operation_type="UNKNOWN")

    def _make_cache_key(self, sql: str, parameters: Any) -> str:
        """Generate cache key.

        Args:
            sql: SQL string
            parameters: Parameter values

        Returns:
            Cache key string
        """

        param_repr = repr(parameters)
        dialect_str = str(self._config.dialect) if self._config.dialect else None
        param_style = self._config.parameter_config.default_parameter_style.value

        hash_data = (
            sql,
            param_repr,
            param_style,
            dialect_str,
            self._config.enable_parsing,
            self._config.enable_transformations,
        )

        hash_str = hashlib.sha256(str(hash_data).encode("utf-8")).hexdigest()[:16]
        return f"sql_{hash_str}"

    def _detect_operation_type(self, expression: "exp.Expression") -> "OperationType":
        """Detect operation type from AST.

        Args:
            expression: AST expression

        Returns:
            Operation type literal
        """

        expr_type = type(expression)
        if expr_type in OPERATION_TYPE_MAP:
            return OPERATION_TYPE_MAP[expr_type]  # pyright: ignore

        if isinstance(expression, exp.Copy):
            copy_kind = expression.args.get("kind")
            if copy_kind is True:
                return "COPY_FROM"
            if copy_kind is False:
                return "COPY_TO"
            return "COPY"

        return "UNKNOWN"

    def _apply_final_transformations(
        self, expression: "Optional[exp.Expression]", sql: str, parameters: Any, dialect_str: "Optional[str]"
    ) -> "tuple[str, Any]":
        """Apply final transformations.

        Args:
            expression: SQLGlot AST expression
            sql: SQL string
            parameters: Execution parameters
            dialect_str: SQL dialect

        Returns:
            Tuple of (final_sql, final_parameters)
        """
        output_transformer = self._config.output_transformer
        if output_transformer:
            if expression is not None:
                ast_sql = expression.sql(dialect=dialect_str)
                return output_transformer(ast_sql, parameters)
            return output_transformer(sql, parameters)

        return sql, parameters

    def clear_cache(self) -> None:
        """Clear compilation cache and reset statistics."""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0

    @property
    def cache_stats(self) -> "dict[str, int]":
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
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
