"""SQL compilation and caching.

Components:
- CompiledSQL: Immutable compilation result
- SQLProcessor: SQL compiler with caching
- Parameter processing via ParameterProcessor
"""

import hashlib
import logging
from collections import OrderedDict
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import expressions as exp
from sqlglot.errors import ParseError

import sqlspec.exceptions
from sqlspec.core.parameters import (
    ParameterProcessor,
    ParameterProfile,
    fingerprint_parameters,
    validate_parameter_alignment,
)
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.type_guards import get_value_attribute

if TYPE_CHECKING:
    from sqlspec.core.statement import StatementConfig


__all__ = (
    "CompiledSQL",
    "OperationProfile",
    "OperationType",
    "SQLProcessor",
    "is_copy_from_operation",
    "is_copy_operation",
    "is_copy_to_operation",
)

logger: "logging.Logger" = get_logger("sqlspec.core.compiler")
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
    "MERGE",
    "UNKNOWN",
]

OPERATION_TYPE_MAP: "dict[type[exp.Expression], OperationType]" = {
    exp.Select: "SELECT",
    exp.Union: "SELECT",
    exp.Except: "SELECT",
    exp.Intersect: "SELECT",
    exp.With: "SELECT",
    exp.Insert: "INSERT",
    exp.Update: "UPDATE",
    exp.Delete: "DELETE",
    exp.Pragma: "PRAGMA",
    exp.Command: "EXECUTE",
    exp.Create: "DDL",
    exp.Drop: "DDL",
    exp.Alter: "DDL",
    exp.Merge: "MERGE",
}

COPY_OPERATION_TYPES: "tuple[OperationType, ...]" = ("COPY", "COPY_FROM", "COPY_TO")

COPY_FROM_OPERATION_TYPES: "tuple[OperationType, ...]" = ("COPY", "COPY_FROM")

COPY_TO_OPERATION_TYPES: "tuple[OperationType, ...]" = ("COPY_TO",)

ParseCacheEntry = tuple[exp.Expression | None, OperationType, dict[int, str], tuple[bool, bool]]


def is_copy_operation(operation_type: "OperationType") -> bool:
    """Determine if the operation corresponds to any PostgreSQL COPY variant.

    Args:
        operation_type: Operation type detected by the compiler.

    Returns:
        True when the operation type represents COPY, COPY FROM, or COPY TO.
    """

    return operation_type in COPY_OPERATION_TYPES


def is_copy_from_operation(operation_type: "OperationType") -> bool:
    """Check if the operation streams data into the database using COPY.

    Args:
        operation_type: Operation type detected by the compiler.

    Returns:
        True for COPY operations that read from client input (COPY FROM).
    """

    return operation_type in COPY_FROM_OPERATION_TYPES


def is_copy_to_operation(operation_type: "OperationType") -> bool:
    """Check if the operation streams data out from the database using COPY.

    Args:
        operation_type: Operation type detected by the compiler.

    Returns:
        True for COPY operations that write to client output (COPY TO).
    """

    return operation_type in COPY_TO_OPERATION_TYPES


def _assign_placeholder_position(
    placeholder: "exp.Placeholder", placeholder_positions: "dict[str, int]", placeholder_counter: "list[int]"
) -> "int | None":
    name_expr = placeholder.name if placeholder.name is not None else None
    if name_expr is not None:
        placeholder_key = str(name_expr)
    else:
        value = placeholder.args.get("this")
        placeholder_key = str(value) if value is not None else placeholder.sql()

    if not placeholder_key:
        return None

    if placeholder_key not in placeholder_positions:
        placeholder_counter[0] += 1
        placeholder_positions[placeholder_key] = placeholder_counter[0]

    return placeholder_positions[placeholder_key]


@mypyc_attr(allow_interpreted_subclasses=False)
class OperationProfile:
    """Semantic characteristics derived from the parsed SQL expression."""

    __slots__ = ("modifies_rows", "returns_rows")

    def __init__(self, returns_rows: bool = False, modifies_rows: bool = False) -> None:
        self.returns_rows = returns_rows
        self.modifies_rows = modifies_rows

    @classmethod
    def empty(cls) -> "OperationProfile":
        return cls(returns_rows=False, modifies_rows=False)

    def __repr__(self) -> str:
        return f"OperationProfile(returns_rows={self.returns_rows!r}, modifies_rows={self.modifies_rows!r})"


def _is_effectively_empty_parameters(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, Mapping):
        return len(value) == 0
    if isinstance(value, (list, tuple, set, frozenset)):
        return len(value) == 0
    return False


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
        "operation_profile",
        "operation_type",
        "parameter_casts",
        "parameter_profile",
        "parameter_style",
        "supports_many",
    )

    operation_type: "OperationType"

    def __init__(
        self,
        compiled_sql: str,
        execution_parameters: Any,
        operation_type: "OperationType",
        expression: "exp.Expression | None" = None,
        parameter_style: str | None = None,
        supports_many: bool = False,
        parameter_casts: "dict[int, str] | None" = None,
        parameter_profile: "ParameterProfile | None" = None,
        operation_profile: "OperationProfile | None" = None,
    ) -> None:
        """Initialize compiled result.

        Args:
            compiled_sql: SQL string ready for execution
            execution_parameters: Parameters in driver-specific format
            operation_type: SQL operation type (SELECT, INSERT, etc.)
            expression: SQLGlot AST expression
            parameter_style: Parameter style used in compilation
            supports_many: Whether this supports execute_many operations
            parameter_casts: Mapping of parameter positions to cast types
            parameter_profile: Profile describing detected placeholders
            operation_profile: Profile describing semantic characteristics
        """
        self.compiled_sql = compiled_sql
        self.execution_parameters = execution_parameters
        self.operation_type = operation_type
        self.expression = expression
        self.parameter_style = parameter_style
        self.supports_many = supports_many
        self.parameter_casts = parameter_casts or {}
        self.parameter_profile = parameter_profile
        self.operation_profile = operation_profile or OperationProfile.empty()
        self._hash: int | None = None

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

    __slots__ = (
        "_cache",
        "_cache_enabled",
        "_cache_hits",
        "_cache_misses",
        "_config",
        "_max_cache_size",
        "_parameter_processor",
        "_parse_cache",
        "_parse_cache_hits",
        "_parse_cache_max_size",
        "_parse_cache_misses",
    )

    def __init__(
        self,
        config: "StatementConfig",
        max_cache_size: int = 1000,
        parse_cache_size: int | None = None,
        parameter_cache_size: int | None = None,
        validator_cache_size: int | None = None,
        cache_enabled: bool = True,
    ) -> None:
        """Initialize processor.

        Args:
            config: Statement configuration
            max_cache_size: Maximum number of compilation results to cache
            parse_cache_size: Maximum number of parsed expressions to cache
            parameter_cache_size: Maximum parameter conversion cache entries
            validator_cache_size: Maximum cached parameter metadata entries
            cache_enabled: Toggle compiled SQL caching (parse/parameter caches remain size-driven)
        """
        self._config = config
        self._cache: OrderedDict[str, CompiledSQL] = OrderedDict()
        self._max_cache_size = max(max_cache_size, 0)
        compiled_cache_active = cache_enabled and config.enable_caching and self._max_cache_size > 0
        self._cache_enabled = compiled_cache_active
        parse_cache_max_size = self._max_cache_size if parse_cache_size is None else parse_cache_size
        self._parse_cache_max_size = max(parse_cache_max_size, 0)
        if not config.enable_caching:
            self._parse_cache_max_size = 0
        parameter_cache = parameter_cache_size if parameter_cache_size is not None else self._parse_cache_max_size
        validator_cache = validator_cache_size if validator_cache_size is not None else parameter_cache
        if not config.enable_caching:
            parameter_cache = 0
            validator_cache = 0
        self._parameter_processor = ParameterProcessor(
            converter=config.parameter_converter,
            validator=config.parameter_validator,
            cache_max_size=parameter_cache,
            validator_cache_max_size=validator_cache,
        )
        self._cache_hits = 0
        self._cache_misses = 0
        self._parse_cache: OrderedDict[
            str, tuple[exp.Expression | None, OperationType, dict[int, str], tuple[bool, bool]]
        ] = OrderedDict()
        self._parse_cache_hits = 0
        self._parse_cache_misses = 0

    def compile(
        self, sql: str, parameters: Any = None, is_many: bool = False, expression: "exp.Expression | None" = None
    ) -> CompiledSQL:
        """Compile SQL statement.

        Args:
            sql: SQL string for compilation
            parameters: Parameter values for substitution
            is_many: Whether this is for execute_many operation
            expression: Pre-parsed SQLGlot expression to reuse

        Returns:
            CompiledSQL with execution information
        """
        if not self._config.enable_caching or not self._cache_enabled:
            return self._compile_uncached(sql, parameters, is_many, expression)

        cache_key = self._make_cache_key(sql, parameters, is_many)

        if cache_key in self._cache:
            result = self._cache[cache_key]
            del self._cache[cache_key]
            self._cache[cache_key] = result
            self._cache_hits += 1
            return result

        self._cache_misses += 1
        result = self._compile_uncached(sql, parameters, is_many, expression)

        if len(self._cache) >= self._max_cache_size:
            self._cache.popitem(last=False)

        self._cache[cache_key] = result
        return result

    def _prepare_parameters(
        self, sql: str, parameters: Any, is_many: bool, dialect_str: "str | None"
    ) -> "tuple[str, Any, ParameterProfile, str]":
        """Process SQL parameters for compilation.

        Args:
            sql: SQL string.
            parameters: Raw parameters.
            is_many: Whether this is for execute_many.
            dialect_str: Dialect name.

        Returns:
            Tuple of processed SQL, processed parameters, parameter profile, and SQLGlot SQL.
        """
        process_result = self._parameter_processor.process(
            sql=sql,
            parameters=parameters,
            config=self._config.parameter_config,
            dialect=dialect_str,
            is_many=is_many,
            wrap_types=self._config.enable_parameter_type_wrapping,
        )
        return (
            process_result.sql,
            process_result.parameters,
            process_result.parameter_profile,
            process_result.sqlglot_sql,
        )

    def _normalize_expression_override(
        self, expression_override: "exp.Expression | None", sqlglot_sql: str, sql: str
    ) -> "exp.Expression | None":
        """Validate expression overrides against the input SQL.

        Args:
            expression_override: Pre-parsed SQLGlot expression.
            sqlglot_sql: SQL passed to SQLGlot.
            sql: Original SQL string.

        Returns:
            Expression override when it is safe to reuse.
        """
        if expression_override is None:
            return None
        if sqlglot_sql != sql:
            return None
        return expression_override

    def _parse_expression_uncached(
        self, sqlglot_sql: str, dialect_str: "str | None", expression_override: "exp.Expression | None"
    ) -> "tuple[exp.Expression | None, OperationType, dict[int, str], OperationProfile]":
        """Parse SQL into an expression without cache.

        Args:
            sqlglot_sql: SQL string for SQLGlot.
            dialect_str: Dialect name.
            expression_override: Pre-parsed SQLGlot expression.

        Returns:
            Expression details and derived metadata.
        """
        try:
            if expression_override is not None:
                expression = expression_override
            else:
                expression = sqlglot.parse_one(sqlglot_sql, dialect=dialect_str)
        except ParseError:
            return None, "EXECUTE", {}, OperationProfile.empty()
        else:
            operation_type = self._detect_operation_type(expression)
            parameter_casts = self._detect_parameter_casts(expression)
            operation_profile = self._build_operation_profile(expression, operation_type)
            return expression, operation_type, parameter_casts, operation_profile

    def _store_parse_cache(
        self,
        parse_cache_key: str,
        expression: "exp.Expression | None",
        operation_type: "OperationType",
        parameter_casts: "dict[int, str]",
        operation_profile: "OperationProfile",
    ) -> None:
        """Store parsed expression details in cache.

        Args:
            parse_cache_key: Cache key for the parsed SQL.
            expression: Parsed SQLGlot expression.
            operation_type: Detected operation type.
            parameter_casts: Parameter cast mappings.
            operation_profile: Operation metadata.
        """
        if len(self._parse_cache) >= self._parse_cache_max_size:
            self._parse_cache.popitem(last=False)
        cache_expression = expression.copy() if expression is not None else None
        self._parse_cache[parse_cache_key] = (
            cache_expression,
            operation_type,
            parameter_casts,
            (operation_profile.returns_rows, operation_profile.modifies_rows),
        )

    def _unpack_parse_cache_entry(
        self, parse_cache_entry: "ParseCacheEntry"
    ) -> "tuple[exp.Expression | None, OperationType, dict[int, str], OperationProfile]":
        """Expand cached parse results into runtime objects.

        Args:
            parse_cache_entry: Cached parse entry.

        Returns:
            Parsed expression metadata.
        """
        cached_expression, cached_operation, cached_casts, cached_profile = parse_cache_entry
        expression = cached_expression.copy() if cached_expression is not None else None
        operation_profile = OperationProfile(returns_rows=cached_profile[0], modifies_rows=cached_profile[1])
        return expression, cached_operation, dict(cached_casts), operation_profile

    def _resolve_expression(
        self, sqlglot_sql: str, dialect_str: "str | None", expression_override: "exp.Expression | None"
    ) -> "tuple[exp.Expression | None, OperationType, dict[int, str], OperationProfile, str | None, ParseCacheEntry | None]":
        """Resolve an SQLGlot expression with caching.

        Args:
            sqlglot_sql: SQL string for SQLGlot.
            dialect_str: Dialect name.
            expression_override: Pre-parsed SQLGlot expression.

        Returns:
            Expression metadata and parse cache information.
        """
        parse_cache_key = None
        parse_cache_entry = None
        if self._config.enable_caching and self._parse_cache_max_size > 0:
            parse_cache_key = self._make_parse_cache_key(sqlglot_sql, dialect_str)
            parse_cache_entry = self._parse_cache.get(parse_cache_key)
            if parse_cache_entry is not None:
                self._parse_cache_hits += 1
                self._parse_cache.move_to_end(parse_cache_key)
        if parse_cache_entry is None:
            self._parse_cache_misses += 1
            expression, operation_type, parameter_casts, operation_profile = self._parse_expression_uncached(
                sqlglot_sql, dialect_str, expression_override
            )
            if parse_cache_key is not None:
                self._store_parse_cache(parse_cache_key, expression, operation_type, parameter_casts, operation_profile)
        else:
            expression, operation_type, parameter_casts, operation_profile = self._unpack_parse_cache_entry(
                parse_cache_entry
            )
        return expression, operation_type, parameter_casts, operation_profile, parse_cache_key, parse_cache_entry

    def _apply_ast_transformers(
        self,
        expression: "exp.Expression | None",
        parameters: Any,
        parameter_profile: "ParameterProfile",
        operation_type: "OperationType",
        parameter_casts: "dict[int, str]",
        operation_profile: "OperationProfile",
        parse_cache_key: "str | None",
        parse_cache_entry: "ParseCacheEntry | None",
        expression_override: "exp.Expression | None",
    ) -> "tuple[exp.Expression | None, Any, bool, OperationType, dict[int, str], OperationProfile]":
        """Apply AST transformers and update metadata.

        Args:
            expression: SQLGlot expression to transform.
            parameters: Execution parameters.
            parameter_profile: Parameter profile metadata.
            operation_type: Current operation type.
            parameter_casts: Current parameter cast mapping.
            operation_profile: Current operation profile.
            parse_cache_key: Parse cache key when used.
            parse_cache_entry: Cached parse entry when available.
            expression_override: Expression override reference.

        Returns:
            Updated expression metadata and transformation state.
        """
        statement_transformers = self._config.statement_transformers
        ast_transformer = self._config.parameter_config.ast_transformer
        if expression is None or (not statement_transformers and not ast_transformer):
            return expression, parameters, False, operation_type, parameter_casts, operation_profile

        should_copy = False
        if parse_cache_key is not None and parse_cache_entry is None:
            should_copy = True
        if expression_override is not None and expression is expression_override:
            should_copy = True
        if should_copy:
            expression = expression.copy()

        ast_was_transformed = False
        if statement_transformers:
            for transformer in statement_transformers:
                expression, parameters = transformer(expression, parameters)
            ast_was_transformed = True
        if ast_transformer:
            expression, parameters = ast_transformer(expression, parameters, parameter_profile)
            ast_was_transformed = True
        if ast_was_transformed:
            if expression is None:
                return expression, parameters, ast_was_transformed, operation_type, parameter_casts, operation_profile
            operation_type = self._detect_operation_type(expression)
            parameter_casts = self._detect_parameter_casts(expression)
            operation_profile = self._build_operation_profile(expression, operation_type)

        return expression, parameters, ast_was_transformed, operation_type, parameter_casts, operation_profile

    def _finalize_compilation(
        self,
        processed_sql: str,
        processed_params: Any,
        expression: "exp.Expression | None",
        parameters: Any,
        parameter_profile: "ParameterProfile",
        is_many: bool,
        dialect_str: "str | None",
        ast_was_transformed: bool,
    ) -> "tuple[str, Any, ParameterProfile]":
        """Finalize SQL and parameter conversion for execution.

        Args:
            processed_sql: SQL after parameter processing.
            processed_params: Parameters after initial processing.
            expression: SQLGlot expression if available.
            parameters: Parameters to compile for execution.
            parameter_profile: Parameter profile metadata.
            is_many: Whether this is for execute_many.
            dialect_str: Dialect name.
            ast_was_transformed: Whether AST transformations ran.

        Returns:
            Final SQL, execution parameters, and parameter profile.
        """
        if self._config.parameter_config.needs_static_script_compilation and processed_params is None:
            return processed_sql, processed_params, parameter_profile
        if ast_was_transformed and expression is not None:
            transformed_result = self._parameter_processor.process_for_execution(
                sql=expression.sql(dialect=dialect_str),
                parameters=parameters,
                config=self._config.parameter_config,
                dialect=dialect_str,
                is_many=is_many,
                wrap_types=self._config.enable_parameter_type_wrapping,
            )
            final_sql = transformed_result.sql
            final_params = transformed_result.parameters
            parameter_profile = transformed_result.parameter_profile
            output_transformer = self._config.output_transformer
            if output_transformer:
                final_sql, final_params = output_transformer(final_sql, final_params)
            return final_sql, final_params, parameter_profile

        final_sql, final_params = self._apply_final_transformations(expression, processed_sql, parameters, dialect_str)
        return final_sql, final_params, parameter_profile

    def _should_validate_parameters(self, final_params: Any, raw_parameters: Any, is_many: bool) -> bool:
        """Determine if parameter alignment should be validated.

        Args:
            final_params: Parameters after compilation.
            raw_parameters: Original parameters.
            is_many: Whether this is for execute_many.

        Returns:
            True when validation should run.
        """
        if not self._config.enable_validation:
            return False
        return not (
            _is_effectively_empty_parameters(final_params)
            and _is_effectively_empty_parameters(raw_parameters)
            and not is_many
        )

    def _validate_parameters(self, parameter_profile: "ParameterProfile", final_params: Any, is_many: bool) -> None:
        """Validate parameter alignment and log failures.

        Args:
            parameter_profile: Parameter metadata.
            final_params: Execution parameters.
            is_many: Whether this is for execute_many.

        Raises:
            Exception: Re-raises validation errors from parameter alignment.
        """
        try:
            validate_parameter_alignment(parameter_profile, final_params, is_many=is_many)
        except Exception as exc:
            log_with_context(logger, logging.ERROR, "sql.validate", error_type=type(exc).__name__)
            raise

    def _compile_uncached(
        self, sql: str, parameters: Any, is_many: bool = False, expression_override: "exp.Expression | None" = None
    ) -> CompiledSQL:
        """Compile SQL without caching.

        Args:
            sql: SQL string
            parameters: Parameter values
            is_many: Whether this is for execute_many operation
            expression_override: Pre-parsed SQLGlot expression to reuse

        Returns:
            CompiledSQL result
        """
        parameter_profile = ParameterProfile.empty()
        operation_profile = OperationProfile.empty()

        try:
            dialect_str = str(self._config.dialect) if self._config.dialect else None
            processed_sql, processed_params, parameter_profile, sqlglot_sql = self._prepare_parameters(
                sql, parameters, is_many, dialect_str
            )
            expression_override = self._normalize_expression_override(expression_override, sqlglot_sql, sql)

            final_parameters = processed_params
            ast_was_transformed = False
            expression = None
            operation_type: OperationType = "EXECUTE"
            parameter_casts: dict[int, str] = {}
            parse_cache_key = None
            parse_cache_entry = None

            if self._config.enable_parsing:
                (expression, operation_type, parameter_casts, operation_profile, parse_cache_key, parse_cache_entry) = (
                    self._resolve_expression(sqlglot_sql, dialect_str, expression_override)
                )
                (
                    expression,
                    final_parameters,
                    ast_was_transformed,
                    operation_type,
                    parameter_casts,
                    operation_profile,
                ) = self._apply_ast_transformers(
                    expression,
                    final_parameters,
                    parameter_profile,
                    operation_type,
                    parameter_casts,
                    operation_profile,
                    parse_cache_key,
                    parse_cache_entry,
                    expression_override,
                )

            final_sql, final_params, parameter_profile = self._finalize_compilation(
                processed_sql,
                processed_params,
                expression,
                final_parameters,
                parameter_profile,
                is_many,
                dialect_str,
                ast_was_transformed,
            )

            if self._should_validate_parameters(final_params, parameters, is_many):
                self._validate_parameters(parameter_profile, final_params, is_many)

            return CompiledSQL(
                compiled_sql=final_sql,
                execution_parameters=final_params,
                operation_type=operation_type,
                expression=expression,
                parameter_style=self._config.parameter_config.default_parameter_style.value,
                supports_many=isinstance(final_params, list) and len(final_params) > 0,
                parameter_casts=parameter_casts,
                parameter_profile=parameter_profile,
                operation_profile=operation_profile,
            )

        except sqlspec.exceptions.SQLSpecError:
            raise
        except Exception as exc:
            log_with_context(logger, logging.DEBUG, "sql.compile", error_type=type(exc).__name__, status="fallback")
            return CompiledSQL(
                compiled_sql=sql,
                execution_parameters=parameters,
                operation_type="UNKNOWN",
                parameter_casts={},
                parameter_profile=parameter_profile,
                operation_profile=operation_profile,
            )

    def _make_cache_key(self, sql: str, parameters: Any, is_many: bool = False) -> str:
        """Generate cache key.

        Args:
            sql: SQL string
            parameters: Parameter values
            is_many: Whether this is for execute_many operation

        Returns:
            Cache key string
        """

        param_fingerprint = fingerprint_parameters(parameters)
        dialect_str = str(self._config.dialect) if self._config.dialect else None
        param_style = self._config.parameter_config.default_parameter_style.value

        hash_data = (
            sql,
            param_fingerprint,
            param_style,
            dialect_str,
            self._config.enable_parsing,
            self._config.enable_transformations,
            is_many,
        )

        hash_str = hashlib.blake2b(repr(hash_data).encode("utf-8"), digest_size=8).hexdigest()
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

    def _detect_parameter_casts(self, expression: "exp.Expression | None") -> "dict[int, str]":
        """Detect explicit type casts on parameters in the AST.

        Args:
            expression: SQLGlot AST expression to analyze

        Returns:
            Dict mapping parameter positions (1-based) to cast type names
        """
        if not expression:
            return {}

        cast_positions: dict[int, str] = {}
        placeholder_positions: dict[str, int] = {}
        placeholder_counter = [0]

        # Walk all nodes in order to track parameter positions
        for node in expression.walk():
            if isinstance(node, exp.Placeholder):
                _assign_placeholder_position(node, placeholder_positions, placeholder_counter)
            # Check for cast nodes with parameter children
            if isinstance(node, exp.Cast):
                cast_target = node.this
                position = None

                if isinstance(cast_target, exp.Parameter):
                    # Handle $1, $2 style parameters
                    param_value = cast_target.this
                    if isinstance(param_value, exp.Literal):
                        position = int(param_value.this)
                elif isinstance(cast_target, exp.Placeholder):
                    position = _assign_placeholder_position(cast_target, placeholder_positions, placeholder_counter)
                elif isinstance(cast_target, exp.Column):
                    # Handle cases where $1 gets parsed as a column
                    column_name = str(cast_target.this) if cast_target.this else str(cast_target)
                    if column_name.startswith("$") and column_name[1:].isdigit():
                        position = int(column_name[1:])

                if position is not None:
                    # Extract cast type
                    if isinstance(node.to, exp.DataType):
                        cast_type = str(get_value_attribute(node.to.this))
                    else:
                        cast_type = str(node.to)
                    cast_positions[position] = cast_type.upper()

        return cast_positions

    def _apply_final_transformations(
        self, expression: "exp.Expression | None", sql: str, parameters: Any, dialect_str: "str | None"
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

    def _build_operation_profile(
        self, expression: "exp.Expression | None", operation_type: "OperationType"
    ) -> "OperationProfile":
        if expression is None:
            return OperationProfile.empty()

        returns_rows = False
        modifies_rows = False

        expr = expression
        if isinstance(
            expr, (exp.Select, exp.Union, exp.Except, exp.Intersect, exp.Values, exp.Table, exp.TableSample, exp.With)
        ):
            returns_rows = True
        elif isinstance(expr, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
            modifies_rows = True
            returns_rows = bool(expr.args.get("returning"))
        elif isinstance(expr, exp.Copy):
            copy_kind = expr.args.get("kind")
            modifies_rows = copy_kind is True
            returns_rows = copy_kind is False

        if not returns_rows and operation_type in {"SELECT", "WITH", "VALUES", "TABLE"}:
            returns_rows = True

        if not modifies_rows and operation_type in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
            modifies_rows = True

        return OperationProfile(returns_rows=returns_rows, modifies_rows=modifies_rows)

    def clear_cache(self) -> None:
        """Clear compilation cache and reset statistics."""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        self._parse_cache.clear()
        self._parse_cache_hits = 0
        self._parse_cache_misses = 0
        self._parameter_processor.clear_cache()

    def _make_parse_cache_key(self, sql: str, dialect: "str | None") -> str:
        dialect_marker = dialect or "default"
        hash_str = hashlib.sha256(f"{dialect_marker}:{sql}".encode()).hexdigest()[:16]
        return f"parse_{hash_str}"

    @property
    def cache_stats(self) -> "dict[str, int]":
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        total_requests = self._cache_hits + self._cache_misses
        hit_rate_pct = int((self._cache_hits / total_requests) * 100) if total_requests > 0 else 0
        parse_total = self._parse_cache_hits + self._parse_cache_misses
        parse_hit_rate_pct = int((self._parse_cache_hits / parse_total) * 100) if parse_total > 0 else 0
        parameter_stats = self._parameter_processor.cache_stats()

        return {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "size": len(self._cache),
            "max_size": self._max_cache_size,
            "hit_rate_percent": hit_rate_pct,
            "parse_hits": self._parse_cache_hits,
            "parse_misses": self._parse_cache_misses,
            "parse_size": len(self._parse_cache),
            "parse_max_size": self._parse_cache_max_size,
            "parse_hit_rate_percent": parse_hit_rate_pct,
            "parameter_hits": parameter_stats["hits"],
            "parameter_misses": parameter_stats["misses"],
            "parameter_size": parameter_stats["size"],
            "parameter_max_size": parameter_stats["max_size"],
            "validator_hits": parameter_stats["validator_hits"],
            "validator_misses": parameter_stats["validator_misses"],
            "validator_size": parameter_stats["validator_size"],
            "validator_max_size": parameter_stats["validator_max_size"],
        }
