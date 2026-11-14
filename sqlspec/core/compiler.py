"""SQL compilation and caching.

Components:
    - CompiledSQL: Immutable compilation result
    - SQLProcessor: SQL compiler with caching
    - Parameter processing via ParameterProcessor
"""

import logging
from collections import OrderedDict
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, Literal, final

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import expressions as exp
from sqlglot.errors import ParseError

import sqlspec.exceptions
from sqlspec.core.parameters import (
    ParameterProcessor,
    ParameterProfile,
    structural_fingerprint,
    validate_parameter_alignment,
    value_fingerprint,
)
from sqlspec.core.parameters._processor import _make_cache_key_tuple
from sqlspec.core.sqlcommenter import _append_comment, _comment_attributes
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.type_guards import get_value_attribute

if TYPE_CHECKING:
    from sqlspec.core.statement import StatementConfig


__all__ = (
    "OPERATION_PROFILE_SLOTS",
    "CompiledSQL",
    "OperationProfile",
    "OperationType",
    "SQLProcessor",
    "is_copy_from_operation",
    "is_copy_operation",
    "is_copy_to_operation",
)

OPERATION_PROFILE_SLOTS: Final[tuple[str, ...]] = ("is_read_only", "modifies_rows", "returns_rows")

VOLATILE_FUNCTIONS: Final[frozenset[str]] = frozenset({
    "NEXTVAL",
    "SETVAL",
    "CURRVAL",
    "LASTVAL",
    "GET_LOCK",
    "RELEASE_LOCK",
    "RELEASE_ALL_LOCKS",
    "SLEEP",
    "PG_SLEEP",
    "PG_SLEEP_FOR",
    "PG_SLEEP_UNTIL",
    "PG_TERMINATE_BACKEND",
    "PG_CANCEL_BACKEND",
    "PG_RELOAD_CONF",
    "PG_ROTATE_LOGFILE",
    "LO_CREAT",
    "LO_CREATE",
    "LO_UNLINK",
    "LO_IMPORT",
    "LO_EXPORT",
    "SET_CONFIG",
    "TXID_CURRENT",
    "PG_CURRENT_XACT_ID",
    "PG_NOTIFY",
    "DBLINK_EXEC",
    "PG_SWITCH_WAL",
    "PG_CREATE_RESTORE_POINT",
    "PG_CREATE_LOGICAL_REPLICATION_SLOT",
    "PG_LOGICAL_EMIT_MESSAGE",
    "PG_EXPORT_SNAPSHOT",
    "PG_STAT_RESET",
})

_MODIFIES_ROWS_AST_TYPES: Final[tuple[type[exp.Expr], ...]] = (
    exp.Into,
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.TruncateTable,
)
_NON_READ_ONLY_AST_TYPES: Final[tuple[type[exp.Expr], ...]] = (
    exp.Lock,
    exp.Into,
    exp.NextValueFor,
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.TruncateTable,
    exp.Command,
    exp.DDL,
    exp.Set,
    exp.LoadData,
)
_READ_ONLY_ROOT_TYPES: Final[tuple[type[exp.Expr], ...]] = (
    exp.Select,
    exp.Union,
    exp.Except,
    exp.Intersect,
    exp.Values,
    exp.Table,
    exp.TableSample,
    exp.Show,
    exp.Describe,
    exp.Copy,
)
_RETURNS_ROWS_ROOT_TYPES: Final[tuple[type[exp.Expr], ...]] = (
    exp.Select,
    exp.Union,
    exp.Except,
    exp.Intersect,
    exp.Values,
    exp.Table,
    exp.TableSample,
    exp.Summarize,
)
_READ_ONLY_TABLE_HINTS: Final[frozenset[str]] = frozenset({"NOLOCK", "READUNCOMMITTED"})

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
    "COMMAND",
]

OPERATION_TYPE_MAP: Final[dict[type[exp.Expr], OperationType]] = {
    exp.Select: "SELECT",
    exp.Union: "SELECT",
    exp.Except: "SELECT",
    exp.Intersect: "SELECT",
    exp.Summarize: "SELECT",
    exp.Insert: "INSERT",
    exp.Update: "UPDATE",
    exp.Delete: "DELETE",
    exp.Merge: "MERGE",
    exp.LoadData: "INSERT",
    exp.Create: "DDL",
    exp.Drop: "DDL",
    exp.Alter: "DDL",
    exp.TruncateTable: "DDL",
    exp.Grant: "DDL",
    exp.Revoke: "DDL",
    exp.Comment: "DDL",
    exp.Execute: "EXECUTE",
    exp.Pragma: "PRAGMA",
    exp.Command: "COMMAND",
    exp.Set: "COMMAND",
    exp.Show: "COMMAND",
    exp.Describe: "COMMAND",
    exp.Use: "COMMAND",
    exp.Kill: "COMMAND",
    exp.Analyze: "COMMAND",
    exp.Install: "COMMAND",
    exp.Attach: "COMMAND",
    exp.Transaction: "COMMAND",
    exp.Commit: "COMMAND",
    exp.Rollback: "COMMAND",
}

ROW_RETURNING_COMMANDS: Final[frozenset[str]] = frozenset({"EXPLAIN", "SHOW", "DESC", "DESCRIBE"})

ROW_RETURNING_STATEMENT_KEYWORDS: Final[frozenset[str]] = frozenset({"TABLE"})
"""Leading keywords of row-returning statements that sqlglot does not model."""

PLAN_TABLE_EXPLAIN_PREFIX: Final[str] = "PLAN"

COPY_OPERATION_TYPES: Final[tuple[OperationType, ...]] = ("COPY", "COPY_FROM", "COPY_TO")

COPY_FROM_OPERATION_TYPES: Final[tuple[OperationType, ...]] = ("COPY", "COPY_FROM")

COPY_TO_OPERATION_TYPES: Final[tuple[OperationType, ...]] = ("COPY_TO",)


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


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class OperationProfile:
    """Semantic characteristics derived from the parsed SQL expression."""

    __slots__ = OPERATION_PROFILE_SLOTS

    def __init__(self, returns_rows: bool = False, modifies_rows: bool = False, is_read_only: bool = False) -> None:
        self.returns_rows = returns_rows
        self.modifies_rows = modifies_rows
        self.is_read_only = is_read_only

    def __repr__(self) -> str:
        return (
            f"OperationProfile(returns_rows={self.returns_rows!r}, modifies_rows={self.modifies_rows!r}, "
            f"is_read_only={self.is_read_only!r})"
        )

    @classmethod
    def empty(cls) -> "OperationProfile":
        return cls(returns_rows=False, modifies_rows=False)


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class CompiledSQL:
    """Compiled SQL result.

    Contains the result of SQL compilation with information needed for execution.
    Immutable container holding compiled SQL text, processed parameters, operation
    type, and execution metadata.
    """

    __slots__ = (
        "_hash",
        "applied_wrap_types",
        "compiled_sql",
        "execution_parameters",
        "expression",
        "input_named_parameters",
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
        expression: "exp.Expr | None" = None,
        parameter_style: str | None = None,
        supports_many: bool = False,
        parameter_casts: "dict[int, str] | None" = None,
        parameter_profile: "ParameterProfile | None" = None,
        operation_profile: "OperationProfile | None" = None,
        input_named_parameters: "tuple[str, ...]" = (),
        applied_wrap_types: bool = False,
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
            input_named_parameters: Original input named parameter order
            applied_wrap_types: Whether type wrapping was applied
        """
        self.compiled_sql = compiled_sql
        self.execution_parameters = execution_parameters
        self.operation_type = operation_type
        self.expression = expression
        self.parameter_style = parameter_style
        self.supports_many = supports_many
        self.parameter_casts = parameter_casts or {}
        self.parameter_profile = parameter_profile or ParameterProfile.empty()
        self.operation_profile = operation_profile or OperationProfile.empty()
        self.input_named_parameters = input_named_parameters
        self.applied_wrap_types = applied_wrap_types
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


@final
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
        "_dialect_str",
        "_enable_parameter_type_wrapping",
        "_exec_style",
        "_input_style",
        "_last_cache_key",
        "_last_result",
        "_max_cache_size",
        "_parameter_config",
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
        parameter_config = config.parameter_config
        self._parameter_config = parameter_config
        self._enable_parameter_type_wrapping = config.enable_parameter_type_wrapping
        self._cache: OrderedDict[Any, CompiledSQL] = OrderedDict()
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
        self._parse_cache: OrderedDict[Any, tuple[exp.Expr | None, OperationType, tuple[bool, bool, bool]]] = (
            OrderedDict()
        )
        self._parse_cache_hits = 0
        self._parse_cache_misses = 0
        self._last_cache_key: Any | None = None
        self._last_result: CompiledSQL | None = None

        self._dialect_str = str(config.dialect) if config.dialect else None
        self._input_style = parameter_config.default_parameter_style.value
        default_execution_style = parameter_config.default_execution_parameter_style
        self._exec_style = default_execution_style.value if default_execution_style else self._input_style

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

    def compile(
        self,
        sql: str,
        parameters: Any = None,
        is_many: bool = False,
        expression: "exp.Expr | None" = None,
        param_fingerprint: "Any | None" = None,
    ) -> CompiledSQL:
        """Compile SQL statement.

        Args:
            sql: SQL string for compilation
            parameters: Parameter values for substitution
            is_many: Whether this is for execute_many operation
            expression: Pre-parsed SQLGlot expression to reuse
            param_fingerprint: Pre-computed parameter fingerprint.

        Returns:
            CompiledSQL with execution information
        """
        if (
            not self._cache_enabled
            or self._parameter_config.ast_transformer is not None
            or (self._parameter_config.needs_static_script_compilation and parameters)
        ):
            result, _ = self._compile_uncached_with_template(
                sql, parameters, is_many, expression, param_fingerprint=None
            )
            return self._apply_dynamic_sqlcommenter(result)

        if param_fingerprint is None:
            param_fingerprint = self._parameter_fingerprint(parameters, is_many)
        cache_key = self._make_cache_key(sql, param_fingerprint, is_many, expression=expression)

        if cache_key == self._last_cache_key and self._last_result is not None:
            self._cache_hits += 1
            return self._apply_dynamic_sqlcommenter(
                self._materialize_cached_result(self._last_result, parameters, is_many)
            )

        cached_result = self._cache.get(cache_key)
        if cached_result is not None:
            self._cache.move_to_end(cache_key)
            self._cache_hits += 1

            self._last_cache_key = cache_key
            self._last_result = cached_result
            return self._apply_dynamic_sqlcommenter(self._materialize_cached_result(cached_result, parameters, is_many))

        self._cache_misses += 1
        result, cached_expression_template = self._compile_uncached_with_template(
            sql, parameters, is_many, expression, param_fingerprint=param_fingerprint
        )

        if len(self._cache) >= self._max_cache_size:
            self._cache.popitem(last=False)

        if result.expression is not None:
            cached_expr = (
                cached_expression_template if cached_expression_template is not None else result.expression.copy()
            )
            cached_entry = CompiledSQL(
                compiled_sql=result.compiled_sql,
                execution_parameters=result.execution_parameters,
                operation_type=result.operation_type,
                expression=cached_expr,
                parameter_style=result.parameter_style,
                supports_many=result.supports_many,
                parameter_casts=result.parameter_casts,
                parameter_profile=result.parameter_profile,
                operation_profile=result.operation_profile,
                input_named_parameters=result.input_named_parameters,
                applied_wrap_types=result.applied_wrap_types,
            )
        else:
            cached_entry = result
        self._cache[cache_key] = cached_entry
        self._last_cache_key = cache_key
        self._last_result = cached_entry
        return self._apply_dynamic_sqlcommenter(result)

    def clear_cache(self) -> None:
        """Clear compilation cache and reset statistics."""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        self._last_cache_key = None
        self._last_result = None
        self._parse_cache.clear()
        self._parse_cache_hits = 0
        self._parse_cache_misses = 0
        self._parameter_processor.clear_cache()

    def _has_dynamic_sqlcommenter(self) -> bool:
        """Return whether SQLCommenter resolves per-call context during compilation."""
        return bool(
            self._config.enable_sqlcommenter
            and (self._config.sqlcommenter_enable_context or self._config.sqlcommenter_enable_traceparent)
        )

    def _effective_statement_transformers(self) -> "tuple[Any, ...]":
        statement_transformers = self._config.statement_transformers
        if statement_transformers and self._has_dynamic_sqlcommenter():
            return statement_transformers[:-1]
        return statement_transformers

    def _apply_dynamic_sqlcommenter(self, result: CompiledSQL) -> CompiledSQL:
        """Append current dynamic SQLCommenter attributes to a compiled result."""
        if not self._has_dynamic_sqlcommenter():
            return result

        attrs = _comment_attributes(
            self._config.sqlcommenter_attributes or {},
            enable_traceparent=self._config.sqlcommenter_enable_traceparent,
            enable_context=self._config.sqlcommenter_enable_context,
        )
        compiled_sql = _append_comment(result.compiled_sql, attrs)
        if compiled_sql == result.compiled_sql:
            return result

        return CompiledSQL(
            compiled_sql=compiled_sql,
            execution_parameters=result.execution_parameters,
            operation_type=result.operation_type,
            expression=result.expression,
            parameter_style=result.parameter_style,
            supports_many=result.supports_many,
            parameter_casts=result.parameter_casts,
            parameter_profile=result.parameter_profile,
            operation_profile=result.operation_profile,
            input_named_parameters=result.input_named_parameters,
            applied_wrap_types=result.applied_wrap_types,
        )

    def _materialize_cached_result(self, cached_result: CompiledSQL, parameters: Any, is_many: bool) -> CompiledSQL:
        """Return cached compilation metadata with parameters for the current call."""
        if self._parameter_config.needs_static_script_compilation:
            return cached_result

        processed_params = self._parameter_processor.transform_cached_parameters(
            parameters,
            cached_result.parameter_profile,
            self._parameter_config,
            input_named_parameters=cached_result.input_named_parameters,
            is_many=is_many,
            apply_wrap_types=cached_result.applied_wrap_types,
        )

        final_sql = cached_result.compiled_sql
        output_transformer = self._config.output_transformer
        if output_transformer:
            final_sql, processed_params = output_transformer(final_sql, processed_params)

        return CompiledSQL(
            compiled_sql=final_sql,
            execution_parameters=processed_params,
            operation_type=cached_result.operation_type,
            expression=cached_result.expression,
            parameter_style=cached_result.parameter_style,
            supports_many=cached_result.supports_many,
            parameter_casts=cached_result.parameter_casts,
            parameter_profile=cached_result.parameter_profile,
            operation_profile=cached_result.operation_profile,
            input_named_parameters=cached_result.input_named_parameters,
            applied_wrap_types=cached_result.applied_wrap_types,
        )

    @staticmethod
    def _normalize_expression_override(
        expression_override: "exp.Expr | None", sqlglot_sql: str, sql: str
    ) -> "exp.Expr | None":
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
        self,
        sqlglot_sql: str,
        dialect_str: "str | None",
        expression_override: "exp.Expr | None",
        *,
        collect_casts: bool = False,
    ) -> "tuple[exp.Expr | None, OperationType, OperationProfile, dict[int, str]]":
        """Parse SQL into an expression without cache.

        Args:
            sqlglot_sql: SQL string for SQLGlot.
            dialect_str: Dialect name.
            expression_override: Pre-parsed SQLGlot expression.
            collect_casts: Whether to collect parameter casts in the same AST walk.

        Returns:
            Expression details, derived metadata, and optional parameter casts.
        """
        try:
            if expression_override is not None:
                expression: exp.Expr = expression_override
            else:
                expression = sqlglot.parse_one(sqlglot_sql, dialect=dialect_str)
        except ParseError:
            returns_rows = SQLProcessor._statement_text_returns_rows(sqlglot_sql)
            return None, "COMMAND", OperationProfile(returns_rows=returns_rows), {}
        else:
            operation_type = SQLProcessor._operation_type(expression)
            operation_profile, parameter_casts = _inspect_operation_profile(
                expression, operation_type, dialect_str, collect_casts=collect_casts
            )
            if (
                operation_type == "COMMAND"
                and not operation_profile.returns_rows
                and SQLProcessor._statement_text_returns_rows(sqlglot_sql)
            ):
                operation_profile.returns_rows = True
            return expression, operation_type, operation_profile, parameter_casts

    def _store_parse_cache(
        self,
        parse_cache_key: Any,
        expression: "exp.Expr | None",
        operation_type: "OperationType",
        operation_profile: "OperationProfile",
    ) -> "exp.Expr | None":
        """Store parsed expression details in cache.

        Args:
            parse_cache_key: Cache key for the parsed SQL.
            expression: Parsed SQLGlot expression.
            operation_type: Detected operation type.
            operation_profile: Operation metadata.

        Returns:
            Cached copy of the parsed expression when present.
        """
        if len(self._parse_cache) >= self._parse_cache_max_size:
            self._parse_cache.popitem(last=False)
        cached_expression = expression.copy() if expression is not None else None
        self._parse_cache[parse_cache_key] = (
            cached_expression,
            operation_type,
            (operation_profile.returns_rows, operation_profile.modifies_rows, operation_profile.is_read_only),
        )
        return cached_expression

    @staticmethod
    def _unpack_parse_cache_entry(
        parse_cache_entry: "tuple[exp.Expr | None, OperationType, tuple[bool, bool, bool]]",
    ) -> "tuple[exp.Expr | None, OperationType, OperationProfile]":
        """Expand cached parse results into runtime objects.

        Args:
            parse_cache_entry: Cached parse entry.

        Returns:
            Parsed expression metadata.
        """
        cached_expression, cached_operation, cached_profile = parse_cache_entry
        operation_profile = OperationProfile(
            returns_rows=cached_profile[0], modifies_rows=cached_profile[1], is_read_only=cached_profile[2]
        )
        return (cached_expression, cached_operation, operation_profile)

    def _resolve_expression(
        self,
        sqlglot_sql: str,
        dialect_str: "str | None",
        expression_override: "exp.Expr | None",
        cache_parse: bool = True,
        *,
        collect_casts: bool = False,
    ) -> "tuple[exp.Expr | None, OperationType, OperationProfile, Any | None, dict[int, str] | None, exp.Expr | None]":
        """Resolve an SQLGlot expression with caching.

        Args:
            sqlglot_sql: SQL string for SQLGlot.
            dialect_str: Dialect name.
            expression_override: Pre-parsed SQLGlot expression.
            cache_parse: Whether the parse result may be read from and stored in the parse cache.
            collect_casts: Whether to collect parameter casts on a parse miss.

        Returns:
            Expression metadata, parse cache information, optional parameter casts, and shared template.
        """
        parse_cache_key = None
        parse_cache_entry = None
        if cache_parse and self._parse_cache_max_size > 0 and expression_override is None:
            parse_cache_key = SQLProcessor._make_parse_cache_key(sqlglot_sql, dialect_str)
            parse_cache_entry = self._parse_cache.get(parse_cache_key)
            if parse_cache_entry is not None:
                self._parse_cache_hits += 1
                self._parse_cache.move_to_end(parse_cache_key)
        if parse_cache_entry is None:
            self._parse_cache_misses += 1
            expression, operation_type, operation_profile, parameter_casts = self._parse_expression_uncached(
                sqlglot_sql, dialect_str, expression_override, collect_casts=collect_casts
            )
            cached_template: exp.Expr | None = None
            if parse_cache_key is not None:
                cached_template = self._store_parse_cache(
                    parse_cache_key, expression, operation_type, operation_profile
                )
            resolved_casts: dict[int, str] | None = parameter_casts if collect_casts else None
            return expression, operation_type, operation_profile, parse_cache_key, resolved_casts, cached_template
        expression, operation_type, operation_profile = SQLProcessor._unpack_parse_cache_entry(parse_cache_entry)
        return expression, operation_type, operation_profile, parse_cache_key, None, expression

    def _apply_ast_transformers(
        self,
        expression: "exp.Expr | None",
        parameters: Any,
        parameter_profile: "ParameterProfile",
        operation_type: "OperationType",
        operation_profile: "OperationProfile",
        parse_cache_key: "Any | None",
        expression_override: "exp.Expr | None",
        is_many: bool = False,
    ) -> "tuple[exp.Expr | None, Any, bool, OperationType, OperationProfile]":
        """Apply AST transformers and update metadata.

        Args:
            expression: SQLGlot expression to transform.
            parameters: Execution parameters.
            parameter_profile: Parameter profile metadata.
            operation_type: Current operation type.
            operation_profile: Current operation profile.
            parse_cache_key: Parse cache key when used.
            expression_override: Expression override reference.
            is_many: Whether the statement runs as execute_many; forwarded to the
                parameter AST transformer so it treats batched payloads correctly.

        Returns:
            Updated expression metadata and transformation state.
        """
        statement_transformers = self._effective_statement_transformers()
        ast_transformer = self._parameter_config.ast_transformer
        if expression is None or (not statement_transformers and not ast_transformer):
            return expression, parameters, False, operation_type, operation_profile

        should_copy = False
        if parse_cache_key is not None:
            should_copy = True
        if expression_override is not None and expression is expression_override:
            should_copy = True
        if should_copy:
            expression = expression.copy()

        ast_was_transformed = False
        if statement_transformers:
            for transformer in statement_transformers:
                current_expression = expression
                current_parameters = parameters
                expression, parameters = transformer(expression, parameters)
                if expression is not current_expression or parameters is not current_parameters:
                    ast_was_transformed = True
        if ast_transformer:
            current_expression = expression
            current_parameters = parameters
            expression, parameters = ast_transformer(expression, parameters, parameter_profile, is_many)
            if expression is not current_expression or parameters is not current_parameters:
                ast_was_transformed = True
        if ast_was_transformed:
            if expression is None:
                return expression, parameters, ast_was_transformed, operation_type, operation_profile
            operation_type = SQLProcessor._operation_type(expression)
            operation_profile = SQLProcessor._operation_profile(expression, operation_type, dialect=self._dialect_str)

        return expression, parameters, ast_was_transformed, operation_type, operation_profile

    def _finalize_compilation(
        self,
        processed_sql: str,
        processed_params: Any,
        expression: "exp.Expr | None",
        parameters: Any,
        parameter_profile: "ParameterProfile",
        is_many: bool,
        dialect_str: "str | None",
        ast_was_transformed: bool,
        original_input_named_parameters: "tuple[str, ...]" = (),
    ) -> "tuple[str, Any, ParameterProfile, tuple[str, ...], bool]":
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
            original_input_named_parameters: Input named parameter order captured before
                AST transformation, used to map named parameters on cache hits.

        Returns:
            Final SQL, execution parameters, parameter profile, input named parameters,
            and applied wrap types flag.
        """
        if self._parameter_config.needs_static_script_compilation and processed_params is None:
            return processed_sql, processed_params, parameter_profile, (), False
        if ast_was_transformed and expression is not None:
            transformed_result = self._parameter_processor.process_for_execution(
                sql=expression.sql(dialect=dialect_str),
                parameters=parameters,
                config=self._parameter_config,
                dialect=dialect_str,
                is_many=is_many,
                wrap_types=self._enable_parameter_type_wrapping,
                parsed_expression=expression,
            )
            final_sql = transformed_result.sql
            final_params = transformed_result.parameters
            parameter_profile = transformed_result.parameter_profile
            output_transformer = self._config.output_transformer
            if output_transformer:
                final_sql, final_params = output_transformer(final_sql, final_params)
            final_input_named = transformed_result.input_named_parameters
            if original_input_named_parameters and len(original_input_named_parameters) == len(final_input_named):
                final_input_named = original_input_named_parameters
            return (
                final_sql,
                final_params,
                parameter_profile,
                final_input_named,
                transformed_result.applied_wrap_types,
            )

        final_sql, final_params = self._apply_transformers(expression, processed_sql, parameters, dialect_str)
        return final_sql, final_params, parameter_profile, (), False

    def _should_validate_parameters(self, final_params: Any, raw_parameters: Any, is_many: bool) -> bool:
        """Determine if parameter alignment should be validated.

        Args:
            final_params: Parameters after compilation.
            raw_parameters: Original parameters.
            is_many: Whether this is for execute_many.

        Returns:
            True when validation should run.
        """
        validation_enabled = self._config.enable_validation
        has_final_params = not _parameters_empty(final_params)
        has_raw_params = not _parameters_empty(raw_parameters)
        has_params = has_final_params or has_raw_params
        return (has_params or is_many) and validation_enabled

    @staticmethod
    def _validate_parameters(parameter_profile: "ParameterProfile", final_params: Any, is_many: bool) -> None:
        """Validate parameter alignment and log failures.

        Args:
            parameter_profile: Parameter metadata.
            final_params: Execution parameters.
            is_many: Whether this is for execute_many.
        """
        try:
            validate_parameter_alignment(parameter_profile, final_params, is_many=is_many)
        except Exception as exc:
            log_with_context(logger, logging.ERROR, "sql.validate", error_type=type(exc).__name__)
            raise

    def _compile_uncached(
        self,
        sql: str,
        parameters: Any,
        is_many: bool = False,
        expression_override: "exp.Expr | None" = None,
        *,
        param_fingerprint: Any | None,
    ) -> CompiledSQL:
        """Compile SQL without caching.

        Scripts compiled with embedded parameter values also bypass the parse
        cache, since each parameter set yields distinct SQL text.

        Args:
            sql: SQL string
            parameters: Parameter values
            is_many: Whether this is for execute_many operation
            expression_override: Pre-parsed SQLGlot expression to reuse
            param_fingerprint: Pre-computed parameter fingerprint for cache key.

        Returns:
            CompiledSQL result
        """
        result, _ = self._compile_uncached_with_template(
            sql, parameters, is_many, expression_override, param_fingerprint=param_fingerprint
        )
        return result

    def _compile_uncached_with_template(
        self,
        sql: str,
        parameters: Any,
        is_many: bool = False,
        expression_override: "exp.Expr | None" = None,
        *,
        param_fingerprint: Any | None,
    ) -> "tuple[CompiledSQL, exp.Expr | None]":
        parameter_profile = ParameterProfile.empty()
        operation_profile = OperationProfile.empty()

        try:
            process_result = self._parameter_processor.process(
                sql=sql,
                parameters=parameters,
                config=self._parameter_config,
                dialect=self._dialect_str,
                is_many=is_many,
                wrap_types=self._enable_parameter_type_wrapping,
                param_fingerprint=param_fingerprint,
            )
            expression_override = SQLProcessor._normalize_expression_override(
                expression_override, process_result.sqlglot_sql, sql
            )

            final_parameters = process_result.parameters
            ast_was_transformed = False
            expression = None
            operation_type: OperationType = "COMMAND"
            parameter_casts: dict[int, str] = {}
            parse_cache_key = None
            cached_template: exp.Expr | None = None

            if self._config.enable_parsing:
                has_ast_transforms = bool(
                    self._effective_statement_transformers() or self._parameter_config.ast_transformer is not None
                )
                collect_casts = not has_ast_transforms
                (expression, operation_type, operation_profile, parse_cache_key, resolved_casts, cached_template) = (
                    self._resolve_expression(
                        process_result.sqlglot_sql,
                        self._dialect_str,
                        expression_override,
                        cache_parse=not (self._parameter_config.needs_static_script_compilation and parameters),
                        collect_casts=collect_casts,
                    )
                )
                (expression, final_parameters, ast_was_transformed, operation_type, operation_profile) = (
                    self._apply_ast_transformers(
                        expression,
                        final_parameters,
                        process_result.parameter_profile,
                        operation_type,
                        operation_profile,
                        parse_cache_key,
                        expression_override,
                        is_many=is_many,
                    )
                )
                if expression is not None:
                    if resolved_casts is not None and not ast_was_transformed:
                        parameter_casts = resolved_casts
                    else:
                        parameter_casts = SQLProcessor._parameter_casts(expression)
                if ast_was_transformed:
                    cached_template = None

            final_sql, final_params, parameter_profile, input_named_params, applied_wrap = self._finalize_compilation(
                process_result.sql,
                process_result.parameters,
                expression,
                final_parameters,
                process_result.parameter_profile,
                is_many,
                self._dialect_str,
                ast_was_transformed,
                original_input_named_parameters=process_result.input_named_parameters,
            )

            if not ast_was_transformed:
                input_named_params = process_result.input_named_parameters
                applied_wrap = process_result.applied_wrap_types

            if self._should_validate_parameters(final_params, parameters, is_many):
                self._validate_parameters(parameter_profile, final_params, is_many)

            compiled = CompiledSQL(
                compiled_sql=final_sql,
                execution_parameters=final_params,
                operation_type=operation_type,
                expression=expression,
                parameter_style=self._input_style,
                supports_many=isinstance(final_params, list) and len(final_params) > 0,
                parameter_casts=parameter_casts,
                parameter_profile=parameter_profile,
                operation_profile=operation_profile,
                input_named_parameters=input_named_params,
                applied_wrap_types=applied_wrap,
            )

        except sqlspec.exceptions.SQLSpecError:
            raise
        except ParseError as exc:
            log_with_context(logger, logging.DEBUG, "sql.compile", error_type=type(exc).__name__, status="fallback")
            fallback = CompiledSQL(
                compiled_sql=sql,
                execution_parameters=parameters,
                operation_type="COMMAND",
                parameter_casts={},
                parameter_profile=parameter_profile,
                operation_profile=operation_profile,
            )
            return fallback, None
        except Exception as exc:
            log_with_context(logger, logging.ERROR, "sql.compile", error_type=type(exc).__name__, status="error")
            raise
        else:
            return compiled, cached_template

    def _parameter_fingerprint(self, parameters: Any, is_many: bool) -> Any:
        if self._parameter_config.needs_static_script_compilation:
            return value_fingerprint(parameters)
        return structural_fingerprint(parameters, is_many)

    def _make_cache_key(
        self, sql: str, param_fingerprint: Any, is_many: bool = False, expression: "exp.Expr | None" = None
    ) -> tuple[Any, ...]:
        """Generate cache key.

        Args:
            sql: SQL string
            param_fingerprint: Precomputed parameter fingerprint
            is_many: Whether this is for execute_many operation
            expression: Optional pre-parsed SQLGlot expression override

        Returns:
            Cache key tuple
        """
        expr_key = hash(expression) if expression is not None else None
        return _make_cache_key_tuple(
            sql, param_fingerprint, self._input_style, self._exec_style, self._dialect_str, is_many, extra_key=expr_key
        )

    @staticmethod
    def _operation_type(expression: "exp.Expr") -> "OperationType":
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

        return "COMMAND"

    @staticmethod
    def _parameter_casts(expression: "exp.Expr | None") -> "dict[int, str]":
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

        for node in expression.walk():
            if isinstance(node, exp.Placeholder):
                _placeholder_position(node, placeholder_positions, placeholder_counter)
            if isinstance(node, exp.Cast):
                _record_cast_position(node, cast_positions, placeholder_positions, placeholder_counter)

        return cast_positions

    def _apply_transformers(
        self, expression: "exp.Expr | None", sql: str, parameters: Any, dialect_str: "str | None"
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
            return output_transformer(sql, parameters)

        return sql, parameters

    @staticmethod
    def _operation_profile(
        expression: "exp.Expr | None", operation_type: "OperationType", dialect: "str | None" = None
    ) -> "OperationProfile":
        operation_profile, _ = _inspect_operation_profile(expression, operation_type, dialect, collect_casts=False)
        return operation_profile

    @staticmethod
    def _expression_is_read_only(expression: "exp.Expr", modifies_rows: bool, dialect: "str | None" = None) -> bool:
        if modifies_rows:
            return False
        if isinstance(expression, exp.Command):
            return _command_explain_is_read_only(expression, dialect)
        if not isinstance(expression, _READ_ONLY_ROOT_TYPES):
            return False
        if isinstance(expression, exp.Copy) and expression.args.get("kind") is not False:
            return False
        if isinstance(expression, exp.Describe) and _describe_is_analyze(expression):
            return False
        for node in expression.walk():
            if isinstance(node, _NON_READ_ONLY_AST_TYPES) or _node_disqualifies_read_only_extra(node):
                return False
        return True

    @staticmethod
    def _command_returns_rows(expression: "exp.Command") -> bool:
        """Determine whether an unparsed command produces a result set.

        The command keyword decides the result shape, except for the
        ``EXPLAIN PLAN FOR`` form, which writes a plan table and returns nothing.

        Args:
            expression: Command expression carrying the keyword and its raw payload.

        Returns:
            True when the command yields result rows.
        """
        keyword = expression.this
        if not isinstance(keyword, str):
            return False

        normalized = keyword.upper()
        if normalized not in ROW_RETURNING_COMMANDS:
            return False

        if normalized != "EXPLAIN":
            return True

        payload = expression.args.get("expression")
        if not isinstance(payload, exp.Literal):
            return True

        return not payload.name.lstrip().upper().startswith(PLAN_TABLE_EXPLAIN_PREFIX)

    @staticmethod
    def _statement_text_returns_rows(sql: str) -> bool:
        """Determine whether the leading keyword names a row-returning statement.

        Covers statements sqlglot has no expression for, where the parse either fails
        or yields an unrelated shape. ``TABLE t`` is shorthand for ``SELECT * FROM t``
        and parses as a column aliased to ``t``.

        Args:
            sql: Raw statement text.

        Returns:
            True when the statement text opens with a row-returning keyword and an operand.
        """
        parts = sql.lstrip().split(maxsplit=1)
        if not parts[1:]:
            return False

        return parts[0].upper() in ROW_RETURNING_STATEMENT_KEYWORDS

    @staticmethod
    def _make_parse_cache_key(sql: str, dialect: "str | None") -> Any:
        dialect_marker = dialect or "default"
        return (dialect_marker, sql)


def _describe_is_analyze(expression: "exp.Describe") -> bool:
    for key in ("style", "kind", "format"):
        val = expression.args.get(key)
        if isinstance(val, str) and "ANALYZE" in val.upper():
            return True
    return False


def _func_is_volatile(node: "exp.Func") -> bool:
    name = node.name.upper() if isinstance(node, exp.Anonymous) else node.sql_name().upper()
    if name in VOLATILE_FUNCTIONS or name.startswith(("PG_ADVISORY_", "PG_TRY_ADVISORY_")):
        return True
    if name == "SET_IDENTIFIER":
        parent = node.parent
        if isinstance(parent, exp.Dot):
            qualifier = parent.this
            if isinstance(qualifier, exp.Identifier) and qualifier.name.upper() == "DBMS_SESSION":
                return True
            if isinstance(qualifier, exp.Dot):
                inner = qualifier.expression
                if isinstance(inner, (exp.Identifier, exp.Column)) and inner.name.upper() == "DBMS_SESSION":
                    return True
    return False


def _node_disqualifies_read_only_extra(node: "exp.Expr") -> bool:
    if isinstance(node, exp.Column) and node.table and node.name.upper() in {"NEXTVAL", "CURRVAL"}:
        identifier = node.this
        if isinstance(identifier, exp.Identifier) and not identifier.args.get("quoted"):
            return True
    if isinstance(node, exp.WithTableHint):
        for hint in node.expressions:
            if not isinstance(hint, exp.Var) or hint.name.upper() not in _READ_ONLY_TABLE_HINTS:
                return True
    if isinstance(node, exp.Func):
        return _func_is_volatile(node)
    return False


def _command_explain_is_read_only(expression: "exp.Command", dialect: "str | None") -> bool:
    if expression.name.upper() != "EXPLAIN":
        return False
    payload = expression.args.get("expression")
    if not isinstance(payload, exp.Literal):
        return False
    text = payload.name.lstrip()
    prefix = text.upper()
    if prefix.startswith("PLAN") or "ANALYZE" in prefix:
        return False
    try:
        explained = sqlglot.parse_one(text, dialect=dialect)
    except ParseError:
        return False
    return SQLProcessor._expression_is_read_only(explained, False, dialect=dialect)  # pyright: ignore[reportPrivateUsage]


def _record_cast_position(
    node: "exp.Cast",
    cast_positions: "dict[int, str]",
    placeholder_positions: "dict[str, int]",
    placeholder_counter: "list[int]",
) -> None:
    cast_target = node.this
    position: int | None = None

    if isinstance(cast_target, exp.Parameter):
        param_value = cast_target.this
        if isinstance(param_value, exp.Literal):
            position = int(param_value.this)
    elif isinstance(cast_target, exp.Placeholder):
        position = _placeholder_position(cast_target, placeholder_positions, placeholder_counter)
    elif isinstance(cast_target, exp.Column):
        column_name = str(cast_target.this) if cast_target.this else str(cast_target)
        if column_name.startswith("$") and column_name[1:].isdigit():
            position = int(column_name[1:])

    if position is not None:
        cast_type = str(get_value_attribute(node.to.this)) if isinstance(node.to, exp.DataType) else str(node.to)
        cast_positions[position] = cast_type.upper()


def _classify_root_operation(
    expr: "exp.Expr", operation_type: "OperationType", dialect: "str | None"
) -> "tuple[bool, bool, bool, bool]":
    returns_rows = False
    modifies_rows = False

    if isinstance(expr, _RETURNS_ROWS_ROOT_TYPES):
        returns_rows = True
    elif isinstance(expr, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        modifies_rows = True
        returning = expr.args.get("returning")
        returns_rows = bool(returning) and not (isinstance(returning, exp.Returning) and returning.args.get("into"))
    elif isinstance(expr, exp.TruncateTable):
        modifies_rows = True
    elif isinstance(expr, exp.Copy):
        copy_kind = expr.args.get("kind")
        modifies_rows = copy_kind is True
        returns_rows = copy_kind is False
    elif isinstance(expr, (exp.Describe, exp.Show)):
        returns_rows = True
    elif isinstance(expr, exp.Command):
        returns_rows = SQLProcessor._command_returns_rows(expr)  # pyright: ignore[reportPrivateUsage]

    if not returns_rows and operation_type == "SELECT":
        returns_rows = True

    if not modifies_rows and operation_type in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
        modifies_rows = True

    is_read_only = False
    need_ro_walk = False
    if not modifies_rows:
        if isinstance(expr, exp.Command):
            is_read_only = _command_explain_is_read_only(expr, dialect)
        elif isinstance(expr, _READ_ONLY_ROOT_TYPES):
            if (isinstance(expr, exp.Copy) and expr.args.get("kind") is not False) or (
                isinstance(expr, exp.Describe) and _describe_is_analyze(expr)
            ):
                is_read_only = False
            else:
                is_read_only = True
                need_ro_walk = True

    return returns_rows, modifies_rows, is_read_only, need_ro_walk


def _inspect_operation_profile(
    expression: "exp.Expr | None", operation_type: "OperationType", dialect: "str | None", *, collect_casts: bool
) -> "tuple[OperationProfile, dict[int, str]]":
    if expression is None:
        return OperationProfile.empty(), {}

    returns_rows, modifies_rows, is_read_only, need_ro_walk = _classify_root_operation(
        expression, operation_type, dialect
    )

    cast_positions: dict[int, str] = {}
    if not modifies_rows or need_ro_walk or collect_casts:
        placeholder_positions: dict[str, int] = {}
        placeholder_counter = [0]
        for node in expression.walk():
            if collect_casts:
                if isinstance(node, exp.Placeholder):
                    _placeholder_position(node, placeholder_positions, placeholder_counter)
                if isinstance(node, exp.Cast):
                    _record_cast_position(node, cast_positions, placeholder_positions, placeholder_counter)
            if need_ro_walk:
                if isinstance(node, _NON_READ_ONLY_AST_TYPES):
                    is_read_only = False
                    need_ro_walk = False
                    if isinstance(node, _MODIFIES_ROWS_AST_TYPES):
                        modifies_rows = True
                elif _node_disqualifies_read_only_extra(node):
                    is_read_only = False
                    need_ro_walk = False
            elif not modifies_rows and isinstance(node, _MODIFIES_ROWS_AST_TYPES):
                modifies_rows = True
            if modifies_rows and not need_ro_walk and not collect_casts:
                break

    return OperationProfile(
        returns_rows=returns_rows, modifies_rows=modifies_rows, is_read_only=is_read_only
    ), cast_positions


def _placeholder_position(
    placeholder: "exp.Placeholder", placeholder_positions: "dict[str, int]", placeholder_counter: "list[int]"
) -> "int | None":
    value = placeholder.args.get("this")
    if value is None:
        if not placeholder.args:
            placeholder_counter[0] += 1
            return placeholder_counter[0]
        placeholder_key = placeholder.sql(copy=False)
    else:
        name_expr = placeholder.name
        placeholder_key = str(name_expr) if name_expr else str(value)

    if not placeholder_key:
        return None

    if placeholder_key not in placeholder_positions:
        placeholder_counter[0] += 1
        placeholder_positions[placeholder_key] = placeholder_counter[0]

    return placeholder_positions[placeholder_key]


def _parameters_empty(value: Any) -> bool:
    if value is None:
        return True
    value_type = type(value)
    if value_type is dict or value_type is list or value_type is tuple:
        return len(value) == 0
    if value_type is set or value_type is frozenset:
        return len(value) == 0
    if isinstance(value, Mapping):
        return len(value) == 0
    return False
