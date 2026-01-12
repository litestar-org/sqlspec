"""SQLSpec Core Module - SQL Processing System.

This module provides the core SQL processing infrastructure for SQLSpec, implementing
a complete pipeline for SQL statement compilation, parameter processing, caching,
and result management. All components are optimized for MyPyC compilation to
reduce overhead.

Architecture Overview:
    The core module implements a single-pass processing pipeline where SQL statements
    are parsed once, transformed once, and validated once. The SQL object serves as
    the single source of truth throughout the system.

Key Components:
    statement.py: SQL statement representation and configuration management
        - SQL class for statement encapsulation with lazy compilation
        - StatementConfig for processing pipeline configuration
        - ProcessedState for cached compilation results
        - Support for execute_many and script execution modes

    parameters.py: Type-safe parameter processing and style conversion
        - Automatic parameter style detection and conversion
        - Support for QMARK (?), NAMED (:name), NUMERIC ($1), FORMAT (%s) styles
        - Parameter validation and type coercion
        - Batch parameter handling for execute_many operations

    compiler.py: SQL compilation with validation and optimization
        - SQLProcessor for statement compilation and validation
        - Operation type detection (SELECT, INSERT, UPDATE, DELETE, etc.)
        - AST-based SQL analysis using SQLGlot
        - Support for multiple SQL dialects
        - Compiled result caching for performance

    result.py: Comprehensive result handling for all SQL operations
        - SQLResult for standard query results with metadata
        - ArrowResult for Apache Arrow format integration
        - Support for DML operations with RETURNING clauses
        - Script execution result aggregation
        - Iterator protocol support for result rows

    filters.py: Composable SQL statement filters
        - BeforeAfterFilter for date range filtering
        - InCollectionFilter for IN clause generation
        - LimitOffsetFilter for pagination
        - OrderByFilter for dynamic sorting
        - SearchFilter for text search operations
        - Parameter conflict resolution

    cache.py: Caching system with LRU eviction
        - LRUCache with configurable TTL and size limits
        - NamespacedCache for statement, expression, optimized, builder, and file caching
        - Thread-safe operations with fine-grained locking
        - Cache statistics and monitoring

    splitter.py: Dialect-aware SQL script splitting
        - Support for Oracle PL/SQL, T-SQL, PostgreSQL, MySQL
        - Proper handling of block structures (BEGIN/END)
        - Dollar-quoted string support for PostgreSQL
        - Batch separator recognition (GO for T-SQL)
        - Comment and string literal preservation

    hashing.py: Efficient cache key generation
        - SQL statement hashing with parameter consideration
        - Expression tree hashing for AST caching
        - Parameter set hashing for batch operations
        - Optimized hash computation with caching

Performance Optimizations:
    - MyPyC compilation support with proper annotations
    - __slots__ usage for memory efficiency
    - Final annotations for constant folding
    - Lazy evaluation and compilation
    - Comprehensive result caching
    - Minimal object allocation in hot paths

Thread Safety:
    All caching components are thread-safe with RLock protection.
    The processing pipeline is stateless and safe for concurrent use.

Example Usage:
    >>> from sqlspec.core import SQL, StatementConfig
    >>> config = StatementConfig(dialect="postgresql")
    >>> stmt = SQL(
    ...     "SELECT * FROM users WHERE id = ?",
    ...     1,
    ...     statement_config=config,
    ... )
    >>> compiled_sql, params = stmt.compile()
"""

from sqlspec.core import filters
from sqlspec.core._correlation import CorrelationExtractor
from sqlspec.core.cache import (
    CacheConfig,
    CachedStatement,
    CacheKey,
    CacheStats,
    FiltersView,
    LRUCache,
    NamespacedCache,
    canonicalize_filters,
    clear_all_caches,
    create_cache_key,
    get_cache,
    get_cache_config,
    get_cache_statistics,
    get_cache_stats,
    get_default_cache,
    get_pipeline_metrics,
    log_cache_stats,
    reset_cache_stats,
    reset_pipeline_registry,
    update_cache_config,
)
from sqlspec.core.compiler import (
    CompiledSQL,
    OperationProfile,
    OperationType,
    SQLProcessor,
    is_copy_from_operation,
    is_copy_operation,
    is_copy_to_operation,
)
from sqlspec.core.explain import ExplainFormat, ExplainOptions
from sqlspec.core.filters import (
    AnyCollectionFilter,
    BeforeAfterFilter,
    FilterTypes,
    FilterTypeT,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
    OrderByFilter,
    SearchFilter,
    StatementFilter,
    apply_filter,
)
from sqlspec.core.hashing import (
    hash_expression,
    hash_expression_node,
    hash_filters,
    hash_optimized_expression,
    hash_parameters,
    hash_sql_statement,
)
from sqlspec.core.metrics import StackExecutionMetrics
from sqlspec.core.parameters import (
    DRIVER_PARAMETER_PROFILES,
    EXECUTE_MANY_MIN_ROWS,
    PARAMETER_REGEX,
    DriverParameterProfile,
    ParameterConverter,
    ParameterInfo,
    ParameterProcessingResult,
    ParameterProcessor,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    ParameterValidator,
    TypedParameter,
    build_literal_inlining_transform,
    build_null_pruning_transform,
    build_statement_config_from_profile,
    collect_null_parameter_ordinals,
    get_driver_profile,
    is_iterable_parameters,
    looks_like_execute_many,
    normalize_parameter_key,
    register_driver_profile,
    replace_null_parameters_with_literals,
    replace_placeholders_with_literals,
    validate_parameter_alignment,
    wrap_with_type,
)
from sqlspec.core.query_modifiers import (
    ConditionFactory,
    apply_limit,
    apply_offset,
    apply_or_where,
    apply_select_only,
    apply_where,
    create_between_condition,
    create_condition,
    create_exists_condition,
    create_in_condition,
    create_not_exists_condition,
    create_not_in_condition,
    expr_eq,
    expr_gt,
    expr_gte,
    expr_ilike,
    expr_is_not_null,
    expr_is_null,
    expr_like,
    expr_lt,
    expr_lte,
    expr_neq,
    expr_not_like,
    extract_column_name,
    parse_column_for_condition,
    safe_modify_with_cte,
)
from sqlspec.core.result import (
    ArrowResult,
    SQLResult,
    StackResult,
    StatementResult,
    build_arrow_result_from_table,
    create_arrow_result,
    create_sql_result,
)
from sqlspec.core.splitter import split_sql_script
from sqlspec.core.stack import StackOperation, StatementStack
from sqlspec.core.statement import (
    SQL,
    ProcessedState,
    Statement,
    StatementConfig,
    get_default_config,
    get_default_parameter_config,
)
from sqlspec.core.type_converter import (
    DEFAULT_CACHE_SIZE,
    DEFAULT_SPECIAL_CHARS,
    BaseInputConverter,
    BaseTypeConverter,
    CachedOutputConverter,
    convert_decimal,
    convert_iso_date,
    convert_iso_datetime,
    convert_iso_time,
    convert_json,
    convert_uuid,
    format_datetime_rfc3339,
    parse_datetime_rfc3339,
)
from sqlspec.exceptions import StackExecutionError

__all__ = (
    "DEFAULT_CACHE_SIZE",
    "DEFAULT_SPECIAL_CHARS",
    "DRIVER_PARAMETER_PROFILES",
    "EXECUTE_MANY_MIN_ROWS",
    "PARAMETER_REGEX",
    "SQL",
    "AnyCollectionFilter",
    "ArrowResult",
    "BaseInputConverter",
    "BaseTypeConverter",
    "BeforeAfterFilter",
    "CacheConfig",
    "CacheKey",
    "CacheStats",
    "CachedOutputConverter",
    "CachedStatement",
    "CompiledSQL",
    "ConditionFactory",
    "CorrelationExtractor",
    "DriverParameterProfile",
    "ExplainFormat",
    "ExplainOptions",
    "FilterTypeT",
    "FilterTypes",
    "FiltersView",
    "InCollectionFilter",
    "LRUCache",
    "LimitOffsetFilter",
    "NamespacedCache",
    "NotInCollectionFilter",
    "NotNullFilter",
    "NullFilter",
    "OperationProfile",
    "OperationType",
    "OrderByFilter",
    "ParameterConverter",
    "ParameterInfo",
    "ParameterProcessingResult",
    "ParameterProcessor",
    "ParameterProfile",
    "ParameterStyle",
    "ParameterStyleConfig",
    "ParameterValidator",
    "ProcessedState",
    "SQLProcessor",
    "SQLResult",
    "SearchFilter",
    "StackExecutionError",
    "StackExecutionMetrics",
    "StackOperation",
    "StackResult",
    "Statement",
    "StatementConfig",
    "StatementFilter",
    "StatementResult",
    "StatementStack",
    "TypedParameter",
    "apply_filter",
    "apply_limit",
    "apply_offset",
    "apply_or_where",
    "apply_select_only",
    "apply_where",
    "build_arrow_result_from_table",
    "build_literal_inlining_transform",
    "build_null_pruning_transform",
    "build_statement_config_from_profile",
    "canonicalize_filters",
    "clear_all_caches",
    "collect_null_parameter_ordinals",
    "convert_decimal",
    "convert_iso_date",
    "convert_iso_datetime",
    "convert_iso_time",
    "convert_json",
    "convert_uuid",
    "create_arrow_result",
    "create_between_condition",
    "create_cache_key",
    "create_condition",
    "create_exists_condition",
    "create_in_condition",
    "create_not_exists_condition",
    "create_not_in_condition",
    "create_sql_result",
    "expr_eq",
    "expr_gt",
    "expr_gte",
    "expr_ilike",
    "expr_is_not_null",
    "expr_is_null",
    "expr_like",
    "expr_lt",
    "expr_lte",
    "expr_neq",
    "expr_not_like",
    "extract_column_name",
    "filters",
    "format_datetime_rfc3339",
    "get_cache",
    "get_cache_config",
    "get_cache_statistics",
    "get_cache_stats",
    "get_default_cache",
    "get_default_config",
    "get_default_parameter_config",
    "get_driver_profile",
    "get_pipeline_metrics",
    "hash_expression",
    "hash_expression_node",
    "hash_filters",
    "hash_optimized_expression",
    "hash_parameters",
    "hash_sql_statement",
    "is_copy_from_operation",
    "is_copy_operation",
    "is_copy_to_operation",
    "is_iterable_parameters",
    "log_cache_stats",
    "looks_like_execute_many",
    "normalize_parameter_key",
    "parse_column_for_condition",
    "parse_datetime_rfc3339",
    "register_driver_profile",
    "replace_null_parameters_with_literals",
    "replace_placeholders_with_literals",
    "reset_cache_stats",
    "reset_pipeline_registry",
    "safe_modify_with_cte",
    "split_sql_script",
    "update_cache_config",
    "validate_parameter_alignment",
    "wrap_with_type",
)
