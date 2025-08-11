"""SQLSpec Core Module - SQL Processing System.

This module provides the core SQL processing components including statement handling,
parameter processing, compilation, and result management.

Components:
- statement.py: SQL class with StatementConfig
- parameters.py: Parameter processing pipeline
- compiler.py: SQL compilation with caching
- result.py: Result classes for query execution
- filters.py: Statement filter system
- cache.py: Unified caching system
- splitter.py: SQL statement splitter
- hashing.py: Cache key generation
"""

from sqlspec.core import filters
from sqlspec.core.cache import CacheConfig, CacheStats, UnifiedCache, get_statement_cache
from sqlspec.core.compiler import OperationType, SQLProcessor
from sqlspec.core.filters import StatementFilter
from sqlspec.core.hashing import (
    hash_expression,
    hash_expression_node,
    hash_optimized_expression,
    hash_parameters,
    hash_sql_statement,
)
from sqlspec.core.parameters import (
    ParameterConverter,
    ParameterProcessor,
    ParameterStyle,
    ParameterStyleConfig,
    TypedParameter,
)
from sqlspec.core.result import ArrowResult, SQLResult, StatementResult
from sqlspec.core.statement import SQL, Statement, StatementConfig

__all__ = (
    "SQL",
    "ArrowResult",
    "CacheConfig",
    "CacheStats",
    "OperationType",
    "ParameterConverter",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "SQLProcessor",
    "SQLResult",
    "Statement",
    "StatementConfig",
    "StatementFilter",
    "StatementResult",
    "TypedParameter",
    "UnifiedCache",
    "filters",
    "get_statement_cache",
    "hash_expression",
    "hash_expression_node",
    "hash_optimized_expression",
    "hash_parameters",
    "hash_sql_statement",
)
