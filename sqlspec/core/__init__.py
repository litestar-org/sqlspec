"""SQLSpec Core Module - SQL Processing System.

This module implements the core SQL processing architecture that provides
performance improvements and memory reduction while maintaining backward compatibility.

Key Design Principles:
- Single-pass processing (Parse → Transform → Cache → Execute)
- Complete API compatibility with existing statement processing
- MyPyC optimization with __slots__ and efficient patterns
- Zero-copy data access and thread-safe unified caching

Architecture Overview:
- statement.py: SQL class with complete StatementConfig compatibility
- parameters.py: 2-phase parameter processing pipeline
- compiler.py: SQLProcessor with integrated caching
- result.py: Result classes (StatementResult, SQLResult, ArrowResult)
- filters.py: Filter system (StatementFilter interface)
- cache.py: Unified cache system
- splitter.py: SQL statement splitter
- hashing.py: Cache key generation utilities
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
