"""SQLSpec Core Module - High-Performance SQL Processing System.

This module provides the core SQL processing infrastructure for SQLSpec, implementing
a complete pipeline for SQL statement compilation, parameter processing, caching,
and result management. All components are optimized for MyPyC compilation and
designed for maximum performance with minimal overhead.

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

    cache.py: Unified caching system with LRU eviction
        - UnifiedCache with configurable TTL and size limits
        - StatementCache for compiled SQL statements
        - ExpressionCache for parsed SQLGlot expressions
        - ParameterCache for processed parameters
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
