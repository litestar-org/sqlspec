"""SQLSpec Core Module - High-Performance SQL Processing System.

This module implements the CORE_ROUND_3 architecture redesign for 5-10x performance
improvements and 40-60% memory reduction while maintaining 100% backward compatibility.

Key Design Principles:
- Single-pass processing (Parse → Transform → Cache → Execute)
- Complete API compatibility with existing statement processing
- MyPyC optimization with __slots__ and efficient patterns
- Zero-copy data access and thread-safe unified caching
- Research → Build → Migrate → Delete implementation strategy

Architecture Overview:
- statement.py: Enhanced SQL class with complete StatementConfig compatibility
- parameters.py: Consolidated 2-phase parameter processing pipeline
- compiler.py: Single-pass SQLProcessor with integrated caching
- result.py: Preserved result classes (StatementResult, SQLResult, ArrowResult)
- filters.py: Preserved filter system (StatementFilter interface)
- cache.py: Single unified cache system replacing all cache layers
- splitter.py: Moved from statement/ (preserved as-is)
- config.py: Enhanced configuration management

Performance Targets:
- 5-10x compilation speed improvement
- 40-60% memory usage reduction
- 100% test compatibility (2,000+ tests)
- All 12 database adapters migrated successfully
"""

# Import core classes for public API
from sqlspec.core.cache import UnifiedCache, get_statement_cache
from sqlspec.core.compiler import SQLProcessor, create_processor

# Note: Configuration is distributed across adapters/*/config.py - no centralized config
from sqlspec.core.filters import StatementFilter
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.result import ArrowResult, SQLResult, StatementResult
from sqlspec.core.statement import SQL, Statement, StatementConfig

__all__ = (
    "SQL",
    "ArrowResult",
    "ParameterStyle",
    "ParameterStyleConfig",
    "SQLProcessor",
    "SQLResult",
    "Statement",
    "StatementConfig",
    "StatementFilter",
    "StatementResult",
    "UnifiedCache",
    "create_processor",
    "get_statement_cache",
)

# Version tracking for core module implementation
__core_version__ = "0.2.0-beta"
__implementation_phase__ = "MIGRATE"  # SETUP → RESEARCH → BUILD → MIGRATE → DELETE
