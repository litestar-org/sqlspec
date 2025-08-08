# SQLSpec Cheat Sheet Documentation

This directory contains comprehensive reference documentation for SQLSpec development, completely updated to reflect the current implementation with enhanced caching, improved pipeline architecture, and optimized performance patterns.

## Documents

### 1. [SQLSpec Architecture Guide](sqlspec-architecture-guide.md)

A comprehensive 700+ line guide covering:

- Complete architecture overview with single-pass pipeline using SQLTransformContext
- Enhanced data flow from SQL to execution through multi-tier caching system
- Current driver implementation patterns with correct method signatures
- Pipeline system with compose_pipeline and StatementConfig-aware processing
- Parameter handling with TypedParameter and enhanced type preservation
- Multi-layer caching architecture (SQL, optimized, builder, file, analysis caches)
- Special cases (ADBC NULL handling, psycopg COPY operations, etc.)
- Testing and development workflows with current patterns

### 2. [Quick Reference](quick-reference.md)

Essential patterns and commands including:

- Public API with accurate type signatures and method names
- Current driver method signatures (_try_special_handling,_execute_statement,_execute_many,_execute_script)
- Pipeline processing order with caching layers and StatementConfig integration
- Enhanced caching configuration and cache statistics
- Type definitions and StatementFilter implementations
- Parameter styles by database with current ParameterStyleConfig patterns
- Current execution patterns and result tuple handling
- DO's and DON'Ts reflecting current best practices
- Testing patterns with updated command signatures

### 3. [MyPyC Optimization Guide](mypyc-optimization-guide.md)

Updated comprehensive guide including:

- Current SQLSpec MyPyC configuration with hatch-mypyc integration
- Performance-critical module selection based on profiling
- CachedSQLFile optimization (regular class with __slots__ vs dataclass)
- Type annotation patterns for optimal compilation
- Build process with `make install-compiled` and verification steps
- Compilation verification and performance benchmarking
- SQLSpec-specific guidelines reflecting current implementations

### 4. [SQLGlot Best Practices](sqlglot-best-practices-cheat-sheet.md)

Enhanced patterns guide covering:

- Current SQLGlot integration patterns in SQLSpec pipeline
- AST traversal and manipulation in pipeline steps
- Expression construction for QueryBuilder implementations
- Security validation patterns used in validate_step
- Performance optimization techniques for pipeline processing
- Current SQLSpec patterns in statement processing
- Updated anti-patterns to avoid

### 5. [Data Flow Guide](sqlspec-data-flow-guide.md)

Detailed execution flow documentation:

- Complete data flow from user input through caching layers
- Enhanced pipeline architecture with SQLTransformContext
- Driver execution patterns with result tuple handling
- Parameter processing through ParameterStyleConfig
- Caching integration at each processing stage
- Current async/sync execution patterns

## Key Takeaways

### Method Signatures (CRITICAL - UPDATED)

All driver abstract methods must be implemented with these current signatures:

```python
def _try_special_handling(
    self,
    cursor: Any,
    statement: SQL
) -> Optional[tuple[Any, Optional[int], Any]]:
    """Hook for database-specific operations"""

def _execute_statement(
    self,
    cursor: Any,
    sql: str,
    prepared_parameters: Any
) -> Any:
    """Execute single statement"""

def _execute_many(
    self,
    cursor: Any,
    sql: str,
    prepared_parameters: Any
) -> Any:
    """Execute with multiple parameter sets"""

def _execute_script(
    self,
    cursor: Any,
    sql: str,
    prepared_parameters: Any,
    statement_config: StatementConfig
) -> Any:
    """Execute script"""

def _get_selected_data(
    self,
    cursor: Any
) -> tuple[list[dict[str, Any]], list[str], int]:
    """Extract SELECT results"""

def _get_row_count(
    self,
    cursor: Any
) -> int:
    """Extract row count"""
```

### Enhanced Caching Architecture

SQLSpec now implements multi-tier caching:

- __SQL Cache__: Compiled SQL strings with StatementConfig-aware keys
- __Optimized Cache__: Post-optimization AST expressions
- __Builder Cache__: QueryBuilder instances with state serialization
- __File Cache__: SQLFileLoader with checksum validation
- __Analysis Cache__: Pipeline analysis results with step-specific caching

### StatementConfig Integration (NEW)

- __ALWAYS__ use StatementConfig for pipeline configuration
- __CRITICAL__ Cache keys include StatementConfig to prevent cross-contamination
- __USE__ get_pipeline_steps() for enhanced pipeline architecture

### Golden Rules (UPDATED)

1. __Template Method Pattern__ - Base class orchestrates, drivers implement specifics
2. __Parameters flow through enhanced context__ - User → SQL → Pipeline → Driver → Database
3. __Immutability__ - Always return new instances from QueryBuilder methods
4. __AST over strings__ - Use SQLGlot with pipeline transformations
5. __Leverage multi-tier caching__ - Multiple cache layers provide significant performance gains
6. __Use execution result tuples__ - Standard format: (result, row_count, metadata)
7. __StatementConfig awareness__ - All caching and processing respects configuration
8. __Test everything__ - Especially all abstract method implementations with current signatures

### Pipeline Architecture (ENHANCED)

The current pipeline uses:

- __SQLTransformContext__: Carries state through pipeline execution
- __compose_pipeline__: Composes multiple pipeline steps efficiently
- __StatementConfig.get_pipeline_steps()__: Enhanced pipeline with pre/post processing
- __Caching integration__: Each step can leverage analysis_cache for performance

## When to Reference

- __Starting a new adapter__: Review the architecture guide and current SQLite reference implementation
- __Debugging parameter issues__: Check quick reference DO's and DON'Ts with current patterns
- __Adding features__: Ensure you're implementing current abstract method signatures
- __Type errors__: Verify against the exact method signatures (_get_selected_data, not_extract_select_data)
- __Performance issues__: Review multi-tier caching and enhanced pipeline patterns
- __MyPyC optimization__: Follow current patterns with __slots__ classes and compilation verification
- __Pipeline development__: Use SQLTransformContext and compose_pipeline patterns

## Recent Major Changes

This documentation reflects significant recent enhancements:

1. __Comprehensive Caching System__: Multi-tier caching with 12x+ performance improvements
2. __Enhanced Pipeline Architecture__: SQLTransformContext and compose_pipeline
3. __StatementConfig Integration__: Configuration-aware caching and processing
4. __Method Signature Updates__: Current _get_selected_data/_get_row_count signatures
5. __MyPyC Optimization__: CachedSQLFile with __slots__ for compilation compatibility
6. __Performance Optimization__: Mapping-based dispatch and optimized cache operations

All code examples, method signatures, and architectural patterns have been verified against the current implementation.
