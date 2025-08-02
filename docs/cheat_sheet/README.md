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
- Current driver method signatures (_try_special_handling, _execute_statement, _execute_many, _execute_script)
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
    prepared_params: Any
) -> Any:
    """Execute single statement"""

def _execute_many(
    self,
    cursor: Any,
    sql: str,
    prepared_params: Any
) -> Any:
    """Execute with multiple parameter sets"""

def _execute_script(
    self,
    cursor: Any,
    sql: str,
    prepared_params: Any,
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

- **SQL Cache**: Compiled SQL strings with StatementConfig-aware keys
- **Optimized Cache**: Post-optimization AST expressions
- **Builder Cache**: QueryBuilder instances with state serialization
- **File Cache**: SQLFileLoader with checksum validation
- **Analysis Cache**: Pipeline analysis results with step-specific caching

### StatementConfig Integration (NEW)

- **ALWAYS** use StatementConfig for pipeline configuration
- **CRITICAL** Cache keys include StatementConfig to prevent cross-contamination
- **USE** get_pipeline_steps() for enhanced pipeline architecture

### Golden Rules (UPDATED)

1. **Template Method Pattern** - Base class orchestrates, drivers implement specifics
2. **Parameters flow through enhanced context** - User → SQL → Pipeline → Driver → Database
3. **Immutability** - Always return new instances from QueryBuilder methods
4. **AST over strings** - Use SQLGlot with pipeline transformations
5. **Leverage multi-tier caching** - Multiple cache layers provide significant performance gains
6. **Use execution result tuples** - Standard format: (result, row_count, metadata)
7. **StatementConfig awareness** - All caching and processing respects configuration
8. **Test everything** - Especially all abstract method implementations with current signatures

### Pipeline Architecture (ENHANCED)

The current pipeline uses:

- **SQLTransformContext**: Carries state through pipeline execution
- **compose_pipeline**: Composes multiple pipeline steps efficiently
- **StatementConfig.get_pipeline_steps()**: Enhanced pipeline with pre/post processing
- **Caching integration**: Each step can leverage analysis_cache for performance

## When to Reference

- **Starting a new adapter**: Review the architecture guide and current SQLite reference implementation
- **Debugging parameter issues**: Check quick reference DO's and DON'Ts with current patterns
- **Adding features**: Ensure you're implementing current abstract method signatures
- **Type errors**: Verify against the exact method signatures (_get_selected_data, not _extract_select_data)
- **Performance issues**: Review multi-tier caching and enhanced pipeline patterns
- **MyPyC optimization**: Follow current patterns with __slots__ classes and compilation verification
- **Pipeline development**: Use SQLTransformContext and compose_pipeline patterns

## Recent Major Changes

This documentation reflects significant recent enhancements:

1. **Comprehensive Caching System**: Multi-tier caching with 12x+ performance improvements
2. **Enhanced Pipeline Architecture**: SQLTransformContext and compose_pipeline
3. **StatementConfig Integration**: Configuration-aware caching and processing
4. **Method Signature Updates**: Current _get_selected_data/_get_row_count signatures
5. **MyPyC Optimization**: CachedSQLFile with __slots__ for compilation compatibility
6. **Performance Optimization**: Mapping-based dispatch and optimized cache operations

All code examples, method signatures, and architectural patterns have been verified against the current implementation.
