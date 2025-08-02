# SQLSpec Cheat Sheet Documentation

This directory contains comprehensive reference documentation for SQLSpec development.

## Documents

### 1. [SQLSpec Architecture Guide](sqlspec-architecture-guide.md)

A comprehensive 700+ line guide covering:

- Complete architecture overview with single-pass pipeline
- Data flow from SQL to execution through three-tier caching
- All mixin implementations and their methods
- Pipeline system with SQLTransformContext and compose_pipeline
- Driver implementation patterns with correct signatures
- Parameter handling and type preservation
- Special cases (ADBC NULL, psycopg COPY, etc.)
- Testing and development workflows

### 2. [Quick Reference](quick-reference.md)

Essential patterns and commands including:

- Public API with full type signatures
- Driver method signatures (execute, execute_many, execute_script)
- Pipeline processing order with caching layers
- Type definitions and filters
- Parameter styles by database
- Common overrides and special cases
- DO's and DON'Ts
- Testing patterns

## Key Takeaways

### Method Signatures (CRITICAL)

All driver abstract methods must be implemented with these signatures:

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

### ParameterStyleConfig Is King

- **ALWAYS** configure type coercion in ParameterStyleConfig
- **NEVER** process parameters manually
- **USE** prepare_driver_parameters() for parameter formatting

### Golden Rules

1. **Template Method Pattern** - Base class orchestrates, drivers implement specifics
2. **Parameters flow through context** - User → SQL → Pipeline → Driver → Database
3. **Immutability** - Always return new instances
4. **AST over strings** - Use SQLGlot for SQL manipulation
5. **Leverage caching** - _ProcessedState provides performance gains
6. **Use execution result tuples** - Standard format for data flow
7. **Test everything** - Especially all abstract method implementations

## When to Reference

- **Starting a new adapter**: Review the architecture guide and SQLite reference implementation
- **Debugging parameter issues**: Check quick reference DO's and DON'Ts
- **Adding features**: Ensure you're implementing required abstract methods correctly
- **Type errors**: Verify against the exact method signatures
- **Performance issues**: Review caching and execution result tuple patterns
