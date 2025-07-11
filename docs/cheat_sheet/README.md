# SQLSpec Cheat Sheet Documentation

This directory contains comprehensive reference documentation for SQLSpec development.

## Documents

### 1. [SQLSpec Architecture Guide](sqlspec-architecture-guide.md)

A comprehensive 700+ line guide covering:

- Complete architecture overview
- Data flow from SQL to execution
- All mixin implementations and their methods
- Pipeline system details
- Driver implementation patterns with correct signatures
- Parameter handling and type preservation
- Special cases (ADBC NULL, psycopg COPY, etc.)
- Testing and development workflows

### 2. [Quick Reference](quick-reference.md)

Essential patterns and commands including:

- Public API with full type signatures
- Driver method signatures (execute, execute_many, execute_script)
- Type definitions and filters
- Parameter styles by database
- Common overrides and special cases
- DO's and DON'Ts
- Testing patterns

## Key Takeaways

### Method Signatures (CRITICAL)

All driver methods must match these exact signatures:

```python
def _execute_statement(
    self, 
    statement: SQL, 
    connection: Optional[ConnectionT] = None,
    **kwargs: Any
) -> SQLResult[RowT]:
    """Main dispatcher"""

def _execute(
    self,
    sql: str,
    parameters: Any,
    statement: SQL,
    connection: Optional[ConnectionT] = None,
    **kwargs: Any
) -> SQLResult[RowT]:
    """Single execution"""

def _execute_many(
    self,
    sql: str,
    param_list: Any,
    connection: Optional[ConnectionT] = None,
    **kwargs: Any
) -> SQLResult[RowT]:
    """Batch execution"""

def _execute_script(
    self,
    script: str,
    connection: Optional[ConnectionT] = None,
    **kwargs: Any
) -> SQLResult[RowT]:
    """Script execution"""
```

### TypeCoercionMixin Is King

- **ALWAYS** use `_process_parameters()` for parameter extraction
- **NEVER** add custom parameter processing
- **ONLY** override specific `_coerce_*` methods when needed

### Golden Rules

1. **Trust the mixins** - They handle the complexity
2. **Parameters flow one way** - User → Pipeline → Driver → Database
3. **Immutability** - Always return new instances
4. **AST over strings** - Use SQLGlot for SQL manipulation
5. **Test everything** - Especially parameter preservation

## When to Reference

- **Starting a new adapter**: Review the architecture guide
- **Debugging parameter issues**: Check quick reference DO's and DON'Ts
- **Adding features**: Ensure you're not reimplementing mixin functionality
- **Type errors**: Verify against the exact method signatures
