# SQLSpec Architecture Guide

*A comprehensive guide for understanding and implementing SQLSpec components*

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow](#data-flow)
3. [Core Components](#core-components)
4. [Mixin Architecture](#mixin-architecture)
5. [Pipeline System](#pipeline-system)
6. [Driver Implementation](#driver-implementation)
7. [Parameter Handling](#parameter-handling)
8. [Special Cases](#special-cases)
9. [Testing & Development](#testing--development)
10. [Common Patterns](#common-patterns)

---

## Architecture Overview

SQLSpec provides a unified interface for SQL execution across multiple database drivers with a focus on type safety, parameter handling, and consistent behavior.

### Key Design Principles

1. **Single Source of Truth**: The SQL object holds all state
2. **Immutability**: All operations return new instances
3. **Type Safety**: Parameters carry type information through the pipeline
4. **Separation of Concerns**: Clear boundaries between components
5. **Composition over Inheritance**: Use mixins for shared functionality

### Component Hierarchy

```
User API
    ↓
SQL Statement (sql.py)
    ↓
Pipeline Processing (Current Multi-Pass Architecture)
    ↓
Compilation (SQL.compile())
    ↓
Driver Adapter (adapters/*/driver.py)
    ↓
Database Engine
```

**Note**: The master plan analyzed single-pass pipeline refactoring but **decided against it** due to minimal performance gains (0.1-0.2ms) vs. significant architectural complexity costs. Current multi-pass design remains optimal.

---

## Data Flow

### 1. Statement Creation

```python
# User creates SQL
sql = SQL("SELECT * FROM users WHERE id = ?", 1)
```

### 2. Pipeline Processing

```python
# Pipeline transforms SQL (if enabled)
# - Literal parameterization
# - Normalization
# - Validation
_ensure_processed() → Pipeline → _ProcessedState
```

### 3. Compilation

```python
# SQL.compile() produces final SQL and parameters
sql_str, params = sql.compile(placeholder_style="qmark")
```

### 4. Driver Execution

```python
# Driver processes parameters and executes
processed_params = driver._process_parameters(params)  # TypeCoercionMixin
result = driver._execute(sql_str, processed_params, statement)
```

### 5. Result Serialization

```python
# Results wrapped in SQLResult with metadata
return SQLResult(data=rows, statement=statement, ...)
```

---

## Core Components

### SQL Class (`statement/sql.py`)

The central abstraction for SQL statements.

**Key Responsibilities:**

- Parse SQL strings into AST (via SQLGlot)
- Manage parameters (positional and named)
- Apply filters and transformations
- Compile to target parameter styles

**Key Methods:**

```python
# Public API
sql.compile(placeholder_style="qmark") → (str, params)
sql.where(condition) → SQL
sql.limit(n) → SQL
sql.as_many(params) → SQL
sql.as_script() → SQL

# Internal
sql._ensure_processed() → None
sql._build_final_state() → (expression, params)
```

**Important Fields:**

- `_raw_sql`: Original SQL string
- `_statement`: SQLGlot AST expression
- `_positional_params`: List of positional parameters
- `_named_params`: Dict of named parameters
- `_processed_state`: Cached pipeline results

### SQLConfig (`statement/sql.py`)

Configuration for SQL processing behavior.

```python
SQLConfig(
    enable_parsing=True,           # Use SQLGlot parsing
    enable_validation=True,        # Run security validators
    enable_transformations=True,   # Apply transformers
    enable_caching=True,          # Cache processed results
    dialect="postgres",           # Target SQL dialect
)
```

### TypedParameter (`statement/parameters.py`)

Carries type information through the pipeline.

```python
TypedParameter(
    value=123,                    # Actual value
    type_hint="int",             # Type hint for coercion
    sqlglot_type=exp.DataType,   # SQLGlot type expression
    semantic_name="user_id"      # Parameter meaning
)
```

---

## Mixin Architecture

SQLSpec uses mixins to compose driver functionality. Each mixin provides specific capabilities.

### TypeCoercionMixin (`driver/mixins/_type_coercion.py`)

**Purpose**: Extract values from TypedParameter objects and apply database-specific type conversions.

**Required Methods**:

```python
def _process_parameters(self, parameters: Any) -> Any:
    """Main entry point - processes all parameters"""
    # DO NOT BYPASS THIS METHOD
    
def _coerce_parameter_type(self, param: Any) -> Any:
    """Process single parameter"""
    
# Override these for specific databases:
def _coerce_boolean(self, value: Any) -> Any:
    """SQLite/MySQL need 0/1 instead of True/False"""
    
def _coerce_decimal(self, value: Any) -> Any:
    """Some DBs need string decimals"""
    
def _coerce_json(self, value: Any) -> Any:
    """Some DBs need JSON as strings"""
    
def _coerce_array(self, value: Any) -> Any:
    """DBs without array support need JSON"""
```

**Key Rule**: This mixin handles ALL parameter processing. Don't add duplicate processing.

### SyncStorageMixin / AsyncStorageMixin

**Purpose**: Provide data import/export capabilities.

**Required Methods**:

```python
def fetch_arrow_table(self, sql: SQL) -> ArrowResult
def ingest_arrow_table(self, table: Any, table_name: str) -> int
def export_csv(self, sql: SQL, path: str) -> None
def import_csv(self, path: str, table_name: str) -> int
```

### SyncPipelinedExecutionMixin / AsyncPipelinedExecutionMixin

**Purpose**: Integrate with the SQL pipeline system.

**Key Methods**:

```python
def _get_compiled_sql(self, statement: SQL, style: ParameterStyle) -> tuple[str, Any]:
    """Get compiled SQL with proper parameter style"""
    return statement.compile(placeholder_style=style)
```

### SQLTranslatorMixin

**Purpose**: Handle SQL dialect translation.

**Methods**:

```python
def transpile_sql(self, sql: str, from_dialect: str, to_dialect: str) -> str
def optimize_sql(self, sql: str, dialect: str) -> str
```

### ToSchemaMixin

**Purpose**: Convert query results to structured schemas.

```python
def to_schema(self, result: SQLResult, schema_type: type[T]) -> list[T]
```

### SyncAdapterCacheMixin / AsyncAdapterCacheMixin

**Purpose**: Cache prepared statements and query results.

```python
def _get_cache_key(self, sql: str, params: Any) -> str
def _cache_statement(self, key: str, statement: Any) -> None
```

---

## Pipeline System

SQLSpec uses a **multi-pass pipeline architecture** that balances performance with maintainability.

### Current Architecture (KEEP THIS)

The current system uses separate pipeline stages:
- **Transformers**: Modify AST (parameterize literals, simplify expressions)  
- **Validators**: Check safety/security without modifying
- **Analyzers**: Extract metadata without modifying

**Performance Analysis**: Pipeline overhead is minimal (<0.25ms worst case). The real bottleneck is SQL parsing (0.2-1.3ms), already optimized with AST caching (16.99x speedup).

### Why NOT Single-Pass?

The master plan analyzed single-pass refactoring but **rejected it** because:
- **Minimal gain**: Only saves 0.1-0.2ms (< 10% improvement)
- **High cost**: Loss of modularity, flexibility, testability
- **Current design**: Already excellent performance with clean separation

### Pipeline Transformers

For special cases like ADBC null handling, add pipeline transformers:

```python
# In adapter config
if statement_config and dialect == "postgres":
    from sqlspec.adapters.adbc.transformers import adbc_postgres_null_step
    
    transformers = list(statement_config.pipeline_transformers or [])
    if adbc_postgres_null_step not in transformers:
        transformers.append(adbc_postgres_null_step)
        statement_config = statement_config.replace(pipeline_transformers=transformers)
```

### Transform Step Example

```python
def adbc_postgres_null_step(context: SQLTransformContext) -> SQLTransformContext:
    """Transform NULL parameters for ADBC PostgreSQL driver."""
    if context.dialect != "postgres":
        return context
    
    # Transform AST to replace NULL placeholders with typed NULLs
    # Remove NULL values from parameters
    # Update metadata
    
    return context
```

### Literal Parameterization

Extracts literal values from SQL and replaces with placeholders:

```python
# Input
SELECT * FROM users WHERE name = 'John' AND age = 25

# Output
SELECT * FROM users WHERE name = :param_0 AND age = :param_1
# Parameters: {"param_0": "John", "param_1": 25}
```

**Important**: Types are preserved during extraction.

---

## Driver Implementation

### Base Requirements

Every driver must:

1. Inherit from `SyncDriverAdapterProtocol` or `AsyncDriverAdapterProtocol`
2. Include required mixins
3. Implement four execution methods
4. Handle database-specific quirks

### Required Methods

```python
class MyDriver(
    SyncDriverAdapterProtocol[ConnectionT, RowT],
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
):
    def _execute_statement(
        self, 
        statement: SQL, 
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult[RowT]:
        """Main dispatcher - routes to appropriate method"""
        
    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult[RowT]:
        """Execute single statement"""
        
    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult[RowT]:
        """Execute with multiple parameter sets"""
        
    def _execute_script(
        self,
        script: str,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult[RowT]:
        """Execute multi-statement script"""
```

### Implementation Pattern

```python
def _execute_statement(self, statement: SQL, connection: Optional[ConnectionT] = None, **kwargs: Any) -> SQLResult[RowT]:
    # 1. Handle script execution first (no parameters)
    if statement.is_script:
        sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
        return self._execute_script(sql, connection=connection, **kwargs)
    
    # 2. Determine parameter style based on detected placeholders
    target_style = self._determine_target_style(statement)
    
    # 3. Get compiled SQL
    sql, params = self._get_compiled_sql(statement, target_style)
    
    # 4. Process parameters through TypeCoercionMixin
    params = self._process_parameters(params)
    
    # 5. Route to appropriate method
    if statement.is_many:
        return self._execute_many(sql, params, connection=connection, **kwargs)
    else:
        return self._execute(sql, params, statement, connection=connection, **kwargs)
```

### Common Pitfalls

❌ **DON'T** add duplicate parameter processing:

```python
# WRONG - double processing
params = convert_parameter_sequence(params)  # Don't do this!
params = self._process_parameters(params)    # This is enough
```

❌ **DON'T** bypass TypeCoercionMixin:

```python
# WRONG - manual extraction
if has_parameter_value(param):
    value = param.value  # Let TypeCoercionMixin handle this!
```

✅ **DO** override specific coercion methods:

```python
def _coerce_boolean(self, value: Any) -> Any:
    """SQLite needs 0/1 for booleans"""
    if isinstance(value, bool):
        return 1 if value else 0
    return value
```

---

## Parameter Handling

### Parameter Styles

SQLSpec supports multiple parameter styles:

```python
class ParameterStyle(Enum):
    QMARK = "?"              # SELECT * WHERE id = ?
    NUMERIC = "numeric"      # SELECT * WHERE id = $1
    NAMED_COLON = "colon"    # SELECT * WHERE id = :id
    NAMED_AT = "at"          # SELECT * WHERE id = @id
    POSITIONAL_COLON = "oracle"  # SELECT * WHERE id = :1
    POSITIONAL_PYFORMAT = "%s"   # SELECT * WHERE id = %s
    NAMED_PYFORMAT = "pyformat"  # SELECT * WHERE id = %(id)s
```

### Parameter Flow

1. **User provides parameters**

   ```python
   sql = SQL("SELECT * WHERE id = ?", 123)
   ```

2. **Pipeline creates TypedParameter**

   ```python
   TypedParameter(value=123, type_hint="int", ...)
   ```

3. **Compile converts style**

   ```python
   sql_str, params = sql.compile(placeholder_style="numeric")
   # "SELECT * WHERE id = $1", [TypedParameter(...)]
   ```

4. **Driver extracts values**

   ```python
   params = self._process_parameters(params)  # [123]
   ```

### Parameter Validation

The `ParameterValidator` class handles:

- Detecting parameter placeholders in SQL
- Validating parameter counts match
- Checking for SQL injection patterns
- Extracting parameter information

---

## Special Cases

### ADBC NULL Parameters

ADBC PostgreSQL can't determine types for NULL parameters. Solution: AST transformation.

```python
def _handle_null_parameters(self, sql: str, params: list[Any]) -> tuple[str, list[Any]]:
    """Replace NULL parameters with typed CAST expressions"""
    # Parse SQL into AST
    ast = sqlglot.parse_one(sql)
    
    # Find NULL parameters and their positions
    null_indices = [i for i, p in enumerate(params) if p is None]
    
    # Transform AST: $1 → CAST(NULL AS text)
    # Remove NULLs from params and renumber
    
    return modified_sql, non_null_params
```

### Psycopg COPY Commands

COPY FROM STDIN passes data as a parameter, but it's not a SQL parameter.

```python
# In parameter extraction
if "COPY" in sql and "FROM STDIN" in sql:
    return []  # No parameters to extract
    
# In driver
if is_copy_command:
    # Handle data separately from SQL parameters
    cursor.copy_expert(sql, data_parameter)
```

### BigQuery Array Parameters

BigQuery uses UNNEST for array parameters:

```python
# Transform: WHERE id IN (?)
# To: WHERE id IN UNNEST(@param_0)
```

---

## Testing & Development

### Directory Structure

```
sqlspec/
├── .tmp/           # Debug scripts and outputs
├── .todos/         # Requirements and status docs
├── .bugs/          # Bug analysis and reproductions
├── tests/
│   ├── unit/       # Fast, isolated tests
│   └── integration/# Full adapter tests
```

### Development Workflow

1. **Create debug scripts in `.tmp/`**

   ```python
   # .tmp/test_parameter_issue.py
   from sqlspec.adapters.adbc.config import AdbcConfig
   # Debug specific issue
   ```

2. **Document bugs in `.bugs/`**

   ```markdown
   # .bugs/parameter-duplication.md
   ## Issue
   ADBC sees 6 parameters instead of 3
   
   ## Reproduction
   ...
   ```

3. **Track progress in `.todos/`**
   - `current-status.md` - Overall progress
   - `remaining-errors.md` - Test failures
   - `ACTION-CHECKLIST.md` - Next steps

### Testing Commands

```bash
# Run all tests
uv run make test

# Run specific adapter tests
uv run pytest tests/integration/test_adapter_adbc.py -xvs

# Run with coverage
uv run pytest --cov=sqlspec tests/

# Type checking
uv run make type-check

# Linting
uv run make lint
```

### Adding a New Adapter

1. Create adapter module: `sqlspec/adapters/mydb/`
2. Implement driver class with required mixins
3. Add configuration class
4. Create integration tests
5. Document any special cases

---

## Common Patterns

### Pattern: Lazy Processing

SQL objects delay processing until needed:

```python
class SQL:
    def _ensure_processed(self):
        if self._processed_state is Empty:
            # Run pipeline now
            self._processed_state = self._run_pipeline()
```

### Pattern: Immutable Operations

All modifications return new instances:

```python
sql1 = SQL("SELECT * FROM users")
sql2 = sql1.where("active = true")  # New instance
sql3 = sql2.limit(10)               # Another new instance
```

### Pattern: Builder Methods

Chainable API for query construction:

```python
result = (
    SQL("SELECT * FROM users")
    .where("active = true")
    .where("age > 18")
    .limit(100)
    .as_many(user_params)
)
```

### Pattern: Type Preservation

Types flow through the entire pipeline:

```python
# User input: integer
SQL("SELECT * WHERE id = ?", 123)
    ↓
# Pipeline: TypedParameter with type_hint="int"
TypedParameter(value=123, type_hint="int")
    ↓
# Driver: Coercion based on type hint
driver._coerce_parameter_type(param)  # Returns 123 as int
```

### Pattern: Dialect Awareness

Always consider dialect differences:

```python
# Bad: Hardcoded syntax
sql = "SELECT * FROM users LIMIT 10"

# Good: Use AST
ast = exp.Select().from_("users").limit(10)
sql = ast.sql(dialect="mssql")  # SELECT TOP 10 * FROM users
```

---

## Quick Reference Card

### Essential Imports

```python
from sqlspec import SQL
from sqlspec.statement.sql import SQLConfig
from sqlspec.statement.parameters import TypedParameter, ParameterStyle
from sqlspec.driver.mixins import TypeCoercionMixin
import sqlglot.expressions as exp
```

### SQL Creation

```python
# Basic
sql = SQL("SELECT * FROM users WHERE id = ?", 1)

# With config
config = SQLConfig(dialect="postgres", enable_transformations=False)
sql = SQL("SELECT * FROM users", config=config)

# Builder pattern
sql = SQL("SELECT * FROM users").where("active = true").limit(10)
```

### Driver Implementation Checklist

- [ ] Inherit from appropriate protocol
- [ ] Include TypeCoercionMixin
- [ ] Include storage mixin
- [ ] Include pipeline mixin
- [ ] Implement `_execute_statement`
- [ ] Implement `_execute`
- [ ] Implement `_execute_many`
- [ ] Implement `_execute_script`
- [ ] Override necessary `_coerce_*` methods
- [ ] Handle special cases (NULL, arrays, etc.)
- [ ] Add comprehensive tests

### Golden Rules

1. **Trust the mixins** - Don't reimplement their functionality
2. **Parameters flow one way** - User → Pipeline → Driver → Database
3. **Types are preserved** - Use TypedParameter throughout
4. **AST over strings** - Use SQLGlot for SQL manipulation
5. **Immutability** - Return new instances, don't modify
6. **Test everything** - Each adapter needs full coverage

---

*Remember: The architecture is designed to handle complexity through composition. When in doubt, check how existing adapters solve similar problems.*
