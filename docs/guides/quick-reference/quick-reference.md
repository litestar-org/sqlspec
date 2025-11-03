---
orphan: true
---

# SQLSpec Quick Reference

*Essential patterns and commands for SQLSpec development - Updated for current implementation*

## 🚨 CRITICAL UPDATES - Current Implementation

### Method Signature Changes (BREAKING)

- **OLD**: `_extract_select_data()` and `_extract_execute_rowcount()`
- **NEW**: `_get_selected_data()` and `_get_row_count()` ✅

### Enhanced Caching System (NEW)

- Multi-tier caching with StatementConfig-aware cache keys
- File cache with checksum validation providing 12x+ performance improvements
- Analysis cache for pipeline step results

### Pipeline Architecture (ENHANCED)

- SQLTransformContext for state management
- compose_pipeline for efficient step composition
- Enhanced StatementConfig.get_pipeline_steps() support

## Public API - Driver Execute Methods

The public API is consistent for both `SyncDriverAdapterBase` and `AsyncDriverAdapterBase`.

### `execute`

Executes a single statement.

```python
def execute(
    self,
    statement: "SQL | Statement | QueryBuilder",
    /,
    *parameters: "StatementParameters | StatementFilter",
    statement_config: "StatementConfig | None" = None,
    **kwargs: Any,
) -> "SQLResult":
    """Execute a statement with parameter handling."""
```

### `execute_many`

Executes a statement with multiple sets of parameters.

```python
def execute_many(
    self,
    statement: "SQL | Statement | QueryBuilder",
    /,
    parameters: "Sequence[StatementParameters]",
    *filters: "StatementParameters | StatementFilter",
    statement_config: "StatementConfig | None" = None,
    **kwargs: Any,
) -> "SQLResult":
    """Execute statement multiple times with different parameters."""
```

### `execute_script`

Executes a multi-statement script.

```python
def execute_script(
    self,
    statement: "str | SQL",
    /,
    *parameters: "StatementParameters | StatementFilter",
    statement_config: "StatementConfig | None" = None,
    **kwargs: Any,
) -> "SQLResult":
    """Execute a multi-statement script."""
```

## Core Execution Architecture: The Template Method Pattern

The core of the driver is the `dispatch_statement_execution` method, which acts as a template method. It orchestrates the execution flow, calling abstract methods that concrete drivers must implement.

```mermaid
graph TD
    A[Public API Call (e.g., session.execute)] --> B[prepare_statement]
    B --> C[dispatch_statement_execution]
    C --> D{Handle Database Exceptions}
    D --> E{Acquire Cursor}
    E --> F{Try Special Handling?}
    F -- Yes --> G[_try_special_handling]
    F -- No --> H{Operation Type?}
    H -- is_script --> I[_execute_script]
    H -- is_many --> J[_execute_many]
    H -- is_statement --> K[_execute_statement]
    I --> L[ExecutionResult]
    J --> L
    K --> L
    G --> M[SQLResult]
    L --> N[build_statement_result]
    N --> M
    M --> O[Return to Caller]
```

### `ExecutionResult` Dataclass

The abstract `_execute_*` methods do not return raw data. Instead, they return an `ExecutionResult` dataclass instance. This structured object carries all the necessary information about the execution outcome, which the base class then uses to build the final `SQLResult`.

```python
# A simplified representation
@dataclass
class ExecutionResult:
    is_select_result: bool = False
    selected_data: list[dict[str, Any]] | None = None
    column_names: list[str] | None = None
    data_row_count: int = 0
    # ... and other fields for rowcount, etc.
```

## Driver Implementation Pattern (Current)

A correct driver implementation inherits from `SyncDriverAdapterBase` or `AsyncDriverAdapterBase` and implements a specific set of abstract methods.

```python
from sqlspec.driver import SyncDriverAdapterBase, ExecutionResult
from sqlspec.core import SQL, StatementConfig

class MyDriver(SyncDriverAdapterBase):
    """Example of a current, correct driver implementation."""

    dialect = "mydialect"

    def __init__(self, connection, statement_config=None, driver_features=None):
        # ... configuration setup ...
        super().__init__(connection, statement_config, driver_features)

    # 1. Implement transaction methods
    def begin(self) -> None: self.connection.begin()
    def commit(self) -> None: self.connection.commit()
    def rollback(self) -> None: self.connection.rollback()

    # 2. Implement context managers
    def with_cursor(self, connection: Any) -> Any:
        return MyCursorContext(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        return MyExceptionHandler()

    # 3. Implement execution hooks
    def _try_special_handling(self, cursor: Any, statement: SQL) -> "SQLResult | None":
        """Hook for database-specific operations like COPY."""
        return None  # Return None to proceed with standard execution

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a single statement and return an ExecutionResult."""
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, params or ())

        if statement.returns_rows():
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description or []]
            selected_data = [dict(zip(columns, row)) for row in data]
            return self.create_execution_result(
                cursor,
                selected_data=selected_data,
                column_names=columns,
                data_row_count=len(selected_data),
                is_select_result=True
            )
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount)

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a statement with multiple parameter sets."""
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        cursor.executemany(sql, params)
        return self.create_execution_result(cursor, rowcount_override=len(params))

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a multi-statement script."""
        sql, _ = self._get_compiled_sql(statement, self.statement_config)
        # Note: Splitting and execution logic can be driver-specific
        statements = self.split_script_statements(sql, self.statement_config)
        for stmt in statements:
            cursor.execute(stmt)
        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=len(statements),
            is_script_result=True
        )
```

## Enhanced Pipeline Processing Order

```mermaid
graph TD
    A[User SQL + Params] --> B[SQL.__init__]
    B --> C{Lazy Processing}
    C -->|When needed| D[_ensure_processed]
    D --> E[SQLTransformContext]
    E --> F[compose_pipeline]
    F --> G[parameterize_literals_step]
    G --> H[optimize_step + multi-tier caching]
    H --> I[validate_step]
    I --> J[_ProcessedState + analysis_cache]
    J --> K[compile with StatementConfig-aware keys]
    K --> L[Driver._dispatch_execution]
    L --> M[Driver._perform_execute]
    M --> N[cursor.execute]
```

### Multi-Tier Caching Architecture (ENHANCED)

- **SQL Cache**: Compiled SQL strings with StatementConfig-aware cache keys
- **Optimized Cache**: Post-optimization AST expressions for reuse
- **Builder Cache**: QueryBuilder instances with state serialization
- **File Cache**: SQLFileLoader with checksum validation (12x+ speedup)
- **Analysis Cache**: Pipeline analysis results with step-specific caching

## Key Classes & Their Roles

### SQL Statement

```python
sql = SQL("SELECT * WHERE id = ?", 1)
# Holds: AST, parameters, configuration
# Returns: New instances on modification
# Key methods: compile(), copy(),   as_script()
```

### StatementConfig

```python
StatementConfig(
    dialect="postgres",                    # Target SQL dialect
    enable_parsing=True,                   # Use SQLGlot parsing
    enable_validation=True,                # Run security validators
    enable_transformations=True,           # Apply transformers
    enable_caching=True,                   # Cache processed results
    parameter_config=ParameterStyleConfig(...), # Parameter configuration
)
```

### SQLTransformContext

```python
@dataclass
class SQLTransformContext:
    current_expression: exp.Expression     # Modified AST
    original_expression: exp.Expression    # Original AST
    parameters: dict[str, Any]             # Extracted parameters
    dialect: str                           # Target dialect
    metadata: dict[str, Any]               # Step results
    driver_adapter: Any                    # Current driver instance
```




## Driver Implementation Pattern (Current)

A correct driver implementation inherits from `SyncDriverAdapterBase` or `AsyncDriverAdapterBase` and implements a specific set of abstract methods.

```python
from sqlspec.driver import SyncDriverAdapterBase, ExecutionResult
from sqlspec.core import SQL, StatementConfig

class MyDriver(SyncDriverAdapterBase):
    """Example of a current, correct driver implementation."""

    dialect = "mydialect"

    def __init__(self, connection, statement_config=None, driver_features=None):
        # ... configuration setup ...
        super().__init__(connection, statement_config, driver_features)

    # 1. Implement transaction methods
    def begin(self) -> None: self.connection.begin()
    def commit(self) -> None: self.connection.commit()
    def rollback(self) -> None: self.connection.rollback()

    # 2. Implement context managers
    def with_cursor(self, connection: Any) -> Any:
        return MyCursorContext(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        return MyExceptionHandler()

    # 3. Implement execution hooks
    def _try_special_handling(self, cursor: Any, statement: SQL) -> "SQLResult | None":
        """Hook for database-specific operations like COPY."""
        return None  # Return None to proceed with standard execution

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a single statement and return an ExecutionResult."""
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, params or ())

        if statement.returns_rows():
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description or []]
            selected_data = [dict(zip(columns, row)) for row in data]
            return self.create_execution_result(
                cursor,
                selected_data=selected_data,
                column_names=columns,
                data_row_count=len(selected_data),
                is_select_result=True
            )
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount)

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a statement with multiple parameter sets."""
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        cursor.executemany(sql, params)
        return self.create_execution_result(cursor, rowcount_override=len(params))

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a multi-statement script."""
        sql, _ = self._get_compiled_sql(statement, self.statement_config)
        # Note: Splitting and execution logic can be driver-specific
        statements = self.split_script_statements(sql, self.statement_config)
        for stmt in statements:
            cursor.execute(stmt)
        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=len(statements),
            is_script_result=True
        )
```

## Type Coercion Configuration

### ParameterStyleConfig Type Coercion

```python
# Boolean coercion for SQLite/MySQL
type_coercion_map = {
    bool: int,  # Convert True/False to 1/0
    datetime.datetime: lambda v: v.isoformat(),  # ISO format
    Decimal: str,  # String representation
    dict: to_json,  # JSON serialization
    list: to_json,  # JSON serialization
    tuple: lambda v: to_json(list(v)),  # Tuple to JSON
}

parameter_config = ParameterStyleConfig(
    default_parameter_style=ParameterStyle.QMARK,
    supported_parameter_styles={ParameterStyle.QMARK},
    type_coercion_map=type_coercion_map,
    has_native_list_expansion=False,
    needs_static_script_compilation=True,
)
```

## Special Cases

### ADBC NULL Parameters

```python
# Problem: ADBC can't determine NULL types
# Solution: AST transformation in pipeline steps
```

`sqlspec/adapters/adbc/pipeline_steps.py` - Custom pipeline step for NULL type handling

### Psycopg COPY

```python
# Problem: COPY data isn't a SQL parameter
# Solution: Special handling hook in _try_special_handling
```

`sqlspec/adapters/psycopg/pipeline_steps.py` - COPY detection and parameter skipping

## Parameter Styles

| Style | Example | Databases |
|-------|---------|-----------|
| QMARK | `WHERE id = ?` | SQLite, ADBC SQLite |
| NUMERIC | `WHERE id = $1` | PostgreSQL, ADBC PG |
| NAMED_COLON | `WHERE id = :name` | Oracle (named) |
| NAMED_AT | `WHERE id = @name` | BigQuery |
| POSITIONAL_COLON | `WHERE id = :1` | Oracle (positional) |
| POSITIONAL_PYFORMAT | `WHERE id = %s` | MySQL |
| NAMED_PYFORMAT | `WHERE id = %(name)s` | psycopg (named) |

## DO's and DON'Ts

### ✅ DO

- Use ParameterStyleConfig for type coercion
- Implement all abstract methods in driver
- Use _try_special_handling for database-specific operations
- Return execution result tuples from _perform_execute
- Test with proper parameter style conversion

### ❌ DON'T

- Process parameters manually - use prepare_driver_parameters()
- Skip implementing required abstract methods
- Modify SQL strings directly - use AST transformations
- Mix execution concerns - use the provided flow
- Ignore parameter style configuration

## Special Parameters & kwargs

### Common kwargs Usage

```python
# In execute methods, kwargs can include:
# - Driver-specific options
# - Migration flags (_suppress_warnings)
# - Execution hints
# - Custom metadata

def _execute(
    self,
    sql: str,
    parameters: Any,
    statement: SQL,
    connection: "ConnectionT | None" = None,
    **kwargs: Any  # Driver-specific options
) -> SQLResult:
    # Extract known kwargs
    timeout = kwargs.get('timeout', None)
    fetch_size = kwargs.get('fetch_size', 1000)

    # Pass remaining kwargs to cursor if supported
    cursor.execute(sql, parameters, **kwargs)
```

### Parameter Handling Flow

```python
# 1. User provides parameters
result = session.execute(
    "SELECT * FROM users WHERE id = ?",
    123,  # Positional parameter
    LimitOffsetFilter(10, 0),  # Filter (becomes part of SQL)
    timeout=30  # kwargs passed to driver
)

# 2. Driver receives
def execute(self, statement, /, *parameters, **kwargs):
    # parameters includes both values and filters
    # kwargs includes driver options
```

## Testing Patterns

### Debug Script Template

```python
#!/usr/bin/env python
# .tmp/debug_issue.py
from sqlspec.adapters.mydb import MyDbConfig

config = MyDbConfig(
    connectionstatement_config={"host": "localhost"},
    statementstatement_config=StatementConfig(enable_transformations=True)
)

with config.provide_session() as session:
    # Reproduce issue
    result = session.execute(SQL("SELECT ?", 1))
    print(f"Result: {result.data}")
```

### Test Specific Adapter

```bash
# Run one test
uv run pytest tests/integration/test_adapter_adbc.py::test_name -xvs

# Run all adapter tests
uv run pytest tests/integration/test_adapter_adbc.py -xvs

# With output
uv run pytest tests/integration/test_adapter_adbc.py -xvs --tb=short
```

## Common Errors & Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| "parameter count mismatch" | Double processing | Remove `convert_parameter_sequence` |
| "TypeError: 'int' object is not subscriptable" | Type lost | Check `_coerce_*` methods |
| "NULL type mapping" | ADBC NULL parameters | Use AST transformation |
| "COPY data invalid" | Parameter extraction | Skip COPY detection |

## Type Definitions

### Core Types

```python
from typing import Union, Any, Optional
from sqlspec.typing import (
    StatementParameters,  # Union[Sequence[Any], dict[str, Any], Any]
    ConnectionT,         # TypeVar for connection types
    RowT,               # TypeVar for row types (DictRow, TupleRow, etc.)
    ModelDTOT,          # TypeVar for model/DTO types
)

# Statement types
Statement = Union[str, exp.Expression, SQL]

# Filter types
from sqlspec.core import StatementFilter
```

### StatementFilter Protocol

```python
class StatementFilter(ABC):
    """Filters that can be appended to SQL statements."""

    @abstractmethod
    def append_to_statement(self, statement: SQL) -> SQL:
        """Apply filter and return NEW SQL instance (immutability!)."""
        ...

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract (positional_parameters, named_parameters) from filter."""
        return [], {}
```

### Common Filters

```python
from sqlspec.core import (
    LimitOffsetFilter,     # .limit(10).offset(20)
    OrderByFilter,         # .order_by("name", "created_at DESC")
    InCollectionFilter,    # WHERE col IN (...)
    SearchFilter,          # Text search patterns
    BeforeAfterFilter,     # Date/time filtering
)
```

## File Organization (Corrected)

```
sqlspec/
├── core/               # Core SQL handling (SQL, StatementConfig, etc.)
│   ├── statement.py    # SQL class
│   └── parameters/     # Parameter types and conversion
├── driver/             # Shared driver code (base classes, mixins)
│   ├── _sync.py        # SyncDriverAdapterBase
│   └── _async.py       # AsyncDriverAdapterBase
├── adapters/           # Database-specific adapters
│   └── {db}/
│       ├── driver.py   # The concrete driver implementation
│       └── config.py   # Configuration classes
└── ...
```



### Modern Configuration Pattern

**StatementConfig with ParameterStyleConfig:**

```python
from sqlspec.core import ParameterStyle, ParameterStyleConfig
from sqlspec.core import StatementConfig

# Create parameter configuration
parameter_config = ParameterStyleConfig(
    default_parameter_style=ParameterStyle.QMARK,
    supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
    type_coercion_map={
        bool: int,
        datetime.datetime: lambda v: v.isoformat(),
        Decimal: str,
        dict: to_json,
        list: to_json,
    },
    has_native_list_expansion=False,
    needs_static_script_compilation=True,
)

# Create statement configuration
statement_config = StatementConfig(
    dialect="mydb",
    parameter_config=parameter_config,
    enable_parsing=True,
    enable_validation=True,
    enable_transformations=True,
    enable_caching=True,
)
```

### SQLite Driver Example (Reference Implementation)

**Current SQLite Driver Structure:**

```python
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.core import ParameterStyle, ParameterStyleConfig
from sqlspec.core import StatementConfig

class SqliteDriver(SyncDriverAdapterBase):
    """Reference implementation for SQLite."""

    dialect = "sqlite"

    def __init__(self, connection, statement_config=None, driver_features=None):
        if statement_config is None:
            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
                type_coercion_map={
                    bool: int,
                    datetime.datetime: lambda v: v.isoformat(),
                    Decimal: str,
                    dict: to_json,
                    list: to_json,
                },
                has_native_list_expansion=False,
                needs_static_script_compilation=True,
            )
            statement_config = StatementConfig(dialect="sqlite", parameter_config=parameter_config)

        super().__init__(connection, statement_config, driver_features)

    def with_cursor(self, connection):
        return SqliteCursor(connection)

    def _try_special_handling(self, cursor, statement):
        return None  # No special operations for SQLite

    def _execute_script(self, cursor, sql, prepared_parameters, statement_config):
        cursor.executescript(sql)  # Uses static compilation

    def _execute_many(self, cursor, sql, prepared_parameters):
        cursor.executemany(sql, prepared_parameters)

    def _execute_statement(self, cursor, sql, prepared_parameters):
        cursor.execute(sql, prepared_parameters or ())

    def _get_selected_data(self, cursor):
        """CURRENT SIGNATURE: Extract SELECT results."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = [dict(zip(column_names, row)) for row in fetched_data]
        return data, column_names, len(data)

    def _get_row_count(self, cursor):
        """CURRENT SIGNATURE: Extract row count."""
        return cursor.rowcount or 0
```

### Script Execution Patterns by Database

**SQLite (executescript support):**

```python
if statement.is_script:
    sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
    cursor.executescript(sql)  # Native script support
```

**PostgreSQL (no executescript):**

```python
if statement.is_script:
    sql, parameters = statement.compile(placeholder_style=self.statement_config.parameter_config.default_parameter_style)
    prepared_parameters = self._prepare_driver_parameters(parameters)
    statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
    for stmt in statements:
        if stmt.strip():
            cursor.execute(stmt, prepared_parameters or ())
```

**BigQuery (job-based):**

```python
if statement.is_script:
    sql, parameters = statement.compile(placeholder_style=self.statement_config.parameter_config.default_parameter_style)
    prepared_parameters = self._prepare_driver_parameters(parameters)
    statements = self._split_script_statements(sql)
    jobs = []
    for stmt in statements:
        if stmt.strip():
            job = self._run_query_job(stmt, prepared_parameters)
            jobs.append(job)
    cursor.jobs = jobs
```

### Modern Parameter Processing Rules

**DO:**

- Use `_get_compiled_sql()` for consistent compilation
- Use `prepare_driver_parameters()` to prepare parameters
- Configure type coercion in ParameterStyleConfig
- Return execution result tuples from _perform_execute
- Implement _try_special_handling for database-specific operations

**DON'T:**

- Process parameters manually - use the base class methods
- Skip implementing required abstract methods
- Modify compiled SQL - use AST transformations instead
- Mix parameter styles within a single execution
- Override _perform_execute unless absolutely necessary

### Testing Pattern

**Modern Adapter Test Structure:**

```python
def test_script_execution(driver):
    """Test script execution with proper compilation."""
    script = """
    CREATE TABLE test (id INTEGER, name TEXT);
    INSERT INTO test (id, name) VALUES (1, 'test');
    SELECT * FROM test;
    """

    result = driver.execute_script(script)
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 3

    # Verify results
    check = driver.execute("SELECT * FROM test")
    assert len(check.data) == 1
    assert check.data[0]["name"] == "test"

def test_parameter_styles(driver):
    """Test different parameter binding styles."""
    # Positional parameters
    result = driver.execute("SELECT ?", ("value",))
    assert len(result.data) == 1

    # Multiple parameter sets
    result = driver.execute_many(
        "INSERT INTO test (name) VALUES (?)",
        [("name1",), ("name2",)]
    )
    assert result.rows_affected == 2

def test_special_handling(driver):
    """Test _try_special_handling hook."""
    # This would test database-specific operations
    # like PostgreSQL COPY or bulk operations
    pass
```

### Common Pitfalls

| Problem | Symptom | Solution |
|---------|---------|----------|
| Missing abstract methods | NotImplementedError at runtime | Implement all required abstract methods |
| Wrong parameter processing | Type errors, parameter mismatches | Use prepare_driver_parameters() |
| Incorrect execution result | Missing data extraction | Implement _get_selected_data() and_get_row_count() (CURRENT SIGNATURES) |
| Script execution issues | Parameter embedding problems | Configure needs_static_script_compilation correctly |
| Memory leaks | Growing memory usage | Implement proper cursor context managers |
| Method signature errors | AttributeError on *extract** methods | Use _get_selected_data and_get_row_count (current) |

### Development Workflow

1. **Start with SQLite adapter as reference** - `sqlspec/adapters/sqlite/driver.py`
2. **Inherit from SyncDriverAdapterBase** - Use proper base class
3. **Configure ParameterStyleConfig** - Set up parameter handling
4. **Implement all abstract methods** - Don't skip any required methods
5. **Create proper cursor context manager** - Handle cleanup correctly
6. **Test all execution paths** - Single, many, script execution
7. **Add comprehensive integration tests** - Test with real database

## Key Principles

1. **Template Method Pattern** - Base class orchestrates, drivers implement specifics
2. **Configuration over Code** - Use ParameterStyleConfig for behavior
3. **Execution Result Tuples** - Standard format for data flow
4. **Special Handling Hook** - _try_special_handling for database-specific operations
5. **Proper Abstraction** - Clear separation between compilation and execution
6. **Test Coverage** - Comprehensive testing of all execution paths
