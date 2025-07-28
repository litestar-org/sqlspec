# SQLSpec Quick Reference

*Essential patterns and commands for SQLSpec development*

## Public API - Driver Execute Methods

### Execute Method Overloads

```python
from typing import Union, Optional, type
from sqlspec.statement.filters import StatementFilter
from sqlspec.typing import StatementParameters, ModelDTOT

# Execute with schema conversion
def execute(
    self,
    statement: Union[SQL, Statement, QueryBuilder[Any]],
    /,
    *parameters: Union[StatementParameters, StatementFilter],
    schema_type: type[ModelDTOT],
    _connection: Optional[ConnectionT] = None,
    _config: Optional[SQLConfig] = None,
    **kwargs: Any,
) -> SQLResult[ModelDTOT]: ...

# Execute without schema conversion
def execute(
    self,
    statement: Union[SQL, Statement, QueryBuilder[Any]],
    /,
    *parameters: Union[StatementParameters, StatementFilter],
    schema_type: None = None,
    _connection: Optional[ConnectionT] = None,
    _config: Optional[SQLConfig] = None,
    **kwargs: Any,
) -> SQLResult: ...
```

### Execute Many

```python
def execute_many(
    self,
    statement: Union[SQL, Statement, QueryBuilder[Any]],
    /,
    *parameters: Union[StatementParameters, StatementFilter],
    _connection: Optional[ConnectionT] = None,
    _config: Optional[SQLConfig] = None,
    **kwargs: Any,
) -> SQLResult:
    """Execute statement multiple times with different parameters."""
```

### Execute Script

```python
def execute_script(
    self,
    statement: Union[str, SQL],
    /,
    *parameters: Union[StatementParameters, StatementFilter],
    _connection: Optional[ConnectionT] = None,
    _config: Optional[SQLConfig] = None,
    _suppress_warnings: bool = False,
    **kwargs: Any,
) -> SQLResult:
    """Execute multi-statement script."""
```

## Pipeline Processing Order

```mermaid
graph TD
    A[User SQL + Params] --> B[SQL.__init__]
    B --> C{Lazy Processing}
    C -->|When needed| D[_ensure_processed]
    D --> E[SQLTransformContext]
    E --> F[compose_pipeline]
    F --> G[normalize_step]
    G --> H[parameterize_literals_step]
    H --> I[optimize_step + caching]
    I --> J[validate_step]
    J --> K[_ProcessedState]
    K --> L[Three-Tier Cache Check]
    L --> M[compile]
    M --> N[Driver._process_parameters]
    N --> O[cursor.execute]
```

### Caching Layers

- **Base Statement Cache**: Processed SQL objects
- **Filter Result Cache**: Applied filter transformations
- **Optimized Expression Cache**: SQLGlot optimization results

## Key Classes & Their Roles

### SQL Statement

```python
sql = SQL("SELECT * WHERE id = ?", 1)
# Holds: AST, parameters, configuration
# Returns: New instances on modification
```

### TypedParameter

```python
TypedParameter(
    value=123,           # Actual value
    type_hint="int",     # For type coercion
    sqlglot_type=...,    # AST type
    semantic_name="id"   # Parameter meaning
)
```

### SQLTransformContext

```python
@dataclass
class SQLTransformContext:
    current_expression: exp.Expression  # Modified AST
    original_expression: exp.Expression # Original AST
    parameters: dict[str, Any]         # Extracted params
    dialect: str                       # Target dialect
    metadata: dict[str, Any]          # Step results
```

## Mixin Responsibilities

| Mixin | Purpose | Key Method |
|-------|---------|------------|
| TypeCoercionMixin | Extract TypedParameter values | `_process_parameters()` |
| SyncStorageMixin | Import/export data | `fetch_arrow_table()` |
| SyncPipelinedExecutionMixin | Pipeline integration | `_get_compiled_sql()` |
| SQLTranslatorMixin | Dialect translation | `transpile_sql()` |
| ToSchemaMixin | Result conversion | `to_schema()` |

## Driver Implementation Pattern

```python
from typing import Optional, Any, ClassVar
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver.mixins import (
    TypeCoercionMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL
from sqlspec.typing import ConnectionT, RowT

class MyDriver(
    SyncDriverAdapterBase[ConnectionT, RowT],
    TypeCoercionMixin,            # REQUIRED
    SyncPipelinedExecutionMixin,  # REQUIRED
    SyncStorageMixin,             # REQUIRED
):
    # Class attributes
    dialect: ClassVar[str] = "mydialect"
    supported_parameter_styles: ClassVar[tuple[ParameterStyle, ...]] = (ParameterStyle.QMARK,)
    default_parameter_style: ClassVar[ParameterStyle] = ParameterStyle.QMARK

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult:
        # 1. Handle scripts first
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, statement=statement, **kwargs)

        # 2. Get target parameter style
        target_style = self._determine_target_style(statement)

        # 3. Compile SQL
        sql, params = self._get_compiled_sql(statement, target_style)

        # 4. Process parameters (DO NOT ADD MORE PROCESSING)
        params = self._process_parameters(params)

        # 5. Route to method
        if statement.is_many:
            return self._execute_many(sql, params, connection=connection, **kwargs)
        else:
            return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult:
        """Execute single statement"""
        raise NotImplementedError

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult:
        """Execute with multiple parameter sets"""
        raise NotImplementedError

    def _execute_script(
        self,
        script: str,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any
    ) -> SQLResult:
        """Execute multi-statement script"""
        raise NotImplementedError
```

## Common Overrides

### Boolean Coercion (SQLite, MySQL)

```python
def _coerce_boolean(self, value: Any) -> Any:
    if isinstance(value, bool):
        return 1 if value else 0
    return value
```

### JSON Coercion (SQLite)

```python
def _coerce_json(self, value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value
```

### Array Coercion (Non-array DBs)

```python
def _coerce_array(self, value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value))
    return value
```

## Special Cases

### ADBC NULL Parameters

```python
# Problem: ADBC can't determine NULL types
# Solution: AST transformation
```

@sqlspec/adapters/adbc/pipeline_steps.py

### Psycopg COPY

```python
# Problem: COPY data isn't a SQL parameter
# Solution: Detect and skip parameter extraction
```

@sqlspec/adapters/psycopg/pipeline_steps.py

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

- Trust TypeCoercionMixin for parameter processing
- Override specific `_coerce_*` methods only
- Use AST transformation for SQL changes
- Return new SQL instances (immutability)
- Test with type preservation

### ❌ DON'T

- Add `convert_parameter_sequence` calls
- Manually extract TypedParameter values
- Process parameters multiple times
- Modify SQL strings directly
- Mix concerns between pipeline and driver

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
    connection: Optional[ConnectionT] = None,
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
    connection_config={"host": "localhost"},
    statement_config=SQLConfig(enable_transformations=True)
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
| "NULL type mapping" | ADBC NULL params | Use AST transformation |
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
from sqlspec.statement.filters import StatementFilter
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
        """Extract (positional_params, named_params) from filter."""
        return [], {}
```

### Common Filters

```python
from sqlspec.statement.filters import (
    LimitOffsetFilter,     # .limit(10).offset(20)
    OrderByFilter,         # .order_by("name", "created_at DESC")
    InCollectionFilter,    # WHERE col IN (...)
    SearchFilter,          # Text search patterns
    BeforeAfterFilter,     # Date/time filtering
)
```

## File Organization

```
sqlspec/
├── statement/          # Core SQL handling
│   ├── sql.py         # SQL class
│   ├── pipeline.py    # Transform pipeline
│   └── parameters.py  # Parameter types
├── driver/            # Shared driver code
│   └── mixins/       # Mixin implementations
├── adapters/         # Database adapters
│   └── {db}/
│       ├── driver.py  # Driver implementation
│       └── config.py  # Configuration
└── tests/
    ├── .tmp/         # Debug scripts
    ├── .bugs/        # Bug reports
    └── .todos/       # Task tracking
```

## Adapter Development Patterns

### Script Execution (CRITICAL FIX)

**❌ WRONG - Double Compilation:**

```python
def _perform_execute(self, cursor, statement):
    if statement.is_script:
        sql = self._prepare_script_sql(statement)  # ← Compiles internally
        cursor.executescript(sql)  # ← Another compilation path
```

**✅ CORRECT - Single Compilation:**

```python
def _perform_execute(self, cursor, statement):
    sql, params = statement.compile()
    if statement.is_script:
        # Single compilation with STATIC style to embed parameters
        sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
        cursor.executescript(sql)
    else:
        # Regular execution with driver's parameter style
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
        prepared_params = self._prepare_driver_parameters(params)
        cursor.execute(sql, prepared_params or ())
```

### Four-Method Execution Pattern

**Standard Implementation Structure:**

```python
def _perform_execute(self, cursor, statement):
    """Main dispatch method - SINGLE compilation point."""
    if statement.is_script:
        return self._execute_script(cursor, statement)
    elif statement.is_many:
        return self._execute_many(cursor, statement)
    else:
        return self._execute_single(cursor, statement)

def _execute_script(self, cursor, statement):
    """Script execution - uses STATIC compilation."""
    sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
    # Database-specific script execution

def _execute_many(self, cursor, statement):
    """Batch execution - uses driver's parameter style."""
    sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
    prepared_params = self._prepare_driver_parameters_many(params)
    # Database-specific batch execution

def _execute_single(self, cursor, statement):
    """Single statement execution - uses driver's parameter style."""
    sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
    prepared_params = self._prepare_driver_parameters(params)
    # Database-specific single execution
```

### Configuration Pattern

**Complete Adapter Config:**

```python
from typing import ClassVar
from sqlspec.adapters.base import BaseConfig
from sqlspec.parameters import DriverParameterConfig, ParameterStyle

class MyDbConfig(BaseConfig[MyDbConnection, MyDbDriver]):
    """Configuration for MyDb adapter."""

    driver_class: ClassVar[type[MyDbDriver]] = MyDbDriver

    # Driver parameter configuration
    parameter_config: ClassVar[DriverParameterConfig] = DriverParameterConfig(
        supported_parameter_styles=[ParameterStyle.QMARK, ParameterStyle.NAMED_COLON],
        default_parameter_style=ParameterStyle.QMARK,
        type_coercion_map={
            bool: int,  # Convert booleans to integers
            datetime.datetime: lambda v: v.isoformat(),  # ISO format
            Decimal: str,  # String representation
            dict: to_json,  # JSON serialization
            list: to_json,  # JSON serialization
        },
        has_native_list_expansion=False,  # Whether DB supports IN (?, ?, ?)
    )

    def _create_connection(self) -> MyDbConnection:
        """Create database connection from config."""
        return MyDbConnection(**self.connection_config)
```

### Driver Class Structure

**Complete Driver Implementation:**

```python
from typing import ClassVar, Optional, Any
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle

class MyDbDriver(SyncDriverAdapterBase):
    """Driver for MyDb database."""

    # Required class attributes
    dialect: ClassVar[str] = "mydb"
    parameter_config: ClassVar[DriverParameterConfig] = DriverParameterConfig(
        supported_parameter_styles=[ParameterStyle.QMARK],
        default_parameter_style=ParameterStyle.QMARK,
        type_coercion_map={},
        has_native_list_expansion=False,
    )

    @contextmanager
    def with_cursor(self, connection):
        """Context manager for cursor lifecycle."""
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _perform_execute(self, cursor, statement):
        """Main execution dispatch - SINGLE compilation point."""
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            cursor.executescript(sql)
        else:
            sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
            if statement.is_many:
                prepared_params = self._prepare_driver_parameters_many(params) if params else []
                cursor.executemany(sql, prepared_params)
            else:
                prepared_params = self._prepare_driver_parameters(params)
                cursor.execute(sql, prepared_params or ())

    def _extract_select_data(self, cursor):
        """Extract data from SELECT results."""
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description or []]
        return [dict(zip(columns, row)) for row in data], columns, len(data)

    def _extract_execute_rowcount(self, cursor):
        """Extract row count from INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0
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
    sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
    prepared_params = self._prepare_driver_parameters(params)
    statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
    for stmt in statements:
        if stmt.strip():
            cursor.execute(stmt, prepared_params or ())
```

**BigQuery (job-based):**

```python
if statement.is_script:
    sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
    prepared_params = self._prepare_driver_parameters(params)
    statements = self._split_script_statements(sql)
    jobs = []
    for stmt in statements:
        if stmt.strip():
            job = self._run_query_job(stmt, prepared_params)
            jobs.append(job)
    cursor.jobs = jobs
```

### Parameter Processing Rules

**DO:**

- Use `statement.compile()` ONCE per execution path
- Use `ParameterStyle.STATIC` for scripts that don't support parameters
- Use driver's `default_parameter_style` for regular execution
- Call `_prepare_driver_parameters()` to unwrap TypedParameter objects
- Trust the type coercion system

**DON'T:**

- Call `_prepare_script_sql()` followed by another compilation
- Manually process TypedParameter objects
- Add custom parameter conversion logic
- Modify SQL strings directly after compilation
- Mix parameter styles within a single execution

### Testing Pattern

**Adapter Test Structure:**

```python
def test_script_execution(session):
    """Test script execution without double compilation."""
    script = """
    CREATE TABLE test (id INTEGER, name TEXT);
    INSERT INTO test (id, name) VALUES (1, 'test');
    SELECT * FROM test;
    """

    result = session.execute_script(script)
    assert isinstance(result, SQLResult)

    # Verify results
    check = session.execute("SELECT * FROM test")
    assert len(check.data) == 1
    assert check.data[0]["name"] == "test"

def test_parameter_styles(session):
    """Test different parameter binding styles."""
    # Tuple parameters
    result = session.execute("SELECT ?", ("value",))

    # Dict parameters
    result = session.execute("SELECT :name", {"name": "value"})

    # Many parameters
    result = session.execute_many(
        "INSERT INTO test (name) VALUES (?)",
        [("name1",), ("name2",)]
    )
```

### Common Pitfalls

| Problem | Symptom | Solution |
|---------|---------|----------|
| Double compilation | Performance degradation, parameter loss | Use single `statement.compile()` call |
| Script parameter loss | NULL values in script execution | Use `ParameterStyle.STATIC` for scripts |
| Type coercion errors | Database type mismatches | Define proper `type_coercion_map` |
| Parameter count mismatch | Database driver errors | Don't manually process parameters |
| Memory leaks | Growing memory usage | Properly close cursors in context managers |

### Development Workflow

1. **Start with SQLite adapter as reference** - `sqlspec/adapters/sqlite/driver.py`
2. **Copy the four-method execution structure** - Main dispatch + 3 execution methods
3. **Define parameter configuration** - Supported styles and type coercion
4. **Implement cursor management** - Context manager pattern
5. **Test script execution** - Verify single compilation
6. **Add database-specific optimizations** - Connection pooling, etc.
7. **Create comprehensive tests** - Unit + integration coverage

## Remember

1. **Single Compilation Rule** - One `statement.compile()` call per execution path
2. **Pipeline transforms SQL only** - not parameters
3. **TypeCoercionMixin processes parameters** - trust it
4. **Script execution uses STATIC style** - to embed parameters as literals
5. **Types flow through the system** - preserve them
6. **Test everything** - especially edge cases and script execution
