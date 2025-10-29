---
orphan: true
---

# SQLSpec Data Flow: From Input to Result

*A comprehensive guide to understanding SQLSpec's execution flow - Updated for current implementation*

This document provides a detailed, in-depth analysis of how SQL statements and parameters flow through the `sqlspec` library, from user input to the final result set. Understanding this flow is crucial for debugging, extending the library, and using it effectively.

## High-Level Overview

The core of `sqlspec` is designed around a **single-pass processing pipeline** with **enhanced multi-tier caching**. A user's SQL input, whether a raw string or a `QueryBuilder` object, is converted into a `SQL` object. This object then flows through a series of transformations and validations via `SQLTransformContext` and `compose_pipeline` before being executed by a database-specific driver. The result is then packaged into a standardized `SQLResult` object.

**Key Enhancements in Current Implementation:**

- **Multi-tier caching system** providing 12x+ performance improvements
- **SQLTransformContext** for state management through pipeline steps
- **StatementConfig-aware processing** with cache key generation
- **Enhanced template method pattern** in driver execution

Here is a high-level Mermaid diagram illustrating the enhanced flow:

```mermaid
graph TD
    A[User Input: SQL("SELECT ..."), sql.select(), etc.] --> B{SQL Object};
    B --> C{Driver.execute()};
    C --> D[Enhanced Pipeline: SQLTransformContext + Multi-tier Caching];
    D --> E[StatementConfig-Aware Processing];
    E --> F{Driver Template Method Pattern};
    F --> G[Database-Specific Execution];
    G --> H[SQLResult];
    H --> I[User Code];
```

## Detailed Step-by-Step Flow

Let's break down each stage of the enhanced process in detail.

### 1. User Input and Enhanced `SQL` Object Initialization

The journey begins when a user creates a `SQL` object. This can be done in several ways:

- **Directly:** `SQL("SELECT * FROM users WHERE id = ?", 1)`
- **Via the `sql` factory:** `sql("SELECT * FROM users WHERE id = ?", 1)`
- **Using a `QueryBuilder`:** `sql.select().from_("users").where("id = 1")`

When a `SQL` object is initialized, the following enhanced process occurs:

1. **Statement Normalization**: The input (string, `QueryBuilder`, or `sqlglot` expression) is converted into a `sqlglot` expression tree via the `_to_expression` method. This creates the Abstract Syntax Tree (AST) that represents the SQL query.

2. **Enhanced Parameter and Filter Processing**: Parameters and filters are processed and stored with type preservation:
   - Positional arguments become `_positional_parameters` with `TypedParameter` support
   - Keyword arguments become `_named_parameters` with enhanced type information
   - `StatementFilter` objects are stored in `_filters` for later pipeline application

3. **StatementConfig Integration**: A `StatementConfig` object controls all processing aspects:

   ```python
   StatementConfig(
       dialect="postgres",
       enable_parsing=True,
       enable_validation=True,
       enable_transformations=True,
       enable_caching=True,  # NEW: Multi-tier caching control
       parameter_config=ParameterStyleConfig(...)  # Enhanced parameter handling
   )
   ```

4. **Lazy Processing Initialization**: The SQL object sets up for lazy processing, deferring expensive operations until `_ensure_processed()` is called during compilation.

### 2. The Enhanced `Driver.execute()` Method

The user then calls the `execute()` method on a driver instance (e.g., `sqlite_driver.execute(sql_obj)`). This is the main entry point for the execution flow.

The `AsyncDriverAdapterBase` and `SyncDriverAdapterBase` classes define the enhanced `execute` method. This method is responsible for:

1. **Enhanced SQL Preparation**: It calls `_prepare_sql` to ensure the `SQL` object is ready for execution with caching integration. This includes applying filters to the `sqlglot` expression with cached results.

2. **Enhanced Dispatching**: It calls `_dispatch_execution`, which is the central orchestrator with improved error handling and resource management.

3. **StatementConfig Coordination**: All operations respect the StatementConfig for consistent behavior across execution contexts.

### 3. The Enhanced `_dispatch_execution` Method

This method, present in both sync and async base drivers, follows the enhanced Template Method Pattern. It manages the overall execution flow with improved coordination and error handling:

The key steps are:

1. **Enhanced Context Management**: It sets the current driver in a context variable using `set_current_driver(self)` with improved cleanup handling.

2. **Advanced Cursor Management**: It acquires a database cursor using `with self.with_cursor(connection) as cursor:` with enhanced resource tracking.

3. **Template Method Execution**: It calls `self._perform_execute(cursor, statement)`, which uses the enhanced template method pattern for database interaction.

4. **Enhanced Result Building**: It calls `self._build_result(cursor, statement)` to create a `SQLResult` object with improved metadata.

5. **Comprehensive Context Cleanup**: It clears the driver context using `set_current_driver(None)` with proper error handling.

### 4. Enhanced SQL Processing Pipeline

Before execution, the `SQL` object undergoes comprehensive processing through the enhanced pipeline architecture. This is where the significant performance and functionality improvements are realized.

The `SQL.compile()` method triggers the enhanced processing pipeline:

1. **Multi-Tier Caching Architecture**: The system now implements comprehensive caching at multiple levels:

   ```python
   # Cache types and their benefits:
   sql_cache: Dict[str, str]              # Compiled SQL strings (avoids recompilation)
   optimized_cache: Dict[str, Expression] # Post-optimization AST expressions
   builder_cache: Dict[str, bytes]        # QueryBuilder state serialization
   file_cache: Dict[str, CachedSQLFile]   # File loading with checksums (12x+ speedup)
   analysis_cache: Dict[str, Any]         # Pipeline step results for reuse
   ```

2. **Enhanced Pipeline Execution**: The `_ensure_processed` method runs the advanced pipeline using `SQLTransformContext` and `compose_pipeline`:

   ```python
   context = SQLTransformContext(
       current_expression=expression,
       original_expression=expression.copy(),
       parameters=combined_params,
       dialect=config.dialect,
       metadata={},
       statement_config=config
   )

   # Pipeline steps with caching integration:
   pipeline = compose_pipeline([
       parameterize_literals_step,  # Extract literals → parameters
       optimize_step,               # SQLGlot optimization with caching
       validate_step                # Security validation with cached results
   ])
   ```

3. **StatementConfig-Aware Processing**: All pipeline operations respect StatementConfig for consistent behavior:
   - Cache keys include StatementConfig hash to prevent cross-contamination
   - Different configurations maintain separate processing paths
   - Pipeline steps can access configuration via `context.statement_config`

4. **Enhanced Parameter Style Conversion**: The modernized parameter system provides:

   ```python
   # Advanced parameter configuration:
   ParameterStyleConfig(
       default_parameter_style=ParameterStyle.QMARK,
       supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
       type_coercion_map={...},
       has_native_list_expansion=False,
       needs_static_script_compilation=True  # NEW: Script handling flag
   )
   ```

   **Key enhancements:**
   - **Cache Integration**: Parameter conversion results cached with StatementConfig keys
   - **Type Preservation**: Enhanced `TypedParameter` support through entire pipeline
   - **Script Compilation**: New `needs_static_script_compilation` for script handling
   - **Performance**: Type coercion results cached for identical input patterns

5. **Pipeline Metadata Tracking**: Each step updates context metadata for debugging and optimization:

   ```python
   context.metadata.update({
       "parameterize_literals": "completed",
       "optimization_applied": True,
       "security_validation": "passed",
       "cache_hits": 3,
       "performance_metrics": {...}
   })
   ```

### 5. Enhanced `dispatch_statement_execution` Method

This is where the enhanced template method pattern provides significant improvements. The base class coordinates execution while drivers implement specific methods.

The current implementation uses an enhanced template method pattern with better separation of concerns:

```python
def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
    """Central execution dispatcher using the Template Method Pattern."""
    with self.handle_database_exceptions(), self.with_cursor(connection) as cursor:
        special_result = self._try_special_handling(cursor, statement)
        if special_result is not None:
            return special_result

        if statement.is_script:
            execution_result = self._execute_script(cursor, statement)
        elif statement.is_many:
            execution_result = self._execute_many(cursor, statement)
        else:
            execution_result = self._execute_statement(cursor, statement)

        return self.build_statement_result(statement, execution_result)
```

**Drivers implement these specific methods:**

```python
def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
    """Hook for database-specific operations (COPY, bulk ops, etc.)"""
    return None  # Use standard execution

def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    """Execute single statement."""
    # ...

def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    """Execute with parameter batches."""
    # ...

def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    """Execute multi-statement script."""
    # ...
```

This enhanced template method pattern provides better separation of concerns and enables database-specific optimizations through the `_try_special_handling` hook.

### 6. Enhanced `build_statement_result` Method

After `dispatch_statement_execution` is done, `build_statement_result` is called with enhanced metadata support. This method inspects the cursor to determine the outcome and packages it into a `SQLResult` object with improved information.

It uses information from the `ExecutionResult` object returned by the `_execute_*` methods.

The `build_statement_result` method then uses this information to create a `SQLResult` object with the appropriate `operation_type` and enhanced metadata.

### 7. Enhanced `SQLResult` Object

The `SQLResult` object is the final product of the execution flow. It's a standardized container for the results of any SQL operation with enhanced capabilities:

- **Data**: `result.data` (a list of dictionaries)
- **Rows Affected**: `result.rows_affected`
- **Column Names**: `result.column_names`
- **Operation Type**: `result.operation_type`
- **Enhanced Metadata**: Performance metrics, cache statistics, execution timing

It also includes convenience methods like `one()`, `one_or_none()`, and `scalar()` for easily accessing the data.

## Enhanced Driver Implementation Patterns

The current implementation uses an enhanced template method pattern that provides better separation of concerns and database-specific optimizations. While the core flow is consistent, drivers can leverage various hooks and patterns.

### Modern Template Method Pattern (CURRENT)

The enhanced `_perform_execute` method in the base class coordinates execution:

```python
def _perform_execute(self, cursor: Any, statement: SQL) -> tuple[Any, Optional[int], Any]:
    """Enhanced execution with special handling and routing."""

    # 1. Try special handling first (COPY, bulk ops, etc.)
    special_result = self._try_special_handling(cursor, statement)
    if special_result is not None:
        return special_result

    # 2. Get compiled SQL with driver's parameter style
    sql, parameters = self._get_compiled_sql(statement, self.statement_config)

    # 3. Route to appropriate execution method
    if statement.is_script:
        if self.statement_config.parameter_config.needs_static_script_compilation:
            static_sql = self._prepare_script_sql(statement)
            result = self._execute_script(cursor, static_sql, None, self.statement_config)
        else:
            prepared_parameters = self.prepare_driver_parameters(parameters, self.statement_config, is_many=False)
            result = self._execute_script(cursor, sql, prepared_parameters, self.statement_config)
    elif statement.is_many:
        prepared_parameters = self.prepare_driver_parameters(parameters, self.statement_config, is_many=True)
        result = self._execute_many(cursor, sql, prepared_parameters)
    else:
        prepared_parameters = self.prepare_driver_parameters(parameters, self.statement_config, is_many=False)
        result = self._execute_statement(cursor, sql, prepared_parameters)

    return create_execution_result(result)
```

### SQLite (`sqlspec.adapters.sqlite`) - Reference Implementation

- **Parameter Style**: `qmark` (`?`)
- **Special Features**:
    - Native `executescript()` support with `needs_static_script_compilation=True`
    - No special handling required (`_try_special_handling` returns `None`)
    - Simple cursor management with context managers
- **Implementation**: This is the cleanest reference implementation, closely following the enhanced base driver protocols.

### Psycopg (`sqlspec.adapters.psycopg`) - PostgreSQL Adapter

- **Parameter Style**: `pyformat` (`%(name)s` for named, `%s` for positional)
- **Special Features**:
    - COPY operation support via `_try_special_handling`
    - Connection pool integration
    - Enhanced async support with `AsyncDriverAdapterBase`
    - Script execution via statement splitting (no native `executescript`)
- **Enhanced Capabilities**: Leverages PostgreSQL-specific features like JSONB, arrays, and bulk operations.

### ADBC (`sqlspec.adapters.adbc`) - Arrow Database Connectivity

- **Parameter Style**: Varies by underlying ADBC driver
    - PostgreSQL: `numeric` (`$1`, `$2`)
    - SQLite: `qmark` (`?`)
- **Special Features**:
    - Apache Arrow integration for high-performance data transfer
    - Enhanced type system with Arrow schema mapping
    - NULL parameter handling via custom pipeline steps
    - `ArrowResult` class for Arrow table results
- **Enhanced Capabilities**: Optimized for analytical workloads with columnar data formats.

### BigQuery (`sqlspec.adapters.bigquery`) - Cloud Analytics

- **Parameter Style**: `named_at` (`@param_name`)
- **Special Features**:
    - Job-based execution model
    - Query parameter arrays and structs
    - Dataset and table reference handling
    - Enhanced retry logic for job completion
- **Enhanced Capabilities**: Massively parallel processing with cloud-native optimizations.

### Async Driver Enhancements

All async drivers inherit from `AsyncDriverAdapterBase` and follow the same enhanced template method pattern:

#### AsyncPG (`sqlspec.adapters.asyncpg`)

- **Parameter Style**: `numeric` (`$1`, `$2`)
- **Special Features**:
    - Connection pool management
    - Prepared statement caching
    - Enhanced type conversion for PostgreSQL types
    - Pipeline integration with async context management

#### AsyncMy (`sqlspec.adapters.asyncmy`)

- **Parameter Style**: `pyformat` (`%s`)
- **Special Features**:
    - MySQL-specific type handling
    - Charset and collation support
    - Enhanced error handling for MySQL-specific errors

#### AIOSQLite (`sqlspec.adapters.aiosqlite`)

- **Parameter Style**: `qmark` (`?`)
- **Special Features**:
    - Thread pool execution for blocking SQLite operations
    - WAL mode optimization
    - Enhanced file I/O with async context managers

**Core Async Pattern**: All async methods follow the same flow as sync drivers but with `async/await`:

```python
async def execute(self, statement, /, *parameters, **kwargs) -> SQLResult:
    async with self._get_connection() as connection:
        async with self.with_cursor(connection) as cursor:
            return await self._dispatch_execution(cursor, prepared_statement)
```

## Extending with a New Driver (CURRENT IMPLEMENTATION)

To add support for a new database using the enhanced architecture:

### 1. Create Driver Class

```python
from sqlspec.driver import SyncDriverAdapterBase  # or AsyncDriverAdapterBase
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig

class MyDatabaseDriver(SyncDriverAdapterBase):
    dialect = "mydatabase"

    def __init__(self, connection, statement_config=None, driver_features=None):
        if statement_config is None:
            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK},
                type_coercion_map={
                    bool: int,
                    datetime.datetime: lambda v: v.isoformat(),
                    # ... database-specific coercions
                },
                has_native_list_expansion=False,
            )
            statement_config = StatementConfig(
                dialect="mydatabase",
                parameter_config=parameter_config,
                enable_caching=True  # Enable multi-tier caching
            )
        super().__init__(connection, statement_config, driver_features)
```

### 2. Implement Required Methods (CURRENT SIGNATURES)

```python
# Context management
def with_cursor(self, connection):
    return MyDatabaseCursor(connection)

# Transaction methods
def begin(self): self.connection.begin()
def commit(self): self.connection.commit()
def rollback(self): self.connection.rollback()

# Special handling hook
def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
    # Return None for standard execution
    # Return result object for special operations (COPY, bulk, etc.)
    return None

# Execution methods (template method pattern)
def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    # ...

def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    # ...

def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    # ...
```

### 3. Create Configuration Class

```python
from sqlspec.config import DatabaseConfig

class MyDatabaseConfig(DatabaseConfig):
    def provide_session(self, **kwargs):
        # Database-specific session setup
        connection = create_my_database_connection(**self.connection_config)
        driver = MyDatabaseDriver(connection, self.statement_config)
        return MyDatabaseSession(driver)
```

### 4. Leverage Enhanced Features

By following this pattern, you automatically get:

- **Multi-tier caching system** with 12x+ performance improvements
- **Enhanced parameter processing** with type preservation
- **SQLTransformContext pipeline** with security validation
- **StatementConfig-aware processing** for consistent behavior
- **Template method coordination** with special handling hooks
- **Standardized SQLResult objects** with consistent API
- **Error handling patterns** and resource management
- **Integration with QueryBuilder** and filter system

### 5. Testing Your Driver

Use the enhanced testing patterns:

```python
def test_my_database_driver():
    config = MyDatabaseConfig(
        connection_config={"host": "localhost"},
        statement_config=StatementConfig(enable_caching=True)
    )

    with config.provide_session() as session:
        # Test basic execution
        result = session.execute("SELECT ?", ("test",))
        assert result.data[0] == {"column": "test"}

        # Test caching performance
        result1 = session.execute("SELECT COUNT(*) FROM table")
        result2 = session.execute("SELECT COUNT(*) FROM table")  # Should hit cache

        # Test special features
        script_result = session.execute_script("""
            CREATE TABLE test (id INT, name TEXT);
            INSERT INTO test VALUES (1, 'test');
        """)
        assert script_result.operation_type == "SCRIPT"
```

The enhanced architecture provides a solid foundation that handles the complex aspects of SQL processing, allowing you to focus on database-specific implementation details.

## Performance Benefits

The enhanced architecture provides significant performance improvements:

1. **Multi-tier Caching**: 12x+ speedup for file operations and frequent queries
2. **StatementConfig-Aware Processing**: Eliminates cross-contamination and processing overhead
3. **Enhanced Pipeline**: Optimized AST operations with cached results
4. **Template Method Pattern**: Reduces duplicate code and improves maintainability
5. **Type Preservation**: Eliminates unnecessary type conversions

## Key Architectural Principles

1. **Single-Pass Processing**: Parse once, transform once, validate once
2. **Configuration-Driven**: StatementConfig controls all processing behavior
3. **Template Method Coordination**: Base class orchestrates, drivers implement specifics
4. **Cache-Aware Design**: Multi-tier caching integrated throughout the pipeline
5. **Type Safety**: Enhanced TypedParameter support preserves type information
6. **Error Resilience**: Comprehensive error handling and resource cleanup

By understanding this enhanced data flow, you can effectively debug issues, extend SQLSpec with new features, and optimize performance for your specific use cases.
