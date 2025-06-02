## [REF-007] Configuration and Driver Protocol Architecture

**DECISION**: Layered architecture with TypedDict configurations and protocol-based driver adapters.

**IMPLEMENTATION**:

### Configuration Hierarchy

- **Base Protocols**: `DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]` defines core interface
- **Sync Configurations**: `SyncDatabaseConfig` (with pooling), `NoPoolSyncConfig` (without pooling)
- **Async Configurations**: `AsyncDatabaseConfig` (with pooling), `NoPoolAsyncConfig` (without pooling)
- **Configuration Registry**: `SQLSpec` class manages configuration instances and provides type-safe access

### Driver Protocol Design

- **Common Base**: `CommonDriverAttributes[ConnectionT, DefaultRowT]` provides shared functionality
- **Instrumentation Mixins**: `SyncInstrumentationMixin` and `AsyncInstrumentationMixin` for telemetry
- **Protocol Classes**: `SyncDriverAdapterProtocol` and `AsyncDriverAdapterProtocol` define public API
- **Abstract Methods**: Driver-specific implementations (`_execute_impl`, `_wrap_select_result`, `_wrap_execute_result`)

**USER BENEFIT**:

- Type-safe configuration management with automatic pool lifecycle
- Consistent API across all database adapters
- Automatic instrumentation and telemetry for all operations
- Clean separation between public API and driver-specific implementation

**CONFIGURATION EXAMPLES**:

```python
# TypedDict configuration approach
config = PsycopgAsyncConfig(
    pool_config={
        "host": "localhost",
        "port": 5432,
        "user": "myapp",
        "dbname": "production",
        "min_size": 5,
        "max_size": 20,
    },
    instrumentation=InstrumentationConfig(
        enable_opentelemetry=True,
        enable_prometheus=True,
    )
)

# Register configuration
sqlspec = SQLSpec()
PsycopgConfig = sqlspec.add_config(config)

# Use through registry
async with sqlspec.provide_session(PsycopgConfig) as driver:
    result = await driver.execute("SELECT * FROM users")
```

**THREE MAIN DRIVER METHODS**:

### 1. `execute()` - Primary Execution Method

**Purpose**: Execute single SQL statements with intelligent return type detection

**Overload Patterns**:

```python
# With schema type - returns SelectResult[User]
users = driver.execute("SELECT * FROM users", schema_type=User)

# Without schema type - returns SelectResult[dict[str, Any]]
raw_data = driver.execute("SELECT * FROM users")

# DML operations - returns ExecuteResult[Any]
result = driver.execute("UPDATE users SET active = true")
```

**Key Features**:

- Automatic detection of SELECT vs DML statements
- Type-safe result conversion based on `schema_type` parameter
- Support for query builders, raw SQL strings, and SQL objects
- Optional statement filters and parameter binding

### 2. `execute_many()` - Batch Operations

**Purpose**: Execute the same statement multiple times with different parameter sets

**Usage Pattern**:

```python
# Batch insert with parameter sequences
parameters = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"},
]
result = driver.execute_many(
    "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
    parameters=parameters
)
```

**Return Type**: Always `ExecuteResult[Any]` since batch operations are typically DML

### 3. `execute_script()` - Script Execution

**Purpose**: Execute multi-statement scripts or database-specific commands

**Usage Pattern**:

```python
# Database schema creation script
script_output = driver.execute_script("""
    CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
    CREATE INDEX idx_users_name ON users(name);
""")
```

**Return Type**: Always `str` representing script execution output

**RETURN TYPE ARCHITECTURE**:

### SelectResult[T]

- **Purpose**: Wraps SELECT query results with metadata
- **Generic Type**: `T` represents row type (`dict[str, Any]` or custom model)
- **Key Properties**:
    - `.rows: list[T]` - The actual result rows
    - `.row_count: int` - Number of rows returned
    - Schema conversion handled automatically

### ExecuteResult[T]

- **Purpose**: Wraps DML operation results with execution metadata
- **Key Properties**:
    - `.affected_count: int` - Number of rows affected
    - `.last_insert_id: Optional[Any]` - For INSERT operations
    - Operation-specific metadata

### Raw String (Scripts)

- **Purpose**: Direct output from script execution
- **Contains**: Database response messages, execution logs, error details

**PROTOCOL IMPLEMENTATION FLOW**:

```python
# User calls public method
result = driver.execute("SELECT * FROM users", schema_type=User)

# Protocol method orchestrates:
# 1. Build SQL statement from input
sql_statement = self._build_statement(statement, config, *filters)

# 2. Execute via driver-specific implementation
raw_result = self._execute_impl(sql_statement, parameters, connection)

# 3. Wrap result based on statement type
if CommonDriverAttributes.returns_rows(sql_statement.expression):
    return self._wrap_select_result(sql_statement, raw_result, schema_type=User)
else:
    return self._wrap_execute_result(sql_statement, raw_result)
```

**INSTRUMENTATION INTEGRATION**:

- **Context Managers**: All public methods use `instrument_operation()` for telemetry
- **Multi-Level Tracking**: Protocol-level operations + driver-specific operations
- **Automatic Metrics**: Query counts, latency, error rates, pool status
- **OpenTelemetry**: Distributed tracing with proper span hierarchy
- **Prometheus**: Service-level metrics with custom labels

**KEY POINTS FOR DOCS**:

- Users interact only with the three public methods, never abstract methods
- Return type intelligence based on SQL statement analysis and `schema_type` parameter
- Configuration registry enables type-safe, dependency-injection-style usage
- All database adapters inherit the same API and instrumentation automatically
- TypedDict configurations provide IDE support while maintaining runtime flexibility

---
