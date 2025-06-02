# SQLSpec Internal Reference Guide

**🎯 PURPOSE**: This document serves as a directory for detailed internal reference guides, architectural decisions, and key concepts for the SQLSpec library. Each section has been moved to its own file within the `docs/internal/` directory for better organization.

**📝 USAGE INSTRUCTIONS FOR AI**: When asked to update or refer to internal documentation, please locate the relevant file within the `docs/internal/` directory based on its title or reference number.

---

## Table of Contents

- **[REF-001] Instrumentation Architecture**: [Context Managers vs Decorators](./internal/001_instrumentation_architecture.md)
- **[REF-002] Psycopg Driver**: [ModelDTOT and Schema Type Patterns](./internal/002_psycopg_driver_modeldtot.md)
- **[REF-003] Connection Pool**: [Lifecycle and Instrumentation](./internal/003_connection_pool_lifecycle.md)
- **[REF-004] Clean Code Patterns**: [Reduced Defensive Programming](./internal/004_clean_code_patterns.md)
- **[REF-005] Driver Protocol**: [Architecture](./internal/005_driver_protocol_architecture.md)
- **[REF-006] Configuration Design**: [TypedDict Approach](./internal/006_configuration_typeddict.md)
- **[REF-007] Configuration and Driver Protocol**: [Overall Architecture](./internal/007_config_driver_protocol_architecture.md)
- **[REF-008] SQL Builder System**: [Architecture](./internal/008_sql_builder_system.md)
- **[REF-009] SQL Factory**: [Unified Builder and Expression API](./internal/009_sql_factory_api.md)
- **[REF-010] AioSQL Integration**: [File-Based SQL with Full SQLSpec Power](./internal/010_aiosql_integration.md)
- **[REF-011] SQLStatement & StatementPipeline**: [Unified SQL Processing](./internal/011_sqlstatement_pipeline.md)
- **[REF-012] Deprecated - Unified Pipeline Architecture (Old)**: [Details](./internal/012_unified_pipeline_deprecated.md)

---

**📋 NOTE ON ONGOING REFACTORING**:

Additional documentation for refactoring specific components can be found in:

- [Refactoring Guide: SQLSpec Statement Processors](./REFACTORING_PROCESSORS.md)
- [Refactoring Guide: SQLSpec Driver Adapters (`_execute_impl`)](./REFACTORING_DRIVERS.md)

---

## 📝 TEMPLATE FOR NEW ENTRIES (in `docs/internal/`)

When adding new reference sections, create a new file in `docs/internal/` (e.g., `XXX_my_new_feature.md`) and use the following template:

```markdown
## [REF-XXX] Title: Brief Description

**DECISION**: What was decided and why.

**IMPLEMENTATION**:
- Key implementation details
- Important code patterns
- Architectural choices

**USER BENEFIT**:
- How this helps users
- What problems it solves
- Performance/DX improvements

**CODE EXAMPLES**:

```python
# Show user-facing examples
# Include both basic and advanced usage
```

**KEY POINTS FOR DOCS**:

- Important concepts to emphasize
- Common pitfalls to avoid
- Integration considerations

---

```

---

## [REF-001] Instrumentation Architecture: Context Managers vs Decorators

**DECISION**: Migrated from decorator-based to context manager-based instrumentation.

**IMPLEMENTATION**:

- **Protocol Layer**: Public methods (`execute`, `execute_many`, `execute_script`) use context managers
- **Driver Layer**: Private methods (`_execute_impl`, `_wrap_select_result`) use context managers
- **Context Managers**: `instrument_operation()` (sync) and `instrument_operation_async()` (async)

**USER BENEFIT**:

- Clean type signatures (no decorator interference)
- Multi-level telemetry (API + driver level)
- Comprehensive tracking of database operations

**CODE EXAMPLES**:

```python
# User calls this
result = driver.execute("SELECT * FROM users")

# Results in telemetry hierarchy:
# 1. High-level: "execute" operation (API usage)
# 2. Low-level: "psycopg_execute" operation (database access)
# 3. Low-level: "psycopg_wrap_select" operation (result processing)
```

**TELEMETRY COVERAGE**:

- OpenTelemetry spans with proper attributes
- Prometheus metrics (counters, histograms, gauges)
- Structured logging with context
- Error tracking and latency monitoring

---

## [REF-002] Psycopg Driver: ModelDTOT and Schema Type Patterns

**DECISION**: Preserve exact `ModelDTOT` and `schema_type` behavior from main branch.

**IMPLEMENTATION**:

- `SelectResult.rows` always contains `dict[str, Any]` objects
- Schema conversion handled by type system and result converter patterns
- `_wrap_select_result` uses conditional return types based on `schema_type` parameter

**USER BENEFIT**:

- Type-safe result conversion with intelligent typing
- Seamless integration with DTO patterns
- Backwards compatibility with existing code

**CODE EXAMPLES**:

```python
# With schema type - gets SelectResult[User]
users = driver.execute("SELECT * FROM users", schema_type=User)

# Without schema type - gets SelectResult[dict[str, Any]]
raw_data = driver.execute("SELECT * FROM users")

# Both work, but typing provides safety
user_name = users.rows[0].name        # ✅ Type-safe
user_name = raw_data.rows[0]["name"]  # ✅ Dict access
```

**OVERLOAD PATTERNS**:

```python
@overload
def execute(statement: SelectBuilder, *, schema_type: type[ModelDTOT]) -> SelectResult[ModelDTOT]: ...

@overload
def execute(statement: SelectBuilder, *, schema_type: None = None) -> SelectResult[dict[str, Any]]: ...
```

---

## [REF-003] Connection Pool Lifecycle and Instrumentation

**DECISION**: Instrument pool operations for observability into connection management.

**IMPLEMENTATION**:

- Pool creation: `_create_pool_impl()` with timing and logging
- Pool closure: `_close_pool_impl()` with cleanup tracking
- Connection provision: Context managers for connection lifecycle
- Session provision: Context managers for driver instances

**USER BENEFIT**:

- Visibility into pool health and performance
- Connection leak detection capabilities
- Pool sizing optimization data

**CONFIG PATTERNS**:

```python
# TypedDict approach for clean configuration
config = PsycopgAsyncConfig(
    pool_config={
        "min_size": 5,
        "max_size": 20,
        "max_lifetime": 3600,
    },
    instrumentation=InstrumentationConfig(
        log_pool_operations=True,
        enable_prometheus=True,
    )
)

# Usage
async with config.provide_session() as driver:
    result = await driver.execute("SELECT 1")
```

**INSTRUMENTATION POINTS**:

- Pool create/destroy operations
- Connection acquire/release timing
- Pool size and utilization metrics
- Connection error rates and types

---

## [REF-004] Clean Code Patterns: Reduced Defensive Programming

**DECISION**: Trust type checker instead of excessive runtime validation.

**IMPLEMENTATION**:

- Removed verbose parameter validation chains
- Simplified error handling patterns
- Trust type hints for parameter contracts
- Focus defensive coding on specific critical paths

**USER BENEFIT**:

- Cleaner, more readable codebase
- Better performance (less runtime checks)
- Clearer error messages when issues occur

**BEFORE/AFTER EXAMPLES**:

```python
# ❌ Old: Defensive bloat
if parameters is not None and isinstance(parameters, Sequence):
    for param_set in parameters:
        if isinstance(param_set, dict):
            many_params_list.append(param_set)
        else:
            logger.warning("executemany expects dict, got %s", type(param_set))

# ✅ New: Trust types
if parameters and isinstance(parameters, Sequence):
    final_exec_params = [p for p in parameters if isinstance(p, dict)]
```

**LOGGING PATTERNS**:

```python
# ❌ Old: Verbose logging
logger.debug(
    "Executing SQL (Psycopg Sync): %s",
    final_sql,
    extra={
        "dialect": self.dialect,
        "is_many": is_many,
        "is_script": is_script,
        "param_count": len(final_exec_params) if isinstance(final_exec_params, dict) else 0,
    },
)

# ✅ New: Clean logging
logger.debug("Executing SQL: %s", final_sql)
```

---

## [REF-005] Driver Protocol Architecture

**DECISION**: Layered protocol with abstract methods for driver implementations.

**IMPLEMENTATION**:

- **Protocol classes**: Define public API (`execute`, `execute_many`, `execute_script`)
- **Abstract methods**: Driver-specific implementations (`_execute_impl`, `_wrap_*_result`)
- **Instrumentation mixins**: Provide telemetry capabilities
- **Common attributes**: Shared functionality and setup

**USER BENEFIT**:

- Consistent API across all database drivers
- Automatic instrumentation for all drivers
- Type safety through protocol compliance

**INHERITANCE HIERARCHY**:

```
SyncDriverAdapterProtocol
├── CommonDriverAttributes (connection management, instrumentation setup)
├── SyncInstrumentationMixin (telemetry capabilities)
└── Abstract methods for driver implementation

PsycopgSyncDriver
├── Inherits from SyncDriverAdapterProtocol
└── Implements: _execute_impl, _wrap_select_result, _wrap_execute_result
```

**KEY POINTS FOR DOCS**:

- Users interact with protocol methods, never abstract methods
- All drivers get instrumentation automatically
- Driver implementations focus on database-specific logic
- Protocol handles statement building, type conversion orchestration

---

## [REF-006] Configuration Design: TypedDict Approach

**DECISION**: Use TypedDict for database configuration instead of dataclasses.

**IMPLEMENTATION**:

- `PsycopgConnectionConfig`: Basic connection parameters
- `PsycopgPoolConfig`: Pool-specific configuration (inherits connection params)
- `NotRequired` fields for optional parameters
- Validation happens at runtime, not definition time

**USER BENEFIT**:

- Better IDE support and auto-completion
- Clear documentation of available options
- Type safety without runtime overhead
- Flexible configuration merging

**CONFIG EXAMPLE**:

```python
from sqlspec.adapters.psycopg import PsycopgAsyncConfig

config = PsycopgAsyncConfig(
    pool_config={
        "host": "localhost",
        "port": 5432,
        "user": "myapp",
        "password": "secret",
        "dbname": "production",
        "min_size": 5,
        "max_size": 20,
        "max_lifetime": 3600.0,
    },
    instrumentation=InstrumentationConfig(
        enable_opentelemetry=True,
        enable_prometheus=True,
        service_name="myapp-db",
    )
)
```

---

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

## [REF-008] SQL Builder System Architecture

**DECISION**: Fluent interface builders with automatic parameter binding and type-safe SQL construction.

**IMPLEMENTATION**:

### Builder Hierarchy

- **Abstract Base**: `QueryBuilder[ResultT]` provides common functionality and parameter management
- **Specific Builders**: `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`, `MergeBuilder`
- **Mixin Support**: `WhereClauseMixin` provides common WHERE clause convenience methods
- **Safety Layer**: `SafeQuery` dataclass for validated SQL + parameters before execution

### Core Design Principles

- **Automatic Parameter Binding**: All values are automatically parameterized to prevent SQL injection
- **Fluent Interface**: Method chaining for readable query construction
- **Type Safety**: Generic result types and compile-time validation
- **Dialect Awareness**: SQLGlot-powered SQL generation for multiple database dialects

**USER BENEFIT**:

- **Security**: Automatic parameterization eliminates SQL injection vulnerabilities
- **Readability**: Fluent interface mirrors SQL structure for intuitive query building
- **Type Safety**: Strong typing throughout the build process with intelligent result types
- **Database Portability**: Single API works across multiple database dialects

**BUILDER INTEGRATION WITH DRIVER PROTOCOL**:

The builders integrate seamlessly with the driver protocol - you can pass builder instances directly to `execute()` **without needing to extract parameters manually**:

```python
# Direct builder execution - parameters handled automatically
query = (
    SelectBuilder()
    .select("name", "email", "created_at")
    .from_("users")
    .where("active = true")
    .where_eq("department", "engineering")  # Automatic parameterization
    .order_by("created_at DESC")
    .limit(50)
)

# Execute directly - no manual parameter extraction needed
result = driver.execute(query, schema_type=User)
# Parameters are automatically extracted and bound by the driver
```

**CORE BUILDER PATTERNS**:

### 1. SelectBuilder - Query Construction

**Purpose**: Build type-safe SELECT statements with intelligent result handling

**Key Features**:

```python
# Basic selection with automatic parameter binding
query = (
    SelectBuilder()
    .select("u.name", "u.email", "p.title")
    .from_("users", alias="u")
    .inner_join("profiles p", on="u.id = p.user_id")
    .where_eq("u.active", True)  # Automatically parameterized
    .where_between("u.created_at", start_date, end_date)  # Multiple parameters
    .order_by("u.name")
    .limit(100)
)

# Advanced features
query = (
    SelectBuilder()
    .select("department")
    .count_("*", alias="employee_count")
    .from_("employees")
    .where_in("status", ["active", "on_leave"])  # List automatically parameterized
    .group_by("department")
    .having("COUNT(*) > 5")
)

# Subqueries and CTEs
subquery = SelectBuilder().select("id").from_("active_users").where_eq("verified", True)
main_query = (
    SelectBuilder()
    .select("*")
    .from_("orders")
    .where_exists(subquery)  # Subquery parameters merged automatically
)
```

### 2. InsertBuilder - Data Insertion

**Purpose**: Build safe INSERT statements with flexible value specification

**Key Patterns**:

```python
# Single row insert
insert = (
    InsertBuilder()
    .into("users")
    .columns("name", "email", "department")
    .values("Alice Smith", "alice@company.com", "engineering")
)

# Dictionary-based insert (columns inferred)
insert = (
    InsertBuilder()
    .into("users")
    .values_from_dict({
        "name": "Bob Jones",
        "email": "bob@company.com",
        "department": "marketing"
    })
)

# Bulk insert from data sequence
users_data = [
    {"name": "Carol", "email": "carol@company.com"},
    {"name": "Dave", "email": "dave@company.com"},
]
insert = (
    InsertBuilder()
    .into("users")
    .values_from_dicts(users_data)
)

# INSERT from SELECT
backup_insert = (
    InsertBuilder()
    .into("users_backup")
    .from_select(
        SelectBuilder()
        .select("name", "email", "created_at")
        .from_("users")
        .where("last_login < '2023-01-01'")
    )
)
```

### 3. UpdateBuilder & DeleteBuilder - DML Operations

**Purpose**: Safe modification and deletion with automatic parameter binding

```python
# Update with conditions
update = (
    UpdateBuilder()
    .table("users")
    .set("last_login", datetime.now())
    .set("login_count", "login_count + 1")  # Can use SQL expressions
    .where_eq("email", "user@example.com")
    .where("active = true")
)

# Conditional delete
delete = (
    DeleteBuilder()
    .from_("log_entries")
    .where_between("created_at", start_date, end_date)
    .where_not_in("level", ["ERROR", "CRITICAL"])
)
```

**AUTOMATIC PARAMETER MANAGEMENT**:

### Parameter Binding Strategy

- **Named Parameters**: Builders use dictionary-based parameter storage internally
- **Collision Avoidance**: Auto-generated parameter names (`param_1`, `param_2`, etc.)
- **Merge Logic**: Complex queries automatically merge parameters from subqueries and CTEs
- **Type Preservation**: Parameter values maintain their Python types through to execution

### Security Benefits

```python
# This is SAFE - value is automatically parameterized
user_input = "'; DROP TABLE users; --"
query = SelectBuilder().select("*").from_("users").where_eq("name", user_input)
# Results in: SELECT * FROM users WHERE name = :param_1
# With parameters: {"param_1": "'; DROP TABLE users; --"}
```

**BUILDER-TO-SQL CONVERSION FLOW**:

```python
# 1. Builder constructs SQLGlot expression tree
builder = SelectBuilder().select("name").from_("users").where_eq("active", True)

# 2. build() method produces SafeQuery
safe_query = builder.build()
# safe_query.sql = "SELECT name FROM users WHERE active = :param_1"
# safe_query.parameters = {"param_1": True}

# 3. to_statement() creates SQL object for driver
sql_obj = builder.to_statement(config=SQLConfig())

# 4. Driver protocol accepts builder directly
result = driver.execute(builder, schema_type=User)
# Driver automatically calls to_statement() and extracts parameters
```

**CONVENIENCE METHODS (WhereClauseMixin)**:

The system provides numerous convenience methods for common WHERE conditions:

```python
query = (
    SelectBuilder()
    .select("*")
    .from_("products")
    .where_eq("category", "electronics")        # column = value
    .where_between("price", 100, 500)          # column BETWEEN low AND high
    .where_like("name", "%phone%")             # column LIKE pattern
    .where_in("brand", ["Apple", "Samsung"])   # column IN (values)
    .where_is_not_null("description")          # column IS NOT NULL
    .where_exists(subquery)                    # EXISTS (subquery)
)
```

**INTEGRATION WITH DRIVER EXECUTE METHODS**:

### Direct Builder Execution

```python
# SelectBuilder with driver.execute()
users = await driver.execute(
    SelectBuilder()
    .select("id", "name", "email")
    .from_("users")
    .where_eq("active", True),
    schema_type=User  # Type-safe result conversion
)
# Returns: SelectResult[User]

# InsertBuilder with driver.execute()
result = await driver.execute(
    InsertBuilder()
    .into("users")
    .values_from_dict({"name": "New User", "email": "new@example.com"})
)
# Returns: ExecuteResult[Any]

# No need to pass parameters separately - they're embedded in the builder
```

### Builder with execute_many()

```python
# For batch operations, builders work with parameter sequences
insert_builder = (
    InsertBuilder()
    .into("logs")
    .columns("message", "level", "timestamp")
)

# Execute with multiple parameter sets
result = driver.execute_many(
    insert_builder,
    parameters=[
        {"message": "Info log", "level": "INFO", "timestamp": datetime.now()},
        {"message": "Error log", "level": "ERROR", "timestamp": datetime.now()},
    ]
)
```

**KEY POINTS FOR DOCS**:

- **No Manual Parameter Handling**: Users never need to extract or manage parameters when using builders
- **Type Safety**: Generic result types ensure compile-time validation of operations
- **SQL Injection Prevention**: Automatic parameterization makes SQL injection impossible
- **Fluent Interface**: Method chaining creates readable, maintainable query construction
- **Database Portability**: Single builder API works across PostgreSQL, MySQL, SQLite, BigQuery, etc.
- **Integration Ready**: Builders work directly with all three driver protocol methods (`execute`, `execute_many`, `execute_script`)

---

## [REF-009] SQL Factory: Unified Builder and Expression API

**DECISION**: Single `sql` factory object providing convenient access to all builders and SQL expressions.

**IMPLEMENTATION**:

### Unified Factory Design

- **Single Import**: `from sqlspec import sql` provides access to everything
- **Builder Creation**: `sql.select()`, `sql.insert()`, `sql.update()`, `sql.delete()`, `sql.merge()`
- **Column References**: Dynamic attribute access for columns (e.g., `sql.id`, `sql.name`, `sql.email`)
- **SQL Functions**: Built-in aggregate, string, math, and conversion functions
- **Raw SQL Parsing**: Intelligent detection and parsing of raw SQL strings into builders

### Core Design Philosophy

- **Convenience First**: Single import gives access to all SQL building capabilities
- **Flexible Input**: Accept both traditional builder patterns and raw SQL strings
- **Expression Rich**: Comprehensive set of SQL functions and operators
- **Type Safety**: Maintain strong typing throughout the expression system

**USER BENEFIT**:

- **Developer Experience**: Single `sql` import reduces cognitive load and import complexity
- **Flexibility**: Seamlessly mix raw SQL strings with programmatic builder calls
- **Completeness**: Rich expression API covers most SQL use cases without complex imports
- **Learning Curve**: Intuitive API that mirrors SQL structure and common patterns

**SQL FACTORY USAGE PATTERNS**:

### Traditional Builder Creation

```python
from sqlspec import sql

# Clean, fluent builder creation
query = (
    sql.select(sql.id, sql.name, sql.email)  # Column references via sql.column_name
    .from_("users")
    .where_eq("active", True)
    .order_by("created_at DESC")
    .limit(50)
)

# DML operations
insert = (
    sql.insert("users")
    .columns("name", "email", "department")
    .values("Alice Smith", "alice@company.com", "engineering")
)

update = (
    sql.update("users")
    .set("last_login", sql.now())  # SQL function calls
    .where_eq("id", user_id)
)
```

### Raw SQL Integration

```python
# Parse raw SQL into builders for modification
query = sql.select("SELECT id, name FROM users WHERE active = 1")
enhanced_query = (
    query
    .where_between("created_at", start_date, end_date)  # Add more conditions
    .order_by("name")
    .limit(100)
)

# Mix raw SQL with builder methods
complex_query = (
    sql.select("u.*, p.title")
    .from_("users u")
    .inner_join("profiles p", on="u.id = p.user_id")
    .where("u.department IN ('engineering', 'product')")  # Raw SQL condition
    .where_eq("u.active", True)  # Builder method
)
```

### Dynamic Column References

```python
# Columns accessible as attributes - no string quotes needed
query = (
    sql.select(
        sql.users.id,           # Table-qualified columns
        sql.users.name,
        sql.profiles.title
    )
    .from_("users")
    .inner_join("profiles", on=sql.users.id == sql.profiles.user_id)  # Expression comparison
    .where(sql.users.active == True)
)
```

### Rich Expression API

```python
# Aggregate functions
query = (
    sql.select(
        sql.department,
        sql.count().alias("employee_count"),           # COUNT(*)
        sql.avg(sql.salary).alias("avg_salary"),       # AVG(salary)
        sql.max(sql.created_at).alias("newest_hire")   # MAX(created_at)
    )
    .from_("employees")
    .group_by(sql.department)
    .having(sql.count() > 5)
)

# String and math functions
query = (
    sql.select(
        sql.upper(sql.name).alias("name_upper"),       # UPPER(name)
        sql.concat(sql.first_name, " ", sql.last_name).alias("full_name"),  # CONCAT
        sql.round(sql.salary / 12, 2).alias("monthly_salary")  # ROUND(salary/12, 2)
    )
    .from_("employees")
)
```

### Advanced SQL Functions

```python
# CASE expressions
salary_category = (
    sql.case()
    .when(sql.salary < 50000, "Junior")
    .when(sql.salary < 100000, "Mid-level")
    .when(sql.salary < 150000, "Senior")
    .else_("Executive")
    .end()
)

query = sql.select(sql.name, salary_category.alias("level")).from_("employees")

# Window functions
query = (
    sql.select(
        sql.name,
        sql.salary,
        sql.row_number(
            partition_by=sql.department,
            order_by=sql.salary.desc()
        ).alias("salary_rank")
    )
    .from_("employees")
)

# JSON operations (database-specific)
query = (
    sql.select(
        sql.id,
        sql.json_extract(sql.metadata, "$.tags").alias("tags"),
        sql.json_value(sql.profile, "$.preferences.theme").alias("theme")
    )
    .from_("users")
)
```

### Type Conversion and NULL Handling

```python
query = (
    sql.select(
        sql.id,
        sql.cast(sql.created_at, "DATE").alias("creation_date"),        # CAST conversion
        sql.coalesce(sql.nickname, sql.first_name).alias("display_name"), # NULL handling
        sql.nvl(sql.phone, "No phone provided").alias("contact_phone")    # Oracle-style NVL
    )
    .from_("users")
)
```

**RAW SQL PARSING INTELLIGENCE**:

### Automatic Statement Detection

```python
# Factory automatically detects SQL type and creates appropriate builder
select_builder = sql.select("SELECT * FROM users WHERE active = 1")
insert_builder = sql.insert("INSERT INTO logs (message) VALUES ('System started')")
update_builder = sql.update("UPDATE users SET last_login = NOW() WHERE id = 1")

# Can then enhance with additional builder methods
enhanced = (
    select_builder
    .where_between("created_at", start_date, end_date)
    .order_by("name")
    .limit(50)
)
```

### RETURNING Clause Detection

```python
# Factory detects RETURNING clauses for proper result type handling
returning_insert = sql.insert(
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, created_at"
)
# When executed, will return SelectResult instead of ExecuteResult
result = driver.execute(returning_insert)  # SelectResult[dict[str, Any]]
```

**INTEGRATION WITH DRIVER PROTOCOL**:

### Seamless Builder Integration

```python
# sql factory builders work directly with all driver methods
async with sqlspec.provide_session(MyDatabaseConfig) as driver:
    # Traditional builder
    users = await driver.execute(
        sql.select(sql.id, sql.name, sql.email)
        .from_("users")
        .where_eq("active", True),
        schema_type=User
    )

    # Raw SQL enhanced with builder
    complex_result = await driver.execute(
        sql.select("SELECT u.*, COUNT(o.id) as order_count FROM users u")
        .left_join("orders o", on="u.id = o.user_id")
        .where_between("u.created_at", start_date, end_date)
        .group_by("u.id")
        .having("COUNT(o.id) > 0"),
        schema_type=UserWithOrders
    )

    # Batch operations
    await driver.execute_many(
        sql.insert("audit_log").columns("action", "user_id", "timestamp"),
        [
            {"action": "login", "user_id": 1, "timestamp": datetime.now()},
            {"action": "logout", "user_id": 1, "timestamp": datetime.now()},
        ]
    )
```

### Expression Composition

```python
# Build complex expressions and reuse them
active_users_filter = sql.active == True
recent_filter = sql.created_at > (datetime.now() - timedelta(days=30))

# Compose into queries
new_active_users = (
    sql.select(sql.id, sql.name, sql.email)
    .from_("users")
    .where(active_users_filter)
    .where(recent_filter)
)

# Reuse expressions across different queries
user_count = (
    sql.select(sql.count().alias("total"))
    .from_("users")
    .where(active_users_filter)
    .where(recent_filter)
)
```

**KEY POINTS FOR DOCS**:

- **Single Import Philosophy**: `from sqlspec import sql` gives access to all SQL building capabilities
- **Raw SQL Integration**: Seamlessly parse and enhance existing SQL strings with builder methods
- **Column Attribute Access**: `sql.column_name` provides clean, IDE-friendly column references
- **Rich Expression Library**: Comprehensive functions covering aggregates, strings, math, JSON, and more
- **Type Safety Preservation**: Factory maintains strong typing throughout expression and builder systems
- **Database Agnostic**: Same API works across PostgreSQL, MySQL, SQLite, BigQuery, and other supported databases
- **Builder Enhancement**: Raw SQL can be parsed into builders and then enhanced with additional conditions, joins, etc.
- **Zero Import Complexity**: Reduces cognitive load by providing everything through a single, intuitive interface

---

## [REF-010] AioSQL Integration: File-Based SQL with Full SQLSpec Power

**DECISION**: Complete integration with AioSQL enabling file-based SQL organization while preserving all SQLSpec capabilities.

**IMPLEMENTATION**:

### Integration Architecture

- **Singleton Caching**: `AiosqlLoader` parses SQL files once and caches forever using metaclass
- **Typed Query Objects**: `AiosqlQuery` wraps SQL with type annotations and builder API support
- **Adapter Bridge**: `AiosqlSyncAdapter` and `AiosqlAsyncAdapter` implement aiosql protocol with SQLSpec drivers
- **Service Layer**: `AiosqlService` provides high-level abstractions and advanced configuration
- **Builder API Magic**: Loaded queries support full SQLSpec builder patterns (.where(), .limit(), .order_by())
- **Filter Integration**: Special `_sqlspec_filters` parameter for dynamic filter application

### Core Design Principles

- **Best of Both Worlds**: Combine aiosql's file organization with SQLSpec's power
- **Zero Compromise**: Full compatibility with entire SQLSpec ecosystem
- **Performance First**: Singleton caching and optimized query execution
- **Type Safety**: Full type annotation support with return type inference

**USER BENEFIT**:

- **File Organization**: Organize SQL in files using familiar aiosql conventions
- **Performance**: Singleton caching eliminates re-parsing overhead
- **Builder Integration**: Use SQLSpec builder API on file-loaded queries seamlessly
- **Full Ecosystem**: Access to all SQLSpec features (filters, instrumentation, validation)
- **Migration Path**: Easy migration from existing aiosql setups to SQLSpec power
- **Developer Experience**: Type-safe queries with IDE support and autocompletion

**AIOSQL INTEGRATION PATTERNS**:

### File-Based Query Organization

```sql
# queries.sql - Standard aiosql format
-- name: get_all_users^
SELECT
    id, name, email, department, age, salary, hire_date, active,
    ARRAY['SQL', 'Python', 'Analytics'] as skills
FROM users
WHERE active = TRUE
ORDER BY hire_date DESC

-- name: get_user_by_id$
SELECT * FROM users WHERE id = :user_id AND active = TRUE

-- name: create_user<!
INSERT INTO users (name, email, department, age, salary, hire_date, active)
VALUES (:name, :email, :department, :age, :salary, :hire_date, :active)
RETURNING id, name, email, department

-- name: search_high_performers^
SELECT * FROM users
WHERE salary > :min_salary
  AND active = TRUE
  AND (name ILIKE '%' || :search_term || '%'
       OR email ILIKE '%' || :search_term || '%')
```

### Singleton Caching Power

```python
from sqlspec.extensions.aiosql import AiosqlLoader

# First load - parses file and caches
loader1 = AiosqlLoader("queries.sql", dialect="postgresql")

# Second load - instant retrieval from cache
loader2 = AiosqlLoader("queries.sql", dialect="postgresql")

# loader1 is loader2 -> True (same instance!)
print(f"Cached loading is {first_load_time / cached_load_time:.1f}x faster!")
```

### Typed Query Objects with Builder API Magic

```python
from sqlspec.extensions.aiosql import AiosqlLoader
from sqlspec import sql

# Load with type annotations
loader = AiosqlLoader("queries.sql", dialect="postgresql")
get_users = loader.get_query("get_all_users", return_type=User)

# Use as-is
result = driver.execute(get_users, schema_type=User)

# OR enhance with builder API magic
enhanced_query = (
    get_users
    .where("salary > 75000")  # Add dynamic conditions
    .where_eq("department", "Engineering")  # Builder method chaining
    .order_by("salary DESC", "hire_date ASC")  # Complex ordering
    .limit(10)  # Pagination
)

# Execute enhanced query
top_engineers = driver.execute(enhanced_query, schema_type=User)
```

### Advanced Filter Integration

```python
from sqlspec.statement.filters import SearchFilter, LimitOffsetFilter

# Traditional aiosql execution with SQLSpec filters
loader = AiosqlLoader("queries.sql", dialect="postgresql")

with driver.provide_connection() as conn:
    # Special _sqlspec_filters parameter
    result = queries.get_all_users(
        conn,
        department="Engineering",
        _sqlspec_filters=[
            SearchFilter("name", "John"),      # Text search
            LimitOffsetFilter(10, 0),          # Pagination
        ]
    )
```

### Service Layer for Advanced Workflows

```python
from sqlspec.extensions.aiosql import AiosqlService
from sqlspec.statement.filters import LimitOffsetFilter

# Create service with default configuration
service = AiosqlService(
    driver,
    default_filters=[LimitOffsetFilter(limit=100, offset=0)],  # Auto-pagination
    allow_sqlspec_filters=True  # Enable _sqlspec_filters
)

# Load queries through service (with caching)
queries = service.load_queries("user_queries.sql")

# Execute with service enhancements
result = service.execute_query_with_filters(
    queries.get_users_by_department,
    connection=None,
    parameters={"department": "Engineering"},
    filters=[SearchFilter("email", "@company.com")],
    schema_type=User
)
```

**ADAPTER PATTERN BRIDGE**:

### Native AioSQL Protocol Support

```python
import aiosql
from sqlspec.extensions.aiosql import AiosqlSyncAdapter, AiosqlAsyncAdapter

# Create SQLSpec driver
config = PsycopgAsyncConfig(pool_config={"host": "localhost", "dbname": "myapp"})
driver = config.create_driver()

# Bridge to aiosql
adapter = AiosqlAsyncAdapter(
    driver,
    default_filters=[LimitOffsetFilter(limit=1000)],  # Default pagination
    allow_sqlspec_filters=True  # Enable advanced filtering
)

# Use with standard aiosql
queries = aiosql.from_path("queries.sql", adapter)

# Now all queries have SQLSpec power under the hood!
users = await queries.get_all_users(
    conn,
    department="Engineering",
    _sqlspec_filters=[SearchFilter("name", "John")]  # SQLSpec magic!
)
```

### Seamless Migration from AioSQL

```python
# Before: Standard aiosql
import aiosql
import psycopg2

queries = aiosql.from_path("queries.sql", "psycopg2")
result = queries.get_users(conn, department="Engineering")

# After: AioSQL + SQLSpec (drop-in replacement!)
from sqlspec.extensions.aiosql import AiosqlSyncAdapter
from sqlspec.adapters.psycopg import PsycopgSyncConfig

driver = PsycopgSyncConfig(...).create_driver()
adapter = AiosqlSyncAdapter(driver)
queries = aiosql.from_path("queries.sql", adapter)

# Same interface, but now with SQLSpec power!
result = queries.get_users(
    conn,
    department="Engineering",
    _sqlspec_filters=[LimitOffsetFilter(50, 0)]  # NEW: SQLSpec filters!
)
```

**COMPREHENSIVE QUERY LIFECYCLE**:

### File to Execution Flow

#### 1. File-based SQL organization (aiosql format)

The contents of `analytics.sql`:

```sql
-- name: complex_analytics^
SELECT
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MAX(hire_date) as newest_hire
FROM users
WHERE active = TRUE
GROUP BY department
HAVING COUNT(*) > :min_employees
```

```python
# 2. Singleton-cached loading
loader = AiosqlLoader("analytics.sql", dialect="postgresql")
analytics_query = loader.get_query("complex_analytics", return_type=Analytics)

# 3. Builder API enhancement
enhanced_query = (
    analytics_query
    .where("department IN ('Engineering', 'Product')")  # Additional filters
    .order_by("avg_salary DESC")  # Dynamic ordering
    .limit(5)  # Top 5 departments
)

# 4. Advanced filter application
filters = [
    SearchFilter("department", "eng"),  # Text search in department
    LimitOffsetFilter(3, 0),           # Override limit for pagination
]

# 5. Type-safe execution with full ecosystem
result = await driver.execute(
    enhanced_query,
    parameters={"min_employees": 5},
    filters=filters,
    schema_type=Analytics  # Type-safe result conversion
)
```

### Real-World Complex Scenario

```python
# Enterprise data pipeline with file-based queries
from sqlspec.extensions.aiosql import AiosqlLoader, AiosqlService
from sqlspec.statement.filters import SearchFilter, LimitOffsetFilter
from datetime import datetime, timedelta

# Load different query collections
user_loader = AiosqlLoader("user_queries.sql", dialect="postgresql")
analytics_loader = AiosqlLoader("analytics_queries.sql", dialect="postgresql")
reporting_loader = AiosqlLoader("reporting_queries.sql", dialect="postgresql")

# Create service for advanced workflows
service = AiosqlService(
    driver,
    default_filters=[LimitOffsetFilter(limit=1000, offset=0)],
    allow_sqlspec_filters=True
)

# Complex data pipeline
async def generate_department_report(department: str, start_date: datetime):
    # 1. Get users with builder enhancement
    users_query = user_loader.get_query("get_users_by_department", return_type=User)
    users = await driver.execute(
        users_query
        .where("hire_date >= :start_date")
        .where_eq("active", True)
        .order_by("salary DESC"),
        parameters={"department": department, "start_date": start_date},
        schema_type=User
    )

    # 2. Analytics with filters
    analytics_query = analytics_loader.get_query("department_performance", return_type=Analytics)
    performance = await driver.execute(
        analytics_query,
        parameters={"department": department, "period_days": 90},
        filters=[LimitOffsetFilter(10, 0)],
        schema_type=Analytics
    )

    # 3. Complex reporting with service
    reports = service.load_queries("complex_reports.sql")
    revenue_data = await service.execute_query_with_filters(
        reports.department_revenue_analysis,
        connection=None,
        parameters={"department": department},
        filters=[SearchFilter("status", "completed")],
        schema_type=RevenueReport
    )

    return {
        "users": users.rows,
        "performance": performance.rows,
        "revenue": revenue_data
    }
```

**PERFORMANCE CHARACTERISTICS**:

### Optimization Features

```python
# Singleton caching - parse once, use forever
loader1 = AiosqlLoader("huge_file.sql")  # ~100ms first time
loader2 = AiosqlLoader("huge_file.sql")  # ~0.1ms cached (1000x faster!)

# Memory efficiency - shared instances
assert loader1 is loader2  # Same object in memory

# Lazy query enhancement
query = loader.get_query("complex_query")
# Enhancement only happens when executed
enhanced = query.where("active = true").limit(10)  # Fast transformation
```

### Production Metrics

```python
# Performance showcase from real usage
metrics = {
    "File Parsing": "Once per file",          # Singleton eliminates re-parsing
    "Query Loading": "< 1ms (cached)",        # Lightning-fast retrieval
    "Filter Application": "< 0.1ms",          # Efficient SQL transformation
    "Memory Usage": "Minimal",                # Shared cached queries
    "Type Safety": "100%",                    # Full validation support
    "Builder Integration": "Seamless",        # Zero overhead enhancement
}
```

**ECOSYSTEM INTEGRATION SHOWCASE**:

### All SQLSpec Features Work

```python
# File-based queries + Full SQLSpec ecosystem
loader = AiosqlLoader("enterprise_queries.sql", dialect="postgresql")

# 1. Builder API integration
query = (
    loader.get_query("user_analytics")
    .where(sql.salary > 75000)        # Expression trees
    .where_between("age", 25, 45)     # Convenience methods
    .order_by("department", "salary DESC")  # Complex ordering
    .limit(100)                       # Pagination
)

# 2. Advanced filters
filters = [
    SearchFilter("name", "john"),           # Text search
    LimitOffsetFilter(50, 0),              # Pagination
    # CustomFilter(),                     # User-defined filters
]

# 3. Type-safe execution with validation
result = await driver.execute(
    query,
    parameters={"department": "Engineering"},
    filters=filters,
    schema_type=User,  # Pydantic/msgspec validation
    # All driver protocol features work!
)

# 4. Instrumentation and monitoring (automatic)
# - OpenTelemetry tracing
# - Prometheus metrics
# - Query performance tracking
# - Error reporting
```

**KEY POINTS FOR DOCS**:

- **Zero Migration Cost**: Drop-in replacement for existing aiosql setups
- **Performance Revolution**: Singleton caching provides massive speedups
- **Builder API Magic**: Enhance file-loaded queries with SQLSpec builder patterns
- **Filter Ecosystem**: Use powerful SQLSpec filters through `_sqlspec_filters` parameter
- **Type Safety**: Full type annotation support with return type inference
- **Service Abstractions**: High-level service for complex workflows and default configurations
- **Comprehensive Integration**: Works with ALL SQLSpec features (drivers, validation, instrumentation)
- **File Organization**: Maintain clean separation of SQL logic in organized files
- **Developer Experience**: Best-in-class IDE support, autocompletion, and error detection

---

## [REF-011] SQLStatement & StatementPipeline: Unified SQL Processing

**DECISION**: Implement a cohesive SQL processing system where `SQLStatement` (`sqlspec.statement.sql.SQL`) is the central immutable object representing a query and its state. Processing (transformation, validation, analysis) is delegated to a `StatementPipeline` (`sqlspec.statement.pipelines.StatementPipeline`) which operates on a shared `SQLProcessingContext`.

**ARCHITECTURE OVERVIEW**:

1. **`SQL` Object (The "What")**:
    - Represents a specific SQL statement, its parameters, dialect, and configuration (`SQLConfig`).
    - Immutable: Methods like `.where()`, `.limit()`, `.transform()`, `.copy()` return *new* `SQL` instances.
    - Upon instantiation (`__init__`), it prepares an `SQLProcessingContext`.
    - It then invokes the `StatementPipeline` to process this context.
    - Finally, it populates its internal state (parsed expression, validation results, analysis results, final parameters) from the `StatementPipelineResult`.

2. **`SQLConfig` (The "How-To Customize")**:
    - Controls all aspects of processing: parsing, transformation, validation, analysis enablement.
    - Defines which processor components (transformers, validators, analyzers) are part of the pipeline, allowing for distinct lists for each stage.
    - Includes a flag `input_sql_had_placeholders`, determined by `SQL.__init__`, to inform transformers like `ParameterizeLiterals`.

3. **`SQLProcessingContext` (The "Shared Workspace")**:
    - A dataclass (`sqlspec.statement.pipelines.context.SQLProcessingContext`) passed through the pipeline stages.
    - Holds mutable state during a single pipeline run:
        - `initial_sql_string`, `dialect`, `config` (from `SQL` object).
        - `initial_parameters`, `initial_kwargs`, `merged_parameters`, `parameter_info` (from `SQL` object's parameter processing).
        - `current_expression`: The `sqlglot.exp.Expression`, potentially modified by transformers.
        - `extracted_parameters_from_pipeline`: Parameters extracted by transformers.
        - `validation_result: Optional[ValidationResult]`: Populated by the validation stage.
        - `analysis_result: Optional[StatementAnalysis]`: Populated by the analysis stage.
        - `input_sql_had_placeholders`: Copied from `SQLConfig`.
        - `statement_type`: (Future) Could be populated by an early analysis step.

4. **`StatementPipeline` (The "Orchestrator")**:
    - Defined in `sqlspec.statement.pipelines.base.StatementPipeline`.
    - Its `execute_pipeline(context: SQLProcessingContext)` method orchestrates the stages:
        - **Parsing (Implicit/Initial)**: Ensures `context.current_expression` is populated from `context.initial_sql_string` if not already an expression (respecting `context.config.enable_parsing`).
        - **Transformation Stage**: Iterates through configured transformers. Each transformer receives the `context`, can modify `context.current_expression` and add to `context.extracted_parameters_from_pipeline`.
        - **Validation Stage**: Iterates through configured validators. Each receives `context`, performs checks on `context.current_expression`, and contributes to an aggregated `ValidationResult` which is then stored in `context.validation_result`.
        - **Analysis Stage**: Iterates through configured analyzers. Each receives `context` (including `context.validation_result`), performs analysis on `context.current_expression`, and the primary analyzer sets `context.analysis_result`.
    - Returns a `StatementPipelineResult` dataclass containing the final state from the context.

5. **`ProcessorProtocol` (The "Component Contract")**:
    - Base protocol (`sqlspec.statement.pipelines.base.ProcessorProtocol`) for all transformers, validators, and analyzers.
    - Defines `process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]`.
        - Concrete implementations adapt this: transformers usually update `context.current_expression` and return `(context.current_expression, None)`. Validators return `(context.current_expression, ValidationResult_part)`. Analyzers update `context.analysis_result` and return `(context.current_expression, None)`.

6. **`StatementPipelineResult` (The "Outcome")**:
    - A dataclass (`sqlspec.statement.pipelines.context.StatementPipelineResult`) bundling the final outputs of a pipeline run, which the `SQL` object uses to set its state.

**USER BENEFIT & KEY DESIGN PRINCIPLES**:

- **Parse Once, Process Many Ways**: The SQL string is parsed into a `sqlglot` expression once (if parsing is enabled). This expression (or its transformed versions) is then passed through validation and analysis stages. This is efficient.
- **Clear Data Flow**: `SQLProcessingContext` makes the data available to each processing stage explicit, reducing side effects and making the pipeline easier to reason about.
- **Extensibility**: New transformers, validators, or analyzers can be created by implementing `ProcessorProtocol` and added to `SQLConfig`.
- **Configurability**: Users can precisely control each stage (enable/disable, provide custom components) via `SQLConfig`.
- **Improved Testability**: Individual processors can be tested by mocking the `SQLProcessingContext`.
- **Separation of Concerns**:
    - `SQL` object: User-facing API and final state holder.
    - `SQLConfig`: Defines processing rules.
    - `SQLProcessingContext`: Transient state during a single processing run.
    - `StatementPipeline`: Orchestrates the run.
    - Processors: Implement specific logic for transformation, validation, or analysis.
- **Robust Parameter Handling**: The system distinguishes between parameters provided initially to the `SQL` object and those extracted by transformers (e.g., `ParameterizeLiterals`), merging them correctly.
- **Informed Analysis**: The analysis stage can leverage results from the validation stage (e.g., a cartesian product validator can provide data that an analyzer then reports), promoting synergy between stages.

**EXAMPLE PIPELINE EXECUTION FLOW (Conceptual)**:

```python
# 1. User creates SQL object
# config = SQLConfig(transformers=[T1, T2], validators=[V1], analyzers=[A1])
# sql_obj = SQL("SELECT * FROM data WHERE id = 1", config=my_config)

# 2. SQL.__init__ -> SQL._initialize_statement:
#    - Creates SQLProcessingContext (ctx)
#    - ctx.initial_sql_string = "SELECT * FROM data WHERE id = 1"
#    - Determines ctx.input_sql_had_placeholders = False
#    - Processes initial parameters (none here) -> ctx.merged_parameters = []
#    - Parses SQL -> ctx.current_expression = sqlglot.parse_one(...)
#    - Calls pipeline = self.config.get_statement_pipeline()
#    - pipeline_result = pipeline.execute_pipeline(ctx)

# 3. StatementPipeline.execute_pipeline(ctx):
#    - Stage 0: Parsing (already done by SQL._initialize_statement, or done here if ctx.current_expression is None)
#    - Stage 1: Transformers
#        - T1.process(ctx) -> updates ctx.current_expression, maybe ctx.extracted_parameters_from_pipeline
#        - T2.process(ctx) -> updates ctx.current_expression, maybe ctx.extracted_parameters_from_pipeline
#    - Stage 2: Validators
#        - V1.process(ctx) -> returns (ctx.current_expression, v1_result). Pipeline aggregates into ctx.validation_result.
#    - Stage 3: Analyzers
#        - A1.process(ctx) -> updates ctx.analysis_result.
#    - Returns StatementPipelineResult (with final ctx.current_expression, ctx.validation_result, etc.)

# 4. SQL._initialize_statement (continues):
#    - self._parsed_expression = pipeline_result.final_expression
#    - self._validation_result = pipeline_result.validation_result
#    - self._analysis_result = pipeline_result.analysis_result
#    - self._merge_extracted_parameters(ctx.extracted_parameters_from_pipeline)
#    - self._check_and_raise_for_strict_mode()

# 5. User can now access results:
#    print(sql_obj.sql) # Potentially transformed SQL
#    print(sql_obj.parameters) # Final merged parameters
#    print(sql_obj.validation_result)
#    print(sql_obj.analysis_result)
```

**KEY POINTS FOR DOCS**:

- Emphasize the "Parse Once, Process Many Ways" philosophy.
- Explain the roles of `SQL`, `SQLConfig`, `SQLProcessingContext`, `StatementPipeline`, and `ProcessorProtocol`.
- Highlight how `SQLConfig` allows fine-grained control over the pipeline.
- Detail how information (like `input_sql_had_placeholders` or `validation_result`) flows via the `SQLProcessingContext` to inform later stages.
- Show how to create and plug in custom processors.
- Explain the benefits for security (e.g., `ParameterizeLiterals` informed by context), performance (cached parsing, efficient data flow), and extensibility.

---

## [REF-012] Deprecated - Unified Pipeline Architecture (Old)

This section is now superseded by [REF-011] which details the `SQLProcessingContext` and staged `StatementPipeline` approach. The `UnifiedProcessor` concept, while aiming for similar goals, has been refined into the more explicit staged pipeline managed by `StatementPipeline` and orchestrated by the `SQL` object through `SQLProcessingContext`.

**REASON FOR DEPRECATION**: The new model with `SQLProcessingContext` offers a clearer and more flexible way to manage state and data flow between distinct processing stages (transform, validate, analyze) compared to a single `UnifiedProcessor` trying to manage all interactions internally. The staged approach also makes the "process once" concept for each *type* of operation more explicit.

---

## 📝 TEMPLATE FOR NEW ENTRIES

Copy this template when adding new reference sections:

```markdown
## [REF-XXX] Title: Brief Description
