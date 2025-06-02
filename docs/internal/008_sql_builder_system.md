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
