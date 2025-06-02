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
