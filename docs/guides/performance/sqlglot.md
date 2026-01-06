---
orphan: true
---

# SQLglot Guide for SQLSpec

Comprehensive guide for SQL parsing, transformation, and optimization using SQLglot in SQLSpec.

## Core Principles

### Single-Pass Processing (CRITICAL)

**The golden rule**: Parse once → transform once → validate once. The SQL object is the single source of truth.

```python
# ✅ GOOD: Single-pass processing
sql = SQL("SELECT * FROM users WHERE id = :id")
result = driver.execute(sql)  # Parse happens once internally

# ❌ BAD: Multiple passes
sql_str = "SELECT * FROM users"
parsed = sqlglot.parse_one(sql_str)  # Parse 1
sql_str = parsed.sql()  # Back to string
parsed_again = sqlglot.parse_one(sql_str)  # Parse 2 - WASTEFUL!
```

**Why this matters**:

- SQL parsing is expensive (10-100µs per query)
- String conversions lose AST benefits
- Multiple passes compound memory allocation overhead
- Cached expressions become useless after string conversion

### Performance First

```python
# ✅ GOOD: Cache and reuse expressions
CACHED_EXPR = sqlglot.parse_one("user_id = :id")

def build_query(table: str) -> str:
    # Reuse cached expression
    select = sqlglot.select("*").from_(table).where(CACHED_EXPR)
    return select.sql()

# ❌ BAD: Re-parse every time
def build_query_slow(table: str) -> str:
    # Parsing same expression repeatedly
    where = sqlglot.parse_one("user_id = :id")
    select = sqlglot.select("*").from_(table).where(where)
    return select.sql()
```

**Performance comparison**:

- Parse once, reuse: ~1µs per query
- Re-parse every time: ~50µs per query (50x slower!)

### Transpile, Don't Re-Parse

```python
# ✅ GOOD: Direct transpilation
postgres_sql = sqlglot.transpile(
    "SELECT * FROM users LIMIT 10",
    read="sqlite",
    write="postgres"
)[0]

# ❌ BAD: Parse → modify → re-parse
sqlite_ast = sqlglot.parse_one("SELECT * FROM users LIMIT 10", read="sqlite")
postgres_str = sqlite_ast.sql(dialect="postgres")
postgres_ast = sqlglot.parse_one(postgres_str, read="postgres")  # Wasteful!
```

### Avoid Unnecessary Copies (MANDATORY)

```python
# ✅ GOOD: Mutate in-place with copy=False
predicate = sqlglot.parse_one("user_id = :id")
query = sqlglot.select("*").from_("users").where(predicate, copy=False)

# ❌ BAD: copy=True triggers deep clone of the expression tree
query = sqlglot.select("*").from_("users").where(predicate, copy=True)
```

**Why copy=False**:

- Deep copies walk the entire expression tree and allocate new nodes; this is 5-20x slower on medium queries.
- SQLSpec builders expect mutable expressions; reusing nodes with `copy=False` keeps the AST cached and avoids invalidating references.
- Only opt into `copy=True` when crossing thread boundaries or handing the tree to untrusted mutators.

## Quick Reference

### Parsing

```python
import sqlglot
from sqlglot import exp

# Parse single statement
ast = sqlglot.parse_one("SELECT * FROM users")

# Parse with dialect
ast = sqlglot.parse_one("SELECT TOP 10 * FROM users", read="tsql")

# Parse multiple statements
statements = sqlglot.parse("SELECT 1; SELECT 2;")

# Safe parsing with error handling
try:
    ast = sqlglot.parse_one("INVALID SQL", error_level="raise")
except sqlglot.ParseError as e:
    print(f"Parse error: {e}")
```

### Transpilation

```python
# SQLite → PostgreSQL
postgres_sql = sqlglot.transpile(
    "SELECT * FROM users LIMIT 10",
    read="sqlite",
    write="postgres"
)[0]
# Result: SELECT * FROM users LIMIT 10

# MySQL → DuckDB (date functions)
duckdb_sql = sqlglot.transpile(
    "SELECT DATE_FORMAT(created_at, '%Y-%m-%d') FROM users",
    read="mysql",
    write="duckdb"
)[0]
# Result: SELECT STRFTIME(created_at, '%Y-%m-%d') FROM users

# Preserve formatting
formatted = sqlglot.transpile(
    "select*from users",
    pretty=True
)[0]
# Result: SELECT * FROM users (formatted)
```

### Building Queries

```python
from sqlglot import select, condition

# Basic SELECT
query = select("id", "name").from_("users")
# SELECT id, name FROM users

# With WHERE
query = select("*").from_("users").where("active = TRUE")
# SELECT * FROM users WHERE active = TRUE

# Complex conditions
query = (
    select("*")
    .from_("users")
    .where(
        condition("age > 18")
        .and_("country = 'US'")
        .or_("premium = TRUE")
    )
)
# SELECT * FROM users WHERE (age > 18 AND country = 'US') OR premium = TRUE

# JOINs
query = (
    select("u.id", "u.name", "o.total")
    .from_("users u")
    .join("orders o", on="u.id = o.user_id")
)
# SELECT u.id, u.name, o.total FROM users AS u JOIN orders AS o ON u.id = o.user_id

# Aggregation
query = (
    select("country", "COUNT(*) as count")
    .from_("users")
    .group_by("country")
    .having("COUNT(*) > 100")
)
# SELECT country, COUNT(*) AS count FROM users GROUP BY country HAVING COUNT(*) > 100
```

### AST Manipulation

```python
from sqlglot import exp, parse_one

# Find all table references
ast = parse_one("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
tables = [table.name for table in ast.find_all(exp.Table)]
# ['users', 'orders']

# Replace table names
ast = parse_one("SELECT * FROM old_table")
for table in ast.find_all(exp.Table):
    if table.name == "old_table":
        table.set("this", exp.to_identifier("new_table"))
print(ast.sql())
# SELECT * FROM new_table

# Add WHERE clause
ast = parse_one("SELECT * FROM users")
ast = ast.where("active = TRUE")
print(ast.sql())
# SELECT * FROM users WHERE active = TRUE

# Modify column list
ast = parse_one("SELECT id FROM users")
ast.args["expressions"].append(exp.column("name"))
print(ast.sql())
# SELECT id, name FROM users
```

### Expression Types

```python
from sqlglot import exp

# Column reference
col = exp.column("user_id", table="users")
# users.user_id

# Literal values
exp.Literal.string("hello")  # 'hello'
exp.Literal.number(42)       # 42
exp.Literal.number(3.14)     # 3.14

# NULL
exp.null()  # NULL

# Boolean operators
exp.and_(
    exp.column("age").gt(18),
    exp.column("active").eq(True)
)
# age > 18 AND active = TRUE

# Functions
exp.func("COUNT", "*")  # COUNT(*)
exp.func("UPPER", exp.column("name"))  # UPPER(name)
exp.func("COALESCE", exp.column("email"), exp.Literal.string(""))
# COALESCE(email, '')
```

## SQLSpec Integration

### Current Pipeline (via SQLProcessor)

SQLSpec uses `SQLProcessor` for single-pass compilation:

```python
from sqlspec.core import SQLProcessor, StatementConfig

processor = SQLProcessor(StatementConfig(dialect="postgres"))
result = processor.compile(raw_sql, parameters)

final_sql = result.compiled_sql
expression = result.expression
```

**Key benefits**:

- Parse once and reuse cached expressions
- Apply AST transforms without re-parsing
- Convert to SQL once at the end

### Parameter Style Conversion

SQLSpec supports multiple parameter styles:

```python
from sqlspec.parameters import convert_params

# Named → Positional ($1, $2)
sql, params = convert_params(
    "SELECT * FROM users WHERE id = :id AND role = :role",
    {"id": 123, "role": "admin"},
    style="postgres"  # Uses $1, $2
)
# sql: SELECT * FROM users WHERE id = $1 AND role = $2
# params: [123, 'admin']

# Named → qmark (?)
sql, params = convert_params(
    "SELECT * FROM users WHERE id = :id",
    {"id": 123},
    style="qmark"
)
# sql: SELECT * FROM users WHERE id = ?
# params: [123]

# Numeric → format (%s)
sql, params = convert_params(
    "SELECT * FROM users WHERE id = $1 AND role = $2",
    [123, "admin"],
    style="format"
)
# sql: SELECT * FROM users WHERE id = %s AND role = %s
# params: [123, 'admin']
```

### Dialect Handling

```python
from sqlspec.driver import AsyncDriver

class PostgresDriver(AsyncDriver):
    dialect = "postgres"

    async def execute(self, statement: SQL) -> SQLResult:
        # SQL object already transformed for postgres dialect
        # statement.sql is cached and ready to use
        cursor = await self.connection.execute(statement.sql, statement.params)
        return SQLResult(data=await cursor.fetchall())
```

## Security Patterns

### SQL Injection Prevention

```python
from sqlglot import parse_one, exp

def sanitize_table_name(user_input: str) -> str:
    """Validate table name is safe identifier."""
    try:
        # Parse as identifier
        parsed = parse_one(f"SELECT * FROM {user_input}")
        table = parsed.find(exp.Table)

        if not table:
            raise ValueError("No table found")

        # Ensure it's a simple identifier (no subqueries, etc.)
        if not isinstance(table.this, exp.Identifier):
            raise ValueError("Complex table expression not allowed")

        return table.name
    except Exception:
        raise ValueError(f"Invalid table name: {user_input}")

# ✅ SAFE
sanitize_table_name("users")  # 'users'
sanitize_table_name("public.users")  # 'users' (schema.table)

# ❌ BLOCKED
sanitize_table_name("users; DROP TABLE users")  # ValueError
sanitize_table_name("(SELECT * FROM secrets)")  # ValueError
```

### Dangerous Pattern Detection

```python
from sqlglot import parse_one, exp

def has_dangerous_patterns(sql: str) -> bool:
    """Check for dangerous SQL patterns."""
    ast = parse_one(sql)

    # Block multiple statements
    if ";" in sql and len(sqlglot.parse(sql)) > 1:
        return True

    # Block certain commands
    dangerous_types = (
        exp.Drop,
        exp.Truncate,
        exp.Grant,
        exp.Revoke,
        exp.AlterTable,
    )
    if any(ast.find(t) for t in dangerous_types):
        return True

    # Block INTO OUTFILE
    if ast.find(exp.FileParameter):
        return True

    return False

# Usage in SQLSpec pipeline
if has_dangerous_patterns(sql):
    raise SecurityError("Dangerous SQL pattern detected")
```

### Column-Level Security

```python
from sqlglot import parse_one, exp

def mask_sensitive_columns(sql: str, sensitive_cols: set[str]) -> str:
    """Replace sensitive column values with NULL."""
    ast = parse_one(sql)

    for col in ast.find_all(exp.Column):
        if col.name.lower() in sensitive_cols:
            # Replace column with NULL
            col.replace(exp.null())

    return ast.sql()

# Example
masked = mask_sensitive_columns(
    "SELECT id, email, ssn FROM users",
    sensitive_cols={"email", "ssn"}
)
# SELECT id, NULL, NULL FROM users
```

## AST Manipulation Patterns

### Table Name Rewriting

```python
from sqlglot import parse_one, exp

def rewrite_tables(sql: str, table_map: dict[str, str]) -> str:
    """Rewrite table names according to mapping."""
    ast = parse_one(sql)

    for table in ast.find_all(exp.Table):
        old_name = table.name.lower()
        if old_name in table_map:
            table.set("this", exp.to_identifier(table_map[old_name]))

    return ast.sql()

# Example: Multi-tenant table prefixing
rewritten = rewrite_tables(
    "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    table_map={"users": "tenant_123_users", "orders": "tenant_123_orders"}
)
# SELECT * FROM tenant_123_users JOIN tenant_123_orders ON ...
```

### Adding Filters

```python
from sqlglot import parse_one, exp

def add_tenant_filter(sql: str, tenant_id: int) -> str:
    """Add tenant_id filter to all queries."""
    ast = parse_one(sql)

    # Find the main SELECT
    select = ast.find(exp.Select)
    if not select:
        return sql

    # Add WHERE tenant_id = X
    tenant_filter = exp.column("tenant_id").eq(tenant_id)

    if select.args.get("where"):
        # AND with existing WHERE
        existing = select.args["where"].this
        select.where(exp.and_(existing, tenant_filter))
    else:
        # Add new WHERE
        select.where(tenant_filter)

    return ast.sql()

# Example
filtered = add_tenant_filter("SELECT * FROM users", tenant_id=123)
# SELECT * FROM users WHERE tenant_id = 123
```

### Query Analysis

```python
from sqlglot import parse_one, exp

def analyze_query(sql: str) -> dict:
    """Extract query metadata."""
    ast = parse_one(sql)

    return {
        "tables": [t.name for t in ast.find_all(exp.Table)],
        "columns": [c.name for c in ast.find_all(exp.Column)],
        "has_joins": bool(ast.find(exp.Join)),
        "has_subquery": bool(ast.find(exp.Subquery)),
        "is_aggregate": bool(ast.find(exp.AggFunc)),
        "has_where": bool(ast.find(exp.Where)),
    }

# Example
info = analyze_query("""
    SELECT u.name, COUNT(o.id) as order_count
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.active = TRUE
    GROUP BY u.name
""")
# {
#     'tables': ['users', 'orders'],
#     'columns': ['name', 'id', 'user_id', 'active'],
#     'has_joins': True,
#     'has_subquery': False,
#     'is_aggregate': True,
#     'has_where': True
# }
```

## Dialect-Specific Patterns

### PostgreSQL

```python
# Array operations
sqlglot.transpile(
    "SELECT ARRAY[1, 2, 3]",
    write="postgres"
)[0]
# SELECT ARRAY[1, 2, 3]

# JSONB operations
sqlglot.transpile(
    "SELECT data->>'name' FROM users",
    read="postgres",
    write="postgres"
)[0]
# SELECT data->>'name' FROM users

# CTEs (Common Table Expressions)
sqlglot.transpile("""
    WITH active_users AS (
        SELECT * FROM users WHERE active = TRUE
    )
    SELECT * FROM active_users
""", write="postgres")[0]
```

### MySQL

```python
# LIMIT with OFFSET
sqlglot.transpile(
    "SELECT * FROM users LIMIT 10 OFFSET 20",
    read="mysql",
    write="mysql"
)[0]

# DATE_FORMAT
sqlglot.transpile(
    "SELECT DATE_FORMAT(created_at, '%Y-%m-%d') FROM users",
    read="mysql",
    write="mysql"
)[0]
```

### DuckDB

```python
# Parquet reading
sqlglot.transpile(
    "SELECT * FROM 'data.parquet'",
    read="duckdb",
    write="duckdb"
)[0]

# List/Array operations
sqlglot.transpile(
    "SELECT unnest([1, 2, 3])",
    read="duckdb",
    write="duckdb"
)[0]
```

### Oracle

```python
# ROWNUM → LIMIT conversion
postgres = sqlglot.transpile(
    "SELECT * FROM users WHERE ROWNUM <= 10",
    read="oracle",
    write="postgres"
)[0]
# SELECT * FROM users LIMIT 10

# Dual table handling
sqlglot.transpile(
    "SELECT SYSDATE FROM DUAL",
    read="oracle",
    write="oracle"
)[0]
```

## Common Patterns

### Safe Column Extraction

```python
from sqlglot import parse_one, exp

def extract_columns(sql: str) -> list[str]:
    """Extract all column references safely."""
    try:
        ast = parse_one(sql)
        return [
            col.name
            for col in ast.find_all(exp.Column)
            if isinstance(col.this, exp.Identifier)
        ]
    except Exception:
        return []
```

### Query Type Detection

```python
from sqlglot import parse_one, exp

def get_query_type(sql: str) -> str:
    """Determine SQL statement type."""
    ast = parse_one(sql)

    if ast.find(exp.Select):
        return "SELECT"
    elif ast.find(exp.Insert):
        return "INSERT"
    elif ast.find(exp.Update):
        return "UPDATE"
    elif ast.find(exp.Delete):
        return "DELETE"
    elif ast.find(exp.Create):
        return "CREATE"
    elif ast.find(exp.Drop):
        return "DROP"
    else:
        return "UNKNOWN"
```

### Parameterized Query Builder

```python
from sqlglot import select, exp

def build_search_query(
    table: str,
    columns: list[str],
    filters: dict[str, Any]
) -> tuple[str, list[Any]]:
    """Build parameterized search query."""
    query = select(*columns).from_(table)

    params = []
    for col, val in filters.items():
        # Use placeholder
        query = query.where(f"{col} = ?")
        params.append(val)

    return query.sql(), params

# Usage
sql, params = build_search_query(
    "users",
    ["id", "name", "email"],
    {"active": True, "country": "US"}
)
# sql: SELECT id, name, email FROM users WHERE active = ? AND country = ?
# params: [True, 'US']
```

## Error Handling

### Graceful Parsing

```python
from sqlglot import parse_one, ParseError

def safe_parse(sql: str) -> Optional[exp.Expression]:
    """Parse SQL with error handling."""
    try:
        return parse_one(sql, error_level="raise")
    except ParseError as e:
        logger.error(f"Failed to parse SQL: {e}")
        return None

# Usage
ast = safe_parse(user_input)
if ast is None:
    return {"error": "Invalid SQL"}
```

### Validation

```python
from sqlglot import parse_one
from sqlglot.errors import ParseError, TokenError

def validate_sql(sql: str) -> tuple[bool, Optional[str]]:
    """Validate SQL syntax."""
    try:
        parse_one(sql, error_level="raise")
        return True, None
    except (ParseError, TokenError) as e:
        return False, str(e)

# Usage
valid, error = validate_sql("SELECT * FROM users WHERE")
if not valid:
    print(f"Invalid SQL: {error}")
```

## Performance Tips

### Caching

```python
from functools import lru_cache
from sqlglot import parse_one

@lru_cache(maxsize=1000)
def cached_parse(sql: str) -> exp.Expression:
    """Cache parsed SQL expressions."""
    return parse_one(sql)

# Subsequent calls with same SQL are instant
ast1 = cached_parse("SELECT * FROM users")  # Parse
ast2 = cached_parse("SELECT * FROM users")  # Cached
```

### Minimize .sql() Calls

```python
# ✅ GOOD: Chain transformations, call .sql() once
ast = parse_one("SELECT * FROM old_table")
ast = ast.where("active = TRUE")
for table in ast.find_all(exp.Table):
    table.set("this", exp.to_identifier("new_table"))
final_sql = ast.sql()  # Convert to SQL once

# ❌ BAD: Multiple .sql() calls
ast = parse_one("SELECT * FROM old_table")
sql1 = ast.sql()  # First conversion
ast = ast.where("active = TRUE")
sql2 = ast.sql()  # Second conversion
```

### Reuse Expression Objects

```python
# ✅ GOOD: Reuse expression objects
active_filter = exp.column("active").eq(True)

queries = [
    select("*").from_("users").where(active_filter),
    select("*").from_("orders").where(active_filter),
    select("*").from_("products").where(active_filter),
]

# ❌ BAD: Re-create identical expressions
queries = [
    select("*").from_("users").where(exp.column("active").eq(True)),
    select("*").from_("orders").where(exp.column("active").eq(True)),
    select("*").from_("products").where(exp.column("active").eq(True)),
]
```

## Testing Patterns

### Assertion Helpers

```python
import sqlglot

def assert_sql_equal(actual: str, expected: str, dialect: str = None):
    """Assert two SQL statements are semantically equal."""
    actual_ast = sqlglot.parse_one(actual, read=dialect)
    expected_ast = sqlglot.parse_one(expected, read=dialect)

    # Normalize formatting
    actual_norm = actual_ast.sql(pretty=False)
    expected_norm = expected_ast.sql(pretty=False)

    assert actual_norm == expected_norm, f"\nActual:   {actual_norm}\nExpected: {expected_norm}"

# Usage in tests
def test_query_builder():
    query = select("*").from_("users").where("active = TRUE")
    assert_sql_equal(
        query.sql(),
        "SELECT * FROM users WHERE active = TRUE"
    )
```

### Dialect Testing

```python
import pytest
from sqlglot import transpile

@pytest.mark.parametrize("source,target,input_sql,expected", [
    ("sqlite", "postgres", "SELECT * FROM users LIMIT 10", "SELECT * FROM users LIMIT 10"),
    ("mysql", "postgres", "SELECT * FROM users LIMIT 10", "SELECT * FROM users LIMIT 10"),
    ("oracle", "postgres", "SELECT * FROM users WHERE ROWNUM <= 10", "SELECT * FROM users LIMIT 10"),
])
def test_transpilation(source, target, input_sql, expected):
    result = transpile(input_sql, read=source, write=target)[0]
    assert_sql_equal(result, expected, dialect=target)
```

## Troubleshooting

### Common Issues

**Issue**: Parse error on valid SQL

```python
# Problem: Dialect mismatch
ast = parse_one("SELECT TOP 10 * FROM users")  # Error - TOP is TSQL

# Solution: Specify dialect
ast = parse_one("SELECT TOP 10 * FROM users", read="tsql")
```

**Issue**: Missing columns after transformation

```python
# Problem: Mutating shared expression
base_query = select("id", "name").from_("users")
query1 = base_query.where("active = TRUE")  # Mutates base_query!

# Solution: Copy before modifying
base_query = select("id", "name").from_("users")
query1 = base_query.copy().where("active = TRUE")  # Safe
query2 = base_query.copy().where("deleted = FALSE")  # Independent
```

**Issue**: Slow transpilation

```python
# Problem: Re-parsing instead of transpiling
for sql in queries:
    ast = parse_one(sql, read="mysql")
    postgres_str = ast.sql(dialect="postgres")
    postgres_ast = parse_one(postgres_str)  # Unnecessary!

# Solution: Transpile directly
for sql in queries:
    postgres_sql = transpile(sql, read="mysql", write="postgres")[0]
```

## Resources

- **Primary Docs Hub**: <https://sqlglot.com/sqlglot/index.html>
- **Dialects Reference**: <https://sqlglot.com/sqlglot/dialects/>
- **Expression Catalog**: <https://sqlglot.com/sqlglot/expressions/>
- **Optimizer Passes**: <https://sqlglot.com/sqlglot/optimizer/index.html>
- **Planner Guide**: <https://sqlglot.com/sqlglot/planner.html>
- **Token Reference**: <https://sqlglot.com/sqlglot/tokens.html>
- **GitHub Repository**: <https://github.com/tobymao/sqlglot>
