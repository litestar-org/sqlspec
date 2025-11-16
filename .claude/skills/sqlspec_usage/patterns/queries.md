# Query Execution Patterns

Complete guide to executing queries with SQLSpec.

## Basic Query Methods

### execute() - Universal Query Execution

```python
# Returns SQLResult with data, metadata, and helper methods
result = session.execute("SELECT * FROM users WHERE status = ?", "active")

# Access result data
result.data              # list[dict]
result.column_names      # ["id", "name", "email", ...]
result.rows_affected     # For INSERT/UPDATE/DELETE
result.operation_type    # "SELECT", "INSERT", "UPDATE", etc.
result.sql              # Original SQL statement
```

### select_one() - Single Row (Strict)

```python
# Returns dict, raises NotFoundError if no rows
user = session.select_one("SELECT * FROM users WHERE id = ?", 123)

# Raises MultipleResultsFoundError if >1 row
# Use for queries expected to return exactly 1 row
```

### select_one_or_none() - Single Row (Lenient)

```python
# Returns dict or None
user = session.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)

if user is None:
    return {"error": "User not found"}

# Still raises MultipleResultsFoundError if >1 row
```

### select_value() - Scalar Value

```python
# Returns first column of first row
count = session.select_value("SELECT COUNT(*) FROM users")
# Returns int

name = session.select_value("SELECT name FROM users WHERE id = ?", 1)
# Returns str

# Raises NotFoundError if no rows
```

### select_value_or_none() - Scalar Value (Lenient)

```python
# Returns first column of first row, or None
max_id = session.select_value_or_none("SELECT MAX(id) FROM users WHERE status = ?", "deleted")
# Returns None if no matching rows
```

### execute_many() - Batch Operations

```python
# Efficient batch insert/update
users = [
    ("Alice", "alice@example.com"),
    ("Bob", "bob@example.com"),
    ("Charlie", "charlie@example.com"),
]

result = session.execute_many(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    users
)

# Returns SQLResult
result.rows_affected  # Total rows inserted
```

### execute_script() - Multi-Statement SQL

```python
# Execute multiple statements at once
session.execute_script("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
    );

    CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

    INSERT INTO users (name, email) VALUES ('Admin', 'admin@example.com');
""")

# No return value
# Best for schema setup, migrations
```

## Parameter Binding Styles

SQLSpec automatically converts parameter styles based on adapter configuration.

### Positional Parameters

**SQLite/DuckDB - qmark style (?):**
```python
result = session.execute(
    "SELECT * FROM users WHERE status = ? AND age > ?",
    "active", 18
)
```

**PostgreSQL (asyncpg) - numeric style ($1, $2):**
```python
result = await session.execute(
    "SELECT * FROM users WHERE status = $1 AND age > $2",
    "active", 18
)
```

**MySQL - format style (%s):**
```python
result = await session.execute(
    "SELECT * FROM users WHERE status = %s AND age > %s",
    "active", 18
)
```

### Named Parameters

**Oracle - named colon style (:name):**
```python
result = session.execute(
    "SELECT * FROM users WHERE status = :status AND age > :min_age",
    status="active", min_age=18
)

# Or with dict
params = {"status": "active", "min_age": 18}
result = session.execute(
    "SELECT * FROM users WHERE status = :status AND age > :min_age",
    params
)
```

**BigQuery - named at style (@name):**
```python
result = session.execute(
    "SELECT * FROM users WHERE created >= @start_date AND status = @status",
    start_date=date.today(), status="active"
)
```

**Psycopg - pyformat style (%(name)s):**
```python
result = session.execute(
    "SELECT * FROM users WHERE status = %(status)s AND age > %(min_age)s",
    status="active", min_age=18
)
```

### Automatic Conversion

SQLSpec handles parameter style conversion automatically:

```python
# Write once with ? placeholders
sql = "SELECT * FROM users WHERE id = ?"

# Works with any adapter - SQLSpec converts to native style
# SQLite: stays as ?
# PostgreSQL: converted to $1
# MySQL: converted to %s
# Oracle: converted to :1
```

## Working with Results

### Result Helper Methods

```python
result = session.execute("SELECT id, name, email FROM users")

# Get all rows
all_users = result.all()  # list[dict]

# Get single row
user = result.one()  # dict, raises if not exactly 1
user = result.one_or_none()  # dict | None, raises if >1

# Get scalar value
count = result.scalar()  # First column of first row
count = result.scalar_or_none()  # First column or None

# Iterate
for user in result:
    print(user["name"])

# Check if empty
if result:
    print("Has results")

# Length
num_rows = len(result)
```

### Type-Safe Schema Mapping

Map results to typed models using Pydantic, msgspec, attrs, or dataclasses:

**Pydantic:**
```python
from pydantic import BaseModel
from datetime import datetime

class User(BaseModel):
    id: int
    name: str
    email: str
    created_at: datetime
    is_active: bool

# Map single result
user: User = session.select_one(
    "SELECT * FROM users WHERE id = ?",
    123,
    schema_type=User
)

# Map multiple results
users: list[User] = session.execute(
    "SELECT * FROM users WHERE is_active = ?",
    True,
    schema_type=User
).all()
```

**msgspec (High Performance):**
```python
import msgspec

class User(msgspec.Struct):
    id: int
    name: str
    email: str
    is_active: bool = True  # Default value

users: list[User] = session.execute(
    "SELECT * FROM users",
    schema_type=User
).all()
```

**attrs:**
```python
import attrs

@attrs.define
class User:
    id: int
    name: str
    email: str

user: User = session.select_one(
    "SELECT * FROM users WHERE id = ?",
    123,
    schema_type=User
)
```

**Dataclasses:**
```python
from dataclasses import dataclass

@dataclass
class User:
    id: int
    name: str
    email: str

users: list[User] = session.execute(
    "SELECT * FROM users",
    schema_type=User
).all()
```

### Column Name Mapping

Handles snake_case ↔ camelCase automatically:

```python
class User(BaseModel):
    id: int
    firstName: str  # Maps to first_name column
    lastName: str   # Maps to last_name column

# SELECT first_name, last_name FROM users
# Automatically maps to firstName, lastName
users = session.execute(
    "SELECT * FROM users",
    schema_type=User
).all()
```

## Transaction Management

### Automatic Transactions (Context Manager)

```python
# Auto-commit on success, auto-rollback on exception
async with session.begin_transaction():
    await session.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
    await session.execute("INSERT INTO audit_log (action) VALUES ($1)", "user_created")
    # Commits here if no exception
```

### Manual Transaction Control

```python
# Start transaction
await session.begin()

try:
    await session.execute("INSERT INTO users (name) VALUES ($1)", "Bob")
    await session.execute("UPDATE counters SET value = value + 1 WHERE id = $1", 1)

    # Explicit commit
    await session.commit()
except Exception:
    # Explicit rollback
    await session.rollback()
    raise
```

### Savepoints (Nested Transactions)

```python
async with session.begin_transaction():
    await session.execute("INSERT INTO users (name) VALUES ($1)", "Alice")

    # Create savepoint
    await session.execute("SAVEPOINT sp1")

    try:
        await session.execute("INSERT INTO users (name) VALUES ($1)", "Bob")
    except IntegrityError:
        # Rollback to savepoint
        await session.execute("ROLLBACK TO SAVEPOINT sp1")

    # Continue transaction
    await session.execute("INSERT INTO logs (message) VALUES ($1)", "Done")
    # Commits all
```

### Transaction Isolation Levels

```python
# PostgreSQL
await session.begin(isolation_level="SERIALIZABLE")

# Set for specific query
await session.execute(
    "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
)
```

## Apache Arrow Integration

Export results to Apache Arrow format for pandas, Polars, or Arrow ecosystem:

```python
# Execute and get Arrow result
result = await session.select_to_arrow("SELECT * FROM users")

# Convert to pandas DataFrame
df = result.to_pandas()

# Convert to Polars DataFrame
pl_df = result.to_polars()

# Access raw Arrow data
arrow_table = result.data  # pyarrow.Table
arrow_batch = result.data  # pyarrow.RecordBatch (if return_format="batch")

# Arrow with parameters
result = await session.select_to_arrow(
    "SELECT * FROM users WHERE status = $1",
    "active",
    return_format="table",  # or "batch"
)
```

**Native Arrow Support:**
- ADBC adapters (zero-copy)
- DuckDB (native columnar)
- BigQuery (native)

**Automatic Fallback:**
- All other adapters convert dict → Arrow automatically

## Statement Stacks (Batched Operations)

Execute multiple operations in a single database round-trip:

```python
from sqlspec import StatementStack

# Build stack
stack = (
    StatementStack()
    .push_execute("INSERT INTO audit_log (message) VALUES ($1)", ("login",))
    .push_execute("UPDATE users SET last_login = NOW() WHERE id = $1", (user_id,))
    .push_execute("SELECT permissions FROM user_permissions WHERE user_id = $1", (user_id,))
)

# Execute (uses native pipeline if supported)
results = await session.execute_stack(stack)

# Access results
audit_result = results[0]  # INSERT result
update_result = results[1]  # UPDATE result
perms_result = results[2]  # SELECT result

# Continue on error (don't fail entire stack)
results = await session.execute_stack(stack, continue_on_error=True)

# Check for errors
for i, result in enumerate(results):
    if result.error:
        print(f"Operation {i} failed: {result.error}")
```

**Native Stack Support:**
- PostgreSQL (asyncpg with pipeline)
- Oracle (multiple execute)

**Sequential Fallback:**
- All other adapters execute sequentially

## Advanced Patterns

### IN Clause Expansion

```python
# List parameter expands to IN clause
user_ids = [1, 2, 3, 4, 5]

result = session.execute(
    "SELECT * FROM users WHERE id IN (?)",
    user_ids  # Expands to (?, ?, ?, ?, ?)
)
```

### JSON Handling

```python
# Insert JSON
user_data = {"preferences": {"theme": "dark"}, "settings": {...}}

session.execute(
    "INSERT INTO users (id, data) VALUES (?, ?)",
    123, user_data  # Auto-serialized to JSON string
)

# Query JSON
result = session.select_one(
    "SELECT * FROM users WHERE id = ?",
    123
)

# Auto-deserialized
data = result["data"]  # dict, not string
```

### RETURNING Clause (PostgreSQL)

```python
# Get inserted ID
result = await session.execute(
    "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
    "Alice", "alice@example.com"
)

user_id = result.scalar()  # Get the returned ID
```

### Cursor Iteration (Large Result Sets)

```python
# For very large result sets, iterate without loading all into memory
async with session.cursor("SELECT * FROM large_table") as cursor:
    async for row in cursor:
        process_row(row)
```

## Error Handling

### Common Exceptions

```python
from sqlspec.exceptions import (
    NotFoundError,
    MultipleResultsFoundError,
    IntegrityError,
    DatabaseConnectionError,
    QueryExecutionError,
)

# Handle not found
try:
    user = session.select_one("SELECT * FROM users WHERE id = ?", 999)
except NotFoundError:
    return {"error": "User not found"}

# Handle multiple results
try:
    user = session.select_one("SELECT * FROM users WHERE email = ?", "test@example.com")
except MultipleResultsFoundError:
    return {"error": "Multiple users found"}

# Handle integrity violations
try:
    session.execute("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Alice")
except IntegrityError as e:
    return {"error": f"Constraint violation: {e}"}

# Handle connection errors
try:
    result = await session.execute("SELECT 1")
except DatabaseConnectionError:
    return {"error": "Database unavailable"}
```

### Wrapping Exceptions

```python
from sqlspec.exceptions import wrap_exceptions

# Wrap database exceptions in custom exceptions
class UserRepositoryError(Exception):
    pass

with wrap_exceptions(exception_class=UserRepositoryError):
    user = session.select_one("SELECT * FROM users WHERE id = ?", 123)

# Suppress specific exceptions
with wrap_exceptions(suppress=NotFoundError):
    user = session.select_one("SELECT * FROM users WHERE id = ?", 999)
    # No exception raised, returns None
```

## Best Practices

### 1. Always Use Parameter Binding

```python
# ❌ BAD - SQL injection risk
user_id = request.params["id"]
session.execute(f"SELECT * FROM users WHERE id = {user_id}")

# ✅ GOOD - Safe parameter binding
session.execute("SELECT * FROM users WHERE id = ?", user_id)
```

### 2. Choose Appropriate Query Method

```python
# ❌ BAD - Using execute() when result is known
result = session.execute("SELECT * FROM users WHERE id = ?", 123)
user = result.one()  # Extra step

# ✅ GOOD - Use select_one()
user = session.select_one("SELECT * FROM users WHERE id = ?", 123)

# ✅ GOOD - Use select_value() for scalars
count = session.select_value("SELECT COUNT(*) FROM users")
```

### 3. Handle NotFoundError Appropriately

```python
# ❌ BAD - Not handling potential exception
user = session.select_one("SELECT * FROM users WHERE id = ?", request_id)

# ✅ GOOD - Use select_one_or_none()
user = session.select_one_or_none("SELECT * FROM users WHERE id = ?", request_id)
if user is None:
    raise HTTPException(404, "User not found")

# ✅ GOOD - Or handle exception
try:
    user = session.select_one("SELECT * FROM users WHERE id = ?", request_id)
except NotFoundError:
    raise HTTPException(404, "User not found")
```

### 4. Use Type-Safe Schema Mapping

```python
# ❌ Acceptable - Plain dicts
users = session.execute("SELECT * FROM users").all()
for user in users:
    name = user["name"]  # No type checking

# ✅ BETTER - Type-safe models
class User(BaseModel):
    id: int
    name: str
    email: str

users = session.execute("SELECT * FROM users", schema_type=User).all()
for user in users:
    name = user.name  # Type-checked!
```

### 5. Use Batch Operations

```python
# ❌ BAD - Loop with individual inserts
for name, email in user_data:
    session.execute("INSERT INTO users (name, email) VALUES (?, ?)", name, email)

# ✅ GOOD - Single batch operation
session.execute_many(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    user_data
)
```

### 6. Use Transactions for Related Operations

```python
# ❌ BAD - No transaction
session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
session.execute("INSERT INTO audit_log (action) VALUES (?)", "user_created")

# ✅ GOOD - Transaction ensures atomicity
async with session.begin_transaction():
    await session.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
    await session.execute("INSERT INTO audit_log (action) VALUES ($1)", "user_created")
```

## Performance Tips

1. **Use execute_many() for batch operations** - Single round-trip to database
2. **Enable statement caching** - Avoid re-parsing SQL
3. **Use Arrow format for large result sets** - Columnar format is more efficient
4. **Use scalar methods directly** - Avoid extra data conversion
5. **Disable validation for trusted queries** - Skip security checks for performance
6. **Use statement stacks for multiple operations** - Reduce round-trips

## Common Pitfalls

### Pitfall: Forgetting await

```python
# ❌ WRONG - Async adapter without await
user = session.execute("SELECT * FROM users WHERE id = $1", 123)
# Returns coroutine, not result!

# ✅ CORRECT
user = await session.execute("SELECT * FROM users WHERE id = $1", 123)
```

### Pitfall: Wrong Parameter Style

```python
# ❌ WRONG - Using ? with PostgreSQL asyncpg
result = await session.execute("SELECT * FROM users WHERE id = ?", 123)
# May cause errors or unexpected behavior

# ✅ CORRECT - Use $1 for asyncpg
result = await session.execute("SELECT * FROM users WHERE id = $1", 123)
```

### Pitfall: Not Handling Exceptions

```python
# ❌ WRONG - select_one() can raise NotFoundError
user = session.select_one("SELECT * FROM users WHERE email = ?", email)

# ✅ CORRECT - Handle or use _or_none variant
user = session.select_one_or_none("SELECT * FROM users WHERE email = ?", email)
```

### Pitfall: Inefficient Queries

```python
# ❌ BAD - Loading all users to count
users = session.execute("SELECT * FROM users").all()
count = len(users)

# ✅ GOOD - Use COUNT in SQL
count = session.select_value("SELECT COUNT(*) FROM users")
```
