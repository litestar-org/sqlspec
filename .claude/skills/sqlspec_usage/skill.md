# SQLSpec Usage Expert Skill

**Version:** 1.0.0
**Category:** Database, Python, SQLSpec
**Status:** Active

## Description

This skill provides comprehensive guidance on using SQLSpec - a type-safe SQL query mapper for Python. It covers configuration, query execution, framework integration, migrations, testing, and performance optimization across all supported database adapters.

## Activation Triggers

This skill activates when users ask about:
- SQLSpec configuration or setup
- Database connection management
- Query execution patterns
- Framework integration (Litestar, FastAPI, Starlette, Flask)
- Migration management
- Testing with SQLSpec
- Performance optimization
- Troubleshooting SQLSpec issues
- Multi-database setups
- Type-safe query mapping

## Core Capabilities

### 1. Configuration Guidance

Provide accurate configuration advice for all supported adapters:
- AsyncPG (PostgreSQL async)
- Psycopg (PostgreSQL sync/async)
- Psqlpy (PostgreSQL Rust-based)
- SQLite (sync)
- AioSQLite (async)
- DuckDB (sync/async)
- Oracle (sync/async)
- MySQL/Asyncmy (sync/async)
- BigQuery
- ADBC (Apache Arrow Database Connectivity)

**Configuration Structure:**
```python
from sqlspec import SQLSpec
from sqlspec.adapters.{adapter} import {Adapter}Config

spec = SQLSpec()
db = spec.add_config(
    {Adapter}Config(
        connection_config={...},           # Connection parameters
        statement_config={...},      # SQL processing (optional)
        extension_config={...},      # Framework integration (optional)
        driver_features={...},       # Adapter-specific features (optional)
        migration_config={...},      # Migration settings (optional)
    )
)
```

**Key Principles:**
1. Always store the config key returned from `add_config()`
2. Use `connection_config` dict for connection parameters (adapter-specific)
3. Define `driver_features` using TypedDict for type safety
4. Auto-detect optional features when dependencies are available
5. Use unique `session_key` values for multi-database setups

### 2. Query Execution Patterns

**Basic Execution:**
```python
# Any SQL - returns SQLResult
result = session.execute("SELECT * FROM users WHERE id = ?", user_id)

# Single row (raises NotFoundError if not found)
user = session.select_one("SELECT * FROM users WHERE id = ?", user_id)

# Single row or None
user = session.select_one_or_none("SELECT * FROM users WHERE id = ?", user_id)

# Scalar value
count = session.select_value("SELECT COUNT(*) FROM users")

# Batch operations
session.execute_many(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    [("Alice", "alice@example.com"), ("Bob", "bob@example.com")]
)
```

**Parameter Binding Styles:**
- SQLite/DuckDB: `?` (qmark)
- PostgreSQL (asyncpg): `$1, $2` (numeric)
- MySQL: `%s` (format)
- Oracle: `:name` (named colon)
- BigQuery: `@name` (named at)

SQLSpec automatically converts parameter styles based on adapter configuration.

**Type-Safe Results:**
```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str

# Map to typed models
users: list[User] = session.execute(
    "SELECT * FROM users",
    schema_type=User
).all()

# Or use convenience method
user: User = session.select_one(
    "SELECT * FROM users WHERE id = ?",
    user_id,
    schema_type=User
)
```

Supported: Pydantic, msgspec, attrs, dataclasses

### 3. Framework Integration

**Litestar (Gold Standard):**
```python
from litestar import Litestar, get
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.litestar import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db"},
        extension_config={
            "litestar": {
                "commit_mode": "autocommit",  # or "manual", "autocommit_include_redirect"
                "session_key": "db_session",
            }
        }
    )
)

app = Litestar(
    route_handlers=[...],
    plugins=[SQLSpecPlugin(sqlspec=spec)]
)

@get("/users/{user_id:int}")
async def get_user(user_id: int, db_session: AsyncpgDriver) -> dict:
    return await db_session.execute("SELECT * FROM users WHERE id = $1", user_id).one()
```

**FastAPI/Starlette:**
```python
from fastapi import FastAPI, Depends
from sqlspec.extensions.fastapi import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(...))
plugin = SQLSpecPlugin(spec)

app = FastAPI()
plugin.init_app(app)

@app.get("/users/{user_id}")
async def get_user(
    user_id: int,
    db: AsyncpgDriver = Depends(plugin.session_dependency())
):
    return await db.execute("SELECT * FROM users WHERE id = $1", user_id).one()
```

**Flask (Hook-Based with Portal for Async):**
```python
from flask import Flask
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin

app = Flask(__name__)
spec = SQLSpec()
db = spec.add_config(SqliteConfig(connection_config={"database": "app.db"}))
plugin = SQLSpecPlugin(spec)
plugin.init_app(app)

@app.route('/users/<int:user_id>')
def get_user(user_id):
    db = plugin.get_session(request)
    return db.execute("SELECT * FROM users WHERE id = ?", user_id).one()
```

**Commit Modes:**
- `manual`: Explicit transaction control
- `autocommit`: Auto-commit on 2xx responses, rollback otherwise
- `autocommit_include_redirect`: Auto-commit on 2xx and 3xx responses

### 4. Session Management

**Always Use Context Managers:**
```python
# Async
async with spec.provide_session(db) as session:
    result = await session.execute("SELECT * FROM users")

# Sync
with spec.provide_session(db) as session:
    result = session.execute("SELECT * FROM users")
```

**Transaction Control:**
```python
# Auto-commit/rollback
async with session.begin_transaction():
    await session.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
    await session.execute("INSERT INTO logs (action) VALUES ($1)", "user_created")
    # Auto-commits on success, auto-rollbacks on exception

# Manual control
await session.begin()
try:
    await session.execute("INSERT INTO users (name) VALUES ($1)", "Bob")
    await session.commit()
except Exception:
    await session.rollback()
    raise
```

### 5. Migration Management

**CLI Commands:**
```bash
# Initialize
sqlspec --config myapp.config init

# Create migration
sqlspec --config myapp.config create-migration -m "Add users table"

# Apply migrations
sqlspec --config myapp.config upgrade

# Rollback
sqlspec --config myapp.config downgrade -1

# Show current
sqlspec --config myapp.config show-current-revision
```

**Hybrid Versioning Workflow:**
```bash
# Development: timestamp migrations (no conflicts)
$ sqlspec create-migration -m "add users"
Created: 20251115120000_add_users.sql

# Before merging: convert to sequential
$ sqlspec fix --yes
✓ Converted to 0003_add_users.sql
```

**Programmatic Control:**
```python
# Async
await config.migrate_up("head")
await config.migrate_down("-1")

# Sync
config.migrate_up("0003")
config.migrate_down("base")
```

### 6. Testing Best Practices

**Test Isolation with Pooled Connections:**
```python
import tempfile

def test_with_pooling():
    """Use temp files, NOT :memory: with pooling!"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}  # Isolated per test
        )
        # Test logic here
```

**Why:** `:memory:` with pooling creates shared state across parallel tests.

**pytest-databases Integration:**
```python
@pytest.fixture(scope="session")
def asyncpg_config(postgres_service: PostgresService):
    return AsyncpgConfig(
        connection_config={"dsn": postgres_service.connection_url()}
    )

@pytest.fixture(scope="session")
def asyncpg_driver(asyncpg_config):
    spec = SQLSpec()
    config = spec.add_config(asyncpg_config)

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE IF NOT EXISTS users (...)")

    with spec.provide_session(config) as driver:
        yield driver
```

**Parallel Execution:**
```bash
uv run pytest -n auto --dist=loadgroup
```

### 7. Performance Optimization

**Connection Pooling:**
```python
config = AsyncpgConfig(
    connection_config={
        "dsn": "postgresql://localhost/db",
        "min_size": 10,
        "max_size": 20,
        "max_inactive_connection_lifetime": 300,
    }
)
```

**Statement Caching:**
```python
from sqlspec.core import update_cache_config, CacheConfig

update_cache_config(CacheConfig(enabled=True, maxsize=1000))
```

**Batch Operations:**
```python
# GOOD
session.execute_many(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    [("Alice", "a@example.com"), ("Bob", "b@example.com")]
)

# BAD
for name, email in users:
    session.execute("INSERT INTO users (name, email) VALUES (?, ?)", name, email)
```

**Apache Arrow:**
```python
# Export to Arrow for pandas/Polars
result = await session.select_to_arrow("SELECT * FROM users")
df = result.to_pandas()
pl_df = result.to_polars()
```

## Anti-Pattern Detection

Watch for and warn about these common mistakes:

### Configuration Anti-Patterns

❌ **Not storing config key:**
```python
spec.add_config(AsyncpgConfig(...))  # Lost reference!
```

✅ **Store the key:**
```python
db = spec.add_config(AsyncpgConfig(...))
```

❌ **Missing connection_config:**
```python
config = AsyncpgConfig(dsn="postgresql://...")  # Wrong!
```

✅ **Use connection_config dict:**
```python
config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
```

### Session Management Anti-Patterns

❌ **No context manager:**
```python
session = spec.provide_session(db).__enter__()
result = session.execute("SELECT 1")
# Resource leak!
```

✅ **Use context manager:**
```python
with spec.provide_session(db) as session:
    result = session.execute("SELECT 1")
```

❌ **Mixing sync/async:**
```python
config = AsyncpgConfig(...)
with config.provide_session() as session:  # Wrong! Need async with
    pass
```

✅ **Match async config with async context:**
```python
async with config.provide_session() as session:
    pass
```

### Query Execution Anti-Patterns

❌ **String concatenation (SQL injection!):**
```python
session.execute(f"SELECT * FROM users WHERE id = {user_id}")
```

✅ **Parameter binding:**
```python
session.execute("SELECT * FROM users WHERE id = ?", user_id)
```

❌ **Not handling NotFoundError:**
```python
user = session.select_one("SELECT * FROM users WHERE id = ?", 999)
# Raises NotFoundError!
```

✅ **Use select_one_or_none or handle exception:**
```python
user = session.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)
if user is None:
    return {"error": "Not found"}
```

### Framework Integration Anti-Patterns

❌ **Duplicate session_key values:**
```python
spec.add_config(AsyncpgConfig(
    extension_config={"litestar": {"session_key": "db"}}
))
spec.add_config(DuckDBConfig(
    extension_config={"litestar": {"session_key": "db"}}  # Conflict!
))
```

✅ **Unique keys:**
```python
spec.add_config(AsyncpgConfig(
    extension_config={"litestar": {"session_key": "postgres_db"}}
))
spec.add_config(DuckDBConfig(
    extension_config={"litestar": {"session_key": "duckdb"}}
))
```

### Testing Anti-Patterns

❌ **Using :memory: with pooling:**
```python
config = AiosqliteConfig(connection_config={"database": ":memory:"})
# Shared state in parallel tests!
```

✅ **Use temp files:**
```python
with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
    config = AiosqliteConfig(connection_config={"database": tmp.name})
```

## Troubleshooting Guide

### "No such database adapter"
**Cause:** Adapter not installed
**Solution:** Install with extras: `pip install sqlspec[oracledb]`

### "Table already exists" in parallel tests
**Cause:** Using `:memory:` with pooling
**Solution:** Use `tempfile.NamedTemporaryFile` instead

### "Connection pool is closed"
**Cause:** Accessing pool after shutdown
**Solution:** Don't use sessions after `close_all_pools()` or recreate pool

### "Parameter count mismatch"
**Cause:** Wrong parameter style for adapter
**Solution:** Use correct style (e.g., `$1` for PostgreSQL, `?` for SQLite)

### "NotFoundError" on legitimate query
**Cause:** Using `select_one()` when row might not exist
**Solution:** Use `select_one_or_none()` instead

## Quick Reference

### Adapter Selection Guide

| Database | Sync Adapter | Async Adapter | Best For |
|----------|-------------|---------------|----------|
| PostgreSQL | `psycopg` | `asyncpg`, `psqlpy` | General purpose, high performance |
| SQLite | `sqlite` | `aiosqlite` | Embedded, testing, local storage |
| MySQL/MariaDB | `mysql` | `asyncmy` | Legacy systems, wide compatibility |
| Oracle | `oracledb` | `oracledb` (async mode) | Enterprise, Oracle-specific features |
| DuckDB | `duckdb` | `duckdb` (async mode) | Analytics, OLAP, embedded analytics |
| BigQuery | `bigquery` | N/A | Google Cloud, data warehouse |
| Multi-DB | `adbc` | `adbc` | Standardized interface, Arrow-native |

### Parameter Styles by Adapter

| Adapter | Style | Example |
|---------|-------|---------|
| SQLite, DuckDB | `?` | `SELECT * FROM users WHERE id = ?` |
| AsyncPG | `$1, $2` | `SELECT * FROM users WHERE id = $1` |
| Psycopg | `%s` or `%(name)s` | `SELECT * FROM users WHERE id = %s` |
| MySQL | `%s` | `SELECT * FROM users WHERE id = %s` |
| Oracle | `:name` | `SELECT * FROM users WHERE id = :id` |
| BigQuery | `@name` | `SELECT * FROM users WHERE created >= @start_date` |

### Result Methods

| Method | Returns | Error Behavior |
|--------|---------|----------------|
| `.all()` | `list[dict]` | Empty list if no rows |
| `.one()` | `dict` | Raises if not exactly 1 row |
| `.one_or_none()` | `dict \| None` | None if no rows, raises if >1 row |
| `.scalar()` | `Any` | First column of first row |
| `.scalar_or_none()` | `Any \| None` | None if no rows |

## References

For detailed patterns, see:
- [Configuration Patterns](patterns/configuration.md)
- [Query Execution Patterns](patterns/queries.md)
- [Framework Integration](patterns/frameworks.md)
- [Migration Patterns](patterns/migrations.md)
- [Testing Best Practices](patterns/testing.md)
- [Performance Optimization](patterns/performance.md)
- [Troubleshooting Guide](patterns/troubleshooting.md)

## Usage Examples

See `examples/` directory for working code samples:
- `litestar-integration.py`
- `fastapi-integration.py`
- `multi-database.py`
- `migration-workflow.sh`
- `testing-patterns.py`
