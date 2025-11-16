# Troubleshooting Guide

Common SQLSpec issues and solutions.

## Installation Issues

### "No module named 'sqlspec.adapters.{adapter}'"

**Cause:** Adapter not installed

**Solution:**
```bash
# Install specific adapter
pip install sqlspec[asyncpg]
pip install sqlspec[psycopg]
pip install sqlspec[oracle]
pip install sqlspec[duckdb]

# Install all adapters
pip install sqlspec[all]

# Install specific groups
pip install sqlspec[postgres]  # All PostgreSQL adapters
pip install sqlspec[async]     # All async adapters
```

### "ImportError: cannot import name 'AsyncpgConfig'"

**Cause:** Wrong import path

**Solution:**
```python
# ❌ WRONG
from sqlspec.adapters import AsyncpgConfig

# ✅ CORRECT
from sqlspec.adapters.asyncpg import AsyncpgConfig
```

## Configuration Issues

### "TypeError: __init__() got an unexpected keyword argument 'dsn'"

**Cause:** Passing connection parameters directly instead of in `pool_config`

**Solution:**
```python
# ❌ WRONG
config = AsyncpgConfig(dsn="postgresql://localhost/db")

# ✅ CORRECT
config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"})
```

### "ImproperConfigurationError: Duplicate state keys found"

**Cause:** Multiple configs using same `session_key` in framework extensions

**Solution:**
```python
# ❌ WRONG
spec.add_config(AsyncpgConfig(
    extension_config={"litestar": {"session_key": "db"}}
))
spec.add_config(DuckDBConfig(
    extension_config={"litestar": {"session_key": "db"}}  # Duplicate!
))

# ✅ CORRECT
spec.add_config(AsyncpgConfig(
    extension_config={"litestar": {"session_key": "postgres_db"}}
))
spec.add_config(DuckDBConfig(
    extension_config={"litestar": {"session_key": "duckdb"}}
))
```

### "ValueError: Config not found in registry"

**Cause:** Not storing config key from `add_config()`

**Solution:**
```python
# ❌ WRONG
spec.add_config(AsyncpgConfig(...))  # Lost reference
with spec.provide_session(AsyncpgConfig(...)) as session:  # Different instance
    pass

# ✅ CORRECT
db = spec.add_config(AsyncpgConfig(...))
with spec.provide_session(db) as session:
    pass
```

## Connection Issues

### "DatabaseConnectionError: Connection pool is closed"

**Cause:** Accessing pool after shutdown

**Solution:**
```python
# Don't use sessions after closing pools
await spec.close_all_pools()
# with spec.provide_session(db) as session:  # Error!

# Or recreate pool
await config.create_pool()
```

### "TimeoutError: Could not acquire connection"

**Cause:** Pool exhausted, all connections in use

**Solutions:**
```python
# Increase pool size
config = AsyncpgConfig(
    pool_config={
        "dsn": "...",
        "max_size": 40,  # Increase from default 20
        "timeout": 120.0,  # Increase timeout
    }
)

# Or ensure connections are released
async with spec.provide_session(db) as session:  # Use context manager!
    result = await session.execute("SELECT 1")
# Connection released here
```

### "Connection refused: localhost:5432"

**Cause:** Database server not running or wrong host/port

**Solutions:**
```bash
# Check if database is running
pg_isready -h localhost -p 5432

# Start database (example with Docker)
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres

# For SQLSpec tests, use pytest-databases
uv run pytest  # Automatically starts containers
```

## Query Execution Issues

### "NotFoundError: No rows returned"

**Cause:** Using `select_one()` when row might not exist

**Solution:**
```python
# ❌ WRONG - Raises if not found
user = session.select_one("SELECT * FROM users WHERE id = ?", 999)

# ✅ CORRECT - Returns None if not found
user = session.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)
if user is None:
    return {"error": "Not found"}

# ✅ ALSO CORRECT - Handle exception
try:
    user = session.select_one("SELECT * FROM users WHERE id = ?", 999)
except NotFoundError:
    return {"error": "Not found"}
```

### "MultipleResultsFoundError: Expected 1 row, got N"

**Cause:** Query returned multiple rows when using `select_one()`

**Solution:**
```python
# ❌ WRONG - Multiple users might have same email
user = session.select_one("SELECT * FROM users WHERE email = ?", email)

# ✅ CORRECT - Use unique column or handle multiple
user = session.select_one("SELECT * FROM users WHERE id = ?", user_id)

# ✅ OR - Get all results
users = session.execute("SELECT * FROM users WHERE email = ?", email).all()
```

### "ParameterError: Parameter count mismatch"

**Cause:** Wrong number of parameters

**Solution:**
```python
# ❌ WRONG - 2 placeholders, 1 parameter
session.execute("SELECT * FROM users WHERE id = ? AND status = ?", 123)

# ✅ CORRECT - Match placeholders with parameters
session.execute("SELECT * FROM users WHERE id = ? AND status = ?", 123, "active")
```

### "SyntaxError: Incorrect parameter style"

**Cause:** Using wrong parameter style for adapter

**Solution:**
```python
# PostgreSQL (asyncpg) - use $1, $2
await session.execute("SELECT * FROM users WHERE id = $1", user_id)

# SQLite/DuckDB - use ?
session.execute("SELECT * FROM users WHERE id = ?", user_id)

# Oracle - use :name
session.execute("SELECT * FROM users WHERE id = :user_id", user_id=user_id)

# MySQL - use %s
session.execute("SELECT * FROM users WHERE id = %s", user_id)
```

## Transaction Issues

### "TransactionError: Transaction already started"

**Cause:** Nested `begin()` calls without savepoints

**Solution:**
```python
# ❌ WRONG
await session.begin()
await session.begin()  # Error!

# ✅ CORRECT - Use savepoints for nesting
await session.begin()
await session.execute("SAVEPOINT sp1")
# ... operations ...
await session.execute("RELEASE SAVEPOINT sp1")
await session.commit()

# ✅ OR - Use context manager
async with session.begin_transaction():
    # Automatically managed
    pass
```

### "Changes not persisted after request"

**Cause:** Not committing in `manual` commit mode

**Solution:**
```python
# Change to autocommit mode
config = AsyncpgConfig(
    extension_config={"litestar": {"commit_mode": "autocommit"}}
)

# Or manually commit
await session.begin()
await session.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
await session.commit()  # Must commit!
```

## Testing Issues

### "Table already exists" in parallel tests

**Cause:** Using `:memory:` with connection pooling

**Solution:**
```python
# ❌ WRONG - Shared state across tests
config = AiosqliteConfig(pool_config={"database": ":memory:"})

# ✅ CORRECT - Isolated temp files
import tempfile

with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
    config = AiosqliteConfig(pool_config={"database": tmp.name})
```

### "Fixture not found: postgres_service"

**Cause:** pytest-databases not installed

**Solution:**
```bash
pip install pytest-databases[postgres]
```

### "Tests hang indefinitely"

**Cause:** Not using `await` with async adapters

**Solution:**
```python
# ❌ WRONG - Missing await
result = session.execute("SELECT 1")  # Returns coroutine

# ✅ CORRECT
result = await session.execute("SELECT 1")
```

## Migration Issues

### "MigrationError: Revision not found"

**Cause:** Migration file missing or wrong revision ID

**Solution:**
```bash
# Check migration history
sqlspec --config myapp.config history

# Verify migration files exist
ls migrations/

# Recreate missing migration
sqlspec --config myapp.config create-migration -m "Missing migration"
```

### "MigrationError: Duplicate revision ID"

**Cause:** Two migrations with same revision number

**Solution:**
```bash
# Use sqlspec fix to renumber
sqlspec fix --yes

# Or manually rename migration files
mv migrations/0003_duplicate.sql migrations/0004_renamed.sql
```

### "Can't connect to database for migrations"

**Cause:** Wrong config module path

**Solution:**
```bash
# ❌ WRONG - File path
sqlspec --config myapp/config.py upgrade

# ✅ CORRECT - Python module path
sqlspec --config myapp.config upgrade

# Or use environment variable
export SQLSPEC_CONFIG=myapp.config
sqlspec upgrade
```

## Framework Integration Issues

### "AttributeError: 'Request' object has no attribute 'state'"

**Cause:** Using wrong request object or wrong framework version

**Solution:**
```python
# Ensure using latest framework version
pip install --upgrade litestar starlette fastapi

# Verify plugin initialized
plugin = SQLSpecPlugin(spec)
plugin.init_app(app)  # Don't forget this!
```

### "Session not found in dependency injection"

**Cause:** Wrong session_key or plugin not registered

**Solution:**
```python
# Verify plugin registered
app = Litestar(plugins=[SQLSpecPlugin(sqlspec=spec)])

# Check session_key matches
config = AsyncpgConfig(
    extension_config={"litestar": {"session_key": "db_session"}}
)

# Use correct type annotation
@get("/users")
async def get_users(db_session: AsyncpgDriver):  # Must match session_key
    pass
```

## Performance Issues

### "Queries are slow"

**Diagnostic steps:**

```python
# 1. Check if pooling enabled
config = AsyncpgConfig(
    pool_config={
        "min_size": 10,  # Should be > 0 for pooling
        "max_size": 20,
    }
)

# 2. Enable statement caching
from sqlspec.core import update_cache_config, CacheConfig
update_cache_config(CacheConfig(enabled=True, maxsize=1000))

# 3. Use EXPLAIN to analyze queries
explain = session.execute("EXPLAIN ANALYZE SELECT * FROM users").all()

# 4. Check for missing indexes
indexes = session.execute("""
    SELECT tablename, indexname
    FROM pg_indexes
    WHERE schemaname = 'public'
""").all()

# 5. Use batch operations
session.execute_many(
    "INSERT INTO users (name) VALUES (?)",
    [(name,) for name in names]
)
```

### "High memory usage"

**Cause:** Loading large result sets into memory

**Solutions:**
```python
# Use LIMIT for pagination
users = session.execute(
    "SELECT * FROM users LIMIT ? OFFSET ?",
    page_size, offset
).all()

# Use Arrow for large datasets
result = await session.select_to_arrow("SELECT * FROM large_table")
df = result.to_pandas()

# Stream results (if adapter supports)
async with session.cursor("SELECT * FROM large_table") as cursor:
    async for row in cursor:
        process_row(row)
```

## Type Conversion Issues

### "TypeError: Object of type UUID is not JSON serializable"

**Cause:** UUID objects in JSON fields

**Solution:**
```python
# Enable UUID conversion for DuckDB
config = DuckDBConfig(
    driver_features={"enable_uuid_conversion": True}
)

# Or use custom JSON serializer
import json
import uuid

class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)

config = AsyncpgConfig(
    driver_features={
        "json_serializer": lambda x: json.dumps(x, cls=UUIDEncoder)
    }
)
```

### "datetime not JSON serializable"

**Solution:**
```python
import json
from datetime import datetime

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

config = AsyncpgConfig(
    driver_features={
        "json_serializer": lambda x: json.dumps(x, cls=DateTimeEncoder)
    }
)
```

## Debugging Tips

### Enable SQL Logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("sqlspec").setLevel(logging.DEBUG)

# Now see all SQL queries
result = await session.execute("SELECT 1")
# DEBUG:sqlspec:Executing: SELECT 1
```

### Use Python Debugger

```python
# Add breakpoint before query
import pdb; pdb.set_trace()

result = await session.execute("SELECT * FROM users WHERE id = ?", user_id)
```

### Check Pool Status

```python
# Get pool statistics
pool = config.pool_instance

print(f"Pool size: {pool.get_size()}")
print(f"Free connections: {pool.get_idle_size()}")
print(f"Max size: {pool.get_max_size()}")
```

## Getting Help

1. **Check documentation**: [SQLSpec Docs](https://sqlspec.readthedocs.io)
2. **Search issues**: [GitHub Issues](https://github.com/litestar-org/sqlspec/issues)
3. **Ask in Discord**: [Litestar Discord](https://discord.gg/litestar)
4. **Check AGENTS.md**: Project-specific patterns and conventions
5. **Review examples**: Look at example projects (oracle-vertexai-demo, postgres-vertexai-demo, sqlstack)
