# Asyncmy Adapter Skill

**Adapter:** MySQL/MariaDB (Async, High Performance)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's Asyncmy adapter for MySQL and MariaDB databases. Asyncmy is a fast async MySQL driver built on top of PyMySQL with native async/await support, making it ideal for modern async Python web applications.

This adapter provides high-performance asynchronous connectivity to MySQL 5.7+, MySQL 8.0+, and MariaDB 10.3+ with native JSON support, SSL/TLS encryption, and flexible cursor classes. It's the recommended async MySQL driver for frameworks like Litestar, FastAPI, and Starlette.

## When to Use Asyncmy

- **Async web applications** (Litestar, FastAPI, Starlette, aiohttp)
- **MySQL 5.7+ or MySQL 8.0+** deployments
- **MariaDB 10.3+** deployments
- **Modern async/await code** (Python 3.8+)
- **JSON-heavy workloads** (native JSON type support)
- **High concurrency** (connection pooling for async workloads)
- **SSL/TLS requirements** (secure database connections)

## Configuration

```python
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriverFeatures

config = AsyncmyConfig(
    pool_config={
        # Connection parameters:
        "host": "localhost",
        "port": 3306,
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",

        # Pool settings:
        "minsize": 5,
        "maxsize": 20,
        "pool_recycle": 3600,  # Recycle after 1 hour
        "echo": False,         # Log SQL statements

        # Advanced:
        "charset": "utf8mb4",
        "connect_timeout": 10,
        "autocommit": False,
        "unix_socket": None,   # Use socket instead of TCP
    },
    driver_features=AsyncmyDriverFeatures(
        json_serializer=custom_encoder,    # Optional: custom JSON encoder
        json_deserializer=custom_decoder,  # Optional: custom JSON decoder
    )
)

# Use with async context manager
async with config.provide_session() as session:
    result = await session.execute("SELECT * FROM users")
```

### SSL/TLS Configuration

```python
config = AsyncmyConfig(
    pool_config={
        "host": "mysql.example.com",
        "port": 3306,
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",
        "ssl": {
            "ca": "/path/to/ca-cert.pem",
            "cert": "/path/to/client-cert.pem",
            "key": "/path/to/client-key.pem",
            "check_hostname": True,
        }
    }
)
```

### Unix Socket Connection

```python
config = AsyncmyConfig(
    pool_config={
        "unix_socket": "/var/run/mysqld/mysqld.sock",
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",
    }
)
```

### Custom Cursor Class

```python
from asyncmy.cursors import DictCursor

config = AsyncmyConfig(
    pool_config={
        "host": "localhost",
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",
        "cursor_class": DictCursor,  # Use dict cursor by default
    }
)
```

## Parameter Style

**Positional (pyformat)**: `%s`, `%s`, etc.

```python
# Single parameter
result = await session.execute(
    "SELECT * FROM users WHERE id = %s",
    user_id
)

# Multiple parameters
result = await session.execute(
    "SELECT * FROM users WHERE status = %s AND age > %s",
    "active", 18
)

# Tuple for multiple parameters
result = await session.execute(
    "SELECT * FROM users WHERE status = %s AND age > %s",
    ("active", 18)
)
```

**Note**: MySQL uses positional `%s` style, not named parameters. SQLSpec automatically converts from other styles if you use the builder API.

## Special Features

### Native JSON Support

MySQL 5.7+ and MariaDB 10.2+ support native JSON columns. Asyncmy handles JSON automatically:

```python
# Create table with JSON column
await session.execute("""
    CREATE TABLE users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100),
        metadata JSON
    )
""")

# Insert JSON data (automatically serialized)
await session.execute(
    "INSERT INTO users (name, metadata) VALUES (%s, %s)",
    "Alice",
    {"role": "admin", "permissions": ["read", "write", "delete"]}
)

# Query JSON data (automatically deserialized)
result = await session.execute(
    "SELECT metadata FROM users WHERE id = %s",
    1
).one()

metadata = result["metadata"]  # dict
assert isinstance(metadata, dict)
assert metadata["role"] == "admin"
```

### Custom JSON Serializers

For performance-critical applications, use custom JSON serializers:

```python
import orjson

def orjson_serializer(obj):
    """Fast JSON serialization with orjson."""
    return orjson.dumps(obj).decode("utf-8")

def orjson_deserializer(s):
    """Fast JSON deserialization with orjson."""
    return orjson.loads(s)

config = AsyncmyConfig(
    pool_config={...},
    driver_features={
        "json_serializer": orjson_serializer,
        "json_deserializer": orjson_deserializer,
    }
)
```

**Performance**: orjson is 2-3x faster than stdlib json for large objects.

### MariaDB Compatibility

Full compatibility with MariaDB 10.3+:

```python
# MariaDB-specific features work seamlessly
await session.execute("""
    CREATE TABLE events (
        id INT PRIMARY KEY AUTO_INCREMENT,
        event_name VARCHAR(100),
        event_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
    )
""")

# Use MariaDB's microsecond precision
result = await session.execute(
    "SELECT event_time FROM events WHERE id = %s",
    1
).one()

timestamp = result["event_time"]  # datetime with microseconds
```

## Performance Features

### Connection Pooling

Asyncmy provides async connection pooling for high concurrency:

```python
config = AsyncmyConfig(
    pool_config={
        "host": "localhost",
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",

        "minsize": 5,         # Keep 5 connections ready
        "maxsize": 20,        # Allow up to 20 total
        "pool_recycle": 3600, # Recycle after 1 hour
    }
)
```

**Best practices**:
- Set `minsize` to handle typical load (5-10)
- Set `maxsize` for peak load (20-50)
- Use `pool_recycle` to prevent stale connections (3600 seconds)

### Native Arrow Import/Export

Direct Arrow integration for high-performance data transfer:

```python
import pyarrow as pa

# Export to Arrow
result = await session.execute("SELECT * FROM large_table").to_arrow()
arrow_table: pa.Table = result  # Zero-copy when possible

# Import from Arrow
await session.load_arrow(arrow_table, "target_table")
```

**Performance**: 10-100x faster than row-by-row iteration for large datasets.

### Native Parquet Import/Export

Built-in Parquet support without intermediate formats:

```python
# Export to Parquet
await session.execute("SELECT * FROM users").to_parquet("/tmp/users.parquet")

# Import from Parquet
await session.load_parquet("/tmp/users.parquet", "users_import")
```

### Batch Operations

```python
# Efficient bulk insert
users = [
    ("Alice", "alice@example.com"),
    ("Bob", "bob@example.com"),
    ("Carol", "carol@example.com"),
]

await session.execute_many(
    "INSERT INTO users (name, email) VALUES (%s, %s)",
    users
)
```

## MySQL-Specific Features

### AUTO_INCREMENT with RETURNING (MySQL 8.0+)

```python
# Insert and get auto-generated ID
result = await session.execute(
    "INSERT INTO users (name, email) VALUES (%s, %s)",
    "Alice", "alice@example.com"
)

# Get last insert ID
last_id = result.last_insert_id()
```

### ON DUPLICATE KEY UPDATE

```python
# Upsert pattern (MySQL-specific)
await session.execute("""
    INSERT INTO user_stats (user_id, login_count)
    VALUES (%s, 1)
    ON DUPLICATE KEY UPDATE login_count = login_count + 1
""", user_id)
```

### JSON Path Expressions (MySQL 5.7+)

```python
# Query JSON fields with path expressions
result = await session.execute("""
    SELECT name, metadata->>'$.role' as role
    FROM users
    WHERE metadata->>'$.role' = 'admin'
""").all()

for row in result:
    print(f"{row['name']}: {row['role']}")
```

### Generated Columns

```python
# Create table with generated column
await session.execute("""
    CREATE TABLE products (
        id INT PRIMARY KEY AUTO_INCREMENT,
        price DECIMAL(10, 2),
        tax_rate DECIMAL(4, 2),
        price_with_tax DECIMAL(10, 2) GENERATED ALWAYS AS (price * (1 + tax_rate)) STORED
    )
""")
```

### Window Functions (MySQL 8.0+)

```python
# Use window functions
result = await session.execute("""
    SELECT
        name,
        department,
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
""").all()
```

## Best Practices

1. **Use connection pooling** - Essential for async applications (minsize=5, maxsize=20)
2. **Set pool_recycle** - Prevent stale connections (3600 seconds recommended)
3. **Use utf8mb4 charset** - Full Unicode support including emojis
4. **Enable SSL/TLS** - For production deployments
5. **Use prepared statements** - Automatic with parameterized queries (`%s` style)
6. **Leverage native JSON** - Faster than TEXT columns with manual parsing
7. **Use batch operations** - execute_many() for bulk inserts
8. **Monitor connection usage** - Adjust pool size based on load
9. **Use context managers** - Automatic connection cleanup
10. **Consider read replicas** - Configure separate configs for read/write splitting

## Common Issues

### "Too many connections"

**Problem**: MySQL connection limit reached.

**Solution**:
```python
# Reduce pool size
config = AsyncmyConfig(
    pool_config={
        "maxsize": 10,  # Reduce from 20
    }
)

# OR increase MySQL max_connections
# mysql> SET GLOBAL max_connections = 500;
```

### "Lost connection to MySQL server during query"

**Problem**: Long-running query or idle connection timeout.

**Solution**:
```python
# Increase timeouts
config = AsyncmyConfig(
    pool_config={
        "connect_timeout": 30,     # Longer connect timeout
        "pool_recycle": 1800,      # Recycle more frequently
    }
)

# OR increase MySQL wait_timeout
# mysql> SET GLOBAL wait_timeout = 28800;
```

### JSON serialization errors

**Problem**: Cannot serialize complex Python objects to JSON.

**Solution**:
```python
import orjson
from datetime import datetime

def custom_serializer(obj):
    """Handle datetime and other types."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return orjson.dumps(obj).decode("utf-8")

config = AsyncmyConfig(
    driver_features={"json_serializer": custom_serializer}
)
```

### "Incorrect string value" with emojis

**Problem**: Using utf8 charset instead of utf8mb4.

**Solution**:
```python
# Use utf8mb4 for full Unicode support
config = AsyncmyConfig(
    pool_config={
        "charset": "utf8mb4",
    }
)

# Ensure database/table uses utf8mb4
await session.execute("""
    ALTER DATABASE mydb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""")
```

### SSL connection fails

**Problem**: Certificate verification fails or SSL not configured.

**Solution**:
```python
# Verify SSL configuration
config = AsyncmyConfig(
    pool_config={
        "ssl": {
            "ca": "/path/to/ca-cert.pem",  # Use absolute path
            "check_hostname": True,
        }
    }
)

# OR disable SSL for local development (NOT production!)
config = AsyncmyConfig(
    pool_config={
        "ssl": None,
    }
)
```

## Important Notes

### ⚠️ No Transactional DDL

MySQL does **NOT** support transactional DDL for most storage engines (InnoDB included). This means:
- DDL statements (CREATE, ALTER, DROP) are NOT automatically rolled back on error
- Each DDL statement commits immediately (implicit commit)
- Plan DDL operations carefully and consider backups before schema changes
- Use explicit transaction boundaries only for DML (INSERT, UPDATE, DELETE)

**Example of non-transactional behavior**:
```python
async with config.provide_session() as session:
    try:
        await session.begin()

        # This commits immediately - NOT rolled back!
        await session.execute("CREATE TABLE temp_table (id INT)")

        # Subsequent error won't undo the CREATE TABLE
        await session.execute("INSERT INTO nonexistent VALUES (1)")

        await session.commit()  # Never reached
    except Exception:
        await session.rollback()  # Rollback won't affect CREATE TABLE
        # temp_table still exists in database!
```

### Storage Engine Considerations

- **InnoDB**: ACID-compliant, supports transactions for DML (default and recommended)
- **MyISAM**: No transactions, table-level locking, legacy (avoid for new tables)
- **MEMORY**: Fast but volatile, no persistence

Always use InnoDB for transactional tables:
```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100)
) ENGINE=InnoDB;
```

### Connection Security

- Always use SSL/TLS in production
- Store credentials in environment variables, not code
- Use read-only users for read-only operations
- Implement connection retry logic for transient failures

### Performance Tuning

- Create indexes on frequently queried columns
- Use `EXPLAIN` to analyze query plans
- Consider partitioning for very large tables (100M+ rows)
- Use `LIMIT` to prevent accidentally fetching millions of rows
- Monitor slow query log for optimization opportunities

### MySQL vs MariaDB

While Asyncmy works with both, some features differ:

| Feature | MySQL 8.0+ | MariaDB 10.3+ |
|---------|------------|---------------|
| Window Functions | ✅ | ✅ |
| CTEs (WITH clause) | ✅ | ✅ |
| JSON functions | ✅ | ✅ (slightly different syntax) |
| RETURNING clause | ❌ | ✅ |
| Sequences | ❌ | ✅ |

Test thoroughly if switching between MySQL and MariaDB.

## Performance Benchmarks

Compared to other MySQL drivers:

- **asyncmy**: Baseline (fast async driver)
- **aiomysql**: ~10-15% slower (older codebase)
- **mysql-connector-python (async)**: ~20-30% slower (official but slower)
- **PyMySQL (sync)**: Not comparable (synchronous)

**JSON operations**:
- Native JSON vs TEXT with manual parsing: 5-10x faster
- orjson serializer: 2-3x faster than stdlib json

**Connection pooling**:
- Pool overhead: <1ms per acquisition
- Pool recycle overhead: ~50ms per recycled connection

For most applications, asyncmy provides excellent performance with mature async support. Use connection pooling and batch operations for optimal throughput.
